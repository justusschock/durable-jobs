// Package memory provides an in-memory Store implementation for tests and local use.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
)

// Store keeps all job and attempt state behind one mutex.
type Store struct {
	mu          sync.Mutex
	jobs        map[jobs.JobID]jobs.Job
	attempts    map[jobs.AttemptID]jobs.Attempt
	nextAttempt uint64
}

// New creates an empty Store.
func New() *Store {
	return &Store{
		jobs:     make(map[jobs.JobID]jobs.Job),
		attempts: make(map[jobs.AttemptID]jobs.Attempt),
	}
}

// Create atomically persists a new job unless its ID is already in use.
func (store *Store) Create(ctx context.Context, job jobs.Job) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	if _, exists := store.jobs[job.ID]; exists {
		return jobs.Job{}, jobs.ErrJobExists
	}
	job = cloneJob(job)
	normalizeJobTimes(&job)
	store.jobs[job.ID] = job
	return cloneJob(job), nil
}

// Get returns an independently owned job snapshot.
func (store *Store) Get(ctx context.Context, id jobs.JobID) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	job, exists := store.jobs[id]
	if !exists {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	return cloneJob(job), nil
}

// ClaimReady atomically claims the highest-priority ready job in a queue.
func (store *Store) ClaimReady(ctx context.Context, request jobs.ClaimRequest) (jobs.Attempt, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Attempt{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Attempt{}, err
	}

	now := persistedTime(request.Now)
	var selected *jobs.Job
	for _, candidate := range store.jobs {
		if candidate.Queue != request.Queue || !claimable(candidate, now, store.jobs) {
			continue
		}
		if selected == nil || precedes(candidate, *selected) {
			copy := candidate
			selected = &copy
		}
	}
	if selected == nil {
		return jobs.Attempt{}, jobs.ErrNoReadyJob
	}

	job := *selected
	job.State = jobs.StateRunning
	job.Attempt++
	job.UpdatedAt = now
	store.nextAttempt++
	attempt := jobs.Attempt{
		ID:                jobs.AttemptID(fmt.Sprintf("%s-%d", job.ID, store.nextAttempt)),
		Job:               cloneJob(job),
		WorkerID:          request.WorkerID,
		FencingToken:      request.FencingToken,
		StartedAt:         now,
		HeartbeatDeadline: persistedTime(request.HeartbeatDeadline),
	}
	store.jobs[job.ID] = job
	store.attempts[attempt.ID] = cloneAttempt(attempt)
	return cloneAttempt(attempt), nil
}

// Heartbeat records a new deadline for the currently fenced running attempt.
func (store *Store) Heartbeat(ctx context.Context, request jobs.HeartbeatRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	attempt, job, err := store.currentAttempt(request.AttemptID, request.FencingToken)
	if err != nil {
		return err
	}
	deadline := persistedTime(request.Deadline)
	attempt.HeartbeatDeadline = deadline
	job.UpdatedAt = deadline
	store.attempts[attempt.ID] = cloneAttempt(attempt)
	store.jobs[job.ID] = job
	return nil
}

// Complete records the terminal success of a current attempt.
func (store *Store) Complete(ctx context.Context, request jobs.CompleteRequest) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	_, job, err := store.currentAttempt(request.AttemptID, request.FencingToken)
	if err != nil {
		return jobs.Job{}, err
	}
	job.UpdatedAt = persistedTime(request.CompletedAt)
	if job.CancellationRequested {
		job.State = jobs.StateCanceled
		job.Result = nil
	} else {
		job.State = jobs.StateSucceeded
		job.Result = request.Result.Clone().Payload
	}
	store.jobs[job.ID] = job
	return cloneJob(job), nil
}

// Fail records an attempt failure, scheduling a retry when allowed.
func (store *Store) Fail(ctx context.Context, request jobs.FailRequest) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	_, job, err := store.currentAttempt(request.AttemptID, request.FencingToken)
	if err != nil {
		return jobs.Job{}, err
	}
	job.UpdatedAt = persistedTime(request.FailedAt)
	failure := request.Failure
	job.LastFailure = &failure
	if job.CancellationRequested {
		job.State = jobs.StateCanceled
	} else if request.Failure.Retryable && job.Attempt < job.Retry.MaxAttempts {
		job.State = jobs.StateRetryWaiting
		job.AvailableAt = persistedTime(request.NextAvailableAt)
	} else {
		job.State = jobs.StateFailed
	}
	store.jobs[job.ID] = job
	return cloneJob(job), nil
}

// RequestCancel cancels an unclaimed job or marks a running job for cancellation.
func (store *Store) RequestCancel(ctx context.Context, id jobs.JobID, requestedAt time.Time) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	job, exists := store.jobs[id]
	if !exists {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	if job.State == jobs.StateSucceeded || job.State == jobs.StateFailed || job.State == jobs.StateCanceled {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	job.CancellationRequested = true
	job.UpdatedAt = persistedTime(requestedAt)
	if job.State == jobs.StatePending || job.State == jobs.StateRetryWaiting {
		job.State = jobs.StateCanceled
	}
	store.jobs[id] = job
	return cloneJob(job), nil
}

// RecoverAttempt returns a running attempt's job to pending state.
func (store *Store) RecoverAttempt(ctx context.Context, id jobs.AttemptID, recoveredAt time.Time) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	return store.recoverAttempt(id, persistedTime(recoveredAt))
}

// RecoverExpired recovers up to limit attempts whose deadlines have expired.
func (store *Store) RecoverExpired(ctx context.Context, observedAt time.Time, limit int) ([]jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []jobs.Job{}, nil
	}
	now := persistedTime(observedAt)
	ids := make([]jobs.AttemptID, 0)
	for id, attempt := range store.attempts {
		job, exists := store.jobs[attempt.Job.ID]
		if exists && job.State == jobs.StateRunning && attempt.Job.Attempt == job.Attempt && !attempt.HeartbeatDeadline.After(now) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := store.attempts[ids[i]], store.attempts[ids[j]]
		if !left.HeartbeatDeadline.Equal(right.HeartbeatDeadline) {
			return left.HeartbeatDeadline.Before(right.HeartbeatDeadline)
		}
		return left.ID < right.ID
	})
	if len(ids) > limit {
		ids = ids[:limit]
	}
	recovered := make([]jobs.Job, 0, len(ids))
	for _, id := range ids {
		job, err := store.recoverAttempt(id, now)
		if err != nil {
			return nil, err
		}
		recovered = append(recovered, job)
	}
	return recovered, nil
}

// ListRecoveryCandidates returns expired attempts in deterministic deadline order.
func (store *Store) ListRecoveryCandidates(ctx context.Context, observedAt time.Time, limit int) ([]jobs.RecoveryCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []jobs.RecoveryCandidate{}, nil
	}
	now := persistedTime(observedAt)
	candidates := make([]jobs.RecoveryCandidate, 0)
	for _, attempt := range store.attempts {
		job, exists := store.jobs[attempt.Job.ID]
		if exists && job.State == jobs.StateRunning && attempt.Job.Attempt == job.Attempt && !attempt.HeartbeatDeadline.After(now) {
			candidates = append(candidates, jobs.RecoveryCandidate{Attempt: cloneAttempt(attempt), ObservedAt: now})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i].Attempt, candidates[j].Attempt
		if !left.HeartbeatDeadline.Equal(right.HeartbeatDeadline) {
			return left.HeartbeatDeadline.Before(right.HeartbeatDeadline)
		}
		return left.ID < right.ID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates, nil
}

func (store *Store) currentAttempt(id jobs.AttemptID, token coordination.FencingToken) (jobs.Attempt, jobs.Job, error) {
	attempt, exists := store.attempts[id]
	if !exists {
		return jobs.Attempt{}, jobs.Job{}, jobs.ErrStaleAttempt
	}
	job, exists := store.jobs[attempt.Job.ID]
	if !exists {
		return jobs.Attempt{}, jobs.Job{}, jobs.ErrJobNotFound
	}
	if attempt.Job.Attempt != job.Attempt {
		return jobs.Attempt{}, jobs.Job{}, jobs.ErrStaleAttempt
	}
	if job.State != jobs.StateRunning {
		return jobs.Attempt{}, jobs.Job{}, jobs.ErrInvalidTransition
	}
	if attempt.FencingToken != token {
		return jobs.Attempt{}, jobs.Job{}, jobs.ErrStaleAttempt
	}
	return attempt, job, nil
}

func (store *Store) recoverAttempt(id jobs.AttemptID, recoveredAt time.Time) (jobs.Job, error) {
	attempt, exists := store.attempts[id]
	if !exists {
		return jobs.Job{}, jobs.ErrStaleAttempt
	}
	job, exists := store.jobs[attempt.Job.ID]
	if !exists {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	if attempt.Job.Attempt != job.Attempt {
		return jobs.Job{}, jobs.ErrStaleAttempt
	}
	if job.State != jobs.StateRunning {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	job.UpdatedAt = recoveredAt
	if job.CancellationRequested {
		job.State = jobs.StateCanceled
	} else {
		job.State = jobs.StatePending
		job.AvailableAt = recoveredAt
	}
	store.jobs[job.ID] = job
	return cloneJob(job), nil
}

func claimable(job jobs.Job, now time.Time, all map[jobs.JobID]jobs.Job) bool {
	if (job.State != jobs.StatePending && job.State != jobs.StateRetryWaiting) || job.AvailableAt.After(now) {
		return false
	}
	for _, dependency := range job.DependsOn {
		dependencyJob, exists := all[dependency]
		if !exists || dependencyJob.State != jobs.StateSucceeded {
			return false
		}
	}
	return true
}

func precedes(left, right jobs.Job) bool {
	if left.Priority != right.Priority {
		return left.Priority > right.Priority
	}
	if !left.AvailableAt.Equal(right.AvailableAt) {
		return left.AvailableAt.Before(right.AvailableAt)
	}
	return left.ID < right.ID
}

func cloneJob(job jobs.Job) jobs.Job { return job.Clone() }

func cloneAttempt(attempt jobs.Attempt) jobs.Attempt {
	attempt.Job = cloneJob(attempt.Job)
	return attempt
}

func normalizeJobTimes(job *jobs.Job) {
	job.CreatedAt = persistedTime(job.CreatedAt)
	job.AvailableAt = persistedTime(job.AvailableAt)
	job.UpdatedAt = persistedTime(job.UpdatedAt)
}

func persistedTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.Round(0).UTC()
}
