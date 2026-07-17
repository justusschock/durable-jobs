// Package memory provides an in-memory Store implementation for tests and local use.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/internal/storestate"
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
		if candidate.Queue != request.Queue || !storestate.Claimable(candidate, now, store.jobs) {
			continue
		}
		if selected == nil || storestate.Precedes(candidate, *selected) {
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
	advanceUpdatedAt(&job, now)
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
	attempt, job, changed, err := storestate.Heartbeat(attempt, job, request)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
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
	job = storestate.Complete(job, request)
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
	job = storestate.Fail(job, request)
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
	job, err := storestate.RequestCancel(job, requestedAt)
	if err != nil {
		return jobs.Job{}, err
	}
	store.jobs[id] = job
	return cloneJob(job), nil
}

// RecoverAttempt returns a currently fenced, expired attempt's job to pending state.
func (store *Store) RecoverAttempt(ctx context.Context, request jobs.RecoverAttemptRequest) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	attempt, exists := store.attempts[request.AttemptID]
	if !exists {
		return jobs.Job{}, jobs.ErrStaleAttempt
	}
	job, exists := store.jobs[attempt.Job.ID]
	if !exists {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	job, err := storestate.RecoverAttempt(attempt, job, request)
	if err != nil {
		return jobs.Job{}, err
	}
	store.jobs[job.ID] = job
	return cloneJob(job), nil
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
	now := storestate.PersistedTime(observedAt)
	candidates := make([]jobs.RecoveryCandidate, 0)
	for _, attempt := range store.attempts {
		job, exists := store.jobs[attempt.Job.ID]
		if exists && job.State == jobs.StateRunning && attempt.Job.Attempt == job.Attempt && !attempt.HeartbeatDeadline.After(now) {
			candidates = append(candidates, jobs.RecoveryCandidate{Attempt: cloneAttempt(attempt), ObservedAt: now})
		}
	}
	storestate.SortRecoveryCandidates(candidates)
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
	if err := storestate.CurrentAttempt(attempt, job, token); err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	return attempt, job, nil
}

func cloneJob(job jobs.Job) jobs.Job { return storestate.CloneJob(job) }

func cloneAttempt(attempt jobs.Attempt) jobs.Attempt {
	return storestate.CloneAttempt(attempt)
}

func normalizeJobTimes(job *jobs.Job) {
	storestate.NormalizeJobTimes(job)
}

func advanceUpdatedAt(job *jobs.Job, candidate time.Time) {
	storestate.AdvanceUpdatedAt(job, candidate)
}

func persistedTime(value time.Time) time.Time {
	return storestate.PersistedTime(value)
}
