// Package storestate contains the storage-independent job transitions.
package storestate

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
)

// CloneJob returns an independently owned job snapshot.
func CloneJob(job jobs.Job) jobs.Job { return job.Clone() }

// CloneAttempt returns an independently owned attempt snapshot.
func CloneAttempt(attempt jobs.Attempt) jobs.Attempt {
	attempt.Job = CloneJob(attempt.Job)
	return attempt
}

// PersistedTime removes monotonic clock data and normalizes non-zero times to UTC.
func PersistedTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.Round(0).UTC()
}

// NormalizeJobTimes prepares a job for durable storage.
func NormalizeJobTimes(job *jobs.Job) {
	job.CreatedAt = PersistedTime(job.CreatedAt)
	job.AvailableAt = PersistedTime(job.AvailableAt)
	job.UpdatedAt = PersistedTime(job.UpdatedAt)
}

// NormalizeAttemptTimes prepares an attempt read from durable storage.
func NormalizeAttemptTimes(attempt *jobs.Attempt) {
	NormalizeJobTimes(&attempt.Job)
	attempt.StartedAt = PersistedTime(attempt.StartedAt)
	attempt.HeartbeatDeadline = PersistedTime(attempt.HeartbeatDeadline)
}

// AdvanceUpdatedAt preserves the monotonic ordering of job updates.
func AdvanceUpdatedAt(job *jobs.Job, candidate time.Time) {
	candidate = PersistedTime(candidate)
	if candidate.After(job.UpdatedAt) {
		job.UpdatedAt = candidate
	}
}

// Claimable reports whether a job can be claimed at now.
func Claimable(job jobs.Job, now time.Time, all map[jobs.JobID]jobs.Job) bool {
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

// Precedes reports the deterministic claim ordering of two ready jobs.
func Precedes(left, right jobs.Job) bool {
	if left.Priority != right.Priority {
		return left.Priority > right.Priority
	}
	if !left.AvailableAt.Equal(right.AvailableAt) {
		return left.AvailableAt.Before(right.AvailableAt)
	}
	return left.ID < right.ID
}

// RecentFirst reports the deterministic listing order for operator-facing job
// snapshots.
func RecentFirst(left, right jobs.Job) bool {
	if !left.UpdatedAt.Equal(right.UpdatedAt) {
		return left.UpdatedAt.After(right.UpdatedAt)
	}
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.After(right.CreatedAt)
	}
	return left.ID < right.ID
}

// CurrentAttempt validates the current generation, fence, and state.
func CurrentAttempt(attempt jobs.Attempt, job jobs.Job, token coordination.FencingToken) error {
	if attempt.Job.Attempt != job.Attempt || attempt.FencingToken != token {
		return jobs.ErrStaleAttempt
	}
	if job.State != jobs.StateRunning {
		return jobs.ErrInvalidTransition
	}
	return nil
}

// Heartbeat applies a monotonic deadline extension to the current attempt.
func Heartbeat(attempt jobs.Attempt, job jobs.Job, request jobs.HeartbeatRequest) (jobs.Attempt, jobs.Job, bool, error) {
	if err := CurrentAttempt(attempt, job, request.FencingToken); err != nil {
		return jobs.Attempt{}, jobs.Job{}, false, err
	}
	deadline := PersistedTime(request.Deadline)
	if deadline.Equal(attempt.HeartbeatDeadline) {
		return attempt, job, false, nil
	}
	if deadline.Before(attempt.HeartbeatDeadline) {
		return jobs.Attempt{}, jobs.Job{}, false, jobs.ErrInvalidTransition
	}
	attempt.HeartbeatDeadline = deadline
	AdvanceUpdatedAt(&job, deadline)
	return attempt, job, true, nil
}

// Complete transitions a current job to its terminal outcome.
func Complete(job jobs.Job, request jobs.CompleteRequest) jobs.Job {
	AdvanceUpdatedAt(&job, request.CompletedAt)
	if job.CancellationRequested {
		job.State = jobs.StateCanceled
		job.Result = nil
	} else {
		job.State = jobs.StateSucceeded
		job.Result = request.Result.Clone().Payload
	}
	return job
}

// Fail records a failure and either schedules retry or terminates the job.
func Fail(job jobs.Job, request jobs.FailRequest) jobs.Job {
	AdvanceUpdatedAt(&job, request.FailedAt)
	failure := request.Failure
	job.LastFailure = &failure
	if job.CancellationRequested {
		job.State = jobs.StateCanceled
	} else if request.Failure.Retryable && job.Attempt < job.Retry.MaxAttempts {
		job.State = jobs.StateRetryWaiting
		job.AvailableAt = PersistedTime(request.NextAvailableAt)
	} else {
		job.State = jobs.StateFailed
	}
	return job
}

// RequestCancel applies cancellation to a non-terminal job.
func RequestCancel(job jobs.Job, requestedAt time.Time) (jobs.Job, error) {
	if job.State == jobs.StateSucceeded || job.State == jobs.StateFailed || job.State == jobs.StateCanceled {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	job.CancellationRequested = true
	AdvanceUpdatedAt(&job, requestedAt)
	if job.State == jobs.StatePending || job.State == jobs.StateRetryWaiting {
		job.State = jobs.StateCanceled
	}
	return job, nil
}

// RecoverAttempt transitions a still-current expired attempt back to its job state.
func RecoverAttempt(attempt jobs.Attempt, job jobs.Job, request jobs.RecoverAttemptRequest) (jobs.Job, error) {
	if attempt.Job.Attempt != job.Attempt || attempt.FencingToken != request.FencingToken {
		return jobs.Job{}, jobs.ErrStaleAttempt
	}
	if !attempt.HeartbeatDeadline.Equal(PersistedTime(request.HeartbeatDeadline)) {
		return jobs.Job{}, jobs.ErrStaleAttempt
	}
	if job.State != jobs.StateRunning {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	recoveredAt := PersistedTime(request.RecoveredAt)
	if attempt.HeartbeatDeadline.After(recoveredAt) {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	AdvanceUpdatedAt(&job, recoveredAt)
	if job.CancellationRequested {
		job.State = jobs.StateCanceled
	} else {
		job.State = jobs.StatePending
		job.AvailableAt = recoveredAt
	}
	return job, nil
}

// SortRecoveryCandidates orders candidates by deadline and then attempt ID.
func SortRecoveryCandidates(candidates []jobs.RecoveryCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i].Attempt, candidates[j].Attempt
		if !left.HeartbeatDeadline.Equal(right.HeartbeatDeadline) {
			return left.HeartbeatDeadline.Before(right.HeartbeatDeadline)
		}
		return left.ID < right.ID
	})
}

// CloneSchedule returns an independently owned schedule snapshot.
func CloneSchedule(schedule jobs.Schedule) jobs.Schedule { return schedule.Clone() }

// NormalizeScheduleTimes prepares a schedule for durable storage.
func NormalizeScheduleTimes(schedule *jobs.Schedule) {
	schedule.NextRunAt = PersistedTime(schedule.NextRunAt)
}

// ValidateSchedule verifies durable schedule invariants.
func ValidateSchedule(schedule jobs.Schedule) error {
	id := string(schedule.ID)
	if strings.TrimSpace(id) == "" || !utf8.ValidString(id) || strings.ContainsRune(id, '\x00') {
		return fmt.Errorf("%w: invalid schedule identity", jobs.ErrInvalidJob)
	}
	if err := jobs.ValidateEnqueue(schedule.Request); err != nil {
		return err
	}
	if schedule.Interval < 0 {
		return fmt.Errorf("%w: negative schedule interval", jobs.ErrInvalidJob)
	}
	if schedule.Enabled && schedule.NextRunAt.IsZero() {
		return fmt.Errorf("%w: enabled schedule has no next run", jobs.ErrInvalidJob)
	}
	return nil
}

// SortSchedules orders schedules by next run and then ID.
func SortSchedules(schedules []jobs.Schedule) {
	sort.Slice(schedules, func(left, right int) bool {
		if !schedules[left].NextRunAt.Equal(schedules[right].NextRunAt) {
			return schedules[left].NextRunAt.Before(schedules[right].NextRunAt)
		}
		return schedules[left].ID < schedules[right].ID
	})
}

// PropagateDependencyFailures terminally fails pending transitive dependents.
func PropagateDependencyFailures(all map[jobs.JobID]jobs.Job, failedAt time.Time) []jobs.JobID {
	failedAt = PersistedTime(failedAt)
	changed := make([]jobs.JobID, 0)
	for {
		ids := make([]jobs.JobID, 0, len(all))
		for id := range all {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })

		propagated := false
		for _, id := range ids {
			job := all[id]
			if job.State != jobs.StatePending {
				continue
			}
			for _, dependency := range job.DependsOn {
				dependencyJob, exists := all[dependency]
				if !exists || (dependencyJob.State != jobs.StateFailed && dependencyJob.State != jobs.StateCanceled) {
					continue
				}
				job.State = jobs.StateFailed
				job.LastFailure = &jobs.Failure{Code: "dependency_failed", Message: "dependency did not succeed"}
				AdvanceUpdatedAt(&job, failedAt)
				all[id] = job
				changed = append(changed, id)
				propagated = true
				break
			}
		}
		if !propagated {
			return changed
		}
	}
}
