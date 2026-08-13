package jobs

import (
	"context"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

// ClaimRequest identifies a worker's request to claim one ready job.
type ClaimRequest struct {
	Queue             Queue
	WorkerID          string
	Now               time.Time
	HeartbeatDeadline time.Time
	FencingToken      coordination.FencingToken
}

// Attempt is a fenced execution claim for a job.
type Attempt struct {
	ID                AttemptID
	Job               Job
	WorkerID          string
	FencingToken      coordination.FencingToken
	StartedAt         time.Time
	HeartbeatDeadline time.Time
}

// HeartbeatRequest extends the deadline of the current attempt.
type HeartbeatRequest struct {
	AttemptID    AttemptID
	FencingToken coordination.FencingToken
	Deadline     time.Time
}

// CompleteRequest records a successful attempt result.
type CompleteRequest struct {
	AttemptID    AttemptID
	FencingToken coordination.FencingToken
	Result       Result
	CompletedAt  time.Time
}

// FailRequest records an attempt failure and its next eligibility time.
type FailRequest struct {
	AttemptID       AttemptID
	FencingToken    coordination.FencingToken
	Failure         Failure
	FailedAt        time.Time
	NextAvailableAt time.Time
}

// RecoveryCandidate is an expired running attempt observed by a recovery worker.
type RecoveryCandidate struct {
	Attempt    Attempt
	ObservedAt time.Time
}

// RecoverAttemptRequest identifies one expired attempt observation to recover.
type RecoverAttemptRequest struct {
	AttemptID         AttemptID
	FencingToken      coordination.FencingToken
	HeartbeatDeadline time.Time
	RecoveredAt       time.Time
}

// ListJobsRequest selects a bounded queue-scoped job snapshot.
type ListJobsRequest struct {
	Queue Queue
	Limit int
}

// Store atomically persists durable jobs and their fenced execution attempts.
type Store interface {
	Create(context.Context, Job) (Job, error)
	Get(context.Context, JobID) (Job, error)
	ListJobs(context.Context, ListJobsRequest) ([]Job, error)
	ClaimReady(context.Context, ClaimRequest) (Attempt, error)
	Heartbeat(context.Context, HeartbeatRequest) error
	Complete(context.Context, CompleteRequest) (Job, error)
	Fail(context.Context, FailRequest) (Job, error)
	RequestCancel(context.Context, JobID, time.Time) (Job, error)
	RecoverAttempt(context.Context, RecoverAttemptRequest) (Job, error)
	ListRecoveryCandidates(context.Context, time.Time, int) ([]RecoveryCandidate, error)
}
