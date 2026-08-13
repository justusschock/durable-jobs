package jobs

import (
	"context"
	"fmt"
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

// MaxJobPageSize is the largest page accepted by a Catalog.
const MaxJobPageSize = 100

// ListJobPageRequest selects one queue-scoped catalog page in ascending
// bytewise order of raw JobID strings. After is an exclusive position; an
// empty value selects the first page.
type ListJobPageRequest struct {
	Queue Queue
	After JobID
	Limit int
}

// JobPage is one independently owned catalog page. NextAfter is empty when no
// more matching jobs exist in the call's store snapshot.
type JobPage struct {
	Jobs      []Job
	NextAfter JobID
}

// ValidateListJobPageRequest verifies catalog scope, position, and bounds.
func ValidateListJobPageRequest(request ListJobPageRequest) error {
	if !validIdentity(string(request.Queue)) {
		return fmt.Errorf("%w: invalid catalog queue", ErrInvalidJob)
	}
	if request.After != "" && !validIdentity(string(request.After)) {
		return fmt.Errorf("%w: invalid catalog position", ErrInvalidJob)
	}
	if request.Limit <= 0 || request.Limit > MaxJobPageSize {
		return fmt.Errorf("%w: catalog limit must be between 1 and %d", ErrInvalidJob, MaxJobPageSize)
	}
	return nil
}

// Catalog provides bounded, queue-scoped traversal without widening Store.
// Each page is one store snapshot; a traversal spanning pages is not a
// point-in-time snapshot when jobs are created concurrently.
type Catalog interface {
	ListJobPage(context.Context, ListJobPageRequest) (JobPage, error)
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
