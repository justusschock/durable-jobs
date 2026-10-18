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
//
// Every method is atomic, safe for concurrent use, honors ctx, and returns
// independently owned snapshots. The storetest.Run suite defines the full
// observable contract; implementations must pass it.
type Store interface {
	// Create persists a new job, or returns ErrJobExists.
	Create(context.Context, Job) (Job, error)
	// Get returns a job, or ErrJobNotFound.
	Get(context.Context, JobID) (Job, error)
	// ListJobs returns up to Limit jobs of one queue, most recently updated
	// first.
	ListJobs(context.Context, ListJobsRequest) ([]Job, error)
	// ClaimReady starts a new attempt for the first claimable job of the
	// queue (pending or retry-waiting, available, all dependencies
	// succeeded), ordered by priority descending, AvailableAt ascending, and
	// ID ascending. It returns ErrNoReadyJob when none is claimable.
	ClaimReady(context.Context, ClaimRequest) (Attempt, error)
	// Heartbeat extends the current attempt's deadline. It returns
	// ErrStaleAttempt for an old attempt or fence and ErrInvalidTransition
	// for an earlier deadline or a job that is not running.
	Heartbeat(context.Context, HeartbeatRequest) error
	// Complete ends the current attempt as succeeded, or canceled when
	// cancellation was requested, under the same fencing checks as Heartbeat.
	Complete(context.Context, CompleteRequest) (Job, error)
	// Fail records a failure of the current attempt: retry-waiting until
	// NextAvailableAt while a retryable failure has attempts left, otherwise
	// failed (or canceled when requested). Terminal outcomes fail pending
	// dependents transitively.
	Fail(context.Context, FailRequest) (Job, error)
	// RequestCancel cancels a pending or retry-waiting job immediately and
	// marks a running job for cancellation. Terminal jobs return
	// ErrInvalidTransition.
	RequestCancel(context.Context, JobID, time.Time) (Job, error)
	// RecoverAttempt returns a running job whose observed attempt expired to
	// pending (or canceled when requested). The attempt ID, fence, and
	// observed deadline must all still be current.
	RecoverAttempt(context.Context, RecoverAttemptRequest) (Job, error)
	// ListRecoveryCandidates returns up to limit running attempts whose
	// heartbeat deadline is at or before the given time, ordered by deadline
	// and attempt ID.
	ListRecoveryCandidates(context.Context, time.Time, int) ([]RecoveryCandidate, error)
}
