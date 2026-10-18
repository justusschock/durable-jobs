package jobs

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// JobID identifies one durable job.
type JobID string

// AttemptID identifies one fenced execution attempt.
type AttemptID string

// Queue identifies an ordered claim domain.
type Queue string

// State is the durable job lifecycle state.
type State uint8

// Job states. StateSucceeded, StateFailed, and StateCanceled are terminal.
const (
	// StatePending jobs wait for AvailableAt and their dependencies.
	StatePending State = iota + 1
	// StateRunning jobs have a current fenced attempt.
	StateRunning
	// StateSucceeded jobs completed and hold their result.
	StateSucceeded
	// StateRetryWaiting jobs failed retryably and wait for AvailableAt.
	StateRetryWaiting
	// StateFailed jobs failed terminally or had a dependency fail.
	StateFailed
	// StateCanceled jobs were canceled before or during execution.
	StateCanceled
)

// RetryPolicy controls bounded exponential retry scheduling. See RetryDelay.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first;
	// it must be at least 1.
	MaxAttempts uint
	// InitialBackoff is the delay after the first failed attempt; it must be
	// positive.
	InitialBackoff time.Duration
	// MaxBackoff caps every delay; it must be at least InitialBackoff.
	MaxBackoff time.Duration
	// Multiplier scales the delay after each further failure; it must be a
	// finite number of at least 1.
	Multiplier float64
}

// Job is one independently owned durable job snapshot. Stores normalize its
// times to UTC without monotonic clock readings.
type Job struct {
	ID    JobID
	Queue Queue
	// Kind selects the Handler in the worker's Registry.
	Kind    string
	Payload []byte
	Headers map[string]string
	// Priority orders ready jobs in a queue; higher values are claimed first.
	Priority int
	State    State
	// CreatedAt is the enqueue time.
	CreatedAt time.Time
	// AvailableAt is the earliest time the job may be claimed.
	AvailableAt time.Time
	// UpdatedAt is the latest transition time; it never moves backward.
	UpdatedAt time.Time
	// Attempt counts claims, starting at 1 for the first attempt.
	Attempt uint
	Retry   RetryPolicy
	// DependsOn lists jobs that must succeed before this job is claimable.
	DependsOn []JobID
	// CancellationRequested is set by RequestCancel on a running job; the
	// attempt then ends canceled.
	CancellationRequested bool
	// Result holds the handler output of a succeeded job.
	Result []byte
	// LastFailure is the most recent recorded failure, if any.
	LastFailure *Failure
}

// Clone returns a job with independently owned payload, result, header,
// dependency, and failure storage.
func (job Job) Clone() Job {
	job.Payload = bytes.Clone(job.Payload)
	job.Headers = maps.Clone(job.Headers)
	job.DependsOn = slices.Clone(job.DependsOn)
	job.Result = bytes.Clone(job.Result)
	if job.LastFailure != nil {
		failure := *job.LastFailure
		job.LastFailure = &failure
	}
	return job
}

// EnqueueRequest describes one new durable job. A zero AvailableAt makes the
// job available at the client's current time. See ValidateEnqueue for the
// invariants.
type EnqueueRequest struct {
	ID          JobID
	Queue       Queue
	Kind        string
	Payload     []byte
	Headers     map[string]string
	Priority    int
	AvailableAt time.Time
	Retry       RetryPolicy
	DependsOn   []JobID
}

// Clone returns a request with independently owned payload, headers, and
// dependencies.
func (request EnqueueRequest) Clone() EnqueueRequest {
	request.Payload = bytes.Clone(request.Payload)
	request.Headers = maps.Clone(request.Headers)
	request.DependsOn = slices.Clone(request.DependsOn)
	return request
}

// Result is opaque handler output.
type Result struct {
	Payload []byte
}

// Clone returns independently owned result bytes.
func (result Result) Clone() Result {
	result.Payload = bytes.Clone(result.Payload)
	return result
}

// Failure is a sanitized durable failure classification. Workers record the
// codes "handler_error" (retryable), "handler_panic", and "handler_not_found";
// stores record "dependency_failed" when a prerequisite does not succeed.
// Handler error values are never persisted.
type Failure struct {
	Code      string
	Message   string
	Retryable bool
}

// ValidateEnqueue verifies identity, retry, and dependency invariants without
// retaining or mutating request data.
func ValidateEnqueue(request EnqueueRequest) error {
	if !validIdentity(string(request.ID)) || !validIdentity(string(request.Queue)) || !validIdentity(request.Kind) {
		return fmt.Errorf("%w: invalid identity", ErrInvalidJob)
	}
	if request.Retry.MaxAttempts == 0 || request.Retry.InitialBackoff <= 0 || request.Retry.MaxBackoff <= 0 {
		return fmt.Errorf("%w: invalid retry duration", ErrInvalidJob)
	}
	if request.Retry.MaxBackoff < request.Retry.InitialBackoff {
		return fmt.Errorf("%w: invalid retry order", ErrInvalidJob)
	}
	if request.Retry.Multiplier < 1 || math.IsNaN(request.Retry.Multiplier) || math.IsInf(request.Retry.Multiplier, 0) {
		return fmt.Errorf("%w: invalid retry multiplier", ErrInvalidJob)
	}
	dependencies := make(map[JobID]struct{}, len(request.DependsOn))
	for _, dependency := range request.DependsOn {
		if !validIdentity(string(dependency)) || dependency == request.ID {
			return fmt.Errorf("%w: invalid dependency", ErrInvalidJob)
		}
		if _, duplicate := dependencies[dependency]; duplicate {
			return fmt.Errorf("%w: duplicate dependency", ErrInvalidJob)
		}
		dependencies[dependency] = struct{}{}
	}
	return nil
}

func validIdentity(value string) bool {
	return strings.TrimSpace(value) != "" && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}
