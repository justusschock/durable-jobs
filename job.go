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

const (
	StatePending State = iota + 1
	StateRunning
	StateSucceeded
	StateRetryWaiting
	StateFailed
	StateCanceled
)

// RetryPolicy controls bounded exponential retry scheduling.
type RetryPolicy struct {
	MaxAttempts    uint
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// Job is one independently owned durable job snapshot.
type Job struct {
	ID                    JobID
	Queue                 Queue
	Kind                  string
	Payload               []byte
	Headers               map[string]string
	Priority              int
	State                 State
	CreatedAt             time.Time
	AvailableAt           time.Time
	UpdatedAt             time.Time
	Attempt               uint
	Retry                 RetryPolicy
	DependsOn             []JobID
	CancellationRequested bool
	Result                []byte
	LastFailure           *Failure
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

// EnqueueRequest describes one new durable job.
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

// Failure is a sanitized durable failure classification.
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
