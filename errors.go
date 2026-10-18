package jobs

import "errors"

// Sentinel errors returned by clients, workers, schedulers, and stores. They
// are part of the API: callers test them with errors.Is, and implementations
// wrap them with %w to add detail. Error messages never contain job payloads,
// results, or headers.
var (
	// ErrInvalidJob reports a request, job, schedule, or constructor argument
	// that violates an invariant (identity, retry policy, dependencies,
	// catalog bounds, nil dependencies).
	ErrInvalidJob = errors.New("durable jobs: invalid job")
	// ErrJobExists reports that Create found a job with the same ID.
	ErrJobExists = errors.New("durable jobs: job exists")
	// ErrJobNotFound reports an unknown job or schedule ID.
	ErrJobNotFound = errors.New("durable jobs: job not found")
	// ErrNoReadyJob reports that ClaimReady found no claimable job.
	ErrNoReadyJob = errors.New("durable jobs: no ready job")
	// ErrStaleAttempt reports a fencing token, attempt generation, heartbeat
	// observation, or schedule revision that is no longer current.
	ErrStaleAttempt = errors.New("durable jobs: stale attempt")
	// ErrInvalidTransition reports an operation that the job's current
	// state does not allow, such as canceling a terminal job or moving a
	// heartbeat deadline backward.
	ErrInvalidTransition = errors.New("durable jobs: invalid transition")
	// ErrDependencyFailed reports a prerequisite job that failed or was
	// canceled.
	ErrDependencyFailed = errors.New("durable jobs: dependency failed")
	// ErrWakeupPublish reports that Enqueue persisted the job but could not
	// publish the queue wakeup. The job remains durable and claimable.
	ErrWakeupPublish = errors.New("durable jobs: wakeup publish")
)
