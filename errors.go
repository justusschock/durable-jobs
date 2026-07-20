package jobs

import "errors"

var (
	ErrInvalidJob        = errors.New("durable jobs: invalid job")
	ErrJobExists         = errors.New("durable jobs: job exists")
	ErrJobNotFound       = errors.New("durable jobs: job not found")
	ErrNoReadyJob        = errors.New("durable jobs: no ready job")
	ErrStaleAttempt      = errors.New("durable jobs: stale attempt")
	ErrInvalidTransition = errors.New("durable jobs: invalid transition")
	ErrDependencyFailed  = errors.New("durable jobs: dependency failed")
	ErrWakeupPublish     = errors.New("durable jobs: wakeup publish")
)
