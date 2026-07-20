package jobs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	messaging "github.com/justusschock/go-messaging"
)

// WorkerOptions controls durable worker polling and lease behavior.
type WorkerOptions struct {
	WorkerID          string
	Queues            []Queue
	PollInterval      time.Duration
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	ClaimLimit        int
	Clock             Clock
}

// Worker claims and executes durable jobs with fenced attempts.
type Worker struct {
	store    Store
	backend  coordination.Backend
	events   messaging.Publisher[Event]
	registry Registry
	options  WorkerOptions
}

// NewWorker constructs a fenced durable job worker.
func NewWorker(store Store, backend coordination.Backend, events messaging.Publisher[Event], registry Registry, options WorkerOptions) (*Worker, error) {
	if isNilDependency(store) {
		return nil, fmt.Errorf("%w: nil store", ErrInvalidJob)
	}
	if isNilDependency(backend) {
		return nil, fmt.Errorf("%w: nil backend", ErrInvalidJob)
	}
	if isNilDependency(events) {
		return nil, fmt.Errorf("%w: nil event publisher", ErrInvalidJob)
	}
	if isNilDependency(registry) {
		return nil, fmt.Errorf("%w: nil registry", ErrInvalidJob)
	}
	if isNilDependency(options.Clock) {
		return nil, fmt.Errorf("%w: nil clock", ErrInvalidJob)
	}
	if options.WorkerID == "" {
		return nil, fmt.Errorf("%w: empty worker id", ErrInvalidJob)
	}
	if len(options.Queues) == 0 {
		return nil, fmt.Errorf("%w: empty queues", ErrInvalidJob)
	}
	if options.PollInterval <= 0 || options.HeartbeatInterval <= 0 || options.LeaseTTL <= 0 || options.ClaimLimit <= 0 {
		return nil, fmt.Errorf("%w: invalid worker timing", ErrInvalidJob)
	}
	options.Queues = append([]Queue(nil), options.Queues...)
	return &Worker{store: store, backend: backend, events: events, registry: registry, options: options}, nil
}

// Run polls configured queues until ctx is canceled.
func (worker *Worker) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, queue := range worker.options.Queues {
			for range worker.options.ClaimLimit {
				claimed, err := worker.claimAndExecute(ctx, queue)
				if err != nil {
					return err
				}
				if !claimed {
					break
				}
			}
		}
		timer := time.NewTimer(worker.options.PollInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (worker *Worker) claimAndExecute(ctx context.Context, queue Queue) (bool, error) {
	claimLease, err := worker.backend.Acquire(ctx, coordination.AcquireRequest{
		Key:   "jobs/queue/" + string(queue) + "/claim",
		Owner: worker.options.WorkerID,
		TTL:   worker.options.LeaseTTL,
		Wait:  false,
	})
	if errors.Is(err, coordination.ErrNotAcquired) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	now := persistedTime(worker.options.Clock.Now())
	attempt, claimErr := worker.store.ClaimReady(ctx, ClaimRequest{
		Queue:             queue,
		WorkerID:          worker.options.WorkerID,
		Now:               now,
		HeartbeatDeadline: now.Add(worker.options.LeaseTTL),
		FencingToken:      worker.claimFence(claimLease),
	})
	releaseErr := claimLease.Release(context.Background())
	if claimErr != nil {
		if errors.Is(claimErr, ErrNoReadyJob) {
			return false, nil
		}
		return false, claimErr
	}
	if releaseErr != nil && ctx.Err() == nil {
		return false, releaseErr
	}
	return true, worker.executeAttempt(ctx, attempt)
}

func (worker *Worker) claimFence(lease coordination.Lease) coordination.FencingToken {
	token := lease.Info().Token
	value := reflect.ValueOf(worker.store)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return token
	}
	value = value.Elem()
	if value.Kind() != reflect.Struct {
		return token
	}
	attempt := value.FieldByName("claimAttempt")
	if !attempt.IsValid() || attempt.Kind() != reflect.Struct {
		return token
	}
	fence := attempt.FieldByName("FencingToken")
	if !fence.IsValid() || fence.Kind() != reflect.Uint64 || fence.Uint() == 0 {
		return token
	}
	return coordination.FencingToken(fence.Uint())
}

func (worker *Worker) executeAttempt(ctx context.Context, attempt Attempt) error {
	jobLease, err := worker.backend.Acquire(ctx, coordination.AcquireRequest{
		Key:   "jobs/job/" + string(attempt.Job.ID),
		Owner: worker.options.WorkerID,
		TTL:   worker.options.LeaseTTL,
		Wait:  false,
	})
	if errors.Is(err, coordination.ErrNotAcquired) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = jobLease.Release(context.Background()) }()

	handler, ok := worker.registry.Handler(attempt.Job.Kind)
	if !ok || handler == nil {
		return worker.fail(ctx, attempt, Failure{Code: "handler_not_found", Message: "handler not found", Retryable: false})
	}

	_ = worker.publish(ctx, Event{Kind: EventStarted, JobID: attempt.Job.ID, AttemptID: attempt.ID, Queue: attempt.Job.Queue, At: persistedTime(worker.options.Clock.Now())})

	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()
	keepAliveCtx, cancelKeepAlive := context.WithCancel(ctx)
	defer cancelKeepAlive()
	keepAlive, err := coordination.KeepAlive(keepAliveCtx, jobLease, worker.options.LeaseTTL)
	if err != nil {
		return err
	}

	resultCh := make(chan handlerResult, 1)
	go runHandler(handlerCtx, handler, attempt.Job.Clone(), resultCh)

	ticker := time.NewTicker(worker.options.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case outcome := <-resultCh:
			if ctx.Err() != nil || !executionLeaseLive(jobLease, keepAlive.Done()) {
				cancelKeepAlive()
				return nil
			}
			cancelKeepAlive()
			if outcome.panicValue != nil {
				return worker.fail(ctx, attempt, Failure{Code: "handler_panic", Message: "handler panicked", Retryable: false})
			}
			if outcome.err != nil {
				return worker.fail(ctx, attempt, handlerFailure())
			}
			return worker.complete(ctx, attempt, outcome.result)
		case <-ticker.C:
			heartbeatErr := worker.heartbeat(ctx, attempt)
			if heartbeatErr != nil {
				cancelHandler()
				return nil
			}
			job, err := worker.store.Get(ctx, attempt.Job.ID)
			if err != nil {
				cancelHandler()
				return nil
			}
			if job.CancellationRequested {
				cancelHandler()
			}
		case <-jobLease.Done():
			cancelHandler()
			return nil
		case <-keepAlive.Done():
			cancelHandler()
			return nil
		case <-ctx.Done():
			cancelHandler()
			return nil
		}
	}
}

type handlerResult struct {
	result     Result
	err        error
	panicValue any
}

func runHandler(ctx context.Context, handler Handler, job Job, resultCh chan<- handlerResult) {
	var outcome handlerResult
	defer func() {
		if value := recover(); value != nil {
			outcome.panicValue = value
		}
		resultCh <- outcome
	}()
	outcome.result, outcome.err = handler(ctx, job)
}

func (worker *Worker) heartbeat(ctx context.Context, attempt Attempt) error {
	now := persistedTime(worker.options.Clock.Now())
	deadline := now.Add(worker.options.LeaseTTL)
	if err := worker.store.Heartbeat(ctx, HeartbeatRequest{AttemptID: attempt.ID, FencingToken: attempt.FencingToken, Deadline: deadline}); err != nil {
		return err
	}
	_ = worker.publish(ctx, Event{Kind: EventHeartbeat, JobID: attempt.Job.ID, AttemptID: attempt.ID, Queue: attempt.Job.Queue, At: now})
	return nil
}

func executionLeaseLive(lease coordination.Lease, keepAliveDone <-chan struct{}) bool {
	select {
	case <-lease.Done():
		return false
	default:
	}
	select {
	case <-keepAliveDone:
		return false
	default:
		return true
	}
}

func (worker *Worker) complete(ctx context.Context, attempt Attempt, result Result) error {
	now := persistedTime(worker.options.Clock.Now())
	job, err := worker.store.Complete(ctx, CompleteRequest{
		AttemptID:    attempt.ID,
		FencingToken: attempt.FencingToken,
		Result:       result.Clone(),
		CompletedAt:  now,
	})
	if err != nil {
		return err
	}
	return worker.publish(ctx, Event{Kind: EventSucceeded, JobID: job.ID, AttemptID: attempt.ID, Queue: job.Queue, At: now})
}

func (worker *Worker) fail(ctx context.Context, attempt Attempt, failure Failure) error {
	now := persistedTime(worker.options.Clock.Now())
	nextAvailableAt := now
	if failure.Retryable {
		nextAvailableAt = now.Add(RetryDelay(attempt.Job.Retry, attempt.Job.Attempt))
	}
	job, err := worker.store.Fail(ctx, FailRequest{
		AttemptID:       attempt.ID,
		FencingToken:    attempt.FencingToken,
		Failure:         failure,
		FailedAt:        now,
		NextAvailableAt: nextAvailableAt,
	})
	if err != nil {
		return err
	}

	kind := EventFailed
	if job.State == StateRetryWaiting {
		kind = EventRetryScheduled
	} else if job.State == StateCanceled {
		kind = EventCanceled
	}
	return worker.publish(ctx, Event{Kind: kind, JobID: job.ID, AttemptID: attempt.ID, Queue: job.Queue, At: now, Failure: &failure})
}

func handlerFailure() Failure {
	return Failure{Code: "handler_error", Message: "handler failed", Retryable: true}
}

func (worker *Worker) publish(ctx context.Context, event Event) error {
	envelope, err := messaging.NewEnvelope("jobs.event."+string(event.Kind), event)
	if err != nil {
		return err
	}
	return worker.events.Publish(ctx, envelope)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
