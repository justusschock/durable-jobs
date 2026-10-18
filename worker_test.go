package jobs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	coordinationmemory "github.com/justusschock/go-distributed-coordination/memory"
	jobs "github.com/justusschock/go-durable-jobs"
	messaging "github.com/justusschock/go-messaging"
)

const workerTestTimeout = 500 * time.Millisecond

func TestWorkerConstructorRejectsNilAndInvalidOptions(t *testing.T) {
	t.Parallel()

	store := newWorkerStore()
	backend := newWorkerBackend()
	publisher := &workerEventPublisher{}
	registry := jobs.MapRegistry{"send-email": func(context.Context, jobs.Job) (jobs.Result, error) { return jobs.Result{}, nil }}
	clock := workerClock{now: time.Unix(100, 0)}
	var typedNilStore *workerStore
	var typedNilBackend *workerBackend
	var typedNilPublisher *workerEventPublisher
	var typedNilRegistry jobs.MapRegistry
	var typedNilClock *workerClock

	tests := []struct {
		name      string
		store     jobs.Store
		backend   coordination.Backend
		publisher messaging.Publisher[jobs.Event]
		registry  jobs.Registry
		options   jobs.WorkerOptions
	}{
		{name: "nil store", backend: backend, publisher: publisher, registry: registry, options: validWorkerOptions(clock)},
		{name: "typed nil store", store: typedNilStore, backend: backend, publisher: publisher, registry: registry, options: validWorkerOptions(clock)},
		{name: "nil backend", store: store, publisher: publisher, registry: registry, options: validWorkerOptions(clock)},
		{name: "typed nil backend", store: store, backend: typedNilBackend, publisher: publisher, registry: registry, options: validWorkerOptions(clock)},
		{name: "nil publisher", store: store, backend: backend, registry: registry, options: validWorkerOptions(clock)},
		{name: "typed nil publisher", store: store, backend: backend, publisher: typedNilPublisher, registry: registry, options: validWorkerOptions(clock)},
		{name: "nil registry", store: store, backend: backend, publisher: publisher, options: validWorkerOptions(clock)},
		{name: "typed nil registry", store: store, backend: backend, publisher: publisher, registry: typedNilRegistry, options: validWorkerOptions(clock)},
		{name: "nil clock", store: store, backend: backend, publisher: publisher, registry: registry, options: validWorkerOptions(nil)},
		{name: "typed nil clock", store: store, backend: backend, publisher: publisher, registry: registry, options: validWorkerOptions(typedNilClock)},
		{name: "empty worker id", store: store, backend: backend, publisher: publisher, registry: registry, options: withWorkerOption(func(options *jobs.WorkerOptions) { options.WorkerID = "" })},
		{name: "no queues", store: store, backend: backend, publisher: publisher, registry: registry, options: withWorkerOption(func(options *jobs.WorkerOptions) { options.Queues = nil })},
		{name: "zero poll interval", store: store, backend: backend, publisher: publisher, registry: registry, options: withWorkerOption(func(options *jobs.WorkerOptions) { options.PollInterval = 0 })},
		{name: "zero heartbeat interval", store: store, backend: backend, publisher: publisher, registry: registry, options: withWorkerOption(func(options *jobs.WorkerOptions) { options.HeartbeatInterval = 0 })},
		{name: "zero lease ttl", store: store, backend: backend, publisher: publisher, registry: registry, options: withWorkerOption(func(options *jobs.WorkerOptions) { options.LeaseTTL = 0 })},
		{name: "zero claim limit", store: store, backend: backend, publisher: publisher, registry: registry, options: withWorkerOption(func(options *jobs.WorkerOptions) { options.ClaimLimit = 0 })},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := jobs.NewWorker(test.store, test.backend, test.publisher, test.registry, test.options); err == nil {
				t.Fatal("NewWorker() error = nil, want non-nil")
			}
		})
	}
}

func TestWorkerRunOneReturnsFalseWhenNoJobIsReady(t *testing.T) {
	t.Parallel()

	store := newWorkerStore()
	store.claimed = true
	backend := newWorkerBackend()
	worker := mustNewWorker(
		t,
		store,
		backend,
		&workerEventPublisher{},
		newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
			t.Fatal("handler called without a ready job")
			return jobs.Result{}, nil
		}),
		validWorkerOptions(workerClock{now: time.Unix(100, 0)}),
	)

	claimed, err := worker.RunOne(context.Background())
	if err != nil {
		t.Fatalf("RunOne() error = %v", err)
	}
	if claimed {
		t.Fatal("RunOne() claimed = true, want false")
	}
	if len(store.claimRequests) != 1 {
		t.Fatalf("ClaimReady() calls = %d, want 1", len(store.claimRequests))
	}
	if len(backend.acquireRequests) != 1 || backend.acquireRequests[0].Key != "jobs/queue/default/claim" {
		t.Fatalf("Acquire() requests = %#v, want only the default queue claim", backend.acquireRequests)
	}
	if len(backend.leases) != 1 || !backend.leases[0].released {
		t.Fatal("queue claim lease was not released after no ready job")
	}
}

func TestWorkerRunOneUsesQueueOrderAndExecutesAtMostOneJob(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempts = map[jobs.Queue][]jobs.Attempt{
			"third": {
				workerAttemptInQueue("third-1", "third", 1),
				workerAttemptInQueue("third-2", "third", 2),
			},
			"fourth": {workerAttemptInQueue("fourth-1", "fourth", 3)},
		}
		backend := newWorkerBackend()
		backend.acquireErrors = map[int]error{0: coordination.ErrNotAcquired}
		var handled []jobs.JobID
		registry := newWorkerRegistry(t, "send-email", func(_ context.Context, job jobs.Job) (jobs.Result, error) {
			handled = append(handled, job.ID)
			return jobs.Result{}, nil
		})
		options := validWorkerOptions(workerClock{now: time.Unix(100, 0)})
		options.Queues = []jobs.Queue{"first", "second", "third", "fourth"}
		options.PollInterval = time.Hour
		options.ClaimLimit = 10
		worker := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, options)

		claimed, err := worker.RunOne(context.Background())
		if err != nil {
			t.Fatalf("RunOne() error = %v", err)
		}
		if !claimed {
			t.Fatal("RunOne() claimed = false, want true")
		}
		if len(store.claimRequests) != 2 || store.claimRequests[0].Queue != "second" || store.claimRequests[1].Queue != "third" {
			t.Fatalf("ClaimReady() queue order = %#v, want second then third after first-queue lease contention", store.claimRequests)
		}
		if len(handled) != 1 || handled[0] != "third-1" {
			t.Fatalf("handled jobs = %v, want only third-1", handled)
		}
		if remaining := len(store.claimAttempts["third"]); remaining != 1 {
			t.Fatalf("remaining third-queue jobs = %d, want 1", remaining)
		}
		if remaining := len(store.claimAttempts["fourth"]); remaining != 1 {
			t.Fatalf("remaining fourth-queue jobs = %d, want 1", remaining)
		}
	})
}

func TestWorkerRunOneRejectsCanceledContextBeforeClaiming(t *testing.T) {
	t.Parallel()

	store := newWorkerStore()
	backend := newWorkerBackend()
	worker := mustNewWorker(
		t,
		store,
		backend,
		&workerEventPublisher{},
		newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
			t.Fatal("handler called for canceled context")
			return jobs.Result{}, nil
		}),
		validWorkerOptions(workerClock{now: time.Unix(100, 0)}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	claimed, err := worker.RunOne(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOne() error = %v, want context.Canceled", err)
	}
	if claimed {
		t.Fatal("RunOne() claimed = true, want false")
	}
	if len(backend.acquireRequests) != 0 || len(store.claimRequests) != 0 {
		t.Fatalf("canceled RunOne() acquired=%d claimed=%d, want no calls", len(backend.acquireRequests), len(store.claimRequests))
	}
}

func TestWorkerRunOneHandlesSuccessErrorAndPanic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		handler       jobs.Handler
		wantComplete  int
		wantFail      int
		wantCode      string
		wantRetryable bool
	}{
		{
			name: "success",
			handler: func(context.Context, jobs.Job) (jobs.Result, error) {
				return jobs.Result{Payload: []byte("ok")}, nil
			},
			wantComplete: 1,
		},
		{
			name: "error",
			handler: func(context.Context, jobs.Job) (jobs.Result, error) {
				return jobs.Result{}, errors.New("temporary failure")
			},
			wantFail:      1,
			wantCode:      "handler_error",
			wantRetryable: true,
		},
		{
			name: "panic",
			handler: func(context.Context, jobs.Job) (jobs.Result, error) {
				panic("boom")
			},
			wantFail: 1,
			wantCode: "handler_panic",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				store := newWorkerStore()
				worker := mustNewWorker(
					t,
					store,
					newWorkerBackend(),
					&workerEventPublisher{},
					newWorkerRegistry(t, "send-email", test.handler),
					validWorkerOptions(workerClock{now: time.Unix(100, 0)}),
				)

				claimed, err := worker.RunOne(context.Background())
				if err != nil {
					t.Fatalf("RunOne() error = %v", err)
				}
				if !claimed {
					t.Fatal("RunOne() claimed = false, want true")
				}
				if len(store.completeRequests) != test.wantComplete || len(store.failRequests) != test.wantFail {
					t.Fatalf("terminal calls: complete=%d fail=%d, want complete=%d fail=%d", len(store.completeRequests), len(store.failRequests), test.wantComplete, test.wantFail)
				}
				if test.wantFail != 0 {
					failure := store.failRequests[0].Failure
					if failure.Code != test.wantCode || failure.Retryable != test.wantRetryable {
						t.Fatalf("failure = %#v, want code=%q retryable=%t", failure, test.wantCode, test.wantRetryable)
					}
				}
			})
		})
	}
}

func TestWorkerRunOneHeartbeatsAndReleasesExecutionLease(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		backend := newWorkerBackend()
		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		registry := newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
			close(handlerStarted)
			<-releaseHandler
			return jobs.Result{}, nil
		})
		worker := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
		type runOneResult struct {
			claimed bool
			err     error
		}
		done := make(chan runOneResult, 1)
		go func() {
			claimed, err := worker.RunOne(context.Background())
			done <- runOneResult{claimed: claimed, err: err}
		}()

		receive(t, handlerStarted, "handler start")
		heartbeat := receive(t, store.heartbeatCalls, "heartbeat")
		close(releaseHandler)
		result := receive(t, done, "RunOne result")
		if result.err != nil || !result.claimed {
			t.Fatalf("RunOne() = (%t, %v), want (true, nil)", result.claimed, result.err)
		}
		if heartbeat.FencingToken != store.claimRequests[0].FencingToken {
			t.Fatalf("Heartbeat() fencing token = %d, want claim fence %d", heartbeat.FencingToken, store.claimRequests[0].FencingToken)
		}
		if len(backend.leases) != 2 || !backend.leases[1].released {
			t.Fatal("job execution lease was not released")
		}
	})
}

func TestWorkerRunOneReportsClaimWhenExecutionAborts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		abort func(context.CancelFunc, *workerBackend)
	}{
		{
			name: "context canceled",
			abort: func(cancel context.CancelFunc, _ *workerBackend) {
				cancel()
			},
		},
		{
			name: "job lease lost",
			abort: func(_ context.CancelFunc, backend *workerBackend) {
				backend.leases[1].lose()
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				store := newWorkerStore()
				backend := newWorkerBackend()
				handlerStarted := make(chan struct{})
				handlerCanceled := make(chan struct{})
				registry := newWorkerRegistry(t, "send-email", func(ctx context.Context, _ jobs.Job) (jobs.Result, error) {
					close(handlerStarted)
					<-ctx.Done()
					close(handlerCanceled)
					return jobs.Result{}, ctx.Err()
				})
				worker := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				type runOneResult struct {
					claimed bool
					err     error
				}
				done := make(chan runOneResult, 1)
				go func() {
					claimed, err := worker.RunOne(ctx)
					done <- runOneResult{claimed: claimed, err: err}
				}()

				receive(t, handlerStarted, "handler start")
				test.abort(cancel, backend)
				receive(t, handlerCanceled, "handler cancellation")
				result := receive(t, done, "RunOne result")
				if result.err != nil || !result.claimed {
					t.Fatalf("RunOne() = (%t, %v), want (true, nil)", result.claimed, result.err)
				}
				if len(store.completeRequests) != 0 || len(store.failRequests) != 0 {
					t.Fatalf("terminal transitions after execution abort: complete=%d fail=%d", len(store.completeRequests), len(store.failRequests))
				}
			})
		})
	}
}

func TestWorkerRunOneReportsClaimAfterPostClaimError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("post-claim failure")
	tests := []struct {
		name      string
		configure func(*workerStore, *workerBackend, *workerEventPublisher)
	}{
		{
			name: "queue lease release",
			configure: func(_ *workerStore, backend *workerBackend, _ *workerEventPublisher) {
				backend.releaseErrors = map[int]error{0: sentinel}
			},
		},
		{
			name: "job lease acquire",
			configure: func(_ *workerStore, backend *workerBackend, _ *workerEventPublisher) {
				backend.acquireErrors = map[int]error{1: sentinel}
			},
		},
		{
			name: "terminal transition",
			configure: func(store *workerStore, _ *workerBackend, _ *workerEventPublisher) {
				store.completeErr = sentinel
			},
		},
		{
			name: "terminal event",
			configure: func(_ *workerStore, _ *workerBackend, publisher *workerEventPublisher) {
				publisher.publishErr = sentinel
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				store := newWorkerStore()
				backend := newWorkerBackend()
				publisher := &workerEventPublisher{}
				test.configure(store, backend, publisher)
				worker := mustNewWorker(
					t,
					store,
					backend,
					publisher,
					newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
						return jobs.Result{}, nil
					}),
					validWorkerOptions(workerClock{now: time.Unix(100, 0)}),
				)

				claimed, err := worker.RunOne(context.Background())
				if !errors.Is(err, sentinel) {
					t.Fatalf("RunOne() error = %v, want sentinel", err)
				}
				if !claimed {
					t.Fatal("RunOne() claimed = false after durable claim, want true")
				}
			})
		})
	}
}

func TestWorkerSuccessfulHandlerCompletesWithClaimFenceAndReleasesQueueLease(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempt = workerAttempt("success", 7)
		backend := newWorkerBackend()
		publisher := &workerEventPublisher{}
		handlerCalled := make(chan jobs.Job, 1)
		registry := newWorkerRegistry(t, "send-email", func(ctx context.Context, job jobs.Job) (jobs.Result, error) {
			handlerCalled <- job
			return jobs.Result{Payload: []byte("ok")}, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.afterComplete = cancel

		worker := mustNewWorker(t, store, backend, publisher, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
		runDone := runWorker(ctx, worker)

		gotJob := receive(t, handlerCalled, "handler call")
		if gotJob.ID != "success" {
			t.Fatalf("handler job ID = %q, want success", gotJob.ID)
		}
		waitForRun(t, runDone)

		if len(store.completeRequests) != 1 {
			t.Fatalf("Complete() calls = %d, want 1", len(store.completeRequests))
		}
		if got := store.completeRequests[0].FencingToken; got != 7 {
			t.Fatalf("Complete() fencing token = %d, want claim fence 7", got)
		}
		if got := store.claimRequests[0].FencingToken; got != 7 {
			t.Fatalf("ClaimReady() fencing token = %d, want queue lease fence 7", got)
		}
		if len(store.failRequests) != 0 {
			t.Fatalf("Fail() calls = %d, want 0", len(store.failRequests))
		}
		if got := backend.acquireRequests[0]; got.Key != "jobs/queue/default/claim" || got.Wait {
			t.Fatalf("queue claim Acquire() = %#v, want non-waiting queue claim lease", got)
		}
		if !backend.leases[0].released {
			t.Fatal("queue claim lease was not released after ClaimReady")
		}
		if got := backend.acquireRequests[1]; got.Key != "jobs/job/success" || got.Wait {
			t.Fatalf("job Acquire() = %#v, want non-waiting job execution lease", got)
		}
		if kinds := publisher.kinds(); len(kinds) < 2 || kinds[0] != jobs.EventStarted || kinds[len(kinds)-1] != jobs.EventSucceeded {
			t.Fatalf("published event kinds = %v, want started first and succeeded last", kinds)
		}
	})
}

func TestWorkerLeaseDoneCancelsBlockedHandlerWithoutTerminalTransition(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempt = workerAttempt("lease-lost", 11)
		backend := newWorkerBackend()
		handlerStarted := make(chan struct{})
		handlerCanceled := make(chan struct{})
		registry := newWorkerRegistry(t, "send-email", func(ctx context.Context, job jobs.Job) (jobs.Result, error) {
			close(handlerStarted)
			<-ctx.Done()
			close(handlerCanceled)
			return jobs.Result{}, ctx.Err()
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		worker := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
		runDone := runWorker(ctx, worker)
		receive(t, handlerStarted, "handler start")

		backend.leases[1].lose()
		receive(t, handlerCanceled, "handler cancellation")
		cancel()
		waitForRun(t, runDone)

		if len(store.completeRequests) != 0 || len(store.failRequests) != 0 {
			t.Fatalf("terminal transitions after lease loss: complete=%d fail=%d", len(store.completeRequests), len(store.failRequests))
		}
	})
}

func TestWorkerCancellationPollingCancelsHandler(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempt = workerAttempt("cancel-requested", 13)
		backend := newWorkerBackend()
		handlerStarted := make(chan struct{})
		handlerCanceled := make(chan struct{})
		registry := newWorkerRegistry(t, "send-email", func(ctx context.Context, job jobs.Job) (jobs.Result, error) {
			close(handlerStarted)
			<-ctx.Done()
			close(handlerCanceled)
			return jobs.Result{}, ctx.Err()
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.afterFail = cancel
		store.afterGet = func() {
			store.getJob.CancellationRequested = true
		}

		worker := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
		runDone := runWorker(ctx, worker)
		receive(t, handlerStarted, "handler start")
		receive(t, store.heartbeatCalls, "heartbeat")
		receive(t, handlerCanceled, "handler cancellation")
		waitForRun(t, runDone)

		if len(store.completeRequests) != 0 || len(store.failRequests) != 1 {
			t.Fatalf("terminal transitions after cancellation polling: complete=%d fail=%d, want one fenced Fail", len(store.completeRequests), len(store.failRequests))
		}
		wantFence := backend.leases[0].Info().Token
		if got := store.failRequests[0].FencingToken; got != wantFence {
			t.Fatalf("Fail() fencing token = %d, want acquired claim fence %d", got, wantFence)
		}
	})
}

func TestWorkerHandlerErrorSchedulesRetryWithBoundedDelay(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempt = workerAttempt("retry", 15)
		backend := newWorkerBackend()
		registry := newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
			return jobs.Result{}, errors.New("temporary failure")
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.afterFail = cancel

		worker := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
		waitForRun(t, runWorker(ctx, worker))

		if len(store.failRequests) != 1 {
			t.Fatalf("Fail() calls = %d, want 1", len(store.failRequests))
		}
		request := store.failRequests[0]
		wantFence := backend.leases[0].Info().Token
		if request.FencingToken != wantFence || request.Failure.Code != "handler_error" || !request.Failure.Retryable {
			t.Fatalf("Fail() request = %#v, want retryable handler_error with acquired claim fence %d", request, wantFence)
		}
		wantNext := time.Unix(101, 0).UTC()
		if !request.NextAvailableAt.Equal(wantNext) {
			t.Fatalf("NextAvailableAt = %v, want %v", request.NextAvailableAt, wantNext)
		}
	})
}

func TestWorkerPanicRecordsNonRetryableHandlerPanicFailure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempt = workerAttempt("panic", 17)
		backend := newWorkerBackend()
		registry := newWorkerRegistry(t, "send-email", func(ctx context.Context, job jobs.Job) (jobs.Result, error) {
			panic("boom")
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.afterFail = cancel

		worker := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
		waitForRun(t, runWorker(ctx, worker))

		if len(store.failRequests) != 1 {
			t.Fatalf("Fail() calls = %d, want 1", len(store.failRequests))
		}
		failure := store.failRequests[0].Failure
		if failure.Code != "handler_panic" || failure.Retryable {
			t.Fatalf("panic failure = %#v, want nonretryable handler_panic", failure)
		}
		if len(store.completeRequests) != 0 {
			t.Fatalf("Complete() calls = %d, want 0", len(store.completeRequests))
		}
	})
}

func TestWorkerSuccessfulHandlerPublishesCanceledWhenCompleteReturnsCanceled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempt = workerAttempt("canceled-on-complete", 19)
		store.completeState = jobs.StateCanceled
		backend := newWorkerBackend()
		publisher := &workerEventPublisher{}
		registry := newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
			return jobs.Result{Payload: []byte("ignored")}, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.afterComplete = cancel

		worker := mustNewWorker(t, store, backend, publisher, registry, validWorkerOptions(workerClock{now: time.Unix(100, 0)}))
		waitForRun(t, runWorker(ctx, worker))

		kinds := publisher.kinds()
		if len(kinds) < 2 || kinds[len(kinds)-1] != jobs.EventCanceled {
			t.Fatalf("published event kinds = %v, want canceled last", kinds)
		}
	})
}

func TestWorkerConcurrentRunsDoNotDoubleExecute(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimAttempt = workerAttempt("once", 1)
		backend := coordinationmemory.NewBackend(nil)
		started := make(chan struct{})
		release := make(chan struct{})
		var executions atomic.Int32
		registry := newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
			if executions.Add(1) == 1 {
				close(started)
			}
			<-release
			return jobs.Result{}, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.afterComplete = cancel

		firstOptions := validWorkerOptions(workerClock{now: time.Unix(100, 0)})
		secondOptions := firstOptions
		secondOptions.WorkerID = "worker-2"
		first := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, firstOptions)
		second := mustNewWorker(t, store, backend, &workerEventPublisher{}, registry, secondOptions)
		firstDone := runWorker(ctx, first)
		secondDone := runWorker(ctx, second)
		receive(t, started, "first handler start")
		close(release)
		waitForRun(t, firstDone)
		waitForRun(t, secondDone)

		if got := executions.Load(); got != 1 {
			t.Fatalf("handler executions = %d, want 1", got)
		}
		if len(store.completeRequests) != 1 {
			t.Fatalf("Complete() calls = %d, want 1", len(store.completeRequests))
		}
	})
}

func validWorkerOptions(clock jobs.Clock) jobs.WorkerOptions {
	return jobs.WorkerOptions{
		WorkerID:          "worker-1",
		Queues:            []jobs.Queue{"default"},
		PollInterval:      time.Millisecond,
		HeartbeatInterval: time.Millisecond,
		LeaseTTL:          3 * time.Second,
		ClaimLimit:        1,
		Clock:             clock,
	}
}

func withWorkerOption(edit func(*jobs.WorkerOptions)) jobs.WorkerOptions {
	options := validWorkerOptions(workerClock{now: time.Unix(100, 0)})
	edit(&options)
	return options
}

func mustNewWorker(t *testing.T, store jobs.Store, backend coordination.Backend, publisher messaging.Publisher[jobs.Event], registry jobs.Registry, options jobs.WorkerOptions) *jobs.Worker {
	t.Helper()

	worker, err := jobs.NewWorker(store, backend, publisher, registry, options)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func newWorkerRegistry(t *testing.T, kind string, handler jobs.Handler) jobs.Registry {
	t.Helper()
	return jobs.MapRegistry{kind: handler}
}

func runWorker(ctx context.Context, worker *jobs.Worker) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	return done
}

func waitForRun(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(workerTestTimeout):
		t.Fatal("timed out waiting for Run()")
	}
}

func receive[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()

	select {
	case value := <-ch:
		return value
	case <-time.After(workerTestTimeout):
		var zero T
		t.Fatalf("timed out waiting for %s", label)
		return zero
	}
}

func workerAttempt(id jobs.JobID, token coordination.FencingToken) jobs.Attempt {
	now := time.Unix(100, 0)
	return jobs.Attempt{
		ID:           jobs.AttemptID("attempt-" + id),
		WorkerID:     "worker-1",
		FencingToken: token,
		StartedAt:    now,
		Job: jobs.Job{
			ID:          id,
			Queue:       "default",
			Kind:        "send-email",
			State:       jobs.StateRunning,
			Attempt:     1,
			CreatedAt:   now,
			AvailableAt: now,
			UpdatedAt:   now,
			Retry: jobs.RetryPolicy{
				MaxAttempts:    3,
				InitialBackoff: time.Second,
				MaxBackoff:     10 * time.Second,
				Multiplier:     2,
			},
		},
		HeartbeatDeadline: now.Add(3 * time.Second),
	}
}

func workerAttemptInQueue(id jobs.JobID, queue jobs.Queue, token coordination.FencingToken) jobs.Attempt {
	attempt := workerAttempt(id, token)
	attempt.Job.Queue = queue
	return attempt
}

type workerStore struct {
	mu                sync.Mutex
	claimAttempt      jobs.Attempt
	claimAttempts     map[jobs.Queue][]jobs.Attempt
	claimed           bool
	getJob            jobs.Job
	completeState     jobs.State
	completeErr       error
	claimRequests     []jobs.ClaimRequest
	heartbeatRequests []jobs.HeartbeatRequest
	completeRequests  []jobs.CompleteRequest
	failRequests      []jobs.FailRequest
	heartbeatCalls    chan jobs.HeartbeatRequest
	afterGet          func()
	afterComplete     func()
	afterFail         func()
}

func newWorkerStore() *workerStore {
	store := &workerStore{heartbeatCalls: make(chan jobs.HeartbeatRequest, 8)}
	store.claimAttempt = workerAttempt("job-1", 1)
	store.getJob = store.claimAttempt.Job
	return store
}

func (store *workerStore) Create(context.Context, jobs.Job) (jobs.Job, error) {
	panic("Create must not be called by Worker")
}

func (store *workerStore) Get(context.Context, jobs.JobID) (jobs.Job, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.afterGet != nil {
		store.afterGet()
		store.afterGet = nil
	}
	return store.getJob.Clone(), nil
}

func (store *workerStore) ListJobs(context.Context, jobs.ListJobsRequest) ([]jobs.Job, error) {
	panic("ListJobs must not be called by Worker")
}

func (store *workerStore) ClaimReady(_ context.Context, request jobs.ClaimRequest) (jobs.Attempt, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.claimRequests = append(store.claimRequests, request)
	if store.claimAttempts != nil {
		attempts := store.claimAttempts[request.Queue]
		if len(attempts) == 0 {
			return jobs.Attempt{}, jobs.ErrNoReadyJob
		}
		attempt := attempts[0]
		store.claimAttempts[request.Queue] = attempts[1:]
		attempt.FencingToken = request.FencingToken
		store.getJob = attempt.Job
		return attempt, nil
	}
	if store.claimed {
		return jobs.Attempt{}, jobs.ErrNoReadyJob
	}
	store.claimed = true
	store.claimAttempt.FencingToken = request.FencingToken
	store.getJob = store.claimAttempt.Job
	return store.claimAttempt, nil
}

func (store *workerStore) Heartbeat(_ context.Context, request jobs.HeartbeatRequest) error {
	store.mu.Lock()
	store.heartbeatRequests = append(store.heartbeatRequests, request)
	store.mu.Unlock()
	store.heartbeatCalls <- request
	return nil
}

func (store *workerStore) Complete(_ context.Context, request jobs.CompleteRequest) (jobs.Job, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.completeRequests = append(store.completeRequests, request)
	if store.completeErr != nil {
		return jobs.Job{}, store.completeErr
	}
	job := store.getJob.Clone()
	job.State = store.completeState
	if job.State == 0 {
		job.State = jobs.StateSucceeded
	}
	if store.afterComplete != nil {
		store.afterComplete()
	}
	return job, nil
}

func (store *workerStore) Fail(_ context.Context, request jobs.FailRequest) (jobs.Job, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.failRequests = append(store.failRequests, request)
	job := store.getJob.Clone()
	job.State = jobs.StateRetryWaiting
	job.LastFailure = &request.Failure
	if store.afterFail != nil {
		store.afterFail()
	}
	return job, nil
}

func (store *workerStore) RequestCancel(context.Context, jobs.JobID, time.Time) (jobs.Job, error) {
	panic("RequestCancel must not be called by Worker")
}

func (store *workerStore) RecoverAttempt(context.Context, jobs.RecoverAttemptRequest) (jobs.Job, error) {
	panic("RecoverAttempt must not be called by Worker")
}

func (store *workerStore) ListRecoveryCandidates(context.Context, time.Time, int) ([]jobs.RecoveryCandidate, error) {
	panic("ListRecoveryCandidates must not be called by Worker")
}

type workerBackend struct {
	mu              sync.Mutex
	nextToken       coordination.FencingToken
	acquireRequests []coordination.AcquireRequest
	leases          []*workerLease
	acquireErrors   map[int]error
	releaseErrors   map[int]error
}

func newWorkerBackend() *workerBackend {
	return &workerBackend{nextToken: 7}
}

func (backend *workerBackend) Acquire(_ context.Context, request coordination.AcquireRequest) (coordination.Lease, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	requestIndex := len(backend.acquireRequests)
	backend.acquireRequests = append(backend.acquireRequests, request)
	if err := backend.acquireErrors[requestIndex]; err != nil {
		return nil, err
	}
	lease := newWorkerLease(coordination.LeaseInfo{
		Key:        request.Key,
		Owner:      request.Owner,
		Token:      backend.nextToken,
		AcquiredAt: time.Unix(100, 0),
		ExpiresAt:  time.Unix(100, 0).Add(request.TTL),
	})
	lease.releaseErr = backend.releaseErrors[requestIndex]
	backend.nextToken++
	backend.leases = append(backend.leases, lease)
	return lease, nil
}

type workerLease struct {
	mu         sync.Mutex
	info       coordination.LeaseInfo
	done       chan struct{}
	released   bool
	releaseErr error
	once       sync.Once
}

func newWorkerLease(info coordination.LeaseInfo) *workerLease {
	return &workerLease{info: info, done: make(chan struct{})}
}

func (lease *workerLease) Info() coordination.LeaseInfo {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.info
}

func (lease *workerLease) Done() <-chan struct{} {
	return lease.done
}

func (lease *workerLease) Renew(context.Context, time.Duration) (coordination.LeaseInfo, error) {
	return lease.Info(), nil
}

func (lease *workerLease) Release(context.Context) error {
	lease.mu.Lock()
	lease.released = true
	err := lease.releaseErr
	lease.mu.Unlock()
	return err
}

func (lease *workerLease) lose() {
	lease.once.Do(func() {
		close(lease.done)
	})
}

type workerEventPublisher struct {
	mu         sync.Mutex
	envelopes  []messaging.Envelope[jobs.Event]
	publishErr error
}

func (publisher *workerEventPublisher) kinds() []jobs.EventKind {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	kinds := make([]jobs.EventKind, len(publisher.envelopes))
	for index, envelope := range publisher.envelopes {
		kinds[index] = envelope.Payload.Kind
	}
	return kinds
}

func (publisher *workerEventPublisher) Publish(_ context.Context, envelope messaging.Envelope[jobs.Event]) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	publisher.envelopes = append(publisher.envelopes, envelope.Clone())
	return publisher.publishErr
}

type workerClock struct {
	now time.Time
}

func (clock workerClock) Now() time.Time {
	return clock.now
}
