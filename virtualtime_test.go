package jobs_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	coordinationmemory "github.com/justusschock/distributed-coordination/memory"
	jobs "github.com/justusschock/durable-jobs"
	"github.com/justusschock/durable-jobs/backend/memory"
	messaging "github.com/justusschock/messaging"
	reconciliation "github.com/justusschock/reconciliation"
)

// The tests in this file run the real memory store, the real in-memory
// coordination backend, and the library's timers inside a testing/synctest
// bubble. bubbleClock reads the bubble's virtual time, so durations below are
// exact and the tests never sleep in wall-clock time.

// bubbleClock implements jobs.Clock with time.Now, which reports virtual time
// inside a synctest bubble.
type bubbleClock struct{}

func (bubbleClock) Now() time.Time { return time.Now() }

// FR-JOBS-05: Run polls each configured queue once per PollInterval.
func TestWorkerRunPollsOncePerPollIntervalInVirtualTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := newWorkerStore()
		store.claimed = true // every claim reports ErrNoReadyJob
		options := validWorkerOptions(bubbleClock{})
		options.PollInterval = 100 * time.Millisecond
		worker := mustNewWorker(t, store, newWorkerBackend(), &workerEventPublisher{}, newWorkerRegistry(t, "send-email", func(context.Context, jobs.Job) (jobs.Result, error) {
			t.Fatal("handler called without a ready job")
			return jobs.Result{}, nil
		}), options)

		ctx, cancel := context.WithCancel(context.Background())
		done := runWorker(ctx, worker)
		time.Sleep(950 * time.Millisecond)
		cancel()
		waitForRun(t, done)

		store.mu.Lock()
		defer store.mu.Unlock()
		// Polls at 0, 100, ..., 900 ms.
		if got := len(store.claimRequests); got != 10 {
			t.Fatalf("ClaimReady() calls in 950ms = %d, want 10", got)
		}
		for index := 1; index < len(store.claimRequests); index++ {
			gap := store.claimRequests[index].Now.Sub(store.claimRequests[index-1].Now)
			if gap != options.PollInterval {
				t.Fatalf("poll %d gap = %v, want %v", index, gap, options.PollInterval)
			}
		}
	})
}

// FR-JOBS-06, FR-JOBS-12: a retryable handler error is retried after the
// bounded exponential backoff of the job's RetryPolicy, until an attempt
// succeeds, and each retry publishes a retry_scheduled event.
func TestWorkerRunRetriesWithExponentialBackoffInVirtualTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := memory.New()
		events := &workerEventPublisher{}
		client, err := jobs.NewClient(store, discardPublisher[jobs.Wakeup]{}, bubbleClock{})
		if err != nil {
			t.Fatal(err)
		}
		request := validEnqueueRequest()
		request.ID = "flaky"
		request.Queue = "default"
		request.Kind = "flaky"
		request.AvailableAt = time.Time{}
		request.DependsOn = nil
		request.Retry = jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2}
		if _, err := client.Enqueue(context.Background(), request); err != nil {
			t.Fatal(err)
		}

		var (
			mu       sync.Mutex
			attempts []time.Time
		)
		registry := jobs.MapRegistry{"flaky": func(context.Context, jobs.Job) (jobs.Result, error) {
			mu.Lock()
			defer mu.Unlock()
			attempts = append(attempts, time.Now())
			if len(attempts) < 3 {
				return jobs.Result{}, errors.New("temporary failure")
			}
			return jobs.Result{Payload: []byte("done")}, nil
		}}
		options := validWorkerOptions(bubbleClock{})
		options.PollInterval = 100 * time.Millisecond
		options.HeartbeatInterval = time.Second
		worker := mustNewWorker(t, store, coordinationmemory.NewBackend(nil), events, registry, options)

		ctx, cancel := context.WithCancel(context.Background())
		done := runWorker(ctx, worker)
		time.Sleep(10 * time.Second)
		cancel()
		waitForRun(t, done)

		mu.Lock()
		defer mu.Unlock()
		if len(attempts) != 3 {
			t.Fatalf("handler attempts = %d, want 3", len(attempts))
		}
		for index, wantBackoff := range []time.Duration{time.Second, 2 * time.Second} {
			gap := attempts[index+1].Sub(attempts[index])
			if gap < wantBackoff || gap >= wantBackoff+options.PollInterval {
				t.Fatalf("retry %d started %v after the failure, want backoff %v within one poll interval", index+1, gap, wantBackoff)
			}
		}
		job, err := store.Get(context.Background(), "flaky")
		if err != nil {
			t.Fatal(err)
		}
		if job.State != jobs.StateSucceeded || job.Attempt != 3 || string(job.Result) != "done" {
			t.Fatalf("job = state %v attempt %d result %q, want succeeded on attempt 3", job.State, job.Attempt, job.Result)
		}
		kinds := events.kinds()
		retries := 0
		for _, kind := range kinds {
			if kind == jobs.EventRetryScheduled {
				retries++
			}
		}
		if retries != 2 || kinds[len(kinds)-1] != jobs.EventSucceeded {
			t.Fatalf("event kinds = %v, want two retry_scheduled events and succeeded last", kinds)
		}
	})
}

// FR-JOBS-04, FR-JOBS-05: while a handler runs, the worker heartbeats every
// HeartbeatInterval and pushes the attempt's deadline to now+LeaseTTL, so a
// live attempt never becomes a recovery candidate.
func TestWorkerHeartbeatsKeepLongAttemptAliveInVirtualTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := memory.New()
		client, err := jobs.NewClient(store, discardPublisher[jobs.Wakeup]{}, bubbleClock{})
		if err != nil {
			t.Fatal(err)
		}
		request := validEnqueueRequest()
		request.ID = "long"
		request.Queue = "default"
		request.Kind = "long"
		request.AvailableAt = time.Time{}
		request.DependsOn = nil
		if _, err := client.Enqueue(context.Background(), request); err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		registry := jobs.MapRegistry{"long": func(ctx context.Context, _ jobs.Job) (jobs.Result, error) {
			select {
			case <-time.After(10 * time.Second):
				return jobs.Result{}, nil
			case <-ctx.Done():
				return jobs.Result{}, ctx.Err()
			}
		}}
		options := validWorkerOptions(bubbleClock{})
		options.HeartbeatInterval = time.Second
		options.LeaseTTL = 3 * time.Second
		options.PollInterval = time.Hour
		events := &workerEventPublisher{}
		worker := mustNewWorker(t, store, coordinationmemory.NewBackend(nil), events, registry, options)

		done := make(chan error, 1)
		go func() {
			_, err := worker.RunOne(context.Background())
			done <- err
		}()

		// Well past the first lease TTL: the attempt is still live.
		time.Sleep(5*time.Second + 500*time.Millisecond)
		synctest.Wait()
		expired, err := store.ListRecoveryCandidates(context.Background(), time.Now(), 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(expired) != 0 {
			t.Fatalf("recovery candidates during heartbeating = %#v, want none", expired)
		}
		current, err := store.ListRecoveryCandidates(context.Background(), time.Now().Add(time.Hour), 10)
		if err != nil {
			t.Fatal(err)
		}
		wantDeadline := start.Add(5 * time.Second).Add(options.LeaseTTL).UTC()
		if len(current) != 1 || !current[0].Attempt.HeartbeatDeadline.Equal(wantDeadline) {
			t.Fatalf("attempt deadline = %#v, want %v after the heartbeat at 5s", current, wantDeadline)
		}

		if err := <-done; err != nil {
			t.Fatalf("RunOne() error = %v", err)
		}
		heartbeats := 0
		for _, kind := range events.kinds() {
			if kind == jobs.EventHeartbeat {
				heartbeats++
			}
		}
		if heartbeats < 9 {
			t.Fatalf("heartbeat events = %d, want at least 9 during a 10s attempt", heartbeats)
		}
		job, err := store.Get(context.Background(), "long")
		if err != nil {
			t.Fatal(err)
		}
		if job.State != jobs.StateSucceeded {
			t.Fatalf("job state = %v, want succeeded", job.State)
		}
	})
}

// FR-JOBS-08: an attempt whose heartbeat deadline passes is recovered by the
// recovery engine; before expiry it is left alone, and the stale attempt can
// no longer heartbeat once the job has been claimed again.
func TestRecoveryEngineRecoversAttemptAfterLeaseExpiryInVirtualTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := memory.New()
		client, err := jobs.NewClient(store, discardPublisher[jobs.Wakeup]{}, bubbleClock{})
		if err != nil {
			t.Fatal(err)
		}
		request := validEnqueueRequest()
		request.ID = "orphaned"
		request.AvailableAt = time.Time{}
		request.DependsOn = nil
		created, err := client.Enqueue(ctx, request)
		if err != nil {
			t.Fatal(err)
		}

		// A worker claims the job and then disappears without heartbeating.
		const leaseTTL = 3 * time.Second
		now := time.Now()
		stale, err := store.ClaimReady(ctx, jobs.ClaimRequest{
			Queue: created.Queue, WorkerID: "crashed", Now: now, HeartbeatDeadline: now.Add(leaseTTL), FencingToken: 1,
		})
		if err != nil {
			t.Fatal(err)
		}

		engine, err := jobs.NewRecoveryEngine(store, bubbleClock{}, reconciliation.Policy{MaxAttempts: 1})
		if err != nil {
			t.Fatal(err)
		}
		recoverOnce := func() jobs.Job {
			t.Helper()
			if _, err := engine.Reconcile(ctx, reconciliation.Desired[jobs.RecoverySpec]{Key: "recovery", Generation: 1, Spec: jobs.RecoverySpec{Limit: 10}}); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			job, err := store.Get(ctx, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			return job
		}

		time.Sleep(leaseTTL - time.Millisecond)
		if job := recoverOnce(); job.State != jobs.StateRunning {
			t.Fatalf("state before lease expiry = %v, want running", job.State)
		}

		time.Sleep(time.Millisecond)
		recovered := recoverOnce()
		if recovered.State != jobs.StatePending || !recovered.AvailableAt.Equal(time.Now().UTC()) {
			t.Fatalf("recovered job = state %v available %v, want pending and available now", recovered.State, recovered.AvailableAt)
		}

		if _, err := store.ClaimReady(ctx, jobs.ClaimRequest{
			Queue: created.Queue, WorkerID: "replacement", Now: time.Now(), HeartbeatDeadline: time.Now().Add(leaseTTL), FencingToken: 2,
		}); err != nil {
			t.Fatal(err)
		}
		err = store.Heartbeat(ctx, jobs.HeartbeatRequest{AttemptID: stale.ID, FencingToken: stale.FencingToken, Deadline: time.Now().Add(leaseTTL)})
		if !errors.Is(err, jobs.ErrStaleAttempt) {
			t.Fatalf("stale Heartbeat() error = %v, want ErrStaleAttempt", err)
		}
	})
}

// FR-JOBS-09: a scheduler loop driven by a ticker enqueues exactly one
// occurrence per elapsed interval, identified by its due time.
func TestSchedulerLoopEnqueuesOneOccurrencePerIntervalInVirtualTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := memory.New()
		client, err := jobs.NewClient(store, discardPublisher[jobs.Wakeup]{}, bubbleClock{})
		if err != nil {
			t.Fatal(err)
		}
		template := validEnqueueRequest()
		template.ID = "template"
		template.AvailableAt = time.Time{}
		template.DependsOn = nil
		first := time.Now().Add(time.Hour).UTC()
		if _, err := store.PutSchedule(ctx, jobs.Schedule{ID: "hourly", Request: template, NextRunAt: first, Interval: time.Hour, Enabled: true}, 0); err != nil {
			t.Fatal(err)
		}
		scheduler := jobs.NewScheduler(store, client, bubbleClock{})

		loopDone := make(chan error, 1)
		go func() {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					loopDone <- nil
					return
				case <-ticker.C:
					if _, err := scheduler.RunDue(ctx, 10); err != nil {
						loopDone <- err
						return
					}
				}
			}
		}()

		// Stop between two ticks so no RunDue call observes the cancellation.
		time.Sleep(5*time.Hour + 30*time.Minute + 30*time.Second)
		cancel()
		if err := <-loopDone; err != nil {
			t.Fatalf("RunDue() error = %v", err)
		}

		for hour := range 5 {
			dueAt := first.Add(time.Duration(hour) * time.Hour)
			id := jobs.JobID("hourly/" + strconv.FormatInt(dueAt.UnixNano(), 10))
			job, err := store.Get(context.Background(), id)
			if err != nil {
				t.Fatalf("occurrence %d (%s): %v", hour, id, err)
			}
			if !job.AvailableAt.Equal(dueAt) {
				t.Fatalf("occurrence %d AvailableAt = %v, want %v", hour, job.AvailableAt, dueAt)
			}
		}
		page, err := store.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: template.Queue, Limit: jobs.MaxJobPageSize})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Jobs) != 5 {
			t.Fatalf("occurrences after 5.5 hours = %d, want 5", len(page.Jobs))
		}
		schedule, err := store.GetSchedule(context.Background(), "hourly")
		if err != nil {
			t.Fatal(err)
		}
		if want := first.Add(5 * time.Hour); !schedule.NextRunAt.Equal(want) {
			t.Fatalf("NextRunAt = %v, want %v", schedule.NextRunAt, want)
		}
	})
}

type discardPublisher[T any] struct{}

func (discardPublisher[T]) Publish(context.Context, messaging.Envelope[T]) error { return nil }
