// Package storetest exercises the observable jobs.Store contract.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
)

// Run executes the store contract against a fresh store for each subtest.
func Run(t *testing.T, factory func(t *testing.T) jobs.Store) {
	t.Helper()

	t.Run("create is idempotently rejected", func(t *testing.T) {
		store := factory(t)
		job := newJob("create", "queue", now(0))
		job.DependsOn = []jobs.JobID{"dependency"}
		created, err := store.Create(context.Background(), job)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		job.Payload[0] = 'x'
		job.Headers["source"] = "mutated"
		job.DependsOn[0] = "mutated"
		if string(created.Payload) != "payload" || created.Headers["source"] != "contract" || created.DependsOn[0] != "dependency" {
			t.Fatalf("Create() retained caller-owned data: %#v", created)
		}
		if created.CreatedAt.Location() != time.UTC || created.AvailableAt.Location() != time.UTC || created.UpdatedAt.Location() != time.UTC {
			t.Fatalf("Create() returned non-UTC persisted times: %#v", created)
		}
		created.Payload[0] = 'y'
		created.Headers["source"] = "returned"
		created.DependsOn[0] = "returned"
		if _, err := store.Create(context.Background(), newJob("create", "queue", now(0))); !errors.Is(err, jobs.ErrJobExists) {
			t.Fatalf("second Create() error = %v, want ErrJobExists", err)
		}
		got := get(t, store, "create")
		if string(got.Payload) != "payload" || got.Headers["source"] != "contract" || got.DependsOn[0] != "dependency" {
			t.Fatalf("stored job changed after caller/return mutation: %#v", got)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := store.Get(ctx, "create"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Get() with canceled context error = %v, want context.Canceled", err)
		}
	})

	t.Run("claim follows deterministic order", func(t *testing.T) {
		store := factory(t)
		for _, job := range []jobs.Job{
			newJob("available-later", "queue", now(2)),
			withPriority(newJob("priority-low", "queue", now(0)), 1),
			withPriority(newJob("id-second", "queue", now(0)), 5),
			withPriority(newJob("id-first", "queue", now(0)), 5),
			withPriority(newJob("priority-high", "queue", now(1)), 10),
		} {
			if _, err := store.Create(context.Background(), job); err != nil {
				t.Fatalf("Create(%q) error = %v", job.ID, err)
			}
		}
		for index, want := range []jobs.JobID{"priority-high", "id-first", "id-second", "priority-low", "available-later"} {
			attempt, err := store.ClaimReady(context.Background(), claim("queue", now(3), coordination.FencingToken(index+1)))
			if err != nil {
				t.Fatalf("ClaimReady(%d) error = %v", index, err)
			}
			if attempt.Job.ID != want {
				t.Fatalf("ClaimReady(%d) job = %q, want %q", index, attempt.Job.ID, want)
			}
			attempt.Job.Payload[0] = 'x'
			if got := get(t, store, want); string(got.Payload) != "payload" {
				t.Fatalf("ClaimReady() returned aliased job payload: %#v", got)
			}
		}
	})

	t.Run("heartbeat requires current fence", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("heartbeat", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 7)
		before := get(t, store, "heartbeat")
		err := store.Heartbeat(context.Background(), jobs.HeartbeatRequest{
			AttemptID: attempt.ID, FencingToken: 6, Deadline: now(4),
		})
		if !errors.Is(err, jobs.ErrStaleAttempt) {
			t.Fatalf("Heartbeat() error = %v, want ErrStaleAttempt", err)
		}
		if got := get(t, store, "heartbeat"); !sameJob(got, before) {
			t.Fatalf("Heartbeat() changed job with stale fence: got %#v, want %#v", got, before)
		}
		if err := store.Heartbeat(context.Background(), jobs.HeartbeatRequest{
			AttemptID: attempt.ID, FencingToken: 7, Deadline: now(4),
		}); err != nil {
			t.Fatalf("Heartbeat() with current fence error = %v", err)
		}
		if got := get(t, store, "heartbeat"); !got.UpdatedAt.Equal(now(4)) {
			t.Fatalf("Heartbeat() UpdatedAt = %v, want %v", got.UpdatedAt, now(4))
		}
	})

	t.Run("heartbeat deadline never moves backward", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("heartbeat-order", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 7)
		if err := store.Heartbeat(context.Background(), jobs.HeartbeatRequest{
			AttemptID: attempt.ID, FencingToken: 7, Deadline: now(4),
		}); err != nil {
			t.Fatalf("Heartbeat() with later deadline error = %v", err)
		}
		before := get(t, store, "heartbeat-order")
		if err := store.Heartbeat(context.Background(), jobs.HeartbeatRequest{
			AttemptID: attempt.ID, FencingToken: 7, Deadline: now(4),
		}); err != nil {
			t.Fatalf("Heartbeat() with same deadline error = %v", err)
		}
		if got := get(t, store, "heartbeat-order"); !sameJob(got, before) {
			t.Fatalf("Heartbeat() with same deadline changed job: got %#v, want %#v", got, before)
		}
		if err := store.Heartbeat(context.Background(), jobs.HeartbeatRequest{
			AttemptID: attempt.ID, FencingToken: 7, Deadline: now(3),
		}); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("Heartbeat() with earlier deadline error = %v, want ErrInvalidTransition", err)
		}
		if got := get(t, store, "heartbeat-order"); !sameJob(got, before) {
			t.Fatalf("Heartbeat() with earlier deadline changed job: got %#v, want %#v", got, before)
		}
		candidates, err := store.ListRecoveryCandidates(context.Background(), now(5), 1)
		if err != nil {
			t.Fatalf("ListRecoveryCandidates() error = %v", err)
		}
		if len(candidates) != 1 || !candidates[0].Attempt.HeartbeatDeadline.Equal(now(4)) {
			t.Fatalf("ListRecoveryCandidates() = %#v, want deadline %v", candidates, now(4))
		}
	})

	t.Run("complete is terminal", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("complete", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 1)
		result := jobs.Result{Payload: []byte("result")}
		completed, err := store.Complete(context.Background(), jobs.CompleteRequest{
			AttemptID: attempt.ID, FencingToken: 1, Result: result, CompletedAt: now(2),
		})
		if err != nil {
			t.Fatalf("Complete() error = %v", err)
		}
		if completed.State != jobs.StateSucceeded || string(completed.Result) != "result" {
			t.Fatalf("Complete() = %#v, want succeeded job with result", completed)
		}
		result.Payload[0] = 'x'
		completed.Result[0] = 'y'
		if got := get(t, store, "complete"); got.State != jobs.StateSucceeded || string(got.Result) != "result" {
			t.Fatalf("Complete() retained mutable result or wrong state: %#v", got)
		}
		if _, err := store.Complete(context.Background(), jobs.CompleteRequest{
			AttemptID: attempt.ID, FencingToken: 1, Result: jobs.Result{}, CompletedAt: now(3),
		}); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("second Complete() error = %v, want ErrInvalidTransition", err)
		}
	})

	t.Run("retry returns to pending", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("retry", "queue", now(0)))
		first := claimOne(t, store, "queue", now(1), 1)
		retried, err := store.Fail(context.Background(), jobs.FailRequest{
			AttemptID: first.ID, FencingToken: 1,
			Failure:  jobs.Failure{Code: "temporary", Message: "try again", Retryable: true},
			FailedAt: now(2), NextAvailableAt: now(3),
		})
		if err != nil {
			t.Fatalf("Fail() error = %v", err)
		}
		if retried.State != jobs.StateRetryWaiting || !retried.AvailableAt.Equal(now(3)) || retried.LastFailure == nil || retried.LastFailure.Code != "temporary" {
			t.Fatalf("Fail() = %#v, want retry waiting job", retried)
		}
		if _, err := store.ClaimReady(context.Background(), claim("queue", now(2), 2)); !errors.Is(err, jobs.ErrNoReadyJob) {
			t.Fatalf("ClaimReady() before retry time error = %v, want ErrNoReadyJob", err)
		}
		second := claimOne(t, store, "queue", now(3), 2)
		if second.Job.State != jobs.StateRunning || second.Job.Attempt != 2 {
			t.Fatalf("retry ClaimReady() = %#v, want second running attempt", second)
		}
	})

	t.Run("cancel pending", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("cancel-pending", "queue", now(0)))
		canceled, err := store.RequestCancel(context.Background(), "cancel-pending", now(1))
		if err != nil {
			t.Fatalf("RequestCancel() error = %v", err)
		}
		if canceled.State != jobs.StateCanceled || !canceled.CancellationRequested {
			t.Fatalf("RequestCancel() = %#v, want canceled job", canceled)
		}
		if _, err := store.ClaimReady(context.Background(), claim("queue", now(2), 1)); !errors.Is(err, jobs.ErrNoReadyJob) {
			t.Fatalf("ClaimReady() error = %v, want ErrNoReadyJob", err)
		}
	})

	t.Run("request cancel running", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("cancel-running", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 1)
		requested, err := store.RequestCancel(context.Background(), "cancel-running", now(2))
		if err != nil {
			t.Fatalf("RequestCancel() error = %v", err)
		}
		if requested.State != jobs.StateRunning || !requested.CancellationRequested {
			t.Fatalf("RequestCancel() = %#v, want running job with cancellation request", requested)
		}
		completed, err := store.Complete(context.Background(), jobs.CompleteRequest{
			AttemptID: attempt.ID, FencingToken: 1, CompletedAt: now(3),
		})
		if err != nil {
			t.Fatalf("Complete() after cancellation request error = %v", err)
		}
		if completed.State != jobs.StateCanceled {
			t.Fatalf("Complete() after cancellation request state = %v, want canceled", completed.State)
		}
	})

	t.Run("expired attempt recovery", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("expired", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 1)
		candidates, err := store.ListRecoveryCandidates(context.Background(), now(3), 1)
		if err != nil {
			t.Fatalf("ListRecoveryCandidates() error = %v", err)
		}
		if len(candidates) != 1 || candidates[0].Attempt.ID != attempt.ID || !candidates[0].ObservedAt.Equal(now(3)) {
			t.Fatalf("ListRecoveryCandidates() = %#v, want expired attempt", candidates)
		}
		candidates[0].Attempt.Job.Payload[0] = 'x'
		if got := get(t, store, "expired"); string(got.Payload) != "payload" {
			t.Fatalf("ListRecoveryCandidates() returned aliased job payload: %#v", got)
		}
		recovered, err := store.RecoverAttempt(context.Background(), jobs.RecoverAttemptRequest{
			AttemptID: attempt.ID, FencingToken: 1,
			HeartbeatDeadline: candidates[0].Attempt.HeartbeatDeadline,
			RecoveredAt:       now(3),
		})
		if err != nil {
			t.Fatalf("RecoverAttempt() error = %v", err)
		}
		if recovered.State != jobs.StatePending || !recovered.AvailableAt.Equal(now(3)) {
			t.Fatalf("RecoverAttempt() = %#v, want pending job", recovered)
		}
		if _, err := store.RecoverAttempt(context.Background(), jobs.RecoverAttemptRequest{
			AttemptID: attempt.ID, FencingToken: 1,
			HeartbeatDeadline: candidates[0].Attempt.HeartbeatDeadline,
			RecoveredAt:       now(4),
		}); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("second RecoverAttempt() error = %v, want ErrInvalidTransition", err)
		}
	})

	t.Run("recovery before expiry", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("recover-early", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 7)
		before := get(t, store, "recover-early")
		if _, err := store.RecoverAttempt(context.Background(), jobs.RecoverAttemptRequest{
			AttemptID: attempt.ID, FencingToken: 7,
			HeartbeatDeadline: attempt.HeartbeatDeadline,
			RecoveredAt:       now(1),
		}); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("RecoverAttempt() before expiry error = %v, want ErrInvalidTransition", err)
		}
		if got := get(t, store, "recover-early"); !sameJob(got, before) {
			t.Fatalf("RecoverAttempt() before expiry changed job: got %#v, want %#v", got, before)
		}
	})

	t.Run("recovery requires current fence", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("recover-fence", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 7)
		before := get(t, store, "recover-fence")
		if _, err := store.RecoverAttempt(context.Background(), jobs.RecoverAttemptRequest{
			AttemptID: attempt.ID, FencingToken: 6,
			HeartbeatDeadline: attempt.HeartbeatDeadline,
			RecoveredAt:       now(3),
		}); !errors.Is(err, jobs.ErrStaleAttempt) {
			t.Fatalf("RecoverAttempt() with stale fence error = %v, want ErrStaleAttempt", err)
		}
		if got := get(t, store, "recover-fence"); !sameJob(got, before) {
			t.Fatalf("RecoverAttempt() with stale fence changed job: got %#v, want %#v", got, before)
		}
	})

	t.Run("heartbeat invalidates listed recovery", func(t *testing.T) {
		store := factory(t)
		create(t, store, newJob("recover-heartbeat", "queue", now(0)))
		attempt := claimOne(t, store, "queue", now(1), 7)
		candidates, err := store.ListRecoveryCandidates(context.Background(), now(3), 1)
		if err != nil {
			t.Fatalf("ListRecoveryCandidates() error = %v", err)
		}
		if len(candidates) != 1 || candidates[0].Attempt.ID != attempt.ID {
			t.Fatalf("ListRecoveryCandidates() = %#v, want listed attempt", candidates)
		}
		if err := store.Heartbeat(context.Background(), jobs.HeartbeatRequest{
			AttemptID: attempt.ID, FencingToken: 7, Deadline: now(4),
		}); err != nil {
			t.Fatalf("Heartbeat() error = %v", err)
		}
		before := get(t, store, "recover-heartbeat")
		if _, err := store.RecoverAttempt(context.Background(), jobs.RecoverAttemptRequest{
			AttemptID: attempt.ID, FencingToken: 7,
			HeartbeatDeadline: candidates[0].Attempt.HeartbeatDeadline,
			RecoveredAt:       now(5),
		}); !errors.Is(err, jobs.ErrStaleAttempt) {
			t.Fatalf("RecoverAttempt() after heartbeat error = %v, want ErrStaleAttempt", err)
		}
		if got := get(t, store, "recover-heartbeat"); !sameJob(got, before) {
			t.Fatalf("RecoverAttempt() after heartbeat changed job: got %#v, want %#v", got, before)
		}
	})

	t.Run("dependencies gate readiness", func(t *testing.T) {
		store := factory(t)
		dependency := newJob("dependency", "queue", now(0))
		dependent := newJob("dependent", "queue", now(0))
		dependent.DependsOn = []jobs.JobID{"dependency"}
		create(t, store, dependency)
		create(t, store, dependent)
		dependencyAttempt := claimOne(t, store, "queue", now(1), 1)
		if _, err := store.ClaimReady(context.Background(), claim("queue", now(1), 2)); !errors.Is(err, jobs.ErrNoReadyJob) {
			t.Fatalf("ClaimReady() before dependency succeeds error = %v, want ErrNoReadyJob", err)
		}
		if _, err := store.Complete(context.Background(), jobs.CompleteRequest{
			AttemptID: dependencyAttempt.ID, FencingToken: 1, CompletedAt: now(2),
		}); err != nil {
			t.Fatalf("Complete(dependency) error = %v", err)
		}
		claimed := claimOne(t, store, "queue", now(2), 2)
		if claimed.Job.ID != "dependent" {
			t.Fatalf("ClaimReady() after dependency succeeds = %q, want dependent", claimed.Job.ID)
		}
	})
}

func newJob(id jobs.JobID, queue jobs.Queue, availableAt time.Time) jobs.Job {
	return jobs.Job{
		ID: id, Queue: queue, Kind: "kind", Payload: []byte("payload"), Headers: map[string]string{"source": "contract"},
		State: jobs.StatePending, CreatedAt: now(0), AvailableAt: availableAt, UpdatedAt: now(0),
		Retry: jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
	}
}

func withPriority(job jobs.Job, priority int) jobs.Job { job.Priority = priority; return job }

func now(seconds int) time.Time {
	return time.Date(2026, 7, 17, 12, 0, seconds, 0, time.FixedZone("contract", 2*60*60))
}

func claim(queue jobs.Queue, at time.Time, token coordination.FencingToken) jobs.ClaimRequest {
	return jobs.ClaimRequest{Queue: queue, WorkerID: "worker", Now: at, HeartbeatDeadline: at.Add(time.Second), FencingToken: token}
}

func create(t *testing.T, store jobs.Store, job jobs.Job) {
	t.Helper()
	if _, err := store.Create(context.Background(), job); err != nil {
		t.Fatalf("Create(%q) error = %v", job.ID, err)
	}
}

func get(t *testing.T, store jobs.Store, id jobs.JobID) jobs.Job {
	t.Helper()
	job, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", id, err)
	}
	return job
}

func claimOne(t *testing.T, store jobs.Store, queue jobs.Queue, at time.Time, token coordination.FencingToken) jobs.Attempt {
	t.Helper()
	attempt, err := store.ClaimReady(context.Background(), claim(queue, at, token))
	if err != nil {
		t.Fatalf("ClaimReady() error = %v", err)
	}
	return attempt
}

func sameJob(a, b jobs.Job) bool { return fmt.Sprintf("%#v", a) == fmt.Sprintf("%#v", b) }
