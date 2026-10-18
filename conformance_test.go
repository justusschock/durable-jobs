package jobs_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	jobs "github.com/justusschock/durable-jobs"
	boltjobs "github.com/justusschock/durable-jobs/backend/bolt"
	messaging "github.com/justusschock/messaging"
)

// FR-JOBS-15
func TestEnqueueRunRetryRecoverAndComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	clock := &conformanceClock{now: time.Unix(100, 0).UTC()}
	client, err := jobs.NewClient(store, conformancePublisher[jobs.Wakeup]{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	created, err := client.Enqueue(context.Background(), conformanceRequest("job-1"))
	if err != nil {
		t.Fatal(err)
	}

	first, err := store.ClaimReady(context.Background(), jobs.ClaimRequest{
		Queue: created.Queue, WorkerID: "worker-1", Now: clock.Now(), HeartbeatDeadline: clock.Now().Add(time.Second), FencingToken: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Fail(context.Background(), jobs.FailRequest{
		AttemptID: first.ID, FencingToken: 1,
		Failure: jobs.Failure{Code: "retry", Retryable: true}, FailedAt: clock.Now(), NextAvailableAt: clock.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	second, err := store.ClaimReady(context.Background(), jobs.ClaimRequest{
		Queue: created.Queue, WorkerID: "worker-2", Now: clock.Now(), HeartbeatDeadline: clock.Now().Add(-time.Second), FencingToken: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := store.ListRecoveryCandidates(context.Background(), clock.Now(), 1)
	if err != nil || len(candidates) != 1 || candidates[0].Attempt.ID != second.ID {
		t.Fatalf("ListRecoveryCandidates() = %#v, %v", candidates, err)
	}
	if _, err := store.RecoverAttempt(context.Background(), jobs.RecoverAttemptRequest{
		AttemptID: second.ID, FencingToken: 2,
		HeartbeatDeadline: candidates[0].Attempt.HeartbeatDeadline, RecoveredAt: clock.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	third, err := store.ClaimReady(context.Background(), jobs.ClaimRequest{
		Queue: created.Queue, WorkerID: "worker-3", Now: clock.Now(), HeartbeatDeadline: clock.Now().Add(time.Second), FencingToken: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	final, err := store.Complete(context.Background(), jobs.CompleteRequest{
		AttemptID: third.ID, FencingToken: 3, Result: jobs.Result{Payload: []byte("ok")}, CompletedAt: clock.Now(),
	})
	if err != nil || final.State != jobs.StateSucceeded || string(final.Result) != "ok" || final.Attempt != 3 {
		t.Fatalf("Complete() = %#v, %v", final, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	persisted, err := store.Get(context.Background(), created.ID)
	if err != nil || persisted.State != jobs.StateSucceeded || string(persisted.Result) != "ok" || persisted.Attempt != 3 {
		t.Fatalf("Get() after reopen = %#v, %v", persisted, err)
	}
}

func conformanceRequest(id jobs.JobID) jobs.EnqueueRequest {
	return jobs.EnqueueRequest{
		ID: id, Queue: "default", Kind: "report", Payload: []byte("input"),
		Retry: jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
	}
}

type conformanceClock struct{ now time.Time }

func (clock *conformanceClock) Now() time.Time { return clock.now }

type conformancePublisher[T any] struct{}

func (conformancePublisher[T]) Publish(context.Context, messaging.Envelope[T]) error { return nil }
