package jobs_test

import (
	"context"
	"fmt"
	"strconv"
	"time"

	coordinationmemory "github.com/justusschock/go-distributed-coordination/memory"
	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/backend/memory"
	messaging "github.com/justusschock/go-messaging"
	messagingmemory "github.com/justusschock/go-messaging/memory"
)

// systemClock is the production jobs.Clock: it reads the wall clock.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Example enqueues a job and runs it once with a worker. Production code would
// use a persistent store (backend/bolt or backend/postgres), a distributed
// coordination backend, and a real message broker.
func Example() {
	ctx := context.Background()
	store := memory.New()

	client, err := jobs.NewClient(store, messagingmemory.NewBroker[jobs.Wakeup](time.Second), systemClock{})
	if err != nil {
		panic(err)
	}
	if _, err := client.Enqueue(ctx, jobs.EnqueueRequest{
		ID:      "greet-1",
		Queue:   "default",
		Kind:    "greet",
		Payload: []byte("world"),
		Retry:   jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
	}); err != nil {
		panic(err)
	}

	registry := jobs.MapRegistry{
		"greet": func(_ context.Context, job jobs.Job) (jobs.Result, error) {
			return jobs.Result{Payload: append([]byte("hello, "), job.Payload...)}, nil
		},
	}
	worker, err := jobs.NewWorker(store, coordinationmemory.NewBackend(nil), messagingmemory.NewBroker[jobs.Event](time.Second), registry, jobs.WorkerOptions{
		WorkerID:          "worker-1",
		Queues:            []jobs.Queue{"default"},
		PollInterval:      time.Second,
		LeaseTTL:          30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		ClaimLimit:        10,
		Clock:             systemClock{},
	})
	if err != nil {
		panic(err)
	}
	// Run(ctx) polls until ctx ends; RunOne executes at most one job.
	if _, err := worker.RunOne(ctx); err != nil {
		panic(err)
	}

	job, err := client.Get(ctx, "greet-1")
	if err != nil {
		panic(err)
	}
	fmt.Println(job.State == jobs.StateSucceeded, string(job.Result))
	// Output: true hello, world
}

func ExampleDurableClient() {
	now := time.Unix(100, 0).UTC()
	store := memory.New()
	client, _ := jobs.NewClient(store, examplePublisher[jobs.Wakeup]{}, exampleClock{now: now})
	job, _ := client.Enqueue(context.Background(), exampleRequest("job-1"))

	fmt.Println(job.ID, job.State == jobs.StatePending)
	// Output: job-1 true
}

func ExampleScheduler() {
	now := time.Unix(100, 0).UTC()
	store := memory.New()
	client, _ := jobs.NewClient(store, examplePublisher[jobs.Wakeup]{}, exampleClock{now: now})
	_, _ = store.PutSchedule(context.Background(), jobs.Schedule{
		ID: "hourly", Request: exampleRequest("template"), NextRunAt: now, Interval: time.Hour, Enabled: true,
	}, 0)

	processed, _ := jobs.NewScheduler(store, client, exampleClock{now: now}).RunDue(context.Background(), 1)
	occurrenceID := jobs.JobID("hourly/" + strconv.FormatInt(now.UnixNano(), 10))
	occurrence, _ := store.Get(context.Background(), occurrenceID)
	fmt.Println(processed, occurrence.Kind)
	// Output: 1 report
}

func exampleRequest(id jobs.JobID) jobs.EnqueueRequest {
	return jobs.EnqueueRequest{
		ID: id, Queue: "default", Kind: "report", Payload: []byte("input"),
		Retry: jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
	}
}

type exampleClock struct{ now time.Time }

func (clock exampleClock) Now() time.Time { return clock.now }

type examplePublisher[T any] struct{}

func (examplePublisher[T]) Publish(context.Context, messaging.Envelope[T]) error { return nil }
