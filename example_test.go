package jobs_test

import (
	"context"
	"fmt"
	"strconv"
	"time"

	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/backend/memory"
	messaging "github.com/justusschock/go-messaging"
)

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
