package jobs_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/backend/memory"
	messaging "github.com/justusschock/go-messaging"
)

var _ jobs.Client = (*jobs.DurableClient)(nil)

// FR-JOBS-14
func TestClientConstructorRejectsNilDependencies(t *testing.T) {
	t.Parallel()

	store := memory.New()
	publisher := &recordingPublisher[jobs.Wakeup]{}
	clock := fixedClientClock{now: time.Unix(100, 0)}
	var typedNilStore *memory.Store
	var typedNilPublisher *recordingPublisher[jobs.Wakeup]
	var typedNilClock *settableClientClock

	tests := []struct {
		name      string
		construct func() error
	}{
		{
			name: "store",
			construct: func() error {
				_, err := jobs.NewClient(nil, publisher, clock)
				return err
			},
		},
		{
			name: "publisher",
			construct: func() error {
				_, err := jobs.NewClient(store, nil, clock)
				return err
			},
		},
		{
			name: "clock",
			construct: func() error {
				_, err := jobs.NewClient(store, publisher, nil)
				return err
			},
		},
		{
			name: "typed nil store",
			construct: func() error {
				_, err := jobs.NewClient(typedNilStore, publisher, clock)
				return err
			},
		},
		{
			name: "typed nil publisher",
			construct: func() error {
				_, err := jobs.NewClient(store, typedNilPublisher, clock)
				return err
			},
		},
		{
			name: "typed nil clock",
			construct: func() error {
				_, err := jobs.NewClient(store, publisher, typedNilClock)
				return err
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.construct(); err == nil {
				t.Fatal("NewClient() error = nil, want non-nil")
			}
		})
	}
}

// FR-JOBS-01
func TestClientEnqueuePersistsBeforePublishing(t *testing.T) {
	t.Parallel()

	store := memory.New()
	publisher := &recordingPublisher[jobs.Wakeup]{}
	publisher.beforePublish = func(ctx context.Context, envelope messaging.Envelope[jobs.Wakeup]) error {
		persisted, err := store.Get(ctx, envelope.Payload.JobID)
		if err != nil {
			return err
		}
		if persisted.State != jobs.StatePending {
			t.Fatalf("state during Publish() = %v, want pending", persisted.State)
		}
		return nil
	}
	clock := fixedClientClock{now: time.Unix(100, 123).In(time.FixedZone("client", 2*60*60))}
	client, err := jobs.NewClient(store, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}

	request := validEnqueueRequest()
	request.ID = "enqueue-order"
	request.AvailableAt = time.Time{}
	job, err := client.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	wantNow := clock.now.Round(0).UTC()
	if !job.CreatedAt.Equal(wantNow) || job.CreatedAt.Location() != time.UTC {
		t.Fatalf("CreatedAt = %v, want %v UTC", job.CreatedAt, wantNow)
	}
	if !job.AvailableAt.Equal(wantNow) || job.AvailableAt.Location() != time.UTC {
		t.Fatalf("AvailableAt = %v, want %v UTC", job.AvailableAt, wantNow)
	}
	if !job.UpdatedAt.Equal(wantNow) || job.UpdatedAt.Location() != time.UTC {
		t.Fatalf("UpdatedAt = %v, want %v UTC", job.UpdatedAt, wantNow)
	}
	if len(publisher.envelopes) != 1 {
		t.Fatalf("published %d wakeups, want 1", len(publisher.envelopes))
	}
	envelope := publisher.envelopes[0]
	if envelope.Subject != "jobs.wakeup.alerts" {
		t.Fatalf("subject = %q, want %q", envelope.Subject, "jobs.wakeup.alerts")
	}
	if envelope.Payload != (jobs.Wakeup{Queue: job.Queue, JobID: job.ID, AvailableAt: job.AvailableAt}) {
		t.Fatalf("wakeup = %#v, want job metadata", envelope.Payload)
	}
}

// FR-JOBS-01
func TestClientEnqueueValidatesBeforeCreateAndPublish(t *testing.T) {
	t.Parallel()

	store := &createRecordingStore{Store: memory.New()}
	publisher := &recordingPublisher[jobs.Wakeup]{}
	client, err := jobs.NewClient(store, publisher, fixedClientClock{now: time.Unix(100, 0)})
	if err != nil {
		t.Fatal(err)
	}

	request := validEnqueueRequest()
	request.ID = ""
	job, err := client.Enqueue(context.Background(), request)
	if !errors.Is(err, jobs.ErrInvalidJob) {
		t.Fatalf("Enqueue() error = %v, want ErrInvalidJob", err)
	}
	if !reflect.DeepEqual(job, jobs.Job{}) {
		t.Fatalf("Enqueue() job = %#v, want zero value", job)
	}
	if store.createCalls != 0 {
		t.Fatalf("Create() calls = %d, want 0", store.createCalls)
	}
	if len(publisher.envelopes) != 0 {
		t.Fatalf("published %d wakeups, want 0", len(publisher.envelopes))
	}
}

// FR-JOBS-02
func TestClientEnqueueNormalizesAvailableAtAndOwnsResources(t *testing.T) {
	t.Parallel()

	store := memory.New()
	publisher := &recordingPublisher[jobs.Wakeup]{}
	clock := fixedClientClock{now: time.Unix(100, 0)}
	client, err := jobs.NewClient(store, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}

	request := validEnqueueRequest()
	request.ID = "owned-resources"
	request.AvailableAt = time.Date(2026, 7, 18, 10, 30, 0, 42, time.FixedZone("west", -3*60*60))
	wantAvailableAt := request.AvailableAt.Round(0).UTC()
	wantPayload := append([]byte(nil), request.Payload...)
	wantHeaders := map[string]string{"trace": request.Headers["trace"]}
	wantDependencies := append([]jobs.JobID(nil), request.DependsOn...)

	job, err := client.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !job.AvailableAt.Equal(wantAvailableAt) || job.AvailableAt.Location() != time.UTC {
		t.Fatalf("AvailableAt = %v, want %v UTC", job.AvailableAt, wantAvailableAt)
	}

	request.Payload[0] = 'X'
	request.Headers["trace"] = "mutated"
	request.DependsOn[0] = "mutated"
	job.Payload[0] = 'Y'
	job.Headers["trace"] = "returned-mutation"
	job.DependsOn[0] = "returned-mutation"

	got, err := client.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Payload, wantPayload) || !reflect.DeepEqual(got.Headers, wantHeaders) || !reflect.DeepEqual(got.DependsOn, wantDependencies) {
		t.Fatalf("Get() returned aliased resources: %#v", got)
	}

	got.Payload[0] = 'Z'
	got.Headers["trace"] = "get-mutation"
	got.DependsOn[0] = "get-mutation"
	again, err := client.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again.Payload, wantPayload) || !reflect.DeepEqual(again.Headers, wantHeaders) || !reflect.DeepEqual(again.DependsOn, wantDependencies) {
		t.Fatalf("successive Get() calls share resources: %#v", again)
	}
}

// FR-JOBS-01
func TestClientEnqueuePublishFailureLeavesDurableJob(t *testing.T) {
	t.Parallel()

	publishFailure := errors.New("publisher unavailable")
	store := memory.New()
	publisher := &recordingPublisher[jobs.Wakeup]{err: publishFailure}
	client, err := jobs.NewClient(store, publisher, fixedClientClock{now: time.Unix(100, 0)})
	if err != nil {
		t.Fatal(err)
	}
	request := validEnqueueRequest()
	request.ID = "publish-failure"

	job, err := client.Enqueue(context.Background(), request)
	if !errors.Is(err, jobs.ErrWakeupPublish) {
		t.Fatalf("Enqueue() error = %v, want ErrWakeupPublish", err)
	}
	if job.ID != request.ID {
		t.Fatalf("Enqueue() job = %#v, want persisted job", job)
	}
	persisted, getErr := client.Get(context.Background(), request.ID)
	if getErr != nil {
		t.Fatalf("Get() after publication failure: %v", getErr)
	}
	if persisted.ID != request.ID || persisted.State != jobs.StatePending {
		t.Fatalf("persisted job after publication failure = %#v", persisted)
	}
}

// FR-JOBS-07
func TestClientCancelPendingAndRunning(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		run  bool
	}{
		{name: "pending"},
		{name: "running", run: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := memory.New()
			clock := &settableClientClock{now: time.Unix(100, 0)}
			client, err := jobs.NewClient(store, &recordingPublisher[jobs.Wakeup]{}, clock)
			if err != nil {
				t.Fatal(err)
			}
			request := validEnqueueRequest()
			request.ID = jobs.JobID("cancel-" + test.name)
			request.DependsOn = nil
			if _, err := client.Enqueue(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if test.run {
				_, err := store.ClaimReady(context.Background(), jobs.ClaimRequest{
					Queue:             request.Queue,
					WorkerID:          "worker-1",
					Now:               time.Unix(150, 0),
					HeartbeatDeadline: time.Unix(180, 0),
					FencingToken:      coordination.FencingToken(7),
				})
				if err != nil {
					t.Fatal(err)
				}
			}

			clock.now = time.Unix(200, 999).In(time.FixedZone("cancel", 4*60*60))
			if err := client.Cancel(context.Background(), request.ID); err != nil {
				t.Fatal(err)
			}
			got, err := client.Get(context.Background(), request.ID)
			if err != nil {
				t.Fatal(err)
			}
			if test.run {
				if got.State != jobs.StateRunning || !got.CancellationRequested {
					t.Fatalf("running job after Cancel() = %#v", got)
				}
			} else if got.State != jobs.StateCanceled || !got.CancellationRequested {
				t.Fatalf("pending job after Cancel() = %#v", got)
			}
			wantUpdatedAt := clock.now.Round(0).UTC()
			if !got.UpdatedAt.Equal(wantUpdatedAt) || got.UpdatedAt.Location() != time.UTC {
				t.Fatalf("UpdatedAt = %v, want %v UTC", got.UpdatedAt, wantUpdatedAt)
			}
		})
	}
}

type fixedClientClock struct {
	now time.Time
}

func (clock fixedClientClock) Now() time.Time {
	return clock.now
}

type settableClientClock struct {
	now time.Time
}

func (clock *settableClientClock) Now() time.Time {
	return clock.now
}

type createRecordingStore struct {
	jobs.Store
	createCalls int
}

func (store *createRecordingStore) Create(ctx context.Context, job jobs.Job) (jobs.Job, error) {
	store.createCalls++
	return store.Store.Create(ctx, job)
}

type recordingPublisher[T any] struct {
	envelopes     []messaging.Envelope[T]
	beforePublish func(context.Context, messaging.Envelope[T]) error
	err           error
}

func (publisher *recordingPublisher[T]) Publish(ctx context.Context, envelope messaging.Envelope[T]) error {
	if publisher.beforePublish != nil {
		if err := publisher.beforePublish(ctx, envelope); err != nil {
			return err
		}
	}
	publisher.envelopes = append(publisher.envelopes, envelope.Clone())
	return publisher.err
}
