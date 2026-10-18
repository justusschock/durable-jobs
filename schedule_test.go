package jobs_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	jobs "github.com/justusschock/go-durable-jobs"
	boltjobs "github.com/justusschock/go-durable-jobs/backend/bolt"
	"github.com/justusschock/go-durable-jobs/backend/memory"
	"github.com/justusschock/go-durable-jobs/storetest"
)

// FR-JOBS-09, FR-JOBS-17
func TestScheduleStoreContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		storetest.RunSchedule(t, func(t *testing.T) jobs.ScheduleStore {
			t.Helper()
			return memory.New()
		})
	})
	t.Run("bolt", func(t *testing.T) {
		storetest.RunSchedule(t, func(t *testing.T) jobs.ScheduleStore {
			t.Helper()
			store, err := boltjobs.Open(filepath.Join(t.TempDir(), "jobs.db"), boltjobs.Options{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		})
	})
}

// FR-JOBS-09
func TestBoltScheduleSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	want, err := store.PutSchedule(context.Background(), testSchedule("persisted", time.Unix(100, 0)), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	got, err := store.GetSchedule(context.Background(), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetSchedule() after reopen = %#v, want %#v", got, want)
	}
}

// FR-JOBS-09
func TestSchedulerAdvancesFromPreviousDueTime(t *testing.T) {
	store := memory.New()
	client := &scheduleRecordingClient{}
	now := time.Date(2026, 7, 16, 10, 12, 0, 0, time.UTC)
	due := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	scheduler := jobs.NewScheduler(store, client, scheduleClock{now: now})
	created, err := store.PutSchedule(context.Background(), jobs.Schedule{
		ID: "s", Request: scheduleRequest("template"), NextRunAt: due, Interval: 5 * time.Minute, Enabled: true,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}

	n, err := scheduler.RunDue(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("RunDue() = %d, %v; want 1, nil", n, err)
	}
	got, err := store.GetSchedule(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	wantNext := time.Date(2026, 7, 16, 10, 15, 0, 0, time.UTC)
	if !got.NextRunAt.Equal(wantNext) || got.Revision != created.Revision+1 {
		t.Fatalf("advanced schedule = %#v, want next %v and revision %d", got, wantNext, created.Revision+1)
	}
	if len(client.requests) != 1 {
		t.Fatalf("Enqueue() calls = %d, want 1", len(client.requests))
	}
	request := client.requests[0]
	if request.ID != jobs.JobID("s/"+strconv.FormatInt(due.UnixNano(), 10)) || !request.AvailableAt.Equal(due) {
		t.Fatalf("occurrence request = %#v, want deterministic ID for %v", request, due)
	}
}

// FR-JOBS-09
func TestSchedulerOneShotDisablesAfterEnqueue(t *testing.T) {
	store := memory.New()
	client := &scheduleRecordingClient{}
	due := time.Unix(100, 0).UTC()
	scheduler := jobs.NewScheduler(store, client, scheduleClock{now: due})
	if _, err := store.PutSchedule(context.Background(), jobs.Schedule{
		ID: "once", Request: scheduleRequest("template"), NextRunAt: due, Enabled: true,
	}, 0); err != nil {
		t.Fatal(err)
	}

	n, err := scheduler.RunDue(context.Background(), 1)
	if err != nil || n != 1 {
		t.Fatalf("RunDue() = %d, %v; want 1, nil", n, err)
	}
	got, err := store.GetSchedule(context.Background(), "once")
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || !got.NextRunAt.IsZero() {
		t.Fatalf("one-shot schedule after run = %#v, want disabled with zero next run", got)
	}
}

// FR-JOBS-09
func TestSchedulerAdvancesAfterIdempotentOccurrenceConflict(t *testing.T) {
	store := memory.New()
	due := time.Unix(100, 0).UTC()
	client := &scheduleRecordingClient{err: jobs.ErrJobExists}
	scheduler := jobs.NewScheduler(store, client, scheduleClock{now: due})
	created, err := store.PutSchedule(context.Background(), jobs.Schedule{
		ID: "crash", Request: scheduleRequest("template"), NextRunAt: due, Interval: time.Minute, Enabled: true,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}

	n, err := scheduler.RunDue(context.Background(), 1)
	if err != nil || n != 1 {
		t.Fatalf("RunDue() = %d, %v; want idempotent 1, nil", n, err)
	}
	got, err := store.GetSchedule(context.Background(), "crash")
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != created.Revision+1 || !got.NextRunAt.Equal(due.Add(time.Minute)) {
		t.Fatalf("schedule after duplicate occurrence = %#v", got)
	}
}

// FR-JOBS-09
func TestSchedulerDoesNotAdvanceAfterEnqueueFailure(t *testing.T) {
	store := memory.New()
	due := time.Unix(100, 0).UTC()
	wantErr := errors.New("enqueue unavailable")
	scheduler := jobs.NewScheduler(store, &scheduleRecordingClient{err: wantErr}, scheduleClock{now: due})
	created, err := store.PutSchedule(context.Background(), jobs.Schedule{
		ID: "failed", Request: scheduleRequest("template"), NextRunAt: due, Interval: time.Minute, Enabled: true,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}

	n, err := scheduler.RunDue(context.Background(), 1)
	if n != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("RunDue() = %d, %v; want 0, enqueue error", n, err)
	}
	got, getErr := store.GetSchedule(context.Background(), "failed")
	if getErr != nil {
		t.Fatal(getErr)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatalf("schedule advanced after enqueue failure: got %#v, want %#v", got, created)
	}
}

func testSchedule(id jobs.ScheduleID, due time.Time) jobs.Schedule {
	return jobs.Schedule{ID: id, Request: scheduleRequest("template"), NextRunAt: due, Interval: time.Minute, Enabled: true}
}

func scheduleRequest(id jobs.JobID) jobs.EnqueueRequest {
	return jobs.EnqueueRequest{
		ID: id, Queue: "q", Kind: "scheduled", Payload: []byte("payload"), Headers: map[string]string{"source": "schedule"},
		Retry: jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
	}
}

type scheduleRecordingClient struct {
	requests []jobs.EnqueueRequest
	err      error
}

func (client *scheduleRecordingClient) Enqueue(_ context.Context, request jobs.EnqueueRequest) (jobs.Job, error) {
	client.requests = append(client.requests, request.Clone())
	return jobs.Job{ID: request.ID}, client.err
}

func (*scheduleRecordingClient) Get(context.Context, jobs.JobID) (jobs.Job, error) {
	return jobs.Job{}, errors.New("unexpected Get")
}

func (*scheduleRecordingClient) Cancel(context.Context, jobs.JobID) error {
	return errors.New("unexpected Cancel")
}

type scheduleClock struct{ now time.Time }

func (clock scheduleClock) Now() time.Time { return clock.now }
