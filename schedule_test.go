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
)

func TestScheduleStoreContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		runScheduleStoreContract(t, memory.New())
	})
	t.Run("bolt", func(t *testing.T) {
		store, err := boltjobs.Open(filepath.Join(t.TempDir(), "jobs.db"), boltjobs.Options{Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		runScheduleStoreContract(t, store)
	})
}

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

func runScheduleStoreContract(t *testing.T, store jobs.ScheduleStore) {
	t.Helper()
	ctx := context.Background()
	first := testSchedule("b", time.Unix(100, 1).In(time.FixedZone("east", 2*60*60)))
	first.Request.Payload = []byte("owned")
	first.Request.Headers = map[string]string{"source": "caller"}
	first.Request.DependsOn = []jobs.JobID{"parent"}
	created, err := store.PutSchedule(ctx, first, 0)
	if err != nil {
		t.Fatal(err)
	}
	if created.Revision != 1 || created.NextRunAt.Location() != time.UTC {
		t.Fatalf("PutSchedule() = %#v, want revision 1 and UTC", created)
	}
	first.Request.Payload[0] = 'x'
	first.Request.Headers["source"] = "mutated"
	first.Request.DependsOn[0] = "mutated"
	created.Request.Payload[0] = 'y'
	created.Request.Headers["source"] = "returned"
	created.Request.DependsOn[0] = "returned"
	got, err := store.GetSchedule(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Request.Payload) != "owned" || got.Request.Headers["source"] != "caller" || got.Request.DependsOn[0] != "parent" {
		t.Fatalf("schedule retained caller-owned resources: %#v", got)
	}
	if _, err := store.PutSchedule(ctx, testSchedule(first.ID, time.Unix(200, 0)), 0); err == nil {
		t.Fatal("duplicate PutSchedule() error = nil, want non-nil")
	}

	for _, schedule := range []jobs.Schedule{
		testSchedule("a", time.Unix(100, 1)),
		testSchedule("future", time.Unix(101, 0)),
		testSchedule("disabled", time.Unix(99, 0)),
	} {
		if schedule.ID == "disabled" {
			schedule.Enabled = false
		}
		if _, err := store.PutSchedule(ctx, schedule, 0); err != nil {
			t.Fatal(err)
		}
	}
	due, err := store.ListDueSchedules(ctx, time.Unix(100, 1), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 2 || due[0].ID != "a" || due[1].ID != "b" {
		t.Fatalf("ListDueSchedules() = %#v, want a then b", due)
	}
	before, _ := store.GetSchedule(ctx, "a")
	if err := store.AdvanceSchedule(ctx, "a", before.Revision+1, time.Unix(200, 0)); err == nil {
		t.Fatal("AdvanceSchedule() with stale revision error = nil, want non-nil")
	}
	unchanged, _ := store.GetSchedule(ctx, "a")
	if !reflect.DeepEqual(unchanged, before) {
		t.Fatalf("stale AdvanceSchedule() changed schedule: %#v", unchanged)
	}
	if err := store.AdvanceSchedule(ctx, "a", before.Revision, time.Unix(200, 0).In(time.FixedZone("west", -3*60*60))); err != nil {
		t.Fatal(err)
	}
	advanced, _ := store.GetSchedule(ctx, "a")
	if advanced.Revision != before.Revision+1 || !advanced.NextRunAt.Equal(time.Unix(200, 0)) || advanced.NextRunAt.Location() != time.UTC {
		t.Fatalf("AdvanceSchedule() = %#v", advanced)
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
