package storetest

import (
	"context"
	"reflect"
	"testing"
	"time"

	jobs "github.com/justusschock/durable-jobs"
)

// RunSchedule executes the jobs.ScheduleStore contract against a fresh store
// for each subtest.
func RunSchedule(t *testing.T, factory func(t *testing.T) jobs.ScheduleStore) {
	t.Helper()

	t.Run("schedules are owned fenced and listed in due order", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		first := scheduleFixture("b", time.Unix(100, 1).In(time.FixedZone("east", 2*60*60)))
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
		if !got.NextRunAt.Equal(time.Unix(100, 1)) {
			t.Fatalf("GetSchedule().NextRunAt = %v, want %v", got.NextRunAt, time.Unix(100, 1))
		}
		if _, err := store.PutSchedule(ctx, scheduleFixture(first.ID, time.Unix(200, 0)), 0); err == nil {
			t.Fatal("duplicate PutSchedule() error = nil, want non-nil")
		}

		for _, schedule := range []jobs.Schedule{
			scheduleFixture("a", time.Unix(100, 1)),
			scheduleFixture("future", time.Unix(101, 0)),
			scheduleFixture("disabled", time.Unix(99, 0)),
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
	})
}

func scheduleFixture(id jobs.ScheduleID, due time.Time) jobs.Schedule {
	return jobs.Schedule{
		ID: id, NextRunAt: due, Interval: time.Minute, Enabled: true,
		Request: jobs.EnqueueRequest{
			ID: "template", Queue: "q", Kind: "scheduled", Payload: []byte("payload"), Headers: map[string]string{"source": "schedule"},
			Retry: jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
		},
	}
}
