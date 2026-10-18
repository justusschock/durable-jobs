package jobs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ScheduleID identifies one durable schedule.
type ScheduleID string

// Schedule describes a one-shot (zero Interval) or fixed-interval job
// template. Revision is assigned by the ScheduleStore.
type Schedule struct {
	ID        ScheduleID
	Request   EnqueueRequest
	NextRunAt time.Time
	Interval  time.Duration
	Enabled   bool
	Revision  uint64
}

// Clone returns a schedule with independently owned request resources.
func (schedule Schedule) Clone() Schedule {
	schedule.Request = schedule.Request.Clone()
	return schedule
}

// ScheduleStore atomically persists schedules with revision fencing. The
// storetest.RunSchedule suite defines the observable contract.
type ScheduleStore interface {
	// PutSchedule creates (expected revision 0) or replaces (expected
	// current revision) a schedule and returns it with its new revision.
	// Creating an existing schedule returns ErrJobExists, replacing a missing
	// one ErrJobNotFound, and a mismatched revision ErrStaleAttempt.
	PutSchedule(context.Context, Schedule, uint64) (Schedule, error)
	// GetSchedule returns a schedule, or ErrJobNotFound.
	GetSchedule(context.Context, ScheduleID) (Schedule, error)
	// ListDueSchedules returns up to limit enabled schedules whose NextRunAt
	// is at or before the given time, ordered by NextRunAt and ID.
	ListDueSchedules(context.Context, time.Time, int) ([]Schedule, error)
	// AdvanceSchedule moves the schedule's cursor forward to the given time,
	// or disables it when the time is zero, if the revision is still current;
	// otherwise it returns ErrStaleAttempt. A cursor that would not move
	// forward returns ErrInvalidTransition.
	AdvanceSchedule(context.Context, ScheduleID, uint64, time.Time) error
}

// Scheduler enqueues due schedule occurrences and advances their durable cursor.
type Scheduler struct {
	store  ScheduleStore
	client Client
	clock  Clock
}

// NewScheduler constructs a durable schedule runner. Nil dependencies are
// reported by RunDue as ErrInvalidJob.
func NewScheduler(store ScheduleStore, client Client, clock Clock) *Scheduler {
	return &Scheduler{store: store, client: client, clock: clock}
}

// RunDue processes at most limit due schedules. Each occurrence is enqueued
// with the ID "<schedule-id>/<due-unix-nano>" and AvailableAt set to its due
// time, so a retry after a crash between enqueue and cursor advance is
// idempotent. An interval schedule then advances to the first due time
// strictly after now; a one-shot schedule is disabled. RunDue returns the
// number of schedules processed and stops at the first enqueue or store
// error without advancing that schedule.
func (scheduler *Scheduler) RunDue(ctx context.Context, limit int) (int, error) {
	if scheduler == nil || isNilDependency(scheduler.store) || isNilDependency(scheduler.client) || isNilDependency(scheduler.clock) {
		return 0, fmt.Errorf("%w: nil scheduler dependency", ErrInvalidJob)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	now := persistedTime(scheduler.clock.Now())
	schedules, err := scheduler.store.ListDueSchedules(ctx, now, limit)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, schedule := range schedules {
		dueAt := persistedTime(schedule.NextRunAt)
		nextRunAt, err := nextScheduleRun(schedule, now)
		if err != nil {
			return processed, err
		}
		request := schedule.Request.Clone()
		request.ID = JobID(string(schedule.ID) + "/" + strconv.FormatInt(dueAt.UnixNano(), 10))
		request.AvailableAt = dueAt
		if _, err := scheduler.client.Enqueue(ctx, request); err != nil && !errors.Is(err, ErrJobExists) {
			return processed, err
		}

		if err := scheduler.store.AdvanceSchedule(ctx, schedule.ID, schedule.Revision, nextRunAt); err != nil && !errors.Is(err, ErrStaleAttempt) {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func nextScheduleRun(schedule Schedule, now time.Time) (time.Time, error) {
	if schedule.Interval == 0 {
		return time.Time{}, nil
	}
	if schedule.Interval < 0 {
		return time.Time{}, fmt.Errorf("%w: negative schedule interval", ErrInvalidJob)
	}
	dueAt := persistedTime(schedule.NextRunAt)
	remaining := now.Sub(dueAt)
	steps := remaining/schedule.Interval + 1
	if steps <= 0 || steps > time.Duration(1<<63-1)/schedule.Interval {
		return time.Time{}, fmt.Errorf("%w: schedule advance overflow", ErrInvalidJob)
	}
	next := persistedTime(dueAt.Add(steps * schedule.Interval))
	if !next.After(now) || !next.After(dueAt) {
		return time.Time{}, fmt.Errorf("%w: schedule advance overflow", ErrInvalidJob)
	}
	return next, nil
}
