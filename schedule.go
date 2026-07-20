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

// Schedule describes a one-shot or fixed-interval job template.
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

// ScheduleStore atomically persists schedules with revision fencing.
type ScheduleStore interface {
	PutSchedule(context.Context, Schedule, uint64) (Schedule, error)
	GetSchedule(context.Context, ScheduleID) (Schedule, error)
	ListDueSchedules(context.Context, time.Time, int) ([]Schedule, error)
	AdvanceSchedule(context.Context, ScheduleID, uint64, time.Time) error
}

// Scheduler enqueues due schedule occurrences and advances their durable cursor.
type Scheduler struct {
	store  ScheduleStore
	client Client
	clock  Clock
}

// NewScheduler constructs a durable schedule runner.
func NewScheduler(store ScheduleStore, client Client, clock Clock) *Scheduler {
	return &Scheduler{store: store, client: client, clock: clock}
}

// RunDue processes at most limit due schedules.
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
