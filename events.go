package jobs

import "time"

// Wakeup notifies workers that a job may be ready in a queue.
type Wakeup struct {
	Queue       Queue
	JobID       JobID
	AvailableAt time.Time
}

// EventKind identifies a durable job lifecycle event.
type EventKind string

const (
	EventEnqueued       EventKind = "enqueued"
	EventStarted        EventKind = "started"
	EventHeartbeat      EventKind = "heartbeat"
	EventSucceeded      EventKind = "succeeded"
	EventRetryScheduled EventKind = "retry_scheduled"
	EventFailed         EventKind = "failed"
	EventCanceled       EventKind = "canceled"
	EventRecovered      EventKind = "recovered"
)

// Event is a typed durable job lifecycle notification.
type Event struct {
	Kind      EventKind
	JobID     JobID
	AttemptID AttemptID
	Queue     Queue
	At        time.Time
	Failure   *Failure
}
