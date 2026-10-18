package jobs

import "time"

// Wakeup notifies workers that a job may be ready in a queue. DurableClient
// publishes it on the subject "jobs.wakeup.<queue>" after the job is durable.
type Wakeup struct {
	Queue       Queue
	JobID       JobID
	AvailableAt time.Time
}

// EventKind identifies a durable job lifecycle event. Workers publish events
// on the subject "jobs.event.<kind>".
type EventKind string

// Lifecycle event kinds.
const (
	// EventEnqueued is reserved for job creation; the library does not
	// publish it.
	EventEnqueued EventKind = "enqueued"
	// EventStarted is published when a worker starts a handler.
	EventStarted EventKind = "started"
	// EventHeartbeat is published after each successful heartbeat.
	EventHeartbeat EventKind = "heartbeat"
	// EventSucceeded is published after a successful completion.
	EventSucceeded EventKind = "succeeded"
	// EventRetryScheduled is published when a failed attempt will be retried.
	EventRetryScheduled EventKind = "retry_scheduled"
	// EventFailed is published when a job fails terminally.
	EventFailed EventKind = "failed"
	// EventCanceled is published when a running job ends canceled.
	EventCanceled EventKind = "canceled"
	// EventRecovered is reserved for attempt recovery; the library does not
	// publish it.
	EventRecovered EventKind = "recovered"
)

// Event is a typed durable job lifecycle notification. Failure is set for
// events that follow a failed attempt.
type Event struct {
	Kind      EventKind
	JobID     JobID
	AttemptID AttemptID
	Queue     Queue
	At        time.Time
	Failure   *Failure
}
