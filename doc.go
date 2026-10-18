// Package jobs provides durable background jobs for Go: atomic queue claims,
// fenced execution attempts, heartbeats, bounded exponential retries,
// cancellation, recovery of expired attempts, one-shot and fixed-interval
// schedules, dependency workflows, and typed lifecycle events.
//
// A [DurableClient] validates and persists jobs in a [Store] and publishes a
// [Wakeup] for the job's queue. A [Worker] claims ready jobs under a
// coordination lease whose fencing token guards every later heartbeat,
// completion, and failure, so a worker that lost its lease can never commit
// a result. [NewRecoveryEngine] returns expired attempts to their queue, and a
// [Scheduler] enqueues due [Schedule] occurrences idempotently.
//
// Time comes from a caller-supplied [Clock]; worker polling and heartbeats use
// the time package's timers. Stores are provided by the backend/memory,
// backend/bolt, and backend/postgres packages, and the storetest package
// holds the contract suites that every Store implementation must pass.
//
// Unless stated otherwise, exported types are safe for concurrent use once
// constructed, operations that block or perform I/O take a context and honor
// its cancellation, and every Job, Schedule, and Attempt returned to a caller
// is an independently owned snapshot.
package jobs
