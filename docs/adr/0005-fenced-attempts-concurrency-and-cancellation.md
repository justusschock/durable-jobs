# ADR 0005: Fenced attempts, concurrency and cancellation

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-JOBS-03 – FR-JOBS-08, FR-JOBS-13), ADR-0002 |

## Context

Recorded retrospectively for the first public release. Workers in several
processes compete for jobs. A worker can stall (garbage collection pause,
network partition) long enough for its job to be recovered and claimed by
another worker; when it resumes it must not overwrite the newer attempt's
outcome. Handlers must stop promptly when their work is no longer wanted.

## Decision drivers

- D1 At most one attempt of a job can commit an outcome.
- D2 No reliance on synchronized clocks between processes for safety.
- D3 Prompt, cooperative handler cancellation through `context.Context`.
- D4 Simple, predictable worker behaviour.

## Considered options

1. **Store-side visibility timeout only** (claim hides the job until a
   deadline).
   - Pro: no coordination service.
   - Con: a stalled worker can still commit after its deadline unless every
     write is fenced; nothing renews the claim while the handler runs.
2. **Coordination leases with fencing tokens, checked by the store in the
   same atomic step as every write**.
   - Pro: a stale worker's writes are rejected regardless of timing; the
     lease's keep-alive detects loss early and cancels the handler.
   - Con: requires a coordination backend; more moving parts per execution.
3. **Exactly-once via distributed transactions with the handler's side
   effects**.
   - Pro: strongest guarantee.
   - Con: impossible in general for arbitrary handlers.

## Decision

We will execute jobs at least once under fenced attempts:

- A worker acquires a non-waiting per-queue claim lease
  (`jobs/queue/<queue>/claim`) from `distributed-coordination`, passes its
  fencing token to `ClaimReady`, and releases the lease. The store records
  the token and increments the job's attempt generation.
- The worker then holds a non-waiting per-job execution lease
  (`jobs/job/<id>`), kept alive with `coordination.KeepAlive`, while the
  handler runs in its own goroutine with a cancellable context.
- Every `HeartbeatInterval` the worker heartbeats the store (deadline =
  now + `LeaseTTL`) and re-reads the job to observe cancellation requests.
- Heartbeat, completion, failure and recovery compare attempt generation and
  fencing token atomically with the write (`ErrStaleAttempt`). Heartbeat
  deadlines and `UpdatedAt` never move backward.
- If the execution lease is lost, keep-alive fails, a heartbeat fails, or the
  worker's context ends, the handler's context is canceled and the worker
  records no outcome; recovery (ADR-0011) makes the job runnable again.
- A `Worker` executes one job at a time; parallelism comes from running more
  workers. Handler panics are recovered and recorded.
- Cancellation is cooperative: pending jobs are canceled in the store; for
  running jobs the store sets `CancellationRequested`, the worker cancels the
  handler's context, and the outcome is recorded as canceled.
- Timestamps come from the injected `Clock`; polling and heartbeat cadence use
  `time` timers.

## Consequences

### Positive
- A stalled worker can never overwrite a newer attempt, independent of clock
  skew.
- Handlers see cancellation and lease loss through their context.

### Negative
- Handlers may run more than once and must be idempotent.
- Each execution costs two lease acquisitions and periodic store reads.
- Cancellation of running jobs is observed only at heartbeat granularity.

### Follow-ups
- PRD-0001 Q3: decide whether in-worker concurrency is worth adding.

## Links

- [worker.go](../../worker.go)
