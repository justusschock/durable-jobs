# ADR 0011: Recovery and schedules as idempotent, fenced reconciliation

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-JOBS-08, FR-JOBS-09), ADR-0005 |

## Context

Recorded retrospectively for the first public release. Two background
processes act on stored state that other processes change concurrently:
recovery of attempts whose worker vanished, and enqueueing of due schedule
occurrences. Either can crash midway or race with a worker's heartbeat or
another scheduler instance.

## Decision drivers

- D1 Safe to run from any number of processes at once.
- D2 Safe to retry after a crash at any point.
- D3 Never undo progress made by a live worker.

## Considered options

1. **Recovery as a single store call ("recover all expired")**.
   - Pro: simple.
   - Con: no observe/plan separation; harder to bound and to test; races
     with heartbeats unless the store re-checks every condition.
2. **Recovery as a `go-reconciliation` adapter with fenced apply**.
   - Pro: observe, deterministic plan (sorted, limited), and apply steps are
     separately testable; the engine provides retries and policy; each apply
     is a compare-and-swap on attempt ID, fencing token and observed
     deadline, so a heartbeat in between makes the action a no-op.
   - Con: an extra dependency and generic engine types in the API.
3. **Schedules with a lock per schedule** vs. **deterministic occurrence IDs
   plus revision-fenced cursors**.
   - Pro (IDs): enqueue is idempotent through `ErrJobExists`; a crash
     between enqueue and cursor advance is harmless; no locks.
   - Con (IDs): occurrence IDs are a format users can observe.

## Decision

We will implement recovery as `RecoveryAdapter` for `go-reconciliation`
(`Observe` lists expired attempts, `Plan` filters, sorts by attempt ID and
limits, `Apply` calls `RecoverAttempt` with the observed attempt ID, token
and deadline, treating `ErrStaleAttempt` and `ErrInvalidTransition` as
success, `Undo` is a no-op because actions are irreversible), exposed through
`NewRecoveryEngine`.

We will implement schedules with occurrence job IDs
`<schedule-id>/<due-unix-nano>` and `AvailableAt` equal to the due time,
tolerate `ErrJobExists` on enqueue, and advance the schedule cursor with a
revision-fenced `AdvanceSchedule` (tolerating `ErrStaleAttempt` from a
concurrent scheduler). Interval schedules skip missed intervals to the first
due time strictly after now; one-shot schedules are disabled after their
occurrence.

## Consequences

### Positive
- Recovery and schedulers can run on every replica without coordination.
- A crash at any point leaves state that the next run completes correctly.

### Negative
- After downtime, an interval schedule enqueues one occurrence for the
  oldest missed due time, not one per missed interval.
- Recovery does not publish an event (PRD-0001 Q1).

### Follow-ups
- PRD-0001 Q4: pass engine options through `NewRecoveryEngine`.

## Links

- [recovery.go](../../recovery.go), [schedule.go](../../schedule.go)
