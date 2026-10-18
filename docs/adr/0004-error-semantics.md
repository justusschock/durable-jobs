# ADR 0004: Error semantics

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-JOBS-01, FR-JOBS-04, FR-JOBS-14, NFR-JOBS-02) |

## Context

Recorded retrospectively for the first public release. Callers must react to
a small number of conditions programmatically — a duplicate job, no ready
job, a stale attempt, an invalid transition, a wakeup that was not
published — and stores in different packages and modules must report them
identically. Job payloads can contain sensitive data and must not leak into
logs through error strings.

## Decision drivers

- D1 Errors are comparable across all stores.
- D2 Detail can be added without breaking comparisons.
- D3 No payload, result or header data in error messages.
- D4 A partial success (job persisted, wakeup not published) must be
  distinguishable from a failure.

## Considered options

1. **Sentinel errors wrapped with `%w`**.
   - Pro: `errors.Is` works through any amount of context; trivial for store
     implementers to return.
   - Con: no structured fields.
2. **Typed error structs with codes and fields**.
   - Pro: structured data.
   - Con: more API surface; store implementers must construct them exactly;
     fields tempt implementations to include payloads.
3. **Opaque errors with string matching**.
   - Pro: none beyond simplicity.
   - Con: brittle and unusable across stores.

## Decision

We will expose sentinel errors in package `jobs` — `ErrInvalidJob`,
`ErrJobExists`, `ErrJobNotFound`, `ErrNoReadyJob`, `ErrStaleAttempt`,
`ErrInvalidTransition`, `ErrDependencyFailed`, `ErrWakeupPublish` — plus one
`ErrUnsupportedFormat` per persistent backend, and wrap them with `%w` when
adding detail. When a lower-level cause exists (a decode or driver error) it
is wrapped with `%w` as well, so both `errors.Is` on the sentinel and
`errors.As` on the cause work.

- Validation and constructor errors (including nil and typed-nil
  dependencies, detected with reflection) wrap `ErrInvalidJob`.
- Fencing failures return `ErrStaleAttempt`; operations the current state
  forbids return `ErrInvalidTransition`.
- `Enqueue` returns the created job *and* an error wrapping
  `ErrWakeupPublish` when publishing fails, because the job is durable.
- Error messages name identities at most, never payloads, results or headers.
  Handler errors are not persisted; the worker records a fixed `Failure` code
  (`handler_error`, `handler_panic`, `handler_not_found`).
- Context errors are returned unwrapped from cancellation checks.

## Consequences

### Positive
- Callers and the bundled worker, scheduler and recovery code branch on the
  same sentinels for every store; `storetest` checks them.
- Sensitive data stays out of error strings.

### Negative
- Callers cannot get handler error details from the store (PRD-0001 Q2).
- Some sentinel names are broader than their use (`ErrStaleAttempt` also
  reports stale schedule revisions; `ErrJobNotFound` also unknown
  schedules).

### Follow-ups
- Any new sentinel or change of which sentinel an operation returns needs a
  new ADR.

## Links

- [errors.go](../../errors.go)
