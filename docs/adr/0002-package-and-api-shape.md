# ADR 0002: Package and API shape

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0005, ADR-0006 |

## Context

Recorded retrospectively for the first public release. The library offers a
job model, a storage contract, a client, a worker, a scheduler and a
recovery engine, and several storage backends. Its users embed it in their
own services, wire it to their own coordination and messaging
infrastructure, and test it with deterministic time.

## Decision drivers

- D1 One import for the common case; backends only where used.
- D2 All collaborators replaceable (storage, leases, messaging, time) so the
  library fits different deployments and is testable.
- D3 Plain data types that are easy to persist, compare and clone.
- D4 Constructors that fail fast on miswiring.

## Considered options

1. **One root package with small interfaces and explicit constructor
   dependencies; backends as subpackages**.
   - Pro: a single import covers the model, client, worker and scheduler;
     backends add only their own dependencies; every collaborator is an
     interface parameter that tests can replace.
   - Con: the root package is fairly wide.
2. **Separate packages per role (client, worker, scheduler, model)**.
   - Pro: smaller packages.
   - Con: every caller imports several packages that share one data model;
     cross-package types make the API harder to read.
3. **Functional options for every constructor**.
   - Pro: extensible without breaking signatures.
   - Con: hides required dependencies; the worker's settings are all required
     and validated together, which a plain struct expresses directly.

## Decision

We will keep the job model, contracts and runtime types in the root package
`jobs` and put stores in `backend/<name>` packages and the contract suites in
`storetest`.

- Storage is expressed by small interfaces: `Store` (jobs and attempts),
  the optional `Catalog` (paging) and `ScheduleStore` (schedules), so a store
  can implement only what it supports and `Store` does not widen for
  operator features.
- Runtime types take their collaborators explicitly: `NewClient(store,
  publisher, clock)`, `NewWorker(store, backend, events, registry,
  options)`, `NewScheduler(store, client, clock)`, `NewRecoveryEngine(store,
  clock, policy)`. Required settings live in one validated options struct
  (`WorkerOptions`), not functional options.
- Time is injected through the one-method `Clock` interface; handlers are
  plain functions (`Handler`) resolved by a `Registry`, with `MapRegistry` as
  the default.
- Messaging uses the generic `messaging.Publisher[T]` from `go-messaging`, so
  wakeups and events are typed (`Publisher[Wakeup]`, `Publisher[Event]`).
- Recovery is exposed as a `go-reconciliation` adapter (`RecoveryAdapter`)
  plus a convenience constructor returning the typed engine.
- Data types (`Job`, `EnqueueRequest`, `Schedule`, requests and results) are
  plain structs with `Clone` methods; identities are distinct string types
  (`JobID`, `AttemptID`, `Queue`, `ScheduleID`).

## Consequences

### Positive
- Callers swap stores and infrastructure without touching application code.
- Tests replace any collaborator with a fake.
- Miswiring (nil or typed-nil dependencies, invalid options) fails at
  construction with `ErrInvalidJob`.

### Negative
- Adding a required worker setting is a breaking change to `WorkerOptions`
  validation.
- Callers must provide a `Clock` even when they only want the wall clock.

### Follow-ups
- PRD-0001 Q4: let `NewRecoveryEngine` accept engine options.

## Links

- [ADR-0006](0006-pluggable-stores-and-contract-suites.md)
