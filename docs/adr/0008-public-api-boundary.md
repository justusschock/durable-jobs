# ADR 0008: Public API boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0006 |

## Context

Recorded retrospectively for the first public release. Before publishing,
every exported package and identifier was reviewed and classified as public
contract or implementation detail. Known consumers (applications built on
the library) import the root package and the memory and bbolt backends and
use most root identifiers, including the store request types, `MapRegistry`,
`MaxJobPageSize`, `RecoverySpec`, `NewRecoveryEngine` and
`ValidateDependencies`.

## Decision drivers

- D1 The public surface is exactly what users and store implementers need.
- D2 Nothing a known consumer uses is moved or unexported.
- D3 Implementation details can change without a breaking release.

## Considered options

1. **Keep the current boundary and document it**.
   - Pro: no breaking change; the review found no implementation detail
     exported outside `internal/`.
   - Con: the root package keeps a few helpers some users will never need.
2. **Unexport helpers such as `ValidateEnqueue`,
   `ValidateListJobPageRequest`, `RetryDelay` and `RecoveryAdapter`**.
   - Pro: smaller surface.
   - Con: third-party stores need the validators to match the bundled
     stores' behaviour; `RetryDelay` defines the documented backoff;
     `RecoveryAdapter` is the only way to build a recovery engine with custom
     `reconciliation.EngineOption`s. Unexporting would break legitimate uses.
3. **Move the state machine (`internal/storestate`) to a public package**.
   - Pro: third-party stores could reuse it.
   - Con: freezes internal transition helpers as API.

## Decision

We will keep the boundary as follows.

Public:
- `jobs` (root): job model and identities, `RetryPolicy`, `Failure`,
  `Result`, request/response types of the store contract, `Store`,
  `Catalog`, `ScheduleStore`, `Client`, `DurableClient`, `NewClient`,
  `Worker`, `WorkerOptions`, `NewWorker`, `Handler`, `Registry`,
  `MapRegistry`, `Scheduler`, `NewScheduler`, `Schedule`, recovery types
  (`RecoverySpec`, `RecoveryObserved`, `RecoveryAction`, `RecoveryAdapter`,
  `NewRecoveryEngine`), `Clock`, events (`Event`, `EventKind`, `Wakeup`),
  sentinel errors, `MaxJobPageSize`, and the helpers `RetryDelay`,
  `ValidateEnqueue`, `ValidateListJobPageRequest`, `ValidateDependencies`.
- `backend/memory`, `backend/bolt`, `backend/postgres`: store constructors,
  `Options`, `Store`, each backend's `ErrUnsupportedFormat`,
  `postgres.DefaultSchema`.
- `storetest`: the contract suites for implementers.

Internal:
- `internal/storestate`: shared transitions, ordering and validation for the
  bundled stores. It is importable by `backend/postgres` because Go's
  internal rule is path-based.
- All unexported identifiers (codecs, SQL, retry loops, worker internals).

The `Clock` interface stays public: consumers implement it, and it supplies
every timestamp the library records, so it is not a test-only abstraction.

No package was moved and no identifier was unexported or removed in this
review.

## Consequences

### Positive
- No consumer breaks.
- Store implementers have the validators and suites they need.

### Negative
- `ValidateEnqueue`, `ValidateListJobPageRequest` and `RecoveryAdapter` are
  API and can change only through an ADR.

### Follow-ups
- Any new exported identifier needs a justification against this boundary.

## Links

- [ADR-0006](0006-pluggable-stores-and-contract-suites.md)
