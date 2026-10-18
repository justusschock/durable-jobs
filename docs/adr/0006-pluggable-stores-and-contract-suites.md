# ADR 0006: Pluggable stores, shared state machine and contract suites

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-JOBS-15 – FR-JOBS-17), ADR-0002, ADR-0007 |

## Context

Recorded retrospectively for the first public release. Users run the library
in tests, in single-process tools and in multi-replica services, which need
different storage. The job state machine — claimability, ordering,
heartbeats, retries, cancellation, recovery and dependency propagation — is
subtle; every store must implement it identically or the worker's guarantees
break.

## Decision drivers

- D1 Identical observable semantics across stores.
- D2 One implementation of the state transitions where possible.
- D3 Third-party stores can prove conformance.

## Considered options

1. **Each store implements the state machine independently, tested
   separately**.
   - Pro: stores are self-contained.
   - Con: semantics drift; the same bugs must be fixed several times.
2. **Shared transition functions plus a shared public contract suite**.
   - Pro: Go-side stores reuse one implementation; every store, including
     SQL-based ones that cannot reuse it fully, is held to the same tests.
   - Con: the suite must be kept exhaustive.
3. **One generic store over a key-value interface**.
   - Pro: a single implementation.
   - Con: cannot exploit SQL locking (`FOR UPDATE SKIP LOCKED`) or indexes;
     forces the lowest common denominator.

## Decision

We will provide three stores behind the `Store`, `Catalog` and
`ScheduleStore` interfaces — `backend/memory` (one mutex), `backend/bolt`
(one bbolt transaction per operation) and `backend/postgres` (one
row-locking read-committed transaction per operation) — and:

- keep the storage-independent transitions, orderings and validation in
  `internal/storestate` (claimability, claim and listing order,
  heartbeat/complete/fail/cancel/recover transitions, dependency failure
  propagation, schedule validation and ordering), used by all three stores;
- publish the observable contract as the `storetest` package (`Run`,
  `RunCatalog`, `RunSchedule`), run by every bundled store's tests and
  available to third-party implementations;
- keep store-specific behaviour (file safety, read-only mode, migrations,
  transient-transaction retries) in each backend with its own tests.

## Consequences

### Positive
- The memory, bbolt and PostgreSQL stores pass the same suites.
- Third-party stores have an executable specification.

### Negative
- `internal/storestate` is not importable by third-party stores; they
  re-implement the transitions and rely on `storetest`.
- The contract suites are public API; changing them can break implementers.

### Follow-ups
- Every new store behaviour gets a `storetest` case before any store
  implements it.

## Links

- [storetest](../../storetest)
- [ADR-0008](0008-public-api-boundary.md)
