# ADR 0003: Dependency policy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-JOBS-04), ADR-0007 |

## Context

Recorded retrospectively for the first public release. Durable jobs need
distributed leases with fencing tokens, a messaging abstraction for wakeups
and events, a reconciliation loop for recovery, and at least one persistent
store. Each could be written in-house or taken from a dependency, and every
dependency of the core module becomes a dependency of every user.

## Decision drivers

- D1 Small, auditable dependency graph for the core module.
- D2 Reuse well-tested primitives instead of re-implementing them.
- D3 Users who do not use a backend must not pay for its dependencies.

## Considered options

1. **Standard library only, everything in-house**.
   - Pro: no dependencies.
   - Con: duplicates leases, fencing, messaging and reconciliation logic that
     sibling libraries already provide and test; no embedded persistent store.
2. **Sibling libraries plus one embedded store in the core module; heavy
   drivers in nested modules**.
   - Pro: reuses tested primitives; keeps SQL drivers out of the core graph.
   - Con: the core module still carries bbolt; two modules to release.
3. **Large job frameworks or ORMs**.
   - Pro: less code.
   - Con: large transitive graphs and semantics the library does not control.

## Decision

We will use the standard library first and allow these dependencies:

- `github.com/justusschock/distributed-coordination`: leases, fencing
  tokens, keep-alive, and the in-memory coordination backend used in tests.
- `github.com/justusschock/messaging`: typed publishers and envelopes for
  wakeups and lifecycle events.
- `github.com/justusschock/reconciliation`: the observe/plan/apply engine
  behind recovery.
- `go.etcd.io/bbolt`: embedded, transactional, crash-safe key-value store for
  the single-process backend. It is small and pure Go.
- `github.com/jackc/pgx/v5`: PostgreSQL driver, only in the nested
  `backend/postgres` module (ADR-0007).

Tests use only the standard library and these modules. A new dependency
requires an ADR.

## Consequences

### Positive
- The core graph stays small: three sibling libraries, bbolt and their
  transitive dependencies.
- Programs that do not use PostgreSQL never download pgx.

### Negative
- The library moves in step with the sibling libraries' APIs.
- bbolt is compiled into every user of the core module, even those using
  only PostgreSQL or memory stores.

### Follow-ups
- Revisit moving `backend/bolt` into its own module if the bbolt dependency
  becomes a burden for users.

## Links

- [ADR-0007](0007-persistence-formats-and-nested-postgres-module.md)
