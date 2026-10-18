# ADR 0007: Persistence formats and the nested PostgreSQL module

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-JOBS-15, FR-JOBS-16, FR-JOBS-19), ADR-0003, ADR-0006 |

## Context

Recorded retrospectively for the first public release. Persistent stores
outlive releases of the library: data written by one version must be read by
the next, or rejected clearly. Go `time.Time` has nanosecond precision and
counters are unsigned 64-bit, which neither JSON numbers nor PostgreSQL's
native types represent exactly. The PostgreSQL driver is a large dependency
most users do not need.

## Decision drivers

- D1 Exact round-trips of every field, including times and counters.
- D2 Unknown or newer formats are rejected, never misread.
- D3 Rolling upgrades of multi-replica deployments.
- D4 Users of the core module do not depend on pgx.

## Considered options

1. **bbolt: versioned JSON envelopes per record** vs. a binary encoding
   (gob, protobuf).
   - Pro (JSON): human-inspectable, stdlib only, tolerant of added fields.
   - Con (JSON): larger and slower than binary encodings.
2. **PostgreSQL: one typed column per field with explicit precision
   handling** vs. a JSON document per job.
   - Pro (columns): indexes for claims, listings, catalog and recovery;
     row-level locking; queryable by operators.
   - Con (columns): schema migrations for every model change.
3. **PostgreSQL store in the core module** vs. **a nested module**.
   - Pro (nested): pgx stays out of the core dependency graph.
   - Con (nested): two modules to version and release in order.

## Decision

We will use these formats:

- **bbolt**: buckets `jobs`, `attempts`, `schedules` and `meta` (attempt
  sequence). Each value is a JSON envelope `{"version": 1, "payload": …}`;
  any other version yields `ErrUnsupportedFormat`. The database file is
  created owner-only (`0600`), symlinks are rejected, and an incomplete
  schema is never repaired implicitly.
- **PostgreSQL**: tables in a caller-chosen schema (default
  `durable_jobs`), created by embedded, append-only SQL migrations
  (`migrations/NNNN_*.sql`) that `Migrate` applies atomically under a
  transaction-scoped advisory lock and records in `schema_migrations`.
  Unknown newer versions are left in place so older replicas keep running;
  migrations within a major version are additive. Timestamps are stored as a
  microsecond `timestamptz` plus a nanosecond remainder column, unsigned
  counters as `numeric(20, 0)`, identities with the `"C"` collation so
  ordering matches Go's bytewise comparison, and headers, failures and
  snapshots as JSON.
- The PostgreSQL store lives in the nested module
  `github.com/justusschock/go-durable-jobs/backend/postgres`. Its `go.mod`
  replaces the core module with `../..` for development inside the
  repository; the replace directive does not apply to consumers. Releases tag
  the core module (`vX.Y.Z`) first, then update the nested module's
  requirement and tag `backend/postgres/vX.Y.Z` with the same version.

All stores normalize times to UTC without monotonic readings.

## Consequences

### Positive
- Exact round-trips, verified by `TestJobRoundTripPreservesRepresentation`
  and `TestSplitTimeRoundTripsNanoseconds`.
- Format mismatches fail loudly with `ErrUnsupportedFormat`.

### Negative
- PostgreSQL text cannot hold NUL bytes or invalid UTF-8. `ValidateEnqueue`
  already rejects such job identities, but worker IDs and jobs written
  directly through `Store.Create` are rejected with `ErrInvalidJob` only by
  the PostgreSQL store.
- Every model change needs a bbolt format decision and a PostgreSQL
  migration.
- Two modules must be released in order.

### Follow-ups
- A bbolt format version 2 needs a migration path and a new ADR.

## Links

- [backend/postgres/migrations](../../backend/postgres/migrations)
- [README: releasing the two modules](../../README.md#releasing-the-two-modules)
