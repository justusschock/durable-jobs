# go-durable-jobs

`go-durable-jobs` provides durable job primitives for Go:
atomic queue claims, fenced execution attempts, heartbeats, retries,
cancellation, crash recovery, one-shot and fixed-interval schedules, dependency
workflows, and typed lifecycle events.

The module includes a mutex-backed memory store for tests and local use and a
crash-safe bbolt store for persistent single-process workloads. The nested
`backend/postgres` module adds a PostgreSQL store for multi-process and
multi-replica deployments. Public job payloads, results,
headers, schedules, and dependency lists are independently owned snapshots.

Module path: `github.com/justusschock/go-durable-jobs`

## Install the private module

Version `v0.7.0` requires Go 1.25 or newer. Every private sibling dependency is
pinned to `v0.2.0`.

You need access to this repository and its private dependencies. Configure a
Git credential helper or SSH agent; do not place access tokens in Git URLs,
source files, `go env`, or shell history. GitHub CLI can configure Git's
credential helper after browser authentication:

```bash
gh auth login --hostname github.com --git-protocol https --web
gh auth setup-git --hostname github.com
```

Tell Go that the module family is private, then install this release:

```bash
go env -w GOPRIVATE='github.com/justusschock/*'
go env -w GONOSUMDB='github.com/justusschock/*'
go get github.com/justusschock/go-durable-jobs@v0.7.0
```

## Core flow

Create a `DurableClient` with a `Store`, a typed wakeup publisher, and a
deterministic `Clock`. `Enqueue` validates and persists the job before it
publishes a queue wakeup. A publish failure never erases the durable job.

Workers use `go-distributed-coordination` leases to obtain fencing tokens.
Every heartbeat, completion, failure, and recovery operation checks the current
attempt and token atomically in the store. Handler inputs and outputs are
cloned, and losing a lease prevents a stale worker from committing a result.

Use `NewRecoveryEngine` for deterministic recovery of expired attempts through
`go-reconciliation`. The recovery action retains the observed attempt ID,
fencing token, and heartbeat deadline, so a later heartbeat invalidates a stale
recovery plan.

## Catalog and read-only maintenance

All included stores implement the optional `Catalog` interface for offline or
low-volume operator maintenance. `ListJobPage` returns one queue-scoped page in
ascending bytewise order of raw `JobID` strings. `After` is an exclusive cursor;
pass the returned `NextAfter` into the next request. `Limit` must be between 1
and `MaxJobPageSize` (`100`).

Each page is an independently owned store snapshot. A traversal across multiple
pages is not a point-in-time snapshot when jobs are created or updated
concurrently: later pages can observe newer state, and jobs inserted before the
last cursor may be skipped. The returned `Job` values contain raw payload,
result, header, failure, and dependency data, so project and redact them before
displaying or exporting catalog output. The bbolt catalog walks the ordered jobs
bucket and filters by queue; sparse queues can scan much of the database.

For bbolt inspection tools, `bolt.Options{ReadOnly: true}` opens an existing,
initialized database without creating, initializing, chmodding, or writing the
file. The path must already be an owner-only regular file, and schema validation
runs in a read-only transaction. A read-only bbolt handle cannot coexist with a
writable bbolt handle for the same file; close writers first or inspect a copy.

## PostgreSQL backend

`github.com/justusschock/go-durable-jobs/backend/postgres` is a separate
module, so only programs that use it depend on `pgx`. It requires Go 1.25 and
PostgreSQL 12 or newer, and is released in lockstep with the core module under
`backend/postgres/vX.Y.Z` tags; use the same version for both modules.

```bash
go get github.com/justusschock/go-durable-jobs@v0.7.0
go get github.com/justusschock/go-durable-jobs/backend/postgres@v0.7.0
```

```go
pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	return err
}
store, err := postgres.Open(ctx, pool, postgres.Options{Schema: "durable_jobs"})
if err != nil {
	return err
}
if err := store.Migrate(ctx); err != nil {
	return err
}
```

`Open` checks connectivity but never changes the database; the caller keeps
ownership of the pool. `Options.Schema` defaults to `durable_jobs`; distinct
schemas in one database are independent stores. `*postgres.Store` implements
`Store`, `Catalog`, and `ScheduleStore`.

### Migrations

`Migrate` creates the schema if needed and applies the versioned SQL files
embedded from `backend/postgres/migrations`. Applied versions are recorded in
`<schema>.schema_migrations`, so repeated calls are no-ops. Concurrent callers
in any number of processes serialize on a transaction-scoped advisory lock, and
all pending migrations commit atomically or not at all. A replica that finds
versions it does not know leaves them in place, which keeps older replicas
running during a rolling upgrade; migrations within a major version are
additive. Run `Migrate` at startup from every replica, or once from a deploy
step with a role that may create objects in the schema.

### Multi-replica guarantees

The store passes the same `storetest` contract suites as the memory and bbolt
stores: dependency gating, transitive `dependency_failed` propagation, priority
ordering, retries, fenced heartbeats and terminal transitions, cancellation,
recovery candidates, and catalog pagination behave identically. Any number of
replicas can share one schema:

- Every mutation is one read-committed transaction that locks the affected job
  rows (`SELECT ... FOR UPDATE`). Heartbeat, completion, failure, cancellation,
  and recovery of one job are therefore serialized across processes, and the
  fencing-token and attempt-generation checks run under that lock.
- `ClaimReady` selects the next ready job with `FOR UPDATE SKIP LOCKED`. Two
  claimers never receive the same job and never block on each other; a claimer
  takes the next job in priority order while another transaction holds the
  head of the queue, so claim order is strict only per job, not globally
  linearizable across concurrent claimers.
- Attempt IDs come from a database sequence and are unique across processes.
- Dependency propagation locks pending dependents in job ID order.
  Transactions that PostgreSQL aborts for deadlocks or serialization failures
  are retried with bounded exponential backoff.

Timestamps keep full nanosecond precision (a `timestamptz` plus a nanosecond
remainder column), and unsigned counters use `numeric(20, 0)`. PostgreSQL text
cannot contain NUL bytes or invalid UTF-8, so `Create` rejects such job IDs,
queues, kinds, dependency IDs, and worker IDs with `ErrInvalidJob`; payloads,
results, headers, and failures are stored exactly. Attempt rows are retained
for audit, as in the bbolt store.

Run the backend tests against a disposable database; each test creates and
drops its own schema, and the tests skip when the variable is unset:

```bash
cd backend/postgres
DURABLE_JOBS_POSTGRES_DSN='postgres://postgres@localhost:5432/postgres?sslmode=disable' \
  GOWORK=off go test -race ./...
```

## Store contract suites

The public `storetest` package exposes the behavioral contracts every store
must satisfy: `storetest.Run` for `Store`, `storetest.RunCatalog` for
`Catalog`, and `storetest.RunSchedule` for `ScheduleStore`. Call them from a
custom store's tests with a factory that returns a fresh, empty store.

## Schedules and workflows

All included stores implement `ScheduleStore`. A zero interval creates a
one-shot schedule; a positive interval creates a fixed-interval schedule.
`Scheduler.RunDue` uses occurrence IDs of the form
`<schedule-id>/<due-unix-nano>`, making a crash between enqueue and cursor
advance idempotent. Missed intervals advance from the prior due time to the
first time strictly after the scheduler clock.

Jobs may name prerequisite job IDs in `DependsOn`. A job becomes claimable only
after every dependency succeeds. Failed or canceled dependencies atomically and
transitively fail pending dependents with the `dependency_failed` code.
`ValidateDependencies` rejects missing or already failed prerequisites before a
caller submits a workflow job.

## Executable examples

The repository contains complete, compile-checked examples:

- [`ExampleDurableClient`](example_test.go) enqueues and reads a durable job.
- [`ExampleScheduler`](example_test.go) persists and runs a fixed-interval
  schedule occurrence.

Run them directly:

```bash
go test -run '^ExampleDurableClient$' .
go test -run '^ExampleScheduler$' .
```

## Dependency boundaries

The core module is independent of product resource schemas, caches,
object storage, and SQL drivers; `pgx` is required only by the nested
`backend/postgres` module. Its direct platform dependencies are:

- `go-distributed-coordination v0.2.0`
- `go-messaging v0.2.0`
- `go-reconciliation v0.2.0`
- `bbolt v1.3.11`

Optional integrations must remain one-way: concrete adapters import these
neutral contracts, never the reverse.

## Development

Run the repository gates with a standalone module context:

```bash
GOWORK=off go mod verify
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
(cd backend/postgres && GOWORK=off go vet ./... && GOWORK=off go test -race ./...)
pre-commit run --all-files
```

Implementation plans live under `docs/` and are intentionally excluded from
Git. They must never be committed.

## License

Apache License 2.0. See [LICENSE](LICENSE).
