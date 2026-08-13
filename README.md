# go-durable-jobs

`go-durable-jobs` provides durable job primitives for Go:
atomic queue claims, fenced execution attempts, heartbeats, retries,
cancellation, crash recovery, one-shot and fixed-interval schedules, dependency
workflows, and typed lifecycle events.

The module includes a mutex-backed memory store for tests and local use and a
crash-safe bbolt store for persistent workloads. Public job payloads, results,
headers, schedules, and dependency lists are independently owned snapshots.

Module path: `github.com/justusschock/go-durable-jobs`

## Install the private module

Version `v0.5.0` requires Go 1.22 or newer. Every private sibling dependency is
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
go get github.com/justusschock/go-durable-jobs@v0.5.0
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

Both included stores implement the optional `Catalog` interface for offline or
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

## Schedules and workflows

Both included stores implement `ScheduleStore`. A zero interval creates a
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

The core module is independent of product resource schemas, caches, and
object storage. Its direct platform dependencies are:

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
pre-commit run --all-files
```

Implementation plans live under `docs/` and are intentionally excluded from
Git. They must never be committed.

## License

Apache License 2.0. See [LICENSE](LICENSE).
