# go-durable-jobs

`go-durable-jobs` is a Go library for durable background jobs: jobs are
persisted before anything else happens, claimed atomically, executed under
fenced leases with heartbeats, retried with bounded exponential backoff,
recovered when a worker disappears, and scheduled once or at fixed intervals.
It is for Go services that need work to survive crashes and restarts, from a
single process with an embedded database to many replicas sharing PostgreSQL.

## Features

- `DurableClient`: validated enqueue that persists the job before it publishes
  a queue wakeup, plus `Get` and `Cancel`.
- `Worker`: claims jobs under a coordination lease whose fencing token guards
  every heartbeat, completion and failure, so a worker that lost its lease can
  never commit a result. `Run` polls; `RunOne` executes at most one job.
- Retries with bounded exponential backoff (`RetryPolicy`, `RetryDelay`);
  handler panics and missing handlers are recorded as non-retryable failures.
- Cooperative cancellation of pending and running jobs.
- `NewRecoveryEngine`: returns expired attempts to their queue through a
  fenced compare-and-swap, built on `go-reconciliation`.
- `Scheduler`: one-shot and fixed-interval schedules with idempotent
  occurrence IDs and revision-fenced cursors.
- Dependency workflows (`DependsOn`): a job runs only after its prerequisites
  succeed; failed or canceled prerequisites fail their dependents transitively.
- `Catalog`: bounded, queue-scoped, cursor-paginated job listing for
  operator tools.
- Typed lifecycle `Event`s published through `go-messaging`.
- Three stores with identical semantics: in-memory, bbolt (embedded,
  crash-safe) and PostgreSQL (multi-replica), all verified by the public
  `storetest` contract suites.

## Installation

```sh
go get github.com/justusschock/go-durable-jobs@latest
# optional, PostgreSQL store (separate module, pulls in pgx):
go get github.com/justusschock/go-durable-jobs/backend/postgres@latest
```

Requires Go 1.25 or newer. The PostgreSQL store requires PostgreSQL 12 or
newer.

## Quick start

This is [`Example`](example_test.go), compiled and run by `go test`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	coordinationmemory "github.com/justusschock/go-distributed-coordination/memory"
	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/backend/memory"
	messagingmemory "github.com/justusschock/go-messaging/memory"
)

// systemClock is the production jobs.Clock: it reads the wall clock.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func main() {
	ctx := context.Background()
	store := memory.New()

	client, err := jobs.NewClient(store, messagingmemory.NewBroker[jobs.Wakeup](time.Second), systemClock{})
	if err != nil {
		panic(err)
	}
	if _, err := client.Enqueue(ctx, jobs.EnqueueRequest{
		ID:      "greet-1",
		Queue:   "default",
		Kind:    "greet",
		Payload: []byte("world"),
		Retry:   jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
	}); err != nil {
		panic(err)
	}

	registry := jobs.MapRegistry{
		"greet": func(_ context.Context, job jobs.Job) (jobs.Result, error) {
			return jobs.Result{Payload: append([]byte("hello, "), job.Payload...)}, nil
		},
	}
	worker, err := jobs.NewWorker(store, coordinationmemory.NewBackend(nil), messagingmemory.NewBroker[jobs.Event](time.Second), registry, jobs.WorkerOptions{
		WorkerID:          "worker-1",
		Queues:            []jobs.Queue{"default"},
		PollInterval:      time.Second,
		LeaseTTL:          30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		ClaimLimit:        10,
		Clock:             systemClock{},
	})
	if err != nil {
		panic(err)
	}
	// Run(ctx) polls until ctx ends; RunOne executes at most one job.
	if _, err := worker.RunOne(ctx); err != nil {
		panic(err)
	}

	job, err := client.Get(ctx, "greet-1")
	if err != nil {
		panic(err)
	}
	fmt.Println(job.State == jobs.StateSucceeded, string(job.Result)) // true hello, world
}
```

In production, use `backend/bolt` or `backend/postgres` as the store, a
distributed `go-distributed-coordination` backend for leases, and a real
`go-messaging` publisher. [`ExampleDurableClient`](example_test.go) and
[`ExampleScheduler`](example_test.go) show enqueueing and schedules.

## Packages

| Package | Purpose |
|---|---|
| `github.com/justusschock/go-durable-jobs` (package `jobs`) | Job model, `Store`/`Catalog`/`ScheduleStore` contracts, `DurableClient`, `Worker`, `Scheduler`, recovery engine, errors and events. |
| `.../backend/memory` | Mutex-backed in-memory store for tests and local use. |
| `.../backend/bolt` | Crash-safe bbolt store for single-process persistence, with a read-only inspection mode. |
| `.../backend/postgres` | PostgreSQL store for multi-process and multi-replica deployments (nested module). |
| `.../storetest` | Contract suites (`Run`, `RunCatalog`, `RunSchedule`) every store implementation must pass. |

Packages under `internal/` are not part of the API.

### bbolt store

`bolt.Open(path, bolt.Options{})` creates or opens an owner-only (`0600`)
database file and validates its schema; symlinked paths are rejected.
`bolt.Options{ReadOnly: true}` opens an existing, initialized database without
creating, initializing, chmodding or writing it. bbolt allows one writable
process per file, and a read-only handle cannot coexist with a writable one.

### PostgreSQL store

`backend/postgres` is a separate module, so only programs that use it depend
on `pgx`.

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
ownership of the pool. `Migrate` applies the embedded, versioned SQL
migrations under a transaction-scoped advisory lock, so it is safe to call
from every replica at startup; applied versions are recorded in
`<schema>.schema_migrations`. Every mutation is one read-committed transaction
that locks the affected rows, `ClaimReady` uses `FOR UPDATE SKIP LOCKED`, and
transactions aborted by deadlocks or serialization failures are retried with
bounded backoff. Timestamps keep nanosecond precision. PostgreSQL text cannot
hold NUL bytes or invalid UTF-8, so such identities are rejected with
`ErrInvalidJob`; payloads, results and headers are stored exactly.

The integration tests need a disposable database and skip without one:

```sh
cd backend/postgres
DURABLE_JOBS_POSTGRES_DSN='postgres://postgres@localhost:5432/postgres?sslmode=disable' \
  go test -race ./...
```

## Design and guarantees

- **Durability first.** `Enqueue` persists the job before publishing its
  wakeup. If publishing fails it returns the created job together with an
  error wrapping `ErrWakeupPublish`; the job stays claimable.
- **Fencing.** Each claim records the lease's fencing token and an attempt
  generation. Heartbeat, completion, failure and recovery check both
  atomically in the store and return `ErrStaleAttempt` when either is old.
  Heartbeat deadlines and `UpdatedAt` never move backward.
- **At-least-once execution.** A handler may run again after a crash or lease
  loss, so handlers should be idempotent. A worker that loses its lease
  cancels the handler and records nothing.
- **Ordering.** Within a queue, ready jobs are claimed by priority
  (descending), then `AvailableAt`, then ID.
- **Snapshots.** Every `Job`, `Schedule` and `Attempt` returned to callers is
  independently owned; mutating it never affects the store.
- **Concurrency.** Clients, workers, schedulers and all bundled stores are
  safe for concurrent use. Blocking operations take a `context.Context` and
  honour cancellation.
- **Errors.** Sentinel errors (`ErrInvalidJob`, `ErrJobExists`,
  `ErrJobNotFound`, `ErrNoReadyJob`, `ErrStaleAttempt`,
  `ErrInvalidTransition`, `ErrDependencyFailed`, `ErrWakeupPublish`, and each
  backend's `ErrUnsupportedFormat`) are wrapped with `%w`; test them with
  `errors.Is`. Errors never contain payloads, and handler error values are not
  persisted.
- **No logging.** The library reports through return values and lifecycle
  events only.
- **Catalog consistency.** Each `ListJobPage` result is one store snapshot; a
  traversal across pages is weakly consistent. Catalog results contain raw
  payloads, so redact them before display.
- **Stability.** The module is at `v0.x`: minor releases may break the API.
  Every break is listed in [CHANGELOG.md](CHANGELOG.md).

## Releasing the two modules

The repository contains two modules: the core module at the root (tags
`vX.Y.Z`) and `backend/postgres` (tags `backend/postgres/vX.Y.Z`). The
PostgreSQL module requires the core module and, for development inside this
repository, replaces it with `../..`; that replace directive has no effect on
consumers. Release them in order:

1. Tag the core module, `vX.Y.Z`.
2. In `backend/postgres/go.mod`, require `github.com/justusschock/go-durable-jobs vX.Y.Z`,
   run `go mod tidy`, and merge.
3. Tag `backend/postgres/vX.Y.Z` on that commit.

Use the same version for both modules.

## Documentation

- Requirements and design decisions: [docs/README.md](docs/README.md) (PRD and
  ADRs).
- API reference: [pkg.go.dev/github.com/justusschock/go-durable-jobs](https://pkg.go.dev/github.com/justusschock/go-durable-jobs)
  and [the PostgreSQL module](https://pkg.go.dev/github.com/justusschock/go-durable-jobs/backend/postgres).

## Contributing

Changes go through pull requests against `main`; nothing is pushed to `main`
directly. Read [AGENTS.md](AGENTS.md) first: it describes the docs-first
process, the API rules, and the checks every pull request must pass.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
