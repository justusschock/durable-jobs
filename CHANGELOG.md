# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

## [0.1.0] - 2026-10-18

First public release.

### Added

- `DurableClient` (`NewClient`): validated `Enqueue` that persists a job
  before publishing its queue wakeup, plus `Get` and `Cancel`.
- `Worker` (`NewWorker`): claims jobs under a coordination lease whose
  fencing token guards every heartbeat, completion and failure; `Run` polls
  and `RunOne` executes at most one job.
- Retries with bounded exponential backoff (`RetryPolicy`, `RetryDelay`);
  handler panics and missing handlers are recorded as non-retryable
  failures.
- Cooperative cancellation of pending and running jobs.
- `NewRecoveryEngine`: returns expired attempts to their queue through a
  fenced compare-and-swap, built on `reconciliation`.
- `Scheduler` (`NewScheduler`): one-shot and fixed-interval schedules with
  idempotent occurrence IDs and revision-fenced cursors.
- Dependency workflows through `DependsOn`: failed or canceled
  prerequisites fail their dependents transitively.
- `Catalog`: bounded, queue-scoped, cursor-paginated job listing
  (`ListJobPage`), and typed lifecycle `Event`s published through
  `messaging`.
- Stores with identical semantics: `backend/memory` and `backend/bolt`
  (crash-safe bbolt, with a read-only mode).
- `backend/postgres`: a PostgreSQL store for multi-replica deployments with
  embedded migrations (`Migrate`), released as a separate module under the
  tag `backend/postgres/v0.1.0`.
- Package `storetest`: the `Run`, `RunCatalog` and `RunSchedule` contract
  suites for store implementations.
- Requirements and architecture decision records under `docs/`.

Requires Go 1.25 or newer.

[Unreleased]: https://github.com/justusschock/durable-jobs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justusschock/durable-jobs/releases/tag/v0.1.0
