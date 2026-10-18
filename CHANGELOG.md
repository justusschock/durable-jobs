# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

The repository holds two modules, released with the same version: the core
module (tags `vX.Y.Z`) and `backend/postgres` (tags
`backend/postgres/vX.Y.Z`). Entries apply to both unless marked.

## [Unreleased]

Preparation for the first public release.

### Changed

- Public API boundary reviewed and recorded in ADR-0008. No package was
  moved and no identifier was unexported or removed: every exported
  identifier is either used by known consumers or needed by store
  implementers. `internal/storestate` stays internal.
- `backend/postgres`: errors wrapping `ErrUnsupportedFormat` and
  `jobs.ErrInvalidJob` now also wrap the underlying decode or driver error
  with `%w`, so `errors.As` can reach it. `errors.Is` results are unchanged.
- Minimum Go version is 1.25 (both modules), required by `testing/synctest`
  (ADR-0009). CI tests Go 1.25, oldstable and stable.
- Tests that involve time or concurrency run in `testing/synctest` bubbles;
  new virtual-time tests cover poll cadence, retry backoff, heartbeats,
  lease expiry and recovery, and interval schedules.
- CI no longer needs private-module credentials; actions are pinned to
  commit SHAs; golangci-lint and Dependabot were added.

### Added

- Package documentation for every public identifier, a runnable end-to-end
  `Example`, `NOTICE`, `AGENTS.md`, and `docs/` with the retrospective PRD
  and ADRs.

### Removed

- Tests that enforced private-module release policy (sibling versions pinned
  to `v0.2.0`, private-module CI credentials, README install text).
- The pre-commit configuration; the checks are listed in `AGENTS.md` and run
  in CI.

## [0.7.0]

Last release before the public preparation. Includes the PostgreSQL backend
module (`backend/postgres/v0.7.0`), the public `storetest` contract suites,
`Worker.RunOne`, the read-only job catalog, durable schedules and
dependency workflows, fenced recovery, and the memory and bbolt stores.

[Unreleased]: https://github.com/justusschock/durable-jobs/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/justusschock/durable-jobs/releases/tag/v0.7.0
