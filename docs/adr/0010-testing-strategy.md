# ADR 0010: Testing strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-JOBS-01, NFR-JOBS-03), ADR-0006, ADR-0009 |

## Context

Recorded retrospectively for the first public release. The library's
behaviour is dominated by time (poll intervals, heartbeats, lease TTLs,
retry backoff, schedules) and concurrency (handler goroutines, keep-alives,
competing workers). Tests based on real sleeps are slow and flaky; tests of
the PostgreSQL store need a database that is not always available.

## Decision drivers

- D1 Deterministic, fast tests that pass with `-race -count=3`.
- D2 Every store held to the same behaviour.
- D3 Database tests available in CI without burdening local runs.

## Considered options

1. **Real time with short intervals and generous timeouts**.
   - Pro: no special tooling.
   - Con: slow, flaky under load, cannot assert exact durations.
2. **Hand-rolled fake clocks injected everywhere, including timers**.
   - Pro: deterministic.
   - Con: every timer and ticker must go through an abstraction, including
     inside sibling libraries.
3. **`testing/synctest` bubbles for time and concurrency, injected `Clock`
   for recorded timestamps**.
   - Pro: real `time` timers run on virtual time, including inside
     dependencies; `synctest.Wait` makes goroutine coordination
     deterministic; exact durations can be asserted.
   - Con: tests in a bubble must not block on real I/O.

## Decision

We will test in layers:

- **Unit tests** per package, with small fakes for `Store`,
  `coordination.Backend`, leases and publishers where a test isolates one
  component.
- **`testing/synctest`** for everything involving time or concurrency. Worker
  tests run in bubbles; virtual-time tests run the real memory store and
  in-memory coordination backend with a `Clock` that reads `time.Now` inside
  the bubble, and assert poll cadence, retry backoff, heartbeat deadlines,
  recovery at lease expiry and schedule occurrences exactly. The PostgreSQL
  transient-retry loop is tested the same way. No test sleeps in wall-clock
  time.
- **Fixed clocks** remain where a test checks timestamp normalization or
  ordering at specific instants.
- **Contract suites** (`storetest`) run against every store.
- **Real I/O stays outside bubbles**: bbolt tests use temporary files;
  PostgreSQL tests create and drop a schema per test against the database
  named by `DURABLE_JOBS_POSTGRES_DSN` and skip when it is unset. CI runs
  them against a PostgreSQL service container.
- **Examples** (`example_test.go`) are compiled and their output checked;
  the README quick start is taken from them.
- Every test runs with the race detector in CI; `-count=3` locally before a
  pull request.
- Tests cite the requirements they verify (`// FR-JOBS-NN`).

## Consequences

### Positive
- The full suite runs in seconds and is deterministic.
- Durations such as backoff gaps are asserted exactly.

### Negative
- PostgreSQL behaviour is only covered where a database is available.
- Contributors must keep real I/O out of synctest bubbles.

### Follow-ups
- Consider fuzzing `ValidateEnqueue` and the bbolt and PostgreSQL decoders.

## Links

- [virtualtime_test.go](../../virtualtime_test.go)
