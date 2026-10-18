# PRD 0001: go-durable-jobs

| Field | Value |
|---|---|
| Status | Accepted |
| Implementation | Complete |
| Owners | Justus Perillieux |
| Related ADRs | ADR-0001 – ADR-0011 |
| Last updated | 2026-10-18 |

Recorded retrospectively for the first public release; describes the library
as implemented.

## 1. Problem and context

Go services regularly need work that outlives the request that created it:
report generation, data processing, notifications, maintenance tasks. Running
that work in a goroutine loses it on a crash or restart, and naïve database
queues lose results or run a job twice concurrently when a worker stalls and
its job is handed to another worker.

go-durable-jobs provides durable background jobs as a library: jobs are
persisted before anything else happens, claimed atomically, executed under
fenced leases so that a worker that lost ownership can never commit a result,
retried with bounded backoff, recovered when their worker disappears, and
scheduled once or at fixed intervals. It builds on three sibling libraries:
`go-distributed-coordination` (leases and fencing tokens), `go-messaging`
(wakeups and lifecycle events) and `go-reconciliation` (the recovery engine).

## 2. Users and use cases

- As a service developer, I want to enqueue a job and get it back with its
  result later so that long-running work survives restarts.
- As a service developer, I want failed jobs retried with exponential backoff
  and a bounded number of attempts so that transient failures heal without
  retry storms.
- As an operator, I want a job whose worker crashed to become runnable again
  after its lease expires, without risking two workers committing results.
- As a service developer, I want to cancel a pending or running job and have
  the running handler observe the cancellation through its context.
- As a service developer, I want recurring and one-shot schedules whose
  occurrences are enqueued exactly once even if the scheduler crashes midway.
- As a workflow author, I want a job to run only after its prerequisites
  succeed, and to fail automatically when one of them fails.
- As an operator, I want to page through a queue's jobs for inspection
  without loading the whole store.
- As a deployer, I want to choose between an in-memory store (tests), an
  embedded bbolt file (single process) and PostgreSQL (many replicas) without
  changing application code.
- As a store implementer, I want a public test suite that proves my store
  behaves like the bundled ones.

## 3. Goals and non-goals

### Goals
- G1 Durable, at-least-once job execution with fenced attempts.
- G2 Identical observable semantics across all stores, enforced by public
  contract suites.
- G3 Deterministic, injectable time for every timestamp the library records.
- G4 A small dependency footprint in the core module.

### Non-goals
- NG1 Exactly-once execution. Handlers can run again after a crash or lease
  loss; they must be idempotent.
- NG2 A job server, CLI or UI. The library is embedded in the caller's
  process.
- NG3 Cron expressions or calendar schedules. Only one-shot and fixed-interval
  schedules are supported.
- NG4 Parallel execution inside one `Worker`. Parallelism comes from running
  several workers.
- NG5 Logging or metrics. The library reports through return values and
  lifecycle events.

## 4. Functional requirements

| ID | Requirement | Priority (MUST/SHOULD/MAY) |
|---|---|---|
| FR-JOBS-01 | `DurableClient.Enqueue` validates the request (non-blank valid-UTF-8 identities without NUL, a valid retry policy, unique dependencies not naming the job itself), persists a pending job, and only then publishes a `Wakeup` on `jobs.wakeup.<queue>`. A publish failure returns the created job and an error wrapping `ErrWakeupPublish`; the job stays durable. | MUST |
| FR-JOBS-02 | Every `Job`, `EnqueueRequest`, `Result`, `Schedule` and `Attempt` crossing the API is an independently owned snapshot; mutating it never changes stored state. | MUST |
| FR-JOBS-03 | `Store.ClaimReady` atomically starts an attempt for the first claimable job of a queue — pending or retry-waiting, `AvailableAt` reached, every dependency succeeded — ordered by priority descending, `AvailableAt` ascending, ID ascending; otherwise it returns `ErrNoReadyJob`. | MUST |
| FR-JOBS-04 | Heartbeat, completion, failure and recovery check the attempt generation and fencing token atomically with the write and return `ErrStaleAttempt` for an old attempt. Heartbeat deadlines and `UpdatedAt` never move backward. | MUST |
| FR-JOBS-05 | `Worker` obtains the fencing token from a non-waiting per-queue claim lease, releases it after the claim, holds a kept-alive per-job execution lease while the handler runs, heartbeats every `HeartbeatInterval` with deadline now+`LeaseTTL`, and polls queues every `PollInterval`. If the execution lease is lost or the worker's context ends, it cancels the handler and records no terminal transition. | MUST |
| FR-JOBS-06 | A handler success completes the job; a handler error records a retryable `handler_error` failure, retried after `RetryDelay` (bounded exponential backoff) until `MaxAttempts`; a panic records a non-retryable `handler_panic`; a missing handler records `handler_not_found`. | MUST |
| FR-JOBS-07 | Cancellation cancels pending and retry-waiting jobs immediately, marks running jobs so the worker cancels the handler's context and the attempt ends canceled, and rejects terminal jobs with `ErrInvalidTransition`. | MUST |
| FR-JOBS-08 | `NewRecoveryEngine` returns running attempts whose heartbeat deadline has passed to pending (or canceled if requested) through a fenced compare-and-swap on attempt ID, fencing token and observed deadline, in deterministic order and bounded by `RecoverySpec.Limit`. Obsolete observations are idempotent successes. | MUST |
| FR-JOBS-09 | `Scheduler.RunDue` enqueues each due schedule occurrence with ID `<schedule-id>/<due-unix-nano>` and `AvailableAt` at its due time, then advances interval schedules to the first due time strictly after now and disables one-shot schedules. Duplicate occurrences are tolerated, an enqueue failure does not advance the schedule, and `ScheduleStore` updates are revision-fenced. | MUST |
| FR-JOBS-10 | Jobs with `DependsOn` are claimable only after all dependencies succeed. When a dependency fails or is canceled, pending dependents fail transitively with code `dependency_failed`. `ValidateDependencies` rejects missing or failed prerequisites before submission. | MUST |
| FR-JOBS-11 | `Catalog.ListJobPage` returns one queue-scoped page in ascending bytewise `JobID` order with an exclusive `After` cursor and `Limit` between 1 and `MaxJobPageSize`; each page is an owned store snapshot and traversal across pages is weakly consistent. | MUST |
| FR-JOBS-12 | Workers publish typed lifecycle `Event`s (`started`, `heartbeat`, `succeeded`, `retry_scheduled`, `failed`, `canceled`) on `jobs.event.<kind>`. | SHOULD |
| FR-JOBS-13 | `Worker.RunOne` scans the configured queues once, in order, and executes at most one job, reporting whether a claim succeeded even when execution fails afterwards. | MUST |
| FR-JOBS-14 | Constructors reject nil and typed-nil dependencies and invalid worker options with `ErrInvalidJob`. | MUST |
| FR-JOBS-15 | The bbolt store persists jobs, attempts and schedules crash-safely in an owner-only regular file, rejects symlinks and incomplete schemas, offers a read-only mode that never writes, and rejects unknown format versions with `ErrUnsupportedFormat`. | MUST |
| FR-JOBS-16 | The PostgreSQL store (separate module) provides the same semantics for any number of processes sharing one schema, never double-claims a job, applies embedded versioned migrations idempotently and concurrently, and rejects text PostgreSQL cannot store with `ErrInvalidJob`. | MUST |
| FR-JOBS-17 | The public `storetest` package provides `Run`, `RunCatalog` and `RunSchedule` contract suites, and every bundled store passes them. | MUST |
| FR-JOBS-18 | `Store.ListJobs` returns up to `Limit` jobs of one queue, most recently updated first. | SHOULD |
| FR-JOBS-19 | Recorded times are normalized to UTC without monotonic readings, keep nanosecond precision in every store, and preserve the zero time. | MUST |

## 5. Non-functional requirements

| ID | Requirement | Measure |
|---|---|---|
| NFR-JOBS-01 | All exported types are safe for concurrent use as documented; concurrent workers never execute one job twice at the same time. | `go test -race` over all packages; `TestWorkerConcurrentRunsDoNotDoubleExecute` |
| NFR-JOBS-02 | Error messages never contain payloads, results or headers; handler error values are not persisted. | `TestJobErrorsDoNotContainPayloads` |
| NFR-JOBS-03 | Tests are deterministic: time-dependent tests run in `testing/synctest` bubbles and never sleep in wall-clock time. | `go test -race -count=3 ./...` |
| NFR-JOBS-04 | The core module depends only on the three sibling libraries and bbolt; SQL drivers are confined to the `backend/postgres` module. | `go.mod` of each module |
| NFR-JOBS-05 | Supported Go versions: 1.25 (floor) and the two latest upstream releases. | `TestModuleCompatibilityPolicy`; CI matrix |
| NFR-JOBS-06 | The bbolt database file is owner-only, and read-only opens refuse files readable by others. | `TestDatabaseFileMode`, `TestReadOnlyOwnerModes` |
| NFR-JOBS-07 | PostgreSQL transactions aborted by deadlocks or serialization failures are retried a bounded number of times with capped exponential backoff, and the retry wait honours cancellation. | `TestRetryTransient*` |

## 6. API sketch

```go
package jobs

type Clock interface{ Now() time.Time }

type Store interface {
	Create(context.Context, Job) (Job, error)
	Get(context.Context, JobID) (Job, error)
	ListJobs(context.Context, ListJobsRequest) ([]Job, error)
	ClaimReady(context.Context, ClaimRequest) (Attempt, error)
	Heartbeat(context.Context, HeartbeatRequest) error
	Complete(context.Context, CompleteRequest) (Job, error)
	Fail(context.Context, FailRequest) (Job, error)
	RequestCancel(context.Context, JobID, time.Time) (Job, error)
	RecoverAttempt(context.Context, RecoverAttemptRequest) (Job, error)
	ListRecoveryCandidates(context.Context, time.Time, int) ([]RecoveryCandidate, error)
}
type Catalog interface {
	ListJobPage(context.Context, ListJobPageRequest) (JobPage, error)
}
type ScheduleStore interface { /* PutSchedule, GetSchedule, ListDueSchedules, AdvanceSchedule */ }

func NewClient(Store, messaging.Publisher[Wakeup], Clock) (*DurableClient, error)
func (*DurableClient) Enqueue(context.Context, EnqueueRequest) (Job, error)
func (*DurableClient) Get(context.Context, JobID) (Job, error)
func (*DurableClient) Cancel(context.Context, JobID) error

type Handler func(context.Context, Job) (Result, error)
type MapRegistry map[string]Handler
func NewWorker(Store, coordination.Backend, messaging.Publisher[Event], Registry, WorkerOptions) (*Worker, error)
func (*Worker) Run(context.Context) error
func (*Worker) RunOne(context.Context) (bool, error)

func NewRecoveryEngine(Store, Clock, reconciliation.Policy) (*reconciliation.Engine[RecoverySpec, RecoveryObserved, RecoveryAction, RecoveryAction], error)
func NewScheduler(ScheduleStore, Client, Clock) *Scheduler
func (*Scheduler) RunDue(context.Context, int) (int, error)
func RetryDelay(RetryPolicy, uint) time.Duration
func ValidateDependencies(context.Context, Store, []JobID) error

// backend/memory: New() *Store
// backend/bolt: Open(path string, Options) (*Store, error)
// backend/postgres: Open(ctx, *pgxpool.Pool, Options) (*Store, error); (*Store).Migrate(ctx) error
// storetest: Run, RunCatalog, RunSchedule
```

## 7. Acceptance criteria

`storetest` subtests run under each store's `TestStoreContract`,
`TestCatalogContract` and `TestScheduleStoreContract`; they are cited as
`storetest.Run: "<subtest>"`.

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| Enqueue validates first, persists before publishing, and keeps the job when publishing fails | FR-JOBS-01 | `TestValidateEnqueue`, `TestClientEnqueueValidatesBeforeCreateAndPublish`, `TestClientEnqueuePersistsBeforePublishing`, `TestClientEnqueuePublishFailureLeavesDurableJob` |
| Snapshots are independently owned | FR-JOBS-02 | `TestJobAndRequestClonesOwnNestedState`, `TestClonesPreserveNilState`, `TestClientEnqueueNormalizesAvailableAtAndOwnsResources`, `storetest.Run: "create is idempotently rejected"` |
| Claims follow priority, availability and ID order | FR-JOBS-03 | `storetest.Run: "claim follows deterministic order"` |
| Fencing and monotonic deadlines | FR-JOBS-04 | `storetest.Run: "heartbeat requires current fence"`, `storetest.Run: "heartbeat deadline never moves backward"`, `storetest.Run: "updated at never moves backward"`, `TestWorkerHeartbeatsKeepLongAttemptAliveInVirtualTime` |
| Worker leases, heartbeats, polling and abort on lease loss | FR-JOBS-05 | `TestWorkerRunOneHeartbeatsAndReleasesExecutionLease`, `TestWorkerSuccessfulHandlerCompletesWithClaimFenceAndReleasesQueueLease`, `TestWorkerLeaseDoneCancelsBlockedHandlerWithoutTerminalTransition`, `TestWorkerRunOneReportsClaimWhenExecutionAborts`, `TestWorkerRunPollsOncePerPollIntervalInVirtualTime` |
| Handler outcomes and bounded exponential retry | FR-JOBS-06 | `TestWorkerRunOneHandlesSuccessErrorAndPanic`, `TestWorkerHandlerErrorSchedulesRetryWithBoundedDelay`, `TestWorkerPanicRecordsNonRetryableHandlerPanicFailure`, `TestRetryDelayUsesBaseDelayForFirstAttempt`, `TestRetryDelayCapsExponentialBackoff`, `TestRetryDelaySaturatesWithoutOverflow`, `TestWorkerRunRetriesWithExponentialBackoffInVirtualTime`, `storetest.Run: "retry returns to pending"` |
| Cancellation of pending and running jobs | FR-JOBS-07 | `TestClientCancelPendingAndRunning`, `TestWorkerCancellationPollingCancelsHandler`, `TestWorkerSuccessfulHandlerPublishesCanceledWhenCompleteReturnsCanceled`, `storetest.Run: "cancel pending"`, `storetest.Run: "request cancel running"` |
| Fenced recovery of expired attempts | FR-JOBS-08 | `TestRecoveryObserveUsesUTCNowAndUnboundedRecoveryList`, `TestRecoveryPlanFiltersLimitsSortsAndCarriesObservedFence`, `TestRecoveryPlanRejectsNonPositiveLimit`, `TestRecoveryApplySendsExactRecoverAttemptRequest`, `TestRecoveryApplyTreatsObsoleteActionsAsIdempotentSuccess`, `TestRecoveryUndoIsNoOp`, `TestRecoveryEngineRecoversAttemptAfterLeaseExpiryInVirtualTime`, `storetest.Run: "expired attempt recovery"`, `storetest.Run: "recovery before expiry"`, `storetest.Run: "recovery requires current fence"`, `storetest.Run: "heartbeat invalidates listed recovery"` |
| Idempotent, revision-fenced schedules | FR-JOBS-09 | `TestSchedulerAdvancesFromPreviousDueTime`, `TestSchedulerOneShotDisablesAfterEnqueue`, `TestSchedulerAdvancesAfterIdempotentOccurrenceConflict`, `TestSchedulerDoesNotAdvanceAfterEnqueueFailure`, `TestSchedulerLoopEnqueuesOneOccurrencePerIntervalInVirtualTime`, `TestScheduleStoreContract`, `TestBoltScheduleSurvivesReopen` |
| Dependency gating and transitive failure | FR-JOBS-10 | `TestValidateDependencies`, `storetest.Run: "dependencies gate readiness"`, `storetest.Run: "terminal dependency failure propagates"`, `storetest.Run: "terminal dependency cancellation propagates"`, `TestDependencyFailurePropagatesAcrossWideFanOut` (PostgreSQL) |
| Catalog pagination | FR-JOBS-11 | `TestCatalogContract`, `TestCatalogPersistsAcrossReopen`, `TestCatalogScansSparseQueueEntries`, `TestCatalogRejectsMismatchedBucketKeyAndJobID`, `TestListJobPageRejectsMismatchedMapKey` |
| Lifecycle events | FR-JOBS-12 | `TestWorkerSuccessfulHandlerCompletesWithClaimFenceAndReleasesQueueLease`, `TestWorkerSuccessfulHandlerPublishesCanceledWhenCompleteReturnsCanceled`, `TestWorkerRunRetriesWithExponentialBackoffInVirtualTime` |
| RunOne executes at most one job | FR-JOBS-13 | `TestWorkerRunOneReturnsFalseWhenNoJobIsReady`, `TestWorkerRunOneUsesQueueOrderAndExecutesAtMostOneJob`, `TestWorkerRunOneRejectsCanceledContextBeforeClaiming`, `TestWorkerRunOneReportsClaimAfterPostClaimError` |
| Constructors reject nil and invalid dependencies | FR-JOBS-14 | `TestClientConstructorRejectsNilDependencies`, `TestWorkerConstructorRejectsNilAndInvalidOptions`, `TestNewRecoveryEngineRejectsNilDependenciesAndReturnsFourTypeEngine` |
| bbolt durability, safety and read-only mode | FR-JOBS-15 | `TestPersistsAcrossReopen`, `TestEnqueueRunRetryRecoverAndComplete`, `TestOpenRejectsDatabaseSymlink`, `TestOpenRejectsIncompleteSchema`, `TestReadOnlyReadsWithoutChangingDatabase`, `TestReadOnlyMutatorsReturnDatabaseReadOnly`, `TestBoltRejectsUnknownEnvelopeVersion` |
| PostgreSQL multi-replica semantics | FR-JOBS-16 | `TestMigrateIsIdempotentAndConcurrent`, `TestStoreRequiresMigration`, `TestJobRoundTripPreservesRepresentation`, `TestCreateRejectsTextPostgresCannotStore`, `TestConcurrentClaimersAcrossPoolsNeverDoubleClaim`, `TestClaimSkipsRowsLockedByConcurrentTransactions`, `TestLoadMigrationsAreOrderedAndVersioned` |
| Public contract suites pass for every store | FR-JOBS-17 | `TestStoreContract`, `TestCatalogContract`, `TestScheduleStoreContract` (memory, bolt, postgres) |
| Recent-first listing | FR-JOBS-18 | `storetest.Run: "list jobs filters orders bounds and clones"` |
| Time normalization and precision | FR-JOBS-19 | `TestPersistedTimePreservesTimesOutsideUnixNanoRange`, `TestPersistedTimePreservesZero`, `TestSplitTimeRoundTripsNanoseconds` |
| Concurrency safety | NFR-JOBS-01 | `TestWorkerConcurrentRunsDoNotDoubleExecute`, `TestConcurrentClaimersAcrossPoolsNeverDoubleClaim` |
| No payloads in errors | NFR-JOBS-02 | `TestJobErrorsDoNotContainPayloads` |
| Go floor | NFR-JOBS-05 | `TestModuleCompatibilityPolicy` |
| Owner-only database file | NFR-JOBS-06 | `TestDatabaseFileMode`, `TestReadOnlyOwnerModes` |
| Bounded transient retries | NFR-JOBS-07 | `TestRetryTransientBacksOffExponentiallyUntilSuccess`, `TestRetryTransientCapsBackoffAndBoundsAttempts`, `TestRetryTransientReturnsPermanentErrorsImmediately`, `TestRetryTransientStopsWhenContextEndsDuringBackoff` |

## 8. Source references

None.

## 9. Open questions

- Q1 `EventEnqueued` and `EventRecovered` are declared but never published:
  the client publishes a `Wakeup` rather than an event, and the recovery
  engine publishes nothing. Either publish them or document them as
  reserved permanently.
- Q2 Handler error values are discarded (only the code `handler_error` is
  stored). Should handlers be able to return a classified, sanitized
  `Failure` (for example non-retryable errors)?
- Q3 `Worker` executes one job at a time. Is an in-worker concurrency option
  worth its complexity, or should callers keep running several workers?
- Q4 Should `NewRecoveryEngine` accept `reconciliation.EngineOption`s, so
  callers need not construct `RecoveryAdapter` themselves to pass options?
