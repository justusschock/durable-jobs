package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
	pgjobs "github.com/justusschock/go-durable-jobs/backend/postgres"
	"github.com/justusschock/go-durable-jobs/storetest"
)

// dsnVariable names the connection string for an empty, disposable test
// database. Tests skip when it is unset.
const dsnVariable = "DURABLE_JOBS_POSTGRES_DSN"

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) jobs.Store {
		t.Helper()
		return newStore(t)
	})
}

func TestCatalogContract(t *testing.T) {
	storetest.RunCatalog(t, func(t *testing.T) (jobs.Store, jobs.Catalog) {
		t.Helper()
		store := newStore(t)
		return store, store
	})
}

func TestScheduleStoreContract(t *testing.T) {
	storetest.RunSchedule(t, func(t *testing.T) jobs.ScheduleStore {
		t.Helper()
		return newStore(t)
	})
}

func TestOpenValidation(t *testing.T) {
	ctx := context.Background()
	if _, err := pgjobs.Open(ctx, nil, pgjobs.Options{}); err == nil {
		t.Fatal("Open(nil pool) error = nil, want non-nil")
	}
	pool := testPool(t)
	for _, schema := range []string{" ", "bad\x00schema", string([]byte{0xff}), string(make([]byte, 64))} {
		if _, err := pgjobs.Open(ctx, pool, pgjobs.Options{Schema: schema}); err == nil {
			t.Fatalf("Open(schema %q) error = nil, want non-nil", schema)
		}
	}
	store, err := pgjobs.Open(ctx, pool, pgjobs.Options{})
	if err != nil {
		t.Fatalf("Open(default schema) error = %v", err)
	}
	if store.Schema() != pgjobs.DefaultSchema {
		t.Fatalf("Schema() = %q, want %q", store.Schema(), pgjobs.DefaultSchema)
	}
}

func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	pool := testPool(t)
	schema := uniqueSchema(t, pool)
	stores := make([]*pgjobs.Store, 4)
	for index := range stores {
		store, err := pgjobs.Open(context.Background(), pool, pgjobs.Options{Schema: schema})
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		stores[index] = store
	}
	var group sync.WaitGroup
	errs := make(chan error, len(stores))
	for _, store := range stores {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- store.Migrate(context.Background())
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Migrate() error = %v", err)
		}
	}
	if err := stores[0].Migrate(context.Background()); err != nil {
		t.Fatalf("repeated Migrate() error = %v", err)
	}
	rows, err := pool.Query(context.Background(), "SELECT version FROM "+pgx.Identifier{schema, "schema_migrations"}.Sanitize()+" ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(versions, []int32{1}) {
		t.Fatalf("recorded migrations = %v, want [1]", versions)
	}
}

func TestStoreRequiresMigration(t *testing.T) {
	pool := testPool(t)
	schema := uniqueSchema(t, pool)
	store, err := pgjobs.Open(context.Background(), pool, pgjobs.Options{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "missing"); err == nil || errors.Is(err, jobs.ErrJobNotFound) {
		t.Fatalf("Get() before Migrate() error = %v, want schema error", err)
	}
}

func TestJobRoundTripPreservesRepresentation(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	full := jobs.Job{
		ID: "full", Queue: "queue", Kind: "kind", Payload: []byte{0, 1, 2}, Headers: map[string]string{"a": "b", "nul": "x\x00y"},
		Priority: math.MinInt64, State: jobs.StateRetryWaiting,
		CreatedAt:   time.Unix(100, 1).In(time.FixedZone("east", 3600)),
		AvailableAt: time.Unix(101, 999_999_999),
		UpdatedAt:   time.Date(1, time.January, 1, 0, 0, 0, 7, time.UTC),
		Attempt:     math.MaxUint,
		Retry:       jobs.RetryPolicy{MaxAttempts: math.MaxUint, InitialBackoff: 1, MaxBackoff: math.MaxInt64, Multiplier: math.Inf(1)},
		DependsOn:   []jobs.JobID{"z", "a"}, CancellationRequested: true, Result: []byte("result"),
		LastFailure: &jobs.Failure{Code: "code", Message: "message", Retryable: true},
	}
	empty := jobs.Job{
		ID: "empty", Queue: "queue", Payload: []byte{}, Headers: map[string]string{}, DependsOn: []jobs.JobID{},
		Result: []byte{}, LastFailure: &jobs.Failure{},
	}
	zero := jobs.Job{ID: "zero", Queue: "queue"}
	for _, job := range []jobs.Job{full, empty, zero} {
		created, err := store.Create(ctx, job)
		if err != nil {
			t.Fatalf("Create(%q) error = %v", job.ID, err)
		}
		got, err := store.Get(ctx, job.ID)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", job.ID, err)
		}
		if !reflect.DeepEqual(got, created) {
			t.Fatalf("Get(%q) = %#v, want created %#v", job.ID, got, created)
		}
		want := job.Clone()
		want.CreatedAt, want.AvailableAt, want.UpdatedAt = persisted(job.CreatedAt), persisted(job.AvailableAt), persisted(job.UpdatedAt)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Get(%q) = %#v, want %#v", job.ID, got, want)
		}
	}
}

func TestCreateRejectsTextPostgresCannotStore(t *testing.T) {
	store := newStore(t)
	for _, job := range []jobs.Job{
		{ID: "nul\x00id", Queue: "queue"},
		{ID: "invalid-utf8", Queue: jobs.Queue([]byte{0xff})},
		{ID: "nul-dependency", Queue: "queue", DependsOn: []jobs.JobID{"a\x00b"}},
	} {
		if _, err := store.Create(context.Background(), job); !errors.Is(err, jobs.ErrInvalidJob) {
			t.Fatalf("Create(%q) error = %v, want ErrInvalidJob", job.ID, err)
		}
	}
}

func TestConcurrentClaimersAcrossPoolsNeverDoubleClaim(t *testing.T) {
	pool := testPool(t)
	schema := uniqueSchema(t, pool)
	first := openMigrated(t, pool, schema)
	const jobCount = 120
	for index := range jobCount {
		job := contractJob(jobs.JobID(fmt.Sprintf("job-%03d", index)), "queue")
		job.Priority = index % 7
		if _, err := first.Create(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}

	// Separate pools model separate processes sharing one schema.
	const processes, workersPerProcess = 3, 4
	var (
		mu      sync.Mutex
		claimed = make(map[jobs.JobID]int)
		group   sync.WaitGroup
		errs    = make(chan error, processes*workersPerProcess)
	)
	var token sync.Mutex
	nextToken := coordination.FencingToken(0)
	for process := range processes {
		store := openMigrated(t, separatePool(t), schema)
		for worker := range workersPerProcess {
			group.Add(1)
			go func() {
				defer group.Done()
				workerID := fmt.Sprintf("process-%d-worker-%d", process, worker)
				for {
					token.Lock()
					nextToken++
					fence := nextToken
					token.Unlock()
					now := time.Unix(1_000, 0)
					attempt, err := store.ClaimReady(context.Background(), jobs.ClaimRequest{
						Queue: "queue", WorkerID: workerID, Now: now, HeartbeatDeadline: now.Add(time.Minute), FencingToken: fence,
					})
					if errors.Is(err, jobs.ErrNoReadyJob) {
						return
					}
					if err != nil {
						errs <- err
						return
					}
					mu.Lock()
					claimed[attempt.Job.ID]++
					mu.Unlock()
					if _, err := store.Complete(context.Background(), jobs.CompleteRequest{
						AttemptID: attempt.ID, FencingToken: fence, CompletedAt: now.Add(time.Second),
					}); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("worker error = %v", err)
	}
	if len(claimed) != jobCount {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), jobCount)
	}
	for id, count := range claimed {
		if count != 1 {
			t.Fatalf("job %q claimed %d times, want 1", id, count)
		}
	}
	listed, err := first.ListJobs(context.Background(), jobs.ListJobsRequest{Queue: "queue", Limit: 2 * jobCount})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != jobCount {
		t.Fatalf("ListJobs() returned %d jobs, want %d", len(listed), jobCount)
	}
	for _, job := range listed {
		if job.State != jobs.StateSucceeded || job.Attempt != 1 {
			t.Fatalf("job %q = state %v attempt %d, want succeeded once", job.ID, job.State, job.Attempt)
		}
	}
}

func TestClaimSkipsRowsLockedByConcurrentTransactions(t *testing.T) {
	pool := testPool(t)
	schema := uniqueSchema(t, pool)
	store := openMigrated(t, pool, schema)
	ctx := context.Background()
	high := contractJob("high", "queue")
	high.Priority = 10
	for _, job := range []jobs.Job{high, contractJob("low", "queue")} {
		if _, err := store.Create(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT 1 FROM "+pgx.Identifier{schema, "jobs"}.Sanitize()+" WHERE id = 'high' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}

	claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	now := time.Unix(1_000, 0)
	attempt, err := store.ClaimReady(claimCtx, jobs.ClaimRequest{Queue: "queue", WorkerID: "w", Now: now, HeartbeatDeadline: now.Add(time.Minute), FencingToken: 1})
	if err != nil {
		t.Fatalf("ClaimReady() with locked head error = %v", err)
	}
	if attempt.Job.ID != "low" {
		t.Fatalf("ClaimReady() = %q, want the unlocked low-priority job", attempt.Job.ID)
	}
}

func TestDependencyFailurePropagatesAcrossWideFanOut(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	if _, err := store.Create(ctx, contractJob("root", "queue")); err != nil {
		t.Fatal(err)
	}
	unrelated := contractJob("unrelated", "queue")
	unrelated.DependsOn = []jobs.JobID{"missing"}
	if _, err := store.Create(ctx, unrelated); err != nil {
		t.Fatal(err)
	}
	for layer := range 3 {
		for index := range 5 {
			job := contractJob(jobs.JobID(fmt.Sprintf("layer-%d-%d", layer, index)), "queue")
			if layer == 0 {
				job.DependsOn = []jobs.JobID{"root"}
			} else {
				job.DependsOn = []jobs.JobID{jobs.JobID(fmt.Sprintf("layer-%d-%d", layer-1, (index+1)%5)), "unrelated"}
			}
			if _, err := store.Create(ctx, job); err != nil {
				t.Fatal(err)
			}
		}
	}
	failedAt := time.Unix(2_000, 5)
	if _, err := store.RequestCancel(ctx, "root", failedAt); err != nil {
		t.Fatal(err)
	}
	for layer := range 3 {
		for index := range 5 {
			got, err := store.Get(ctx, jobs.JobID(fmt.Sprintf("layer-%d-%d", layer, index)))
			if err != nil {
				t.Fatal(err)
			}
			if got.State != jobs.StateFailed || got.LastFailure == nil || got.LastFailure.Code != "dependency_failed" || !got.UpdatedAt.Equal(failedAt) {
				t.Fatalf("dependent %q = %#v, want dependency_failed at %v", got.ID, got, failedAt)
			}
		}
	}
	if got, err := store.Get(ctx, "unrelated"); err != nil || got.State != jobs.StatePending {
		t.Fatalf("job with missing dependency = %#v, %v, want pending", got, err)
	}
}

func newStore(t *testing.T) *pgjobs.Store {
	t.Helper()
	pool := testPool(t)
	return openMigrated(t, pool, uniqueSchema(t, pool))
}

func openMigrated(t *testing.T, pool *pgxpool.Pool, schema string) *pgjobs.Store {
	t.Helper()
	store, err := pgjobs.Open(context.Background(), pool, pgjobs.Options{Schema: schema})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	return store
}

var (
	sharedPoolOnce sync.Once
	sharedPool     *pgxpool.Pool
	sharedPoolErr  error
)

// testPool returns a pool shared by the package's tests, or skips the test
// when no test database is configured.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(dsnVariable)
	if dsn == "" {
		t.Skipf("%s is not set", dsnVariable)
	}
	sharedPoolOnce.Do(func() {
		sharedPool, sharedPoolErr = pgxpool.New(context.Background(), dsn)
	})
	if sharedPoolErr != nil {
		t.Fatalf("pgxpool.New() error = %v", sharedPoolErr)
	}
	return sharedPool
}

func separatePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testPool(t)
	pool, err := pgxpool.New(context.Background(), os.Getenv(dsnVariable))
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// uniqueSchema reserves an isolated schema name and drops the schema after
// the test.
func uniqueSchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	schema := "durable_jobs_test_" + hex.EncodeToString(suffix)
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})
	return schema
}

func contractJob(id jobs.JobID, queue jobs.Queue) jobs.Job {
	at := time.Unix(100, 0).UTC()
	return jobs.Job{
		ID: id, Queue: queue, Kind: "kind", Payload: []byte("payload"),
		State: jobs.StatePending, CreatedAt: at, AvailableAt: at, UpdatedAt: at,
		Retry: jobs.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2},
	}
}

func persisted(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.Round(0).UTC()
}

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedPool != nil {
		sharedPool.Close()
	}
	os.Exit(code)
}
