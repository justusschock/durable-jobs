package bolt_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	jobs "github.com/justusschock/durable-jobs"
	boltjobs "github.com/justusschock/durable-jobs/backend/bolt"
	"github.com/justusschock/durable-jobs/storetest"
	bbolt "go.etcd.io/bbolt"
)

// FR-JOBS-17
func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) jobs.Store {
		t.Helper()
		store, err := boltjobs.Open(filepath.Join(t.TempDir(), "jobs.db"), boltjobs.Options{Timeout: time.Second})
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Errorf("Close() error = %v", err)
			}
		})
		return store
	})
}

// FR-JOBS-11, FR-JOBS-17
func TestCatalogContract(t *testing.T) {
	storetest.RunCatalog(t, func(t *testing.T) (jobs.Store, jobs.Catalog) {
		t.Helper()
		store, err := boltjobs.Open(filepath.Join(t.TempDir(), "jobs.db"), boltjobs.Options{Timeout: time.Second})
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Errorf("Close() error = %v", err)
			}
		})
		return store, store
	})
}

// FR-JOBS-11
func TestCatalogPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	first, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	for _, id := range []jobs.JobID{"c", "a", "b"} {
		if _, err := first.Create(context.Background(), jobs.Job{ID: id, Queue: "queue", Payload: []byte(id)}); err != nil {
			_ = first.Close()
			t.Fatalf("Create(%q) error = %v", id, err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("second Close() error = %v", err)
		}
	})
	page, err := second.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: "queue", Limit: 2})
	if err != nil {
		t.Fatalf("ListJobPage() error = %v", err)
	}
	if len(page.Jobs) != 2 || page.Jobs[0].ID != "a" || page.Jobs[1].ID != "b" || page.NextAfter != "b" {
		t.Fatalf("ListJobPage() = %#v, want [a b] followed by b", page)
	}
}

// FR-JOBS-11
func TestCatalogScansSparseQueueEntries(t *testing.T) {
	store, err := boltjobs.Open(filepath.Join(t.TempDir(), "jobs.db"), boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	for _, fixture := range []struct {
		id    jobs.JobID
		queue jobs.Queue
	}{
		{id: "000-target", queue: "queue"},
		{id: "100-other", queue: "other"},
		{id: "200-other", queue: "other"},
		{id: "300-target", queue: "queue"},
		{id: "400-other", queue: "other"},
		{id: "500-target", queue: "queue"},
	} {
		if _, err := store.Create(context.Background(), jobs.Job{ID: fixture.id, Queue: fixture.queue}); err != nil {
			t.Fatalf("Create(%q) error = %v", fixture.id, err)
		}
	}

	first, err := store.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: "queue", Limit: 2})
	if err != nil {
		t.Fatalf("first ListJobPage() error = %v", err)
	}
	if len(first.Jobs) != 2 || first.Jobs[0].ID != "000-target" || first.Jobs[1].ID != "300-target" || first.NextAfter != "300-target" {
		t.Fatalf("first ListJobPage() = %#v", first)
	}
	second, err := store.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: "queue", After: first.NextAfter, Limit: 2})
	if err != nil {
		t.Fatalf("second ListJobPage() error = %v", err)
	}
	if len(second.Jobs) != 1 || second.Jobs[0].ID != "500-target" || second.NextAfter != "" {
		t.Fatalf("second ListJobPage() = %#v", second)
	}
}

// FR-JOBS-11
func TestCatalogRejectsMismatchedBucketKeyAndJobID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := store.Create(context.Background(), jobs.Job{ID: "embedded-id", Queue: "queue"}); err != nil {
		_ = store.Close()
		t.Fatalf("Create() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open() error = %v", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("jobs"))
		value := bytes.Clone(bucket.Get([]byte("embedded-id")))
		if err := bucket.Put([]byte("bucket-key"), value); err != nil {
			return err
		}
		return bucket.Delete([]byte("embedded-id"))
	}); err != nil {
		_ = db.Close()
		t.Fatalf("corrupt fixture error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("fixture Close() error = %v", err)
	}

	store, err = boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: "queue", Limit: 1}); !errors.Is(err, boltjobs.ErrUnsupportedFormat) {
		t.Fatalf("ListJobPage() error = %v, want ErrUnsupportedFormat", err)
	}
}

// FR-JOBS-15
func TestPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	location := time.FixedZone("test", 5*60*60+30*60)
	createdAt := time.Date(2026, time.July, 17, 12, 34, 56, 123456789, location)
	job := jobs.Job{
		ID:          "job-1",
		Queue:       "queue",
		Kind:        "kind",
		Payload:     []byte("payload"),
		Headers:     map[string]string{"source": "reopen"},
		Priority:    17,
		State:       jobs.StateRetryWaiting,
		CreatedAt:   createdAt,
		AvailableAt: createdAt.Add(2*time.Minute + 3*time.Nanosecond),
		UpdatedAt:   createdAt.Add(3*time.Minute + 5*time.Nanosecond),
		Attempt:     2,
		Retry: jobs.RetryPolicy{
			MaxAttempts:    5,
			InitialBackoff: 2 * time.Second,
			MaxBackoff:     2 * time.Minute,
			Multiplier:     2.5,
		},
		DependsOn:             []jobs.JobID{"dependency-1", "dependency-2"},
		CancellationRequested: true,
		Result:                []byte("result"),
		LastFailure: &jobs.Failure{
			Code:      "temporary",
			Message:   "retry later",
			Retryable: true,
		},
	}
	want := job.Clone()
	want.CreatedAt = want.CreatedAt.Round(0).UTC()
	want.AvailableAt = want.AvailableAt.Round(0).UTC()
	want.UpdatedAt = want.UpdatedAt.Round(0).UTC()

	first, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	created, err := first.Create(context.Background(), job)
	if err != nil {
		_ = first.Close()
		t.Fatalf("Create() error = %v", err)
	}
	if !reflect.DeepEqual(created, want) {
		_ = first.Close()
		t.Fatalf("Create() = %#v, want %#v", created, want)
	}

	job.Payload[0] = 'x'
	job.Headers["source"] = "mutated input"
	job.DependsOn[0] = "mutated-input"
	job.Result[0] = 'x'
	job.LastFailure.Code = "mutated-input"
	created.Payload[0] = 'y'
	created.Headers["source"] = "mutated result"
	created.DependsOn[0] = "mutated-result"
	created.Result[0] = 'y'
	created.LastFailure.Code = "mutated-result"

	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("second Close() error = %v", err)
		}
	})

	got, err := second.Get(context.Background(), want.ID)
	if err != nil {
		t.Fatalf("Get() after reopen error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Get() after reopen = %#v, want %#v", got, want)
	}
}

// NFR-JOBS-06
func TestDatabaseFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o600); got != want {
		t.Fatalf("database mode = %04o, want %04o", got, want)
	}
}

// FR-JOBS-15
func TestReadOnlyReadsWithoutChangingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	writable, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("writable Open() error = %v", err)
	}
	for _, fixture := range []struct {
		id    jobs.JobID
		queue jobs.Queue
	}{
		{id: "b", queue: "queue"},
		{id: "a", queue: "queue"},
		{id: "other", queue: "other"},
	} {
		job := jobs.Job{ID: fixture.id, Queue: fixture.queue, Payload: []byte("payload"), Headers: map[string]string{"source": "read-only"}}
		if _, err := writable.Create(context.Background(), job); err != nil {
			_ = writable.Close()
			t.Fatalf("Create(%q) error = %v", fixture.id, err)
		}
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("writable Close() error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() before open error = %v", err)
	}
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() before open error = %v", err)
	}

	readOnly, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only Open() error = %v", err)
	}
	got, err := readOnly.Get(context.Background(), "a")
	if err != nil {
		_ = readOnly.Close()
		t.Fatalf("Get() error = %v", err)
	}
	if got.ID != "a" || string(got.Payload) != "payload" || got.Headers["source"] != "read-only" {
		_ = readOnly.Close()
		t.Fatalf("Get() = %#v", got)
	}
	listed, err := readOnly.ListJobs(context.Background(), jobs.ListJobsRequest{Queue: "queue", Limit: 2})
	if err != nil {
		_ = readOnly.Close()
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(listed) != 2 || listed[0].ID != "a" || listed[1].ID != "b" {
		_ = readOnly.Close()
		t.Fatalf("ListJobs() = %#v", listed)
	}
	page, err := readOnly.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: "queue", Limit: 2})
	if err != nil {
		_ = readOnly.Close()
		t.Fatalf("ListJobPage() error = %v", err)
	}
	if len(page.Jobs) != 2 || page.Jobs[0].ID != "a" || page.Jobs[1].ID != "b" || page.NextAfter != "" {
		_ = readOnly.Close()
		t.Fatalf("ListJobPage() = %#v", page)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatalf("read-only Close() error = %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() after open error = %v", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() after open error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("read-only Open() changed database contents")
	}
	if afterInfo.Mode().Perm() != beforeInfo.Mode().Perm() {
		t.Fatalf("database mode changed from %04o to %04o", beforeInfo.Mode().Perm(), afterInfo.Mode().Perm())
	}
}

func TestReadOnlyMissingDatabaseIsNotCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open() error = %v, want os.ErrNotExist", err)
	}
	if store != nil {
		_ = store.Close()
		t.Fatal("Open() returned a store for a missing database")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Lstat() after Open() error = %v, want os.ErrNotExist", err)
	}
}

func TestReadOnlyRejectsZeroLengthDatabaseWithoutChangingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zero.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() before Open() error = %v", err)
	}

	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if !errors.Is(err, boltjobs.ErrUnsupportedFormat) {
		t.Fatalf("Open() error = %v, want ErrUnsupportedFormat", err)
	}
	if store != nil {
		_ = store.Close()
		t.Fatal("Open() returned a store for a zero-length database")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() after Open() error = %v", err)
	}
	if after.Size() != 0 || after.Mode().Perm() != before.Mode().Perm() || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("zero-length database changed: before=%#v after=%#v", before, after)
	}
}

func TestReadOnlyRejectsIncompleteSchemaWithoutRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open() error = %v", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("jobs"))
		return err
	}); err != nil {
		_ = db.Close()
		t.Fatalf("create jobs bucket error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("fixture Close() error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() before Open() error = %v", err)
	}

	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if !errors.Is(err, boltjobs.ErrUnsupportedFormat) {
		t.Fatalf("Open() error = %v, want ErrUnsupportedFormat", err)
	}
	if store != nil {
		_ = store.Close()
		t.Fatal("Open() returned a store for an incomplete schema")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() after Open() error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("read-only Open() changed or repaired the incomplete schema")
	}
}

// NFR-JOBS-06
func TestReadOnlyOwnerModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not authoritative on Windows")
	}

	for _, mode := range []os.FileMode{0o400, 0o600} {
		t.Run(mode.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			initializeBoltDatabase(t, path)
			if err := os.Chmod(path, mode); err != nil {
				t.Fatalf("Chmod(%04o) error = %v", mode, err)
			}
			store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
			if err != nil {
				t.Fatalf("Open() mode %04o error = %v", mode, err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("Stat() error = %v", err)
			}
			if info.Mode().Perm() != mode {
				t.Fatalf("database mode = %04o, want preserved %04o", info.Mode().Perm(), mode)
			}
		})
	}

	for _, mode := range []os.FileMode{0o640, 0o604} {
		t.Run("reject_"+mode.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			initializeBoltDatabase(t, path)
			if err := os.Chmod(path, mode); err != nil {
				t.Fatalf("Chmod(%04o) error = %v", mode, err)
			}
			store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
			if err == nil {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("Open() mode %04o error = nil, want an error", mode)
			}
			info, statErr := os.Stat(path)
			if statErr != nil {
				t.Fatalf("Stat() error = %v", statErr)
			}
			if info.Mode().Perm() != mode {
				t.Fatalf("rejected database mode = %04o, want preserved %04o", info.Mode().Perm(), mode)
			}
		})
	}
}

func TestReadOnlyRejectsSymlinks(t *testing.T) {
	dir := t.TempDir()
	targetParent := filepath.Join(dir, "target")
	if err := os.Mkdir(targetParent, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	target := filepath.Join(targetParent, "jobs.db")
	initializeBoltDatabase(t, target)

	t.Run("database", func(t *testing.T) {
		link := filepath.Join(dir, "linked.db")
		symlinkOrSkip(t, target, link)
		store, err := boltjobs.Open(link, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
		if err == nil {
			if store != nil {
				_ = store.Close()
			}
			t.Fatal("Open() database symlink error = nil, want an error")
		}
	})

	t.Run("parent", func(t *testing.T) {
		linkParent := filepath.Join(dir, "linked-parent")
		symlinkOrSkip(t, targetParent, linkParent)
		store, err := boltjobs.Open(filepath.Join(linkParent, "jobs.db"), boltjobs.Options{Timeout: time.Second, ReadOnly: true})
		if err == nil {
			if store != nil {
				_ = store.Close()
			}
			t.Fatal("Open() symlink parent error = nil, want an error")
		}
	})
}

// FR-JOBS-15
func TestReadOnlyMutatorsReturnDatabaseReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	initializeBoltDatabase(t, path)
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	validSchedule := jobs.Schedule{
		ID: "schedule",
		Request: jobs.EnqueueRequest{
			ID:    "template",
			Queue: "queue",
			Kind:  "kind",
			Retry: jobs.RetryPolicy{MaxAttempts: 1, InitialBackoff: time.Second, MaxBackoff: time.Second, Multiplier: 1},
		},
		NextRunAt: time.Now(),
		Enabled:   true,
	}
	tests := []struct {
		name   string
		mutate func() error
	}{
		{name: "Create", mutate: func() error {
			_, err := store.Create(context.Background(), jobs.Job{ID: "job", Queue: "queue"})
			return err
		}},
		{name: "ClaimReady", mutate: func() error {
			_, err := store.ClaimReady(context.Background(), jobs.ClaimRequest{Queue: "queue"})
			return err
		}},
		{name: "Heartbeat", mutate: func() error { return store.Heartbeat(context.Background(), jobs.HeartbeatRequest{}) }},
		{name: "Complete", mutate: func() error { _, err := store.Complete(context.Background(), jobs.CompleteRequest{}); return err }},
		{name: "Fail", mutate: func() error { _, err := store.Fail(context.Background(), jobs.FailRequest{}); return err }},
		{name: "RequestCancel", mutate: func() error { _, err := store.RequestCancel(context.Background(), "job", time.Now()); return err }},
		{name: "RecoverAttempt", mutate: func() error {
			_, err := store.RecoverAttempt(context.Background(), jobs.RecoverAttemptRequest{})
			return err
		}},
		{name: "PutSchedule", mutate: func() error { _, err := store.PutSchedule(context.Background(), validSchedule, 0); return err }},
		{name: "AdvanceSchedule", mutate: func() error { return store.AdvanceSchedule(context.Background(), "schedule", 1, time.Now()) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.mutate(); !errors.Is(err, bbolt.ErrDatabaseReadOnly) {
				t.Fatalf("mutation error = %v, want bbolt.ErrDatabaseReadOnly", err)
			}
		})
	}
}

func TestReadOnlyLocksAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	initializeBoltDatabase(t, path)
	readOnly, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		t.Fatalf("first read-only Open() error = %v", err)
	}
	secondReadOnly, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		_ = readOnly.Close()
		t.Fatalf("second concurrent read-only Open() error = %v", err)
	}
	if err := secondReadOnly.Close(); err != nil {
		_ = readOnly.Close()
		t.Fatalf("second read-only Close() error = %v", err)
	}

	contender, err := boltjobs.Open(path, boltjobs.Options{Timeout: 50 * time.Millisecond})
	if !errors.Is(err, bbolt.ErrTimeout) {
		if contender != nil {
			_ = contender.Close()
		}
		_ = readOnly.Close()
		t.Fatalf("writable Open() with read-only holder error = %v, want bbolt.ErrTimeout", err)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatalf("first read-only Close() error = %v", err)
	}

	writable, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("writable Open() after read-only Close() error = %v", err)
	}
	contender, err = boltjobs.Open(path, boltjobs.Options{Timeout: 50 * time.Millisecond, ReadOnly: true})
	if !errors.Is(err, bbolt.ErrTimeout) {
		if contender != nil {
			_ = contender.Close()
		}
		_ = writable.Close()
		t.Fatalf("read-only Open() with writable holder error = %v, want bbolt.ErrTimeout", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("writable Close() error = %v", err)
	}

	reopened, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only reopen error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close() error = %v", err)
	}
}

// FR-JOBS-15
func TestOpenRejectsDatabaseSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.db")
	store, err := boltjobs.Open(target, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Open() target error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() target error = %v", err)
	}

	link := filepath.Join(dir, "jobs.db")
	symlinkOrSkip(t, target, link)

	store, err = boltjobs.Open(link, boltjobs.Options{Timeout: time.Second})
	if err == nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatal("Open() symlink error = nil, want an error")
	}
}

func TestOpenRejectsSymlinkParent(t *testing.T) {
	dir := t.TempDir()
	targetParent := filepath.Join(dir, "target")
	if err := os.Mkdir(targetParent, 0o755); err != nil {
		t.Fatalf("Mkdir() target parent error = %v", err)
	}

	linkParent := filepath.Join(dir, "link")
	symlinkOrSkip(t, targetParent, linkParent)

	store, err := boltjobs.Open(filepath.Join(linkParent, "jobs.db"), boltjobs.Options{Timeout: time.Second})
	if err == nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatal("Open() symlink parent error = nil, want an error")
	}
}

func TestOpenRejectsNonDirectoryParent(t *testing.T) {
	dir := t.TempDir()
	parent := filepath.Join(dir, "parent")
	if err := os.WriteFile(parent, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile() parent fixture error = %v", err)
	}

	store, err := boltjobs.Open(filepath.Join(parent, "jobs.db"), boltjobs.Options{Timeout: time.Second})
	if err == nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatal("Open() non-directory parent error = nil, want an error")
	}
}

func TestOpenValidation(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		store, err := boltjobs.Open("", boltjobs.Options{})
		if err == nil {
			if store != nil {
				_ = store.Close()
			}
			t.Fatal("Open() error = nil, want an error")
		}
	})

	t.Run("negative timeout", func(t *testing.T) {
		store, err := boltjobs.Open(filepath.Join(t.TempDir(), "jobs.db"), boltjobs.Options{Timeout: -time.Nanosecond})
		if err == nil {
			if store != nil {
				_ = store.Close()
			}
			t.Fatal("Open() error = nil, want an error")
		}
	})
}

func TestCloseAllowsImmediateReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	first, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	second, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Open() after Close() error = %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

// FR-JOBS-15
func TestOpenRejectsIncompleteSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open() error = %v", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("jobs"))
		return err
	}); err != nil {
		_ = db.Close()
		t.Fatalf("create jobs bucket error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fixture database error = %v", err)
	}

	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if !errors.Is(err, boltjobs.ErrUnsupportedFormat) {
		t.Errorf("Open() error = %v, want ErrUnsupportedFormat", err)
	}
	if store != nil {
		t.Error("Open() store is non-nil for incomplete schema")
		if err := store.Close(); err != nil {
			t.Errorf("Close() unexpectedly returned store error = %v", err)
		}
	}

	db, err = bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen fixture database error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close inspected database error = %v", err)
		}
	})
	if err := db.View(func(tx *bbolt.Tx) error {
		if tx.Bucket([]byte("attempts")) != nil || tx.Bucket([]byte("meta")) != nil {
			return errors.New("Open silently recreated missing buckets")
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// FR-JOBS-15
func TestBoltRejectsUnknownEnvelopeVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte("jobs")).Put([]byte("job-1"), []byte(`{"version":2,"payload":{}}`))
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Get(context.Background(), "job-1"); !errors.Is(err, boltjobs.ErrUnsupportedFormat) {
		t.Fatalf("Get() error = %v, want ErrUnsupportedFormat", err)
	}
}

func symlinkOrSkip(t *testing.T, oldname, newname string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires privileges on many Windows systems")
	}
	if err := os.Symlink(oldname, newname); err != nil {
		t.Skipf("Symlink() unavailable: %v", err)
	}
}

func initializeBoltDatabase(t *testing.T, path string) {
	t.Helper()
	store, err := boltjobs.Open(path, boltjobs.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("initialize Open() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("initialize Close() error = %v", err)
	}
}
