package bolt_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	jobs "github.com/justusschock/go-durable-jobs"
	boltjobs "github.com/justusschock/go-durable-jobs/backend/bolt"
	"github.com/justusschock/go-durable-jobs/internal/storetest"
	bbolt "go.etcd.io/bbolt"
)

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
