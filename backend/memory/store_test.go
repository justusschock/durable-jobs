package memory

import (
	"context"
	"errors"
	"testing"

	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/storetest"
)

var _ jobs.Catalog = (*Store)(nil)

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) jobs.Store {
		t.Helper()
		return New()
	})
}

func TestCatalogContract(t *testing.T) {
	storetest.RunCatalog(t, func(t *testing.T) (jobs.Store, jobs.Catalog) {
		t.Helper()
		store := New()
		return store, store
	})
}

func TestListJobPageRejectsMismatchedMapKey(t *testing.T) {
	store := New()
	store.jobs["map-key"] = jobs.Job{ID: "embedded-id", Queue: "queue"}

	_, err := store.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: "queue", Limit: 1})
	if !errors.Is(err, jobs.ErrInvalidJob) {
		t.Fatalf("ListJobPage() error = %v, want ErrInvalidJob", err)
	}
}
