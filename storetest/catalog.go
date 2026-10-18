package storetest

import (
	"context"
	"errors"
	"testing"

	jobs "github.com/justusschock/durable-jobs"
)

// RunCatalog executes the optional catalog contract against a fresh store for
// each subtest.
func RunCatalog(t *testing.T, factory func(t *testing.T) (jobs.Store, jobs.Catalog)) {
	t.Helper()

	t.Run("catalog request validator rejects invalid identities", func(t *testing.T) {
		for _, request := range []jobs.ListJobPageRequest{
			{Queue: " ", Limit: 1},
			{Queue: "queue\x00", Limit: 1},
			{Queue: "queue", After: "\t", Limit: 1},
			{Queue: "queue", After: "after\x00", Limit: 1},
		} {
			if err := jobs.ValidateListJobPageRequest(request); !errors.Is(err, jobs.ErrInvalidJob) {
				t.Fatalf("ValidateListJobPageRequest(%#v) error = %v, want ErrInvalidJob", request, err)
			}
		}
	})

	t.Run("catalog filters and traverses interleaved pages", func(t *testing.T) {
		store, catalog := factory(t)
		for _, job := range []jobs.Job{
			newJob("g", "queue", now(0)),
			newJob("b", "other", now(0)),
			newJob("a", "queue", now(0)),
			newJob("f", "queue", now(0)),
			newJob("d", "other", now(0)),
			newJob("c", "queue", now(0)),
			newJob("e", "queue", now(0)),
			newJob("h", "other", now(0)),
		} {
			create(t, store, job)
		}

		page := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", Limit: 2})
		assertJobPage(t, page, []jobs.JobID{"a", "c"}, "c")
		page = listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", After: page.NextAfter, Limit: 2})
		assertJobPage(t, page, []jobs.JobID{"e", "f"}, "f")
		page = listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", After: page.NextAfter, Limit: 2})
		assertJobPage(t, page, []jobs.JobID{"g"}, "")
	})

	t.Run("catalog orders raw job ID strings", func(t *testing.T) {
		store, catalog := factory(t)
		for _, id := range []jobs.JobID{"ä", "a", "2", "10", "A"} {
			create(t, store, newJob(id, "queue", now(0)))
		}
		page := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", Limit: 5})
		assertJobPage(t, page, []jobs.JobID{"10", "2", "A", "a", "ä"}, "")
	})

	t.Run("catalog accepts a nonexistent exclusive position", func(t *testing.T) {
		store, catalog := factory(t)
		for _, id := range []jobs.JobID{"a", "c", "e"} {
			create(t, store, newJob(id, "queue", now(0)))
		}
		page := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", After: "b", Limit: 3})
		assertJobPage(t, page, []jobs.JobID{"c", "e"}, "")
	})

	t.Run("catalog omits a position for an exact final page", func(t *testing.T) {
		store, catalog := factory(t)
		create(t, store, newJob("a", "queue", now(0)))
		create(t, store, newJob("b", "queue", now(0)))
		create(t, store, newJob("z", "other", now(0)))
		page := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", Limit: 2})
		assertJobPage(t, page, []jobs.JobID{"a", "b"}, "")
	})

	t.Run("catalog returns an owned nonnil empty page", func(t *testing.T) {
		_, catalog := factory(t)
		page := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", After: "missing", Limit: 1})
		if page.Jobs == nil || len(page.Jobs) != 0 || page.NextAfter != "" {
			t.Fatalf("ListJobPage() = %#v, want nonnil empty jobs and empty NextAfter", page)
		}
	})

	t.Run("catalog validates scope position and bounds", func(t *testing.T) {
		_, catalog := factory(t)
		invalidUTF8 := string([]byte{0xff})
		for _, test := range []struct {
			name    string
			request jobs.ListJobPageRequest
		}{
			{name: "empty queue", request: jobs.ListJobPageRequest{Limit: 1}},
			{name: "blank queue", request: jobs.ListJobPageRequest{Queue: " \t", Limit: 1}},
			{name: "nul queue", request: jobs.ListJobPageRequest{Queue: "queue\x00", Limit: 1}},
			{name: "invalid UTF-8 queue", request: jobs.ListJobPageRequest{Queue: jobs.Queue(invalidUTF8), Limit: 1}},
			{name: "blank position", request: jobs.ListJobPageRequest{Queue: "queue", After: " ", Limit: 1}},
			{name: "nul position", request: jobs.ListJobPageRequest{Queue: "queue", After: "after\x00", Limit: 1}},
			{name: "invalid UTF-8 position", request: jobs.ListJobPageRequest{Queue: "queue", After: jobs.JobID(invalidUTF8), Limit: 1}},
			{name: "zero limit", request: jobs.ListJobPageRequest{Queue: "queue"}},
			{name: "negative limit", request: jobs.ListJobPageRequest{Queue: "queue", Limit: -1}},
			{name: "over maximum limit", request: jobs.ListJobPageRequest{Queue: "queue", Limit: jobs.MaxJobPageSize + 1}},
		} {
			t.Run(test.name, func(t *testing.T) {
				if _, err := catalog.ListJobPage(context.Background(), test.request); !errors.Is(err, jobs.ErrInvalidJob) {
					t.Fatalf("ListJobPage() error = %v, want ErrInvalidJob", err)
				}
			})
		}

		for _, limit := range []int{1, jobs.MaxJobPageSize} {
			if _, err := catalog.ListJobPage(context.Background(), jobs.ListJobPageRequest{Queue: "queue", Limit: limit}); err != nil {
				t.Fatalf("ListJobPage() limit %d error = %v", limit, err)
			}
		}
	})

	t.Run("catalog checks context before request validation", func(t *testing.T) {
		_, catalog := factory(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := catalog.ListJobPage(ctx, jobs.ListJobPageRequest{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("ListJobPage() error = %v, want context.Canceled", err)
		}
	})

	t.Run("catalog deeply clones returned jobs", func(t *testing.T) {
		store, catalog := factory(t)
		job := newJob("clone", "queue", now(0))
		job.DependsOn = []jobs.JobID{"dependency"}
		job.Result = []byte("result")
		job.LastFailure = &jobs.Failure{Code: "failure", Message: "message", Retryable: true}
		create(t, store, job)

		page := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", Limit: 1})
		page.Jobs[0].Payload[0] = 'x'
		page.Jobs[0].Headers["source"] = "mutated"
		page.Jobs[0].DependsOn[0] = "mutated"
		page.Jobs[0].Result[0] = 'x'
		page.Jobs[0].LastFailure.Code = "mutated"

		stored := get(t, store, "clone")
		if string(stored.Payload) != "payload" || stored.Headers["source"] != "contract" ||
			stored.DependsOn[0] != "dependency" || string(stored.Result) != "result" ||
			stored.LastFailure == nil || stored.LastFailure.Code != "failure" {
			t.Fatalf("ListJobPage() returned aliased job: %#v", stored)
		}
		listedAgain := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", Limit: 1})
		if listedAgain.Jobs[0].LastFailure == page.Jobs[0].LastFailure {
			t.Fatal("ListJobPage() reused caller-owned failure storage")
		}
	})

	t.Run("catalog traversal is weakly consistent between pages", func(t *testing.T) {
		store, catalog := factory(t)
		for _, id := range []jobs.JobID{"a", "c", "e"} {
			create(t, store, newJob(id, "queue", now(0)))
		}
		first := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", Limit: 2})
		assertJobPage(t, first, []jobs.JobID{"a", "c"}, "c")

		if _, err := store.RequestCancel(context.Background(), "e", now(3)); err != nil {
			t.Fatalf("RequestCancel() error = %v", err)
		}
		create(t, store, newJob("b", "queue", now(0)))
		create(t, store, newJob("d", "queue", now(0)))

		second := listJobPage(t, catalog, jobs.ListJobPageRequest{Queue: "queue", After: first.NextAfter, Limit: 2})
		assertJobPage(t, second, []jobs.JobID{"d", "e"}, "")
		if second.Jobs[1].State != jobs.StateCanceled {
			t.Fatalf("ListJobPage() returned state %v for mutated job, want StateCanceled", second.Jobs[1].State)
		}
	})
}

func listJobPage(t *testing.T, catalog jobs.Catalog, request jobs.ListJobPageRequest) jobs.JobPage {
	t.Helper()
	page, err := catalog.ListJobPage(context.Background(), request)
	if err != nil {
		t.Fatalf("ListJobPage(%#v) error = %v", request, err)
	}
	return page
}

func assertJobPage(t *testing.T, page jobs.JobPage, wantIDs []jobs.JobID, wantNext jobs.JobID) {
	t.Helper()
	if page.Jobs == nil {
		t.Fatal("ListJobPage() returned nil Jobs on success")
	}
	if len(page.Jobs) != len(wantIDs) {
		t.Fatalf("ListJobPage() returned %d jobs, want %d: %#v", len(page.Jobs), len(wantIDs), page)
	}
	for index, want := range wantIDs {
		if page.Jobs[index].ID != want {
			t.Fatalf("ListJobPage().Jobs[%d].ID = %q, want %q", index, page.Jobs[index].ID, want)
		}
	}
	if page.NextAfter != wantNext {
		t.Fatalf("ListJobPage().NextAfter = %q, want %q", page.NextAfter, wantNext)
	}
}
