package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/backend/memory"
)

// FR-JOBS-10
func TestValidateDependencies(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	for _, job := range []jobs.Job{
		workflowJob("pending", jobs.StatePending),
		workflowJob("succeeded", jobs.StateSucceeded),
		workflowJob("failed", jobs.StateFailed),
		workflowJob("canceled", jobs.StateCanceled),
	} {
		if _, err := store.Create(ctx, job); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name         string
		dependencies []jobs.JobID
		wantErr      error
	}{
		{name: "none"},
		{name: "existing nonterminal and successful", dependencies: []jobs.JobID{"pending", "succeeded"}},
		{name: "missing", dependencies: []jobs.JobID{"missing"}, wantErr: jobs.ErrJobNotFound},
		{name: "failed", dependencies: []jobs.JobID{"failed"}, wantErr: jobs.ErrDependencyFailed},
		{name: "canceled", dependencies: []jobs.JobID{"canceled"}, wantErr: jobs.ErrDependencyFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := jobs.ValidateDependencies(ctx, store, test.dependencies)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ValidateDependencies() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func workflowJob(id jobs.JobID, state jobs.State) jobs.Job {
	now := time.Unix(100, 0).UTC()
	return jobs.Job{
		ID: id, Queue: "q", Kind: "workflow", State: state, CreatedAt: now, AvailableAt: now, UpdatedAt: now,
		Retry: jobs.RetryPolicy{MaxAttempts: 1, InitialBackoff: time.Second, MaxBackoff: time.Second, Multiplier: 1},
	}
}
