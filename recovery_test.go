package jobs_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
	reconciliation "github.com/justusschock/go-reconciliation"
)

var _ reconciliation.Adapter[jobs.RecoverySpec, jobs.RecoveryObserved, jobs.RecoveryAction, jobs.RecoveryAction] = jobs.RecoveryAdapter{}

// FR-JOBS-08
func TestRecoveryObserveUsesUTCNowAndUnboundedRecoveryList(t *testing.T) {
	t.Parallel()

	localNow := time.Date(2026, 7, 20, 11, 30, 0, 99, time.FixedZone("west", -2*60*60))
	wantNow := localNow.Round(0).UTC()
	candidates := []jobs.RecoveryCandidate{
		{Attempt: jobs.Attempt{ID: "b"}},
		{Attempt: jobs.Attempt{ID: "a"}},
	}
	store := &recoveryStore{listCandidates: candidates}
	adapter := jobs.RecoveryAdapter{Store: store, Clock: recoveryClock{now: localNow}}

	observed, err := adapter.Observe(context.Background(), reconciliation.Key("jobs/recovery"))
	if err != nil {
		t.Fatal(err)
	}

	if !store.listNow.Equal(wantNow) || store.listNow.Location() != time.UTC {
		t.Fatalf("ListRecoveryCandidates() now = %v, want %v UTC", store.listNow, wantNow)
	}
	wantLimit := int(^uint(0) >> 1)
	if store.listLimit != wantLimit {
		t.Fatalf("ListRecoveryCandidates() limit = %d, want %d for unbounded observe", store.listLimit, wantLimit)
	}
	if !observed.ObservedAt.Equal(wantNow) || observed.ObservedAt.Location() != time.UTC {
		t.Fatalf("ObservedAt = %v, want %v UTC", observed.ObservedAt, wantNow)
	}
	if !reflect.DeepEqual(observed.Candidates, candidates) {
		t.Fatalf("Candidates = %#v, want %#v", observed.Candidates, candidates)
	}
}

// FR-JOBS-08
func TestRecoveryPlanFiltersLimitsSortsAndCarriesObservedFence(t *testing.T) {
	t.Parallel()

	observedAt := time.Unix(200, 0).UTC()
	expiredDeadline := observedAt.Add(-time.Second)
	equalDeadline := observedAt
	futureDeadline := observedAt.Add(time.Second)
	observed := jobs.RecoveryObserved{
		ObservedAt: observedAt,
		Candidates: []jobs.RecoveryCandidate{
			recoveryCandidate("b", 22, equalDeadline),
			recoveryCandidate("future", 99, futureDeadline),
			recoveryCandidate("a", 11, expiredDeadline),
			recoveryCandidate("c", 33, expiredDeadline),
		},
	}
	desired := reconciliation.Desired[jobs.RecoverySpec]{
		Key:        "jobs/recovery",
		Generation: 1,
		Spec:       jobs.RecoverySpec{Limit: 2},
	}

	plan, err := (jobs.RecoveryAdapter{}).Plan(context.Background(), desired, observed)
	if err != nil {
		t.Fatal(err)
	}

	wantActions := []reconciliation.Action[jobs.RecoveryAction]{
		{
			ID: "recover/a",
			Value: jobs.RecoveryAction{
				AttemptID:         "a",
				FencingToken:      11,
				HeartbeatDeadline: expiredDeadline,
				RecoveredAt:       observedAt,
			},
		},
		{
			ID: "recover/b",
			Value: jobs.RecoveryAction{
				AttemptID:         "b",
				FencingToken:      22,
				HeartbeatDeadline: equalDeadline,
				RecoveredAt:       observedAt,
			},
		},
	}
	if !reflect.DeepEqual(plan.Actions, wantActions) {
		t.Fatalf("Plan() actions = %#v, want %#v", plan.Actions, wantActions)
	}
	for _, action := range plan.Actions {
		if action.Reversible {
			t.Fatalf("action %q is reversible, want non-reversible recovery", action.ID)
		}
	}
}

// FR-JOBS-08
func TestRecoveryPlanRejectsNonPositiveLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit int
	}{
		{name: "zero", limit: 0},
		{name: "negative", limit: -1},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			desired := reconciliation.Desired[jobs.RecoverySpec]{
				Key:        "jobs/recovery",
				Generation: 1,
				Spec:       jobs.RecoverySpec{Limit: test.limit},
			}
			_, err := (jobs.RecoveryAdapter{}).Plan(context.Background(), desired, jobs.RecoveryObserved{})
			if err == nil {
				t.Fatal("Plan() error = nil, want non-nil")
			}
		})
	}
}

// FR-JOBS-08
func TestRecoveryApplySendsExactRecoverAttemptRequest(t *testing.T) {
	t.Parallel()

	recoveredAt := time.Unix(300, 0).UTC()
	deadline := recoveredAt.Add(-time.Minute)
	store := &recoveryStore{}
	action := reconciliation.Action[jobs.RecoveryAction]{
		ID: "recover/a",
		Value: jobs.RecoveryAction{
			AttemptID:         "a",
			FencingToken:      44,
			HeartbeatDeadline: deadline,
			RecoveredAt:       recoveredAt,
		},
	}

	undo, err := (jobs.RecoveryAdapter{Store: store}).Apply(context.Background(), action)
	if err != nil {
		t.Fatal(err)
	}

	if undo != (reconciliation.Undo[jobs.RecoveryAction]{}) {
		t.Fatalf("Apply() undo = %#v, want zero undo for non-reversible action", undo)
	}
	want := jobs.RecoverAttemptRequest{
		AttemptID:         "a",
		FencingToken:      44,
		HeartbeatDeadline: deadline,
		RecoveredAt:       recoveredAt,
	}
	if len(store.recoverRequests) != 1 || store.recoverRequests[0] != want {
		t.Fatalf("RecoverAttempt() requests = %#v, want [%#v]", store.recoverRequests, want)
	}
}

// FR-JOBS-08
func TestRecoveryApplyTreatsObsoleteActionsAsIdempotentSuccess(t *testing.T) {
	t.Parallel()

	for _, recoverErr := range []error{jobs.ErrStaleAttempt, jobs.ErrInvalidTransition} {
		recoverErr := recoverErr
		t.Run(recoverErr.Error(), func(t *testing.T) {
			t.Parallel()

			store := &recoveryStore{recoverErr: recoverErr}
			_, err := (jobs.RecoveryAdapter{Store: store}).Apply(context.Background(), reconciliation.Action[jobs.RecoveryAction]{
				ID:    "recover/a",
				Value: jobs.RecoveryAction{AttemptID: "a"},
			})
			if err != nil {
				t.Fatalf("Apply() error = %v, want nil", err)
			}
			if len(store.recoverRequests) != 1 {
				t.Fatalf("RecoverAttempt() calls = %d, want 1", len(store.recoverRequests))
			}
		})
	}
}

// FR-JOBS-08
func TestRecoveryUndoIsNoOp(t *testing.T) {
	t.Parallel()

	err := (jobs.RecoveryAdapter{}).Undo(context.Background(), reconciliation.Undo[jobs.RecoveryAction]{
		ActionID: "recover/a",
		Value:    jobs.RecoveryAction{AttemptID: "a"},
	})
	if err != nil {
		t.Fatalf("Undo() error = %v, want nil", err)
	}
}

// FR-JOBS-14
func TestNewRecoveryEngineRejectsNilDependenciesAndReturnsFourTypeEngine(t *testing.T) {
	t.Parallel()

	store := &recoveryStore{}
	clock := recoveryClock{now: time.Unix(100, 0)}
	policy := reconciliation.Policy{MaxAttempts: 1}
	var typedNilStore *recoveryStore
	var typedNilClock *typedNilRecoveryClock

	tests := []struct {
		name      string
		store     jobs.Store
		clock     jobs.Clock
		wantError bool
	}{
		{name: "nil store", clock: clock, wantError: true},
		{name: "typed nil store", store: typedNilStore, clock: clock, wantError: true},
		{name: "nil clock", store: store, wantError: true},
		{name: "typed nil clock", store: store, clock: typedNilClock, wantError: true},
		{name: "valid", store: store, clock: clock},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			engine, err := jobs.NewRecoveryEngine(test.store, test.clock, policy)
			if test.wantError {
				if err == nil {
					t.Fatal("NewRecoveryEngine() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRecoveryEngine() error = %v, want nil", err)
			}
			if _, ok := any(engine).(*reconciliation.Engine[jobs.RecoverySpec, jobs.RecoveryObserved, jobs.RecoveryAction, jobs.RecoveryAction]); !ok {
				t.Fatalf("NewRecoveryEngine() engine type = %T, want four-type reconciliation engine", engine)
			}
			if engine == nil {
				t.Fatal("NewRecoveryEngine() engine = nil, want non-nil")
			}
		})
	}
}

func recoveryCandidate(id jobs.AttemptID, fence coordination.FencingToken, deadline time.Time) jobs.RecoveryCandidate {
	return jobs.RecoveryCandidate{
		Attempt: jobs.Attempt{
			ID:                id,
			FencingToken:      fence,
			HeartbeatDeadline: deadline,
		},
	}
}

type recoveryClock struct {
	now time.Time
}

func (clock recoveryClock) Now() time.Time {
	return clock.now
}

type typedNilRecoveryClock struct{}

func (*typedNilRecoveryClock) Now() time.Time {
	return time.Time{}
}

type recoveryStore struct {
	listNow         time.Time
	listLimit       int
	listCandidates  []jobs.RecoveryCandidate
	listErr         error
	recoverRequests []jobs.RecoverAttemptRequest
	recoverErr      error
}

func (store *recoveryStore) Create(context.Context, jobs.Job) (jobs.Job, error) {
	return jobs.Job{}, errors.New("unexpected Create")
}

func (store *recoveryStore) Get(context.Context, jobs.JobID) (jobs.Job, error) {
	return jobs.Job{}, errors.New("unexpected Get")
}

func (store *recoveryStore) ListJobs(context.Context, jobs.ListJobsRequest) ([]jobs.Job, error) {
	return nil, errors.New("unexpected ListJobs")
}

func (store *recoveryStore) ClaimReady(context.Context, jobs.ClaimRequest) (jobs.Attempt, error) {
	return jobs.Attempt{}, errors.New("unexpected ClaimReady")
}

func (store *recoveryStore) Heartbeat(context.Context, jobs.HeartbeatRequest) error {
	return errors.New("unexpected Heartbeat")
}

func (store *recoveryStore) Complete(context.Context, jobs.CompleteRequest) (jobs.Job, error) {
	return jobs.Job{}, errors.New("unexpected Complete")
}

func (store *recoveryStore) Fail(context.Context, jobs.FailRequest) (jobs.Job, error) {
	return jobs.Job{}, errors.New("unexpected Fail")
}

func (store *recoveryStore) RequestCancel(context.Context, jobs.JobID, time.Time) (jobs.Job, error) {
	return jobs.Job{}, errors.New("unexpected RequestCancel")
}

func (store *recoveryStore) RecoverAttempt(_ context.Context, request jobs.RecoverAttemptRequest) (jobs.Job, error) {
	store.recoverRequests = append(store.recoverRequests, request)
	return jobs.Job{}, store.recoverErr
}

func (store *recoveryStore) ListRecoveryCandidates(_ context.Context, now time.Time, limit int) ([]jobs.RecoveryCandidate, error) {
	store.listNow = now
	store.listLimit = limit
	return append([]jobs.RecoveryCandidate(nil), store.listCandidates...), store.listErr
}
