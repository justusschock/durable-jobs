package jobs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
	reconciliation "github.com/justusschock/reconciliation"
)

// RecoverySpec bounds the number of expired attempts recovered in one plan.
type RecoverySpec struct {
	Limit int
}

// RecoveryObserved is one independently owned snapshot of expired attempts.
type RecoveryObserved struct {
	Candidates []RecoveryCandidate
	ObservedAt time.Time
}

// RecoveryAction carries the exact observation needed for a fenced recovery CAS.
type RecoveryAction struct {
	AttemptID         AttemptID
	FencingToken      coordination.FencingToken
	HeartbeatDeadline time.Time
	RecoveredAt       time.Time
}

// RecoveryAdapter observes and recovers expired durable job attempts.
type RecoveryAdapter struct {
	Store Store
	Clock Clock
}

// Observe lists every currently expired candidate so Plan can apply its desired limit.
func (adapter RecoveryAdapter) Observe(ctx context.Context, _ reconciliation.Key) (RecoveryObserved, error) {
	if err := ctx.Err(); err != nil {
		return RecoveryObserved{}, err
	}
	observedAt := persistedTime(adapter.Clock.Now())
	candidates, err := adapter.Store.ListRecoveryCandidates(ctx, observedAt, int(^uint(0)>>1))
	if err != nil {
		return RecoveryObserved{}, err
	}
	return RecoveryObserved{
		Candidates: append([]RecoveryCandidate(nil), candidates...),
		ObservedAt: observedAt,
	}, nil
}

// Plan deterministically selects expired attempts for non-reversible recovery.
func (RecoveryAdapter) Plan(ctx context.Context, desired reconciliation.Desired[RecoverySpec], observed RecoveryObserved) (reconciliation.Plan[RecoveryAction], error) {
	if err := ctx.Err(); err != nil {
		return reconciliation.Plan[RecoveryAction]{}, err
	}
	if desired.Spec.Limit <= 0 {
		return reconciliation.Plan[RecoveryAction]{}, fmt.Errorf("%w: recovery limit must be positive", ErrInvalidJob)
	}

	observedAt := persistedTime(observed.ObservedAt)
	candidates := make([]RecoveryCandidate, 0, len(observed.Candidates))
	for _, candidate := range observed.Candidates {
		if !candidate.Attempt.HeartbeatDeadline.After(observedAt) {
			candidates = append(candidates, candidate)
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		return candidates[left].Attempt.ID < candidates[right].Attempt.ID
	})
	if len(candidates) > desired.Spec.Limit {
		candidates = candidates[:desired.Spec.Limit]
	}

	plan := reconciliation.Plan[RecoveryAction]{Actions: make([]reconciliation.Action[RecoveryAction], len(candidates))}
	for index, candidate := range candidates {
		attempt := candidate.Attempt
		plan.Actions[index] = reconciliation.Action[RecoveryAction]{
			ID: reconciliation.ActionID("recover/" + string(attempt.ID)),
			Value: RecoveryAction{
				AttemptID:         attempt.ID,
				FencingToken:      attempt.FencingToken,
				HeartbeatDeadline: persistedTime(attempt.HeartbeatDeadline),
				RecoveredAt:       observedAt,
			},
			Reversible: false,
		}
	}
	return plan, nil
}

// Apply performs one fenced recovery. Obsolete observations are idempotent success.
func (adapter RecoveryAdapter) Apply(ctx context.Context, action reconciliation.Action[RecoveryAction]) (reconciliation.Undo[RecoveryAction], error) {
	value := action.Value
	_, err := adapter.Store.RecoverAttempt(ctx, RecoverAttemptRequest{
		AttemptID:         value.AttemptID,
		FencingToken:      value.FencingToken,
		HeartbeatDeadline: persistedTime(value.HeartbeatDeadline),
		RecoveredAt:       persistedTime(value.RecoveredAt),
	})
	if errors.Is(err, ErrStaleAttempt) || errors.Is(err, ErrInvalidTransition) {
		return reconciliation.Undo[RecoveryAction]{}, nil
	}
	return reconciliation.Undo[RecoveryAction]{}, err
}

// Undo is a no-op because recovery actions are non-reversible.
func (RecoveryAdapter) Undo(context.Context, reconciliation.Undo[RecoveryAction]) error {
	return nil
}

// NewRecoveryEngine constructs a one-shot fenced recovery engine.
func NewRecoveryEngine(store Store, clock Clock, policy reconciliation.Policy) (*reconciliation.Engine[RecoverySpec, RecoveryObserved, RecoveryAction, RecoveryAction], error) {
	if isNilDependency(store) {
		return nil, fmt.Errorf("%w: nil recovery store", ErrInvalidJob)
	}
	if isNilDependency(clock) {
		return nil, fmt.Errorf("%w: nil recovery clock", ErrInvalidJob)
	}
	adapter := RecoveryAdapter{Store: store, Clock: clock}
	return reconciliation.NewEngine[RecoverySpec, RecoveryObserved, RecoveryAction, RecoveryAction](adapter, policy), nil
}
