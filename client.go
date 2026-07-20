package jobs

import (
	"context"
	"fmt"
	"reflect"

	messaging "github.com/justusschock/go-messaging"
)

// Client exposes durable job submission and cancellation operations.
type Client interface {
	Enqueue(context.Context, EnqueueRequest) (Job, error)
	Get(context.Context, JobID) (Job, error)
	Cancel(context.Context, JobID) error
}

// DurableClient persists jobs and publishes queue wakeups.
type DurableClient struct {
	store     Store
	publisher messaging.Publisher[Wakeup]
	clock     Clock
}

// NewClient constructs a durable job client.
func NewClient(store Store, publisher messaging.Publisher[Wakeup], clock Clock) (*DurableClient, error) {
	if isNilDependency(store) {
		return nil, fmt.Errorf("%w: nil store", ErrInvalidJob)
	}
	if isNilDependency(publisher) {
		return nil, fmt.Errorf("%w: nil publisher", ErrInvalidJob)
	}
	if isNilDependency(clock) {
		return nil, fmt.Errorf("%w: nil clock", ErrInvalidJob)
	}
	return &DurableClient{store: store, publisher: publisher, clock: clock}, nil
}

func isNilDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Enqueue validates and durably creates a pending job before publishing a wakeup.
func (client *DurableClient) Enqueue(ctx context.Context, request EnqueueRequest) (Job, error) {
	if err := ValidateEnqueue(request); err != nil {
		return Job{}, err
	}

	request = request.Clone()
	now := persistedTime(client.clock.Now())
	availableAt := now
	if !request.AvailableAt.IsZero() {
		availableAt = persistedTime(request.AvailableAt)
	}

	job := Job{
		ID:          request.ID,
		Queue:       request.Queue,
		Kind:        request.Kind,
		Payload:     request.Payload,
		Headers:     request.Headers,
		Priority:    request.Priority,
		State:       StatePending,
		CreatedAt:   now,
		AvailableAt: availableAt,
		UpdatedAt:   now,
		Retry:       request.Retry,
		DependsOn:   request.DependsOn,
	}
	created, err := client.store.Create(ctx, job)
	if err != nil {
		return Job{}, err
	}

	wakeup := Wakeup{Queue: created.Queue, JobID: created.ID, AvailableAt: created.AvailableAt}
	envelope, err := messaging.NewEnvelope("jobs.wakeup."+string(created.Queue), wakeup)
	if err != nil {
		return created, fmt.Errorf("%w: %w", ErrWakeupPublish, err)
	}
	if err := client.publisher.Publish(ctx, envelope); err != nil {
		return created, fmt.Errorf("%w: %w", ErrWakeupPublish, err)
	}
	return created, nil
}

// Get returns the current durable job snapshot.
func (client *DurableClient) Get(ctx context.Context, id JobID) (Job, error) {
	return client.store.Get(ctx, id)
}

// Cancel requests cancellation using the client's clock.
func (client *DurableClient) Cancel(ctx context.Context, id JobID) error {
	_, err := client.store.RequestCancel(ctx, id, persistedTime(client.clock.Now()))
	return err
}
