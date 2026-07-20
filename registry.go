package jobs

import "context"

// Handler executes one durable job attempt.
type Handler func(context.Context, Job) (Result, error)

// Registry resolves handlers by job kind.
type Registry interface {
	Handler(kind string) (Handler, bool)
}

// MapRegistry resolves handlers from a map keyed by job kind.
type MapRegistry map[string]Handler

// Handler returns the handler registered for kind.
func (registry MapRegistry) Handler(kind string) (Handler, bool) {
	handler, ok := registry[kind]
	return handler, ok
}
