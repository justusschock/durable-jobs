package jobs

import (
	"context"
	"fmt"
)

// ValidateDependencies verifies that dependencies exist and have not failed.
func ValidateDependencies(ctx context.Context, store Store, dependencies []JobID) error {
	if isNilDependency(store) {
		return fmt.Errorf("%w: nil dependency store", ErrInvalidJob)
	}
	for _, dependency := range dependencies {
		if err := ctx.Err(); err != nil {
			return err
		}
		job, err := store.Get(ctx, dependency)
		if err != nil {
			return err
		}
		if job.State == StateFailed || job.State == StateCanceled {
			return fmt.Errorf("%w: %s", ErrDependencyFailed, dependency)
		}
	}
	return nil
}
