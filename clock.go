package jobs

import "time"

// Clock supplies the current time to clients, workers, schedulers, and
// recovery. Implementations must be safe for concurrent use.
type Clock interface {
	Now() time.Time
}

func persistedTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.Round(0).UTC()
}
