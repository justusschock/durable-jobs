package jobs

import "time"

// Clock supplies deterministic current time to clients and workers.
type Clock interface {
	Now() time.Time
}

func persistedTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.Round(0).UTC()
}
