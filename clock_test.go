package jobs

import (
	"testing"
	"time"
)

// FR-JOBS-19
func TestPersistedTimePreservesTimesOutsideUnixNanoRange(t *testing.T) {
	t.Parallel()

	input := time.Date(2500, time.July, 17, 12, 34, 56, 789, time.FixedZone("offset", 2*60*60))
	got := persistedTime(input)
	if !got.Equal(input) {
		t.Fatalf("persistedTime() = %v, want same instant as %v", got, input)
	}
	if got.Location() != time.UTC {
		t.Fatalf("persistedTime() location = %v, want UTC", got.Location())
	}
	if got != got.Round(0) {
		t.Fatal("persistedTime() retained a monotonic component")
	}
}

// FR-JOBS-19
func TestPersistedTimePreservesZero(t *testing.T) {
	t.Parallel()

	if got := persistedTime(time.Time{}); !got.IsZero() {
		t.Fatalf("persistedTime(zero) = %v", got)
	}
}
