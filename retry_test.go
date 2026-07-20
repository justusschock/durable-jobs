package jobs_test

import (
	"testing"
	"time"

	jobs "github.com/justusschock/go-durable-jobs"
)

func TestRetryDelayUsesBaseDelayForFirstAttempt(t *testing.T) {
	t.Parallel()

	policy := jobs.RetryPolicy{
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     3,
	}

	if got := jobs.RetryDelay(policy, 1); got != 2*time.Second {
		t.Fatalf("RetryDelay(attempt 1) = %v, want %v", got, 2*time.Second)
	}
}

func TestRetryDelayCapsExponentialBackoff(t *testing.T) {
	t.Parallel()

	policy := jobs.RetryPolicy{
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     15 * time.Second,
		Multiplier:     3,
	}

	if got := jobs.RetryDelay(policy, 4); got != 15*time.Second {
		t.Fatalf("RetryDelay(attempt 4) = %v, want capped %v", got, 15*time.Second)
	}
}

func TestRetryDelaySaturatesWithoutOverflow(t *testing.T) {
	t.Parallel()

	policy := jobs.RetryPolicy{
		InitialBackoff: time.Hour,
		MaxBackoff:     time.Duration(1 << 62),
		Multiplier:     1e300,
	}

	if got := jobs.RetryDelay(policy, 2); got != policy.MaxBackoff {
		t.Fatalf("RetryDelay(overflow) = %v, want capped %v", got, policy.MaxBackoff)
	}
}
