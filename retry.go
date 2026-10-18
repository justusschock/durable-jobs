package jobs

import (
	"math"
	"time"
)

// RetryDelay returns the bounded exponential delay before the next attempt
// after attempt failed: InitialBackoff * Multiplier^(attempt-1), capped at
// MaxBackoff. Attempt 0 is treated as 1, and overflow saturates at MaxBackoff.
func RetryDelay(policy RetryPolicy, attempt uint) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	delay := float64(policy.InitialBackoff) * math.Pow(policy.Multiplier, float64(attempt-1))
	if math.IsInf(delay, 0) || math.IsNaN(delay) || delay >= float64(policy.MaxBackoff) {
		return policy.MaxBackoff
	}
	return time.Duration(delay)
}
