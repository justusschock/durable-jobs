package jobs

import (
	"math"
	"time"
)

// RetryDelay returns the bounded exponential delay before the next attempt.
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
