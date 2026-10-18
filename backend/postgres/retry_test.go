package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// NFR-JOBS-07
func TestRetryTransientBacksOffExponentiallyUntilSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var calls []time.Duration
		err := retryTransient(context.Background(), maxTransactionAttempts, func() error {
			calls = append(calls, time.Since(start))
			if len(calls) < 4 {
				return fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "40P01"})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("retryTransient() error = %v", err)
		}
		want := []time.Duration{0, time.Millisecond, 3 * time.Millisecond, 7 * time.Millisecond}
		if fmt.Sprint(calls) != fmt.Sprint(want) {
			t.Fatalf("operation call times = %v, want %v", calls, want)
		}
	})
}

// NFR-JOBS-07
func TestRetryTransientCapsBackoffAndBoundsAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		serialization := &pgconn.PgError{Code: "40001"}
		calls := 0
		err := retryTransient(context.Background(), maxTransactionAttempts, func() error {
			calls++
			return serialization
		})
		if !errors.Is(err, serialization) {
			t.Fatalf("retryTransient() error = %v, want final serialization failure", err)
		}
		if calls != maxTransactionAttempts {
			t.Fatalf("operation calls = %d, want %d", calls, maxTransactionAttempts)
		}
		// 1+2+4+8+16+32+64 ms of backoff between eight attempts.
		if elapsed := time.Since(start); elapsed != 127*time.Millisecond {
			t.Fatalf("elapsed backoff = %v, want 127ms", elapsed)
		}
	})
}

// NFR-JOBS-07
func TestRetryTransientReturnsPermanentErrorsImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		for _, permanent := range []error{
			errors.New("permanent"),
			&pgconn.PgError{Code: "23505"},
			context.Canceled,
		} {
			calls := 0
			err := retryTransient(context.Background(), maxTransactionAttempts, func() error {
				calls++
				return permanent
			})
			if !errors.Is(err, permanent) || calls != 1 {
				t.Fatalf("retryTransient(%v) = %v after %d calls, want one call", permanent, err, calls)
			}
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("permanent errors waited %v, want no backoff", elapsed)
		}
	})
}

// NFR-JOBS-07
func TestRetryTransientStopsWhenContextEndsDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		defer cancel()
		start := time.Now()
		calls := 0
		err := retryTransient(ctx, maxTransactionAttempts, func() error {
			calls++
			return &pgconn.PgError{Code: "40P01"}
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("retryTransient() error = %v, want context.DeadlineExceeded", err)
		}
		if calls != 2 || time.Since(start) != 2*time.Millisecond {
			t.Fatalf("calls = %d after %v, want 2 calls stopped at the 2ms deadline", calls, time.Since(start))
		}
	})
}
