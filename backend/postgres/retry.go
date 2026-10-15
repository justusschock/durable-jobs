package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// maxTransactionAttempts bounds retries of transactions aborted by
	// PostgreSQL deadlock detection or serialization failures.
	maxTransactionAttempts = 8
	initialRetryDelay      = time.Millisecond
	maxRetryDelay          = 64 * time.Millisecond
)

// retryTransient runs operation until it succeeds, fails with a non-transient
// error, or exhausts attempts. Transient failures back off exponentially from
// initialRetryDelay to maxRetryDelay. Context cancellation during a backoff
// returns the context error.
func retryTransient(ctx context.Context, attempts int, operation func() error) error {
	delay := initialRetryDelay
	for attempt := 1; ; attempt++ {
		err := operation()
		if err == nil || attempt >= attempts || !transient(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(2*delay, maxRetryDelay)
	}
}

// transient reports whether PostgreSQL aborted a transaction that can be
// retried from the start without changing its outcome.
func transient(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "40001", "40P01": // serialization_failure, deadlock_detected
		return true
	default:
		return false
	}
}
