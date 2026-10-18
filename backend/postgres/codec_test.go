package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	jobs "github.com/justusschock/durable-jobs"
)

// FR-JOBS-19
func TestSplitTimeRoundTripsNanoseconds(t *testing.T) {
	for _, value := range []time.Time{
		{},
		time.Unix(0, 0),
		time.Unix(100, 1),
		time.Unix(100, 999_999_999).In(time.FixedZone("east", 2*60*60)),
		time.Unix(-1, 123_456_789),
		time.Date(1, time.January, 1, 0, 0, 0, 1, time.UTC),
		time.Date(9999, time.December, 31, 23, 59, 59, 999_999_999, time.UTC),
		time.Now(),
	} {
		micro, nsec := splitTime(value)
		if nsec < 0 || nsec > 999 || micro.Nanosecond()%1000 != 0 {
			t.Fatalf("splitTime(%v) = (%v, %d), want microsecond instant and 0..999 remainder", value, micro, nsec)
		}
		got := joinTime(micro.In(time.Local), nsec)
		if !got.Equal(value) || (!got.IsZero() && got.Location() != time.UTC) {
			t.Fatalf("joinTime(splitTime(%v)) = %v, want equal UTC time", value, got)
		}
		if value.IsZero() && got != (time.Time{}) {
			t.Fatalf("zero time round-tripped to %#v, want time.Time{}", got)
		}
	}
}

func TestParseUintRejectsInvalidStoredCounters(t *testing.T) {
	if got, err := parseUint("18446744073709551615"); err != nil || got != ^uint64(0) {
		t.Fatalf("parseUint(max) = %d, %v", got, err)
	}
	for _, value := range []string{"", "-1", "18446744073709551616", "1.5"} {
		if _, err := parseUint(value); !errors.Is(err, ErrUnsupportedFormat) {
			t.Fatalf("parseUint(%q) error = %v, want ErrUnsupportedFormat", value, err)
		}
	}
}

func TestMapErrorClassifiesTextEncodingRejections(t *testing.T) {
	for _, code := range []string{"22021", "22P05"} {
		if err := mapError(&pgconn.PgError{Code: code}); !errors.Is(err, jobs.ErrInvalidJob) {
			t.Fatalf("mapError(%s) = %v, want ErrInvalidJob", code, err)
		}
	}
	other := &pgconn.PgError{Code: "23505"}
	if err := mapError(other); err != other { //nolint:errorlint // mapError must return other errors unchanged, so this checks identity.
		t.Fatalf("mapError(23505) = %v, want unchanged error", err)
	}
}

func TestStateConstantsMatchSchemaLiterals(t *testing.T) {
	// The migration's partial indexes and the claim, propagation, and recovery
	// queries embed these state values as literals.
	for state, want := range map[jobs.State]int{
		jobs.StatePending: 1, jobs.StateRunning: 2, jobs.StateSucceeded: 3,
		jobs.StateRetryWaiting: 4, jobs.StateFailed: 5, jobs.StateCanceled: 6,
	} {
		if int(state) != want {
			t.Fatalf("state %d changed value; update the postgres schema literals (want %d)", state, want)
		}
	}
}

// FR-JOBS-16
func TestLoadMigrationsAreOrderedAndVersioned(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) == 0 || migrations[0].version != 1 || migrations[0].name != "initial" {
		t.Fatalf("loadMigrations() = %#v, want initial migration first", migrations)
	}
	for index := 1; index < len(migrations); index++ {
		if migrations[index].version != migrations[index-1].version+1 {
			t.Fatalf("migration versions are not contiguous: %d after %d", migrations[index].version, migrations[index-1].version)
		}
	}
}
