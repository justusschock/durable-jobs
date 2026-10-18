// Package postgres provides a PostgreSQL-backed jobs.Store for multi-process
// and multi-replica deployments.
//
// The store implements jobs.Store, jobs.Catalog, and jobs.ScheduleStore with
// the same observable semantics as the bundled memory and bbolt stores, as
// verified by the shared storetest contract suites. Every mutation runs in one
// PostgreSQL transaction that locks the affected job rows, so any number of
// processes can share one schema. ClaimReady selects with
// FOR UPDATE SKIP LOCKED: concurrent claimers never block on, or double-claim,
// the same job.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/internal/storestate"
)

// DefaultSchema is the schema used when Options.Schema is empty.
const DefaultSchema = "durable_jobs"

// maxIdentifierLength is PostgreSQL's NAMEDATALEN-1 identifier limit.
const maxIdentifierLength = 63

// ErrUnsupportedFormat reports a stored value that this release cannot decode.
var ErrUnsupportedFormat = errors.New("durable jobs postgres: unsupported format")

// Options configures Open.
type Options struct {
	// Schema holds the store's tables. It defaults to DefaultSchema. Distinct
	// schemas in one database are fully independent stores.
	Schema string
}

// Store persists jobs, attempts, and schedules in one PostgreSQL schema. It is
// safe for concurrent use by multiple goroutines and processes.
type Store struct {
	pool       *pgxpool.Pool
	schemaName string
	schema     string
}

// Open returns a store using pool and the configured schema after verifying
// that the database is reachable. It does not create or change the schema;
// call Migrate before first use. The caller retains ownership of pool.
func Open(ctx context.Context, pool *pgxpool.Pool, options Options) (*Store, error) {
	if pool == nil {
		return nil, errors.New("durable jobs postgres: nil pool")
	}
	schema := options.Schema
	if schema == "" {
		schema = DefaultSchema
	}
	if strings.TrimSpace(schema) == "" || !utf8.ValidString(schema) || strings.ContainsRune(schema, '\x00') || len(schema) > maxIdentifierLength {
		return nil, fmt.Errorf("durable jobs postgres: invalid schema name %q", schema)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return &Store{pool: pool, schemaName: schema, schema: pgx.Identifier{schema}.Sanitize()}, nil
}

// Schema returns the unquoted schema name used by the store.
func (store *Store) Schema() string { return store.schemaName }

func (store *Store) sql(query string) string {
	return strings.ReplaceAll(query, "{{schema}}", store.schema)
}

// inTx runs fn in a read-committed transaction, retrying the whole
// transaction when PostgreSQL aborts it for a deadlock or serialization
// failure.
func (store *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	err := retryTransient(ctx, maxTransactionAttempts, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return pgx.BeginFunc(ctx, store.pool, fn)
	})
	return mapError(err)
}

// mapError classifies PostgreSQL text encoding rejections, such as NUL bytes
// or invalid UTF-8 in text identities, as invalid jobs.
func mapError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "22021", "22P05": // character_not_in_repertoire, untranslatable_character
			return fmt.Errorf("%w: %w", jobs.ErrInvalidJob, err)
		}
	}
	return err
}

var (
	insertJobSQL = `INSERT INTO {{schema}}.jobs (` + strings.Join(jobColumns, ", ") + `) VALUES (` + jobPlaceholders(1) + `) ON CONFLICT (id) DO NOTHING`
	updateJobSQL = `UPDATE {{schema}}.jobs SET (` + strings.Join(jobColumns[1:], ", ") + `) = (` + jobPlaceholders(1)[len("$1, "):] + `) WHERE id = $1`
	getJobSQL    = `SELECT ` + selectJobColumns("") + ` FROM {{schema}}.jobs WHERE id = $1`
	lockJobSQL   = getJobSQL + ` FOR UPDATE`

	// lockAttemptJobSQL locks the job owning an attempt before the attempt is
	// read, so attempt reads observe every committed heartbeat.
	lockAttemptJobSQL = `SELECT ` + selectJobColumns("") + ` FROM {{schema}}.jobs
WHERE id = (SELECT job_id FROM {{schema}}.attempts WHERE id = $1) FOR UPDATE`
	getAttemptSQL = `SELECT ` + attemptColumns + ` FROM {{schema}}.attempts WHERE id = $1`

	listJobsSQL = `SELECT ` + selectJobColumns("") + ` FROM {{schema}}.jobs WHERE queue = $1
ORDER BY updated_at DESC, updated_at_nsec DESC, created_at DESC, created_at_nsec DESC, id LIMIT $2`
	listJobPageSQL = `SELECT ` + selectJobColumns("") + ` FROM {{schema}}.jobs WHERE queue = $1 AND id > $2 ORDER BY id LIMIT $3`

	// claimReadySQL selects the first ready job in claim order. Rows locked by
	// concurrent claimers are skipped rather than waited on. Dependencies are
	// read without locks because succeeded is a terminal state.
	claimReadySQL = `SELECT ` + selectJobColumns("j") + ` FROM {{schema}}.jobs AS j
WHERE j.queue = $1
  AND j.state IN (1, 4)
  AND (j.available_at, j.available_at_nsec) <= ($2, $3)
  AND NOT EXISTS (
    SELECT 1 FROM unnest(j.depends_on) AS dependency(id)
    LEFT JOIN {{schema}}.jobs AS parent ON parent.id = dependency.id
    WHERE parent.id IS NULL OR parent.state <> 3
  )
ORDER BY j.priority DESC, j.available_at, j.available_at_nsec, j.id
LIMIT 1
FOR UPDATE OF j SKIP LOCKED`
	nextAttemptSQL       = `SELECT nextval($1::regclass)`
	insertAttemptSQL     = `INSERT INTO {{schema}}.attempts (id, job_id, generation, worker_id, fencing_token, started_at, started_at_nsec, heartbeat_deadline, heartbeat_deadline_nsec, job_snapshot) VALUES (` + attemptPlaceholders + `)`
	updateAttemptBeatSQL = `UPDATE {{schema}}.attempts SET heartbeat_deadline = $2, heartbeat_deadline_nsec = $3 WHERE id = $1`

	// pendingDependentsSQL locks, in ID order, every pending job with at
	// least one failed or canceled dependency.
	pendingDependentsSQL = `SELECT ` + selectJobColumns("j") + ` FROM {{schema}}.jobs AS j
WHERE j.state = 1 AND j.depends_on IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM {{schema}}.jobs AS parent
    WHERE parent.id = ANY (j.depends_on) AND parent.state IN (5, 6)
  )
ORDER BY j.id
FOR UPDATE OF j`
	terminalDependenciesSQL = `SELECT ` + selectJobColumns("") + ` FROM {{schema}}.jobs WHERE id = ANY ($1) AND state IN (5, 6)`

	recoveryCandidatesSQL = `SELECT ` + prefixedAttemptColumns("a") + ` FROM {{schema}}.attempts AS a
JOIN {{schema}}.jobs AS j ON j.id = a.job_id
WHERE j.state = 2 AND a.generation = j.attempt
  AND (a.heartbeat_deadline, a.heartbeat_deadline_nsec) <= ($1, $2)
ORDER BY a.heartbeat_deadline, a.heartbeat_deadline_nsec, a.id
LIMIT $3`

	insertScheduleSQL   = `INSERT INTO {{schema}}.schedules (` + plainColumns(scheduleColumns) + `) VALUES (` + schedulePlaceholders + `) ON CONFLICT (id) DO NOTHING`
	updateScheduleSQL   = `UPDATE {{schema}}.schedules SET (` + strings.Join(strings.Split(plainColumns(scheduleColumns), ", ")[1:], ", ") + `) = (` + schedulePlaceholders[len("$1, "):] + `) WHERE id = $1`
	getScheduleSQL      = `SELECT ` + scheduleColumns + ` FROM {{schema}}.schedules WHERE id = $1`
	lockScheduleSQL     = getScheduleSQL + ` FOR UPDATE`
	listDueSchedulesSQL = `SELECT ` + scheduleColumns + ` FROM {{schema}}.schedules
WHERE enabled AND (next_run_at, next_run_at_nsec) <= ($1, $2)
ORDER BY next_run_at, next_run_at_nsec, id LIMIT $3`
)

func prefixedAttemptColumns(alias string) string {
	columns := strings.Split(attemptColumns, ", ")
	for index, column := range columns {
		columns[index] = alias + "." + column
	}
	return strings.Join(columns, ", ")
}

func plainColumns(columns string) string {
	plain := strings.Split(columns, ", ")
	for index, column := range plain {
		plain[index] = strings.TrimSuffix(column, "::text")
	}
	return strings.Join(plain, ", ")
}

// Create atomically persists a new job unless its ID is already in use.
func (store *Store) Create(ctx context.Context, job jobs.Job) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	created := storestate.CloneJob(job)
	storestate.NormalizeJobTimes(&created)
	args, err := jobArgs(created)
	if err != nil {
		return jobs.Job{}, err
	}
	tag, err := store.pool.Exec(ctx, store.sql(insertJobSQL), args...)
	if err != nil {
		return jobs.Job{}, mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return jobs.Job{}, jobs.ErrJobExists
	}
	return storestate.CloneJob(created), nil
}

// Get returns an independently owned job snapshot.
func (store *Store) Get(ctx context.Context, id jobs.JobID) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	job, err := scanJob(store.pool.QueryRow(ctx, store.sql(getJobSQL), string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	if err != nil {
		return jobs.Job{}, mapError(err)
	}
	return job, nil
}

// ListJobs returns jobs for one queue in deterministic recent-first order.
func (store *Store) ListJobs(ctx context.Context, request jobs.ListJobsRequest) ([]jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Queue == "" || request.Limit <= 0 {
		return nil, jobs.ErrInvalidJob
	}
	listed, err := store.queryJobs(ctx, store.pool, listJobsSQL, string(request.Queue), request.Limit)
	if err != nil {
		return nil, err
	}
	return listed, nil
}

// ListJobPage returns one queue-scoped page in ascending raw job ID order.
func (store *Store) ListJobPage(ctx context.Context, request jobs.ListJobPageRequest) (jobs.JobPage, error) {
	if err := ctx.Err(); err != nil {
		return jobs.JobPage{}, err
	}
	if err := jobs.ValidateListJobPageRequest(request); err != nil {
		return jobs.JobPage{}, err
	}
	listed, err := store.queryJobs(ctx, store.pool, listJobPageSQL, string(request.Queue), string(request.After), request.Limit+1)
	if err != nil {
		return jobs.JobPage{}, err
	}
	page := jobs.JobPage{Jobs: listed}
	if len(listed) > request.Limit {
		page.Jobs = listed[:request.Limit]
		page.NextAfter = page.Jobs[request.Limit-1].ID
	}
	return page, nil
}

// ClaimReady atomically claims the highest-priority ready job in a queue.
// Jobs locked by concurrent transactions are skipped, so concurrent claimers
// in any number of processes never wait on or double-claim one job.
func (store *Store) ClaimReady(ctx context.Context, request jobs.ClaimRequest) (jobs.Attempt, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Attempt{}, err
	}
	var attempt jobs.Attempt
	err := store.inTx(ctx, func(tx pgx.Tx) error {
		now := storestate.PersistedTime(request.Now)
		micro, nsec := splitTime(now)
		job, err := scanJob(tx.QueryRow(ctx, store.sql(claimReadySQL), string(request.Queue), micro, nsec))
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.ErrNoReadyJob
		}
		if err != nil {
			return err
		}
		job.State = jobs.StateRunning
		job.Attempt++
		storestate.AdvanceUpdatedAt(&job, now)
		var sequence int64
		if err := tx.QueryRow(ctx, nextAttemptSQL, store.schema+".attempt_sequence").Scan(&sequence); err != nil {
			return err
		}
		attempt = jobs.Attempt{
			ID:                jobs.AttemptID(fmt.Sprintf("%s-%d", job.ID, sequence)),
			Job:               storestate.CloneJob(job),
			WorkerID:          request.WorkerID,
			FencingToken:      request.FencingToken,
			StartedAt:         now,
			HeartbeatDeadline: storestate.PersistedTime(request.HeartbeatDeadline),
		}
		if err := store.updateJob(ctx, tx, job); err != nil {
			return err
		}
		args, err := attemptArgs(attempt)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, store.sql(insertAttemptSQL), args...)
		return err
	})
	if err != nil {
		return jobs.Attempt{}, err
	}
	return storestate.CloneAttempt(attempt), nil
}

// Heartbeat records a new deadline for the currently fenced running attempt.
func (store *Store) Heartbeat(ctx context.Context, request jobs.HeartbeatRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.inTx(ctx, func(tx pgx.Tx) error {
		attempt, job, err := store.currentAttempt(ctx, tx, request.AttemptID, request.FencingToken)
		if err != nil {
			return err
		}
		attempt, job, changed, err := storestate.Heartbeat(attempt, job, request)
		if err != nil || !changed {
			return err
		}
		deadline, deadlineNsec := splitTime(attempt.HeartbeatDeadline)
		if _, err := tx.Exec(ctx, store.sql(updateAttemptBeatSQL), string(attempt.ID), deadline, deadlineNsec); err != nil {
			return err
		}
		return store.updateJob(ctx, tx, job)
	})
}

// Complete records the terminal success of a current attempt.
func (store *Store) Complete(ctx context.Context, request jobs.CompleteRequest) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	var completed jobs.Job
	err := store.inTx(ctx, func(tx pgx.Tx) error {
		_, job, err := store.currentAttempt(ctx, tx, request.AttemptID, request.FencingToken)
		if err != nil {
			return err
		}
		completed = storestate.Complete(job, request)
		return store.putTerminalAndPropagate(ctx, tx, completed)
	})
	if err != nil {
		return jobs.Job{}, err
	}
	return storestate.CloneJob(completed), nil
}

// Fail records an attempt failure, scheduling a retry when allowed.
func (store *Store) Fail(ctx context.Context, request jobs.FailRequest) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	var failed jobs.Job
	err := store.inTx(ctx, func(tx pgx.Tx) error {
		_, job, err := store.currentAttempt(ctx, tx, request.AttemptID, request.FencingToken)
		if err != nil {
			return err
		}
		failed = storestate.Fail(job, request)
		return store.putTerminalAndPropagate(ctx, tx, failed)
	})
	if err != nil {
		return jobs.Job{}, err
	}
	return storestate.CloneJob(failed), nil
}

// RequestCancel cancels an unclaimed job or marks a running job for cancellation.
func (store *Store) RequestCancel(ctx context.Context, id jobs.JobID, requestedAt time.Time) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	var canceled jobs.Job
	err := store.inTx(ctx, func(tx pgx.Tx) error {
		job, err := scanJob(tx.QueryRow(ctx, store.sql(lockJobSQL), string(id)))
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.ErrJobNotFound
		}
		if err != nil {
			return err
		}
		canceled, err = storestate.RequestCancel(job, requestedAt)
		if err != nil {
			return err
		}
		return store.putTerminalAndPropagate(ctx, tx, canceled)
	})
	if err != nil {
		return jobs.Job{}, err
	}
	return storestate.CloneJob(canceled), nil
}

// RecoverAttempt returns a currently fenced, expired attempt's job to pending state.
func (store *Store) RecoverAttempt(ctx context.Context, request jobs.RecoverAttemptRequest) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	var recovered jobs.Job
	err := store.inTx(ctx, func(tx pgx.Tx) error {
		attempt, job, err := store.lockedAttempt(ctx, tx, request.AttemptID)
		if err != nil {
			return err
		}
		recovered, err = storestate.RecoverAttempt(attempt, job, request)
		if err != nil {
			return err
		}
		return store.putTerminalAndPropagate(ctx, tx, recovered)
	})
	if err != nil {
		return jobs.Job{}, err
	}
	return storestate.CloneJob(recovered), nil
}

// ListRecoveryCandidates returns expired attempts in deterministic deadline order.
func (store *Store) ListRecoveryCandidates(ctx context.Context, observedAt time.Time, limit int) ([]jobs.RecoveryCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []jobs.RecoveryCandidate{}, nil
	}
	now := storestate.PersistedTime(observedAt)
	micro, nsec := splitTime(now)
	rows, err := store.pool.Query(ctx, store.sql(recoveryCandidatesSQL), micro, nsec, int64(limit))
	if err != nil {
		return nil, mapError(err)
	}
	attempts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Attempt, error) { return scanAttempt(row) })
	if err != nil {
		return nil, mapError(err)
	}
	candidates := make([]jobs.RecoveryCandidate, len(attempts))
	for index, attempt := range attempts {
		candidates[index] = jobs.RecoveryCandidate{Attempt: attempt, ObservedAt: now}
	}
	storestate.SortRecoveryCandidates(candidates)
	return candidates, nil
}

// PutSchedule creates or revision-fenced updates a durable schedule.
func (store *Store) PutSchedule(ctx context.Context, schedule jobs.Schedule, expectedRevision uint64) (jobs.Schedule, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Schedule{}, err
	}
	if err := storestate.ValidateSchedule(schedule); err != nil {
		return jobs.Schedule{}, err
	}
	var stored jobs.Schedule
	err := store.inTx(ctx, func(tx pgx.Tx) error {
		current, err := scanSchedule(tx.QueryRow(ctx, store.sql(lockScheduleSQL), string(schedule.ID)))
		exists := true
		if errors.Is(err, pgx.ErrNoRows) {
			exists = false
		} else if err != nil {
			return err
		}
		candidate := schedule
		if !exists {
			if expectedRevision != 0 {
				return jobs.ErrJobNotFound
			}
			candidate.Revision = 1
		} else {
			if expectedRevision == 0 {
				return jobs.ErrJobExists
			}
			if current.Revision != expectedRevision {
				return jobs.ErrStaleAttempt
			}
			candidate.Revision = current.Revision + 1
		}
		stored = storestate.CloneSchedule(candidate)
		storestate.NormalizeScheduleTimes(&stored)
		args, err := scheduleArgs(stored)
		if err != nil {
			return err
		}
		if exists {
			_, err = tx.Exec(ctx, store.sql(updateScheduleSQL), args...)
			return err
		}
		tag, err := tx.Exec(ctx, store.sql(insertScheduleSQL), args...)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return jobs.ErrJobExists
		}
		return nil
	})
	if err != nil {
		return jobs.Schedule{}, err
	}
	return storestate.CloneSchedule(stored), nil
}

// GetSchedule returns an independently owned schedule snapshot.
func (store *Store) GetSchedule(ctx context.Context, id jobs.ScheduleID) (jobs.Schedule, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Schedule{}, err
	}
	schedule, err := scanSchedule(store.pool.QueryRow(ctx, store.sql(getScheduleSQL), string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Schedule{}, jobs.ErrJobNotFound
	}
	if err != nil {
		return jobs.Schedule{}, mapError(err)
	}
	return schedule, nil
}

// ListDueSchedules returns enabled due schedules in deterministic order.
func (store *Store) ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]jobs.Schedule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []jobs.Schedule{}, nil
	}
	micro, nsec := splitTime(now)
	rows, err := store.pool.Query(ctx, store.sql(listDueSchedulesSQL), micro, nsec, int64(limit))
	if err != nil {
		return nil, mapError(err)
	}
	due, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Schedule, error) { return scanSchedule(row) })
	if err != nil {
		return nil, mapError(err)
	}
	storestate.SortSchedules(due)
	return due, nil
}

// AdvanceSchedule moves a schedule cursor using its current revision fence.
func (store *Store) AdvanceSchedule(ctx context.Context, id jobs.ScheduleID, expectedRevision uint64, nextRunAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.inTx(ctx, func(tx pgx.Tx) error {
		schedule, err := scanSchedule(tx.QueryRow(ctx, store.sql(lockScheduleSQL), string(id)))
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.ErrJobNotFound
		}
		if err != nil {
			return err
		}
		if schedule.Revision != expectedRevision {
			return jobs.ErrStaleAttempt
		}
		nextRunAt = storestate.PersistedTime(nextRunAt)
		if !nextRunAt.IsZero() && !nextRunAt.After(schedule.NextRunAt) {
			return jobs.ErrInvalidTransition
		}
		micro, nsec := splitTime(nextRunAt)
		_, err = tx.Exec(ctx, store.sql(`UPDATE {{schema}}.schedules
SET next_run_at = $2, next_run_at_nsec = $3, enabled = $4, revision = $5::text::numeric WHERE id = $1`),
			string(id), micro, nsec, !nextRunAt.IsZero(), formatUint(schedule.Revision+1))
		return err
	})
}

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (store *Store) queryJobs(ctx context.Context, db querier, query string, args ...any) ([]jobs.Job, error) {
	rows, err := db.Query(ctx, store.sql(query), args...)
	if err != nil {
		return nil, mapError(err)
	}
	listed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Job, error) { return scanJob(row) })
	if err != nil {
		return nil, mapError(err)
	}
	if listed == nil {
		listed = []jobs.Job{}
	}
	return listed, nil
}

func (store *Store) updateJob(ctx context.Context, tx pgx.Tx, job jobs.Job) error {
	args, err := jobArgs(job)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, store.sql(updateJobSQL), args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return jobs.ErrJobNotFound
	}
	return nil
}

// lockedAttempt locks the attempt's job row and then reads the attempt.
func (store *Store) lockedAttempt(ctx context.Context, tx pgx.Tx, id jobs.AttemptID) (jobs.Attempt, jobs.Job, error) {
	job, err := scanJob(tx.QueryRow(ctx, store.sql(lockAttemptJobSQL), string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Attempt{}, jobs.Job{}, jobs.ErrStaleAttempt
	}
	if err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	attempt, err := scanAttempt(tx.QueryRow(ctx, store.sql(getAttemptSQL), string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Attempt{}, jobs.Job{}, jobs.ErrStaleAttempt
	}
	if err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	return attempt, job, nil
}

func (store *Store) currentAttempt(ctx context.Context, tx pgx.Tx, id jobs.AttemptID, token coordination.FencingToken) (jobs.Attempt, jobs.Job, error) {
	attempt, job, err := store.lockedAttempt(ctx, tx, id)
	if err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	if err := storestate.CurrentAttempt(attempt, job, token); err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	return attempt, job, nil
}

// putTerminalAndPropagate writes job and, when it failed or was canceled,
// transitively fails every pending dependent exactly like the other stores.
func (store *Store) putTerminalAndPropagate(ctx context.Context, tx pgx.Tx, terminal jobs.Job) error {
	if err := store.updateJob(ctx, tx, terminal); err != nil {
		return err
	}
	if terminal.State != jobs.StateFailed && terminal.State != jobs.StateCanceled {
		return nil
	}
	for {
		dependents, err := store.queryJobs(ctx, tx, pendingDependentsSQL)
		if err != nil {
			return err
		}
		if len(dependents) == 0 {
			return nil
		}
		all := make(map[jobs.JobID]jobs.Job, len(dependents))
		var dependencyIDs []string
		for _, dependent := range dependents {
			all[dependent.ID] = dependent
			for _, dependency := range dependent.DependsOn {
				dependencyIDs = append(dependencyIDs, string(dependency))
			}
		}
		dependencies, err := store.queryJobs(ctx, tx, terminalDependenciesSQL, dependencyIDs)
		if err != nil {
			return err
		}
		for _, dependency := range dependencies {
			if _, pending := all[dependency.ID]; !pending {
				all[dependency.ID] = dependency
			}
		}
		changed := storestate.PropagateDependencyFailures(all, terminal.UpdatedAt)
		if len(changed) == 0 {
			return fmt.Errorf("durable jobs postgres: dependency propagation made no progress")
		}
		for _, id := range changed {
			if err := store.updateJob(ctx, tx, all[id]); err != nil {
				return err
			}
		}
	}
}

var _ jobs.Store = (*Store)(nil)
var _ jobs.Catalog = (*Store)(nil)
var _ jobs.ScheduleStore = (*Store)(nil)
