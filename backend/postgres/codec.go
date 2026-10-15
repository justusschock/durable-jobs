package postgres

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/internal/storestate"
)

// splitTime normalizes value like the other stores and splits it into the
// microsecond instant PostgreSQL stores natively and the remaining
// nanoseconds.
func splitTime(value time.Time) (time.Time, int16) {
	value = storestate.PersistedTime(value)
	micro := value.Truncate(time.Microsecond)
	return micro, int16(value.Sub(micro))
}

// joinTime reverses splitTime and returns a normalized UTC time.
func joinTime(micro time.Time, nsec int16) time.Time {
	return storestate.PersistedTime(micro.Add(time.Duration(nsec)))
}

func formatUint(value uint64) string { return strconv.FormatUint(value, 10) }

func parseUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnsupportedFormat, err)
	}
	return parsed, nil
}

// jobColumns lists the stored job columns in scan and write order.
var jobColumns = []string{
	"id", "queue", "kind", "payload", "headers", "priority", "state",
	"created_at", "created_at_nsec", "available_at", "available_at_nsec", "updated_at", "updated_at_nsec",
	"attempt", "retry_max_attempts", "retry_initial_backoff", "retry_max_backoff", "retry_multiplier",
	"depends_on", "cancellation_requested", "result", "last_failure",
}

// jobColumnCasts maps columns whose Go representation travels as text.
var jobColumnCasts = map[string]string{
	"headers":            "json",
	"attempt":            "numeric",
	"retry_max_attempts": "numeric",
	"last_failure":       "json",
}

// selectJobColumns returns the select list for a job row, optionally qualified
// by a table alias.
func selectJobColumns(alias string) string {
	columns := make([]string, len(jobColumns))
	for index, column := range jobColumns {
		qualified := column
		if alias != "" {
			qualified = alias + "." + column
		}
		if _, cast := jobColumnCasts[column]; cast {
			qualified += "::text"
		}
		columns[index] = qualified
	}
	return strings.Join(columns, ", ")
}

// jobPlaceholders returns typed placeholders for jobColumns starting at $first.
func jobPlaceholders(first int) string {
	placeholders := make([]string, len(jobColumns))
	for index, column := range jobColumns {
		placeholders[index] = "$" + strconv.Itoa(first+index)
		if cast, ok := jobColumnCasts[column]; ok {
			placeholders[index] += "::text::" + cast
		}
	}
	return strings.Join(placeholders, ", ")
}

// jobArgs encodes job in jobColumns order. Nil and empty payloads, results,
// headers, and dependency lists remain distinguishable.
func jobArgs(job jobs.Job) ([]any, error) {
	var headers *string
	if job.Headers != nil {
		encoded, err := json.Marshal(job.Headers)
		if err != nil {
			return nil, err
		}
		value := string(encoded)
		headers = &value
	}
	var failure *string
	if job.LastFailure != nil {
		encoded, err := json.Marshal(job.LastFailure)
		if err != nil {
			return nil, err
		}
		value := string(encoded)
		failure = &value
	}
	var dependsOn []string
	if job.DependsOn != nil {
		dependsOn = make([]string, len(job.DependsOn))
		for index, dependency := range job.DependsOn {
			dependsOn[index] = string(dependency)
		}
	}
	createdAt, createdAtNsec := splitTime(job.CreatedAt)
	availableAt, availableAtNsec := splitTime(job.AvailableAt)
	updatedAt, updatedAtNsec := splitTime(job.UpdatedAt)
	return []any{
		string(job.ID), string(job.Queue), job.Kind, job.Payload, headers, int64(job.Priority), int16(job.State),
		createdAt, createdAtNsec, availableAt, availableAtNsec, updatedAt, updatedAtNsec,
		formatUint(uint64(job.Attempt)), formatUint(uint64(job.Retry.MaxAttempts)),
		int64(job.Retry.InitialBackoff), int64(job.Retry.MaxBackoff), job.Retry.Multiplier,
		dependsOn, job.CancellationRequested, job.Result, failure,
	}, nil
}

// scanJob decodes one row selected with selectJobColumns.
func scanJob(row pgx.Row) (jobs.Job, error) {
	var (
		id, queue, kind                               string
		payload, result                               []byte
		headers, failure                              *string
		priority                                      int64
		state                                         int16
		createdAt, availableAt, updatedAt             time.Time
		createdAtNsec, availableAtNsec, updatedAtNsec int16
		attempt, maxAttempts                          string
		initialBackoff, maxBackoff                    int64
		multiplier                                    float64
		dependsOn                                     []string
		cancellationRequested                         bool
	)
	if err := row.Scan(
		&id, &queue, &kind, &payload, &headers, &priority, &state,
		&createdAt, &createdAtNsec, &availableAt, &availableAtNsec, &updatedAt, &updatedAtNsec,
		&attempt, &maxAttempts, &initialBackoff, &maxBackoff, &multiplier,
		&dependsOn, &cancellationRequested, &result, &failure,
	); err != nil {
		return jobs.Job{}, err
	}
	job := jobs.Job{
		ID: jobs.JobID(id), Queue: jobs.Queue(queue), Kind: kind, Payload: payload,
		Priority: int(priority), State: jobs.State(state),
		CreatedAt: joinTime(createdAt, createdAtNsec), AvailableAt: joinTime(availableAt, availableAtNsec), UpdatedAt: joinTime(updatedAt, updatedAtNsec),
		Retry: jobs.RetryPolicy{
			InitialBackoff: time.Duration(initialBackoff), MaxBackoff: time.Duration(maxBackoff), Multiplier: multiplier,
		},
		CancellationRequested: cancellationRequested,
		Result:                result,
	}
	if headers != nil {
		if err := json.Unmarshal([]byte(*headers), &job.Headers); err != nil {
			return jobs.Job{}, fmt.Errorf("%w: headers: %v", ErrUnsupportedFormat, err)
		}
	}
	if failure != nil {
		job.LastFailure = &jobs.Failure{}
		if err := json.Unmarshal([]byte(*failure), job.LastFailure); err != nil {
			return jobs.Job{}, fmt.Errorf("%w: last failure: %v", ErrUnsupportedFormat, err)
		}
	}
	if dependsOn != nil {
		job.DependsOn = make([]jobs.JobID, len(dependsOn))
		for index, dependency := range dependsOn {
			job.DependsOn[index] = jobs.JobID(dependency)
		}
	}
	generation, err := parseUint(attempt)
	if err != nil {
		return jobs.Job{}, err
	}
	limit, err := parseUint(maxAttempts)
	if err != nil {
		return jobs.Job{}, err
	}
	job.Attempt = uint(generation)
	job.Retry.MaxAttempts = uint(limit)
	return job, nil
}

const attemptColumns = "id, job_id, generation::text, worker_id, fencing_token::text, started_at, started_at_nsec, heartbeat_deadline, heartbeat_deadline_nsec, job_snapshot::text"

const attemptPlaceholders = "$1, $2, $3::text::numeric, $4, $5::text::numeric, $6, $7, $8, $9, $10::text::json"

func attemptArgs(attempt jobs.Attempt) ([]any, error) {
	snapshot, err := json.Marshal(attempt.Job)
	if err != nil {
		return nil, err
	}
	startedAt, startedAtNsec := splitTime(attempt.StartedAt)
	deadline, deadlineNsec := splitTime(attempt.HeartbeatDeadline)
	return []any{
		string(attempt.ID), string(attempt.Job.ID), formatUint(uint64(attempt.Job.Attempt)), attempt.WorkerID,
		formatUint(uint64(attempt.FencingToken)), startedAt, startedAtNsec, deadline, deadlineNsec, string(snapshot),
	}, nil
}

func scanAttempt(row pgx.Row) (jobs.Attempt, error) {
	var (
		id, jobID, generation, workerID, fencingToken, snapshot string
		startedAt, deadline                                     time.Time
		startedAtNsec, deadlineNsec                             int16
	)
	if err := row.Scan(&id, &jobID, &generation, &workerID, &fencingToken, &startedAt, &startedAtNsec, &deadline, &deadlineNsec, &snapshot); err != nil {
		return jobs.Attempt{}, err
	}
	attempt := jobs.Attempt{
		ID: jobs.AttemptID(id), WorkerID: workerID,
		StartedAt: joinTime(startedAt, startedAtNsec), HeartbeatDeadline: joinTime(deadline, deadlineNsec),
	}
	if err := json.Unmarshal([]byte(snapshot), &attempt.Job); err != nil {
		return jobs.Attempt{}, fmt.Errorf("%w: attempt job snapshot: %v", ErrUnsupportedFormat, err)
	}
	if attempt.Job.ID != jobs.JobID(jobID) {
		return jobs.Attempt{}, fmt.Errorf("%w: attempt job snapshot does not match job ID", ErrUnsupportedFormat)
	}
	if _, err := parseUint(generation); err != nil {
		return jobs.Attempt{}, err
	}
	token, err := parseUint(fencingToken)
	if err != nil {
		return jobs.Attempt{}, err
	}
	attempt.FencingToken = coordination.FencingToken(token)
	storestate.NormalizeAttemptTimes(&attempt)
	return attempt, nil
}

const scheduleColumns = "id, request::text, next_run_at, next_run_at_nsec, interval_nanos, enabled, revision::text"

const schedulePlaceholders = "$1, $2::text::json, $3, $4, $5, $6, $7::text::numeric"

func scheduleArgs(schedule jobs.Schedule) ([]any, error) {
	request, err := json.Marshal(schedule.Request)
	if err != nil {
		return nil, err
	}
	nextRunAt, nextRunAtNsec := splitTime(schedule.NextRunAt)
	return []any{
		string(schedule.ID), string(request), nextRunAt, nextRunAtNsec, int64(schedule.Interval), schedule.Enabled, formatUint(schedule.Revision),
	}, nil
}

func scanSchedule(row pgx.Row) (jobs.Schedule, error) {
	var (
		id, request, revision string
		nextRunAt             time.Time
		nextRunAtNsec         int16
		interval              int64
		enabled               bool
	)
	if err := row.Scan(&id, &request, &nextRunAt, &nextRunAtNsec, &interval, &enabled, &revision); err != nil {
		return jobs.Schedule{}, err
	}
	schedule := jobs.Schedule{
		ID: jobs.ScheduleID(id), NextRunAt: joinTime(nextRunAt, nextRunAtNsec), Interval: time.Duration(interval), Enabled: enabled,
	}
	if err := json.Unmarshal([]byte(request), &schedule.Request); err != nil {
		return jobs.Schedule{}, fmt.Errorf("%w: schedule request: %v", ErrUnsupportedFormat, err)
	}
	parsed, err := parseUint(revision)
	if err != nil {
		return jobs.Schedule{}, err
	}
	schedule.Revision = parsed
	storestate.NormalizeScheduleTimes(&schedule)
	return schedule, nil
}
