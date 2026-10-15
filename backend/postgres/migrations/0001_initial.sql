-- Initial durable jobs schema.
--
-- Job identities use the "C" collation so that ordering matches Go's bytewise
-- string comparison. Every timestamp is stored as a microsecond timestamptz
-- plus a sub-microsecond nanosecond remainder, so Go time values round-trip
-- exactly. Unsigned 64-bit counters use numeric(20, 0).

CREATE TABLE {{schema}}.jobs (
    id text COLLATE "C" PRIMARY KEY,
    queue text COLLATE "C" NOT NULL,
    kind text NOT NULL,
    payload bytea,
    headers json,
    priority bigint NOT NULL,
    state smallint NOT NULL,
    created_at timestamptz NOT NULL,
    created_at_nsec smallint NOT NULL,
    available_at timestamptz NOT NULL,
    available_at_nsec smallint NOT NULL,
    updated_at timestamptz NOT NULL,
    updated_at_nsec smallint NOT NULL,
    attempt numeric(20, 0) NOT NULL,
    retry_max_attempts numeric(20, 0) NOT NULL,
    retry_initial_backoff bigint NOT NULL,
    retry_max_backoff bigint NOT NULL,
    retry_multiplier double precision NOT NULL,
    depends_on text[] COLLATE "C",
    cancellation_requested boolean NOT NULL,
    result bytea,
    last_failure json,
    CONSTRAINT jobs_nsec_range CHECK (
        created_at_nsec BETWEEN 0 AND 999
        AND available_at_nsec BETWEEN 0 AND 999
        AND updated_at_nsec BETWEEN 0 AND 999
    )
);

-- Ready-job claims: queue-scoped, priority descending, then availability and ID.
CREATE INDEX jobs_claim_idx ON {{schema}}.jobs (
    queue, priority DESC, available_at, available_at_nsec, id
) WHERE state IN (1, 4);

-- Operator listings in recent-first order.
CREATE INDEX jobs_recent_idx ON {{schema}}.jobs (
    queue, updated_at DESC, updated_at_nsec DESC, created_at DESC, created_at_nsec DESC, id
);

-- Catalog pages in bytewise job ID order.
CREATE INDEX jobs_catalog_idx ON {{schema}}.jobs (queue, id);

-- Pending dependents scanned by terminal dependency failure propagation.
CREATE INDEX jobs_pending_dependents_idx ON {{schema}}.jobs (id)
    WHERE state = 1 AND depends_on IS NOT NULL;

-- Running jobs scanned by recovery.
CREATE INDEX jobs_running_idx ON {{schema}}.jobs (id) WHERE state = 2;

CREATE TABLE {{schema}}.attempts (
    id text COLLATE "C" PRIMARY KEY,
    job_id text COLLATE "C" NOT NULL REFERENCES {{schema}}.jobs (id),
    generation numeric(20, 0) NOT NULL,
    worker_id text NOT NULL,
    fencing_token numeric(20, 0) NOT NULL,
    started_at timestamptz NOT NULL,
    started_at_nsec smallint NOT NULL,
    heartbeat_deadline timestamptz NOT NULL,
    heartbeat_deadline_nsec smallint NOT NULL,
    job_snapshot json NOT NULL,
    CONSTRAINT attempts_nsec_range CHECK (
        started_at_nsec BETWEEN 0 AND 999
        AND heartbeat_deadline_nsec BETWEEN 0 AND 999
    )
);

CREATE INDEX attempts_job_generation_idx ON {{schema}}.attempts (job_id, generation);

CREATE SEQUENCE {{schema}}.attempt_sequence AS bigint MINVALUE 1;

CREATE TABLE {{schema}}.schedules (
    id text COLLATE "C" PRIMARY KEY,
    request json NOT NULL,
    next_run_at timestamptz NOT NULL,
    next_run_at_nsec smallint NOT NULL,
    interval_nanos bigint NOT NULL,
    enabled boolean NOT NULL,
    revision numeric(20, 0) NOT NULL,
    CONSTRAINT schedules_nsec_range CHECK (next_run_at_nsec BETWEEN 0 AND 999)
);

CREATE INDEX schedules_due_idx ON {{schema}}.schedules (next_run_at, next_run_at_nsec, id)
    WHERE enabled;
