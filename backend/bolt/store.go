// Package bolt provides a crash-safe bbolt-backed jobs.Store.
package bolt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/internal/storestate"
	bbolt "go.etcd.io/bbolt"
)

var (
	jobsBucket         = []byte("jobs")
	attemptsBucket     = []byte("attempts")
	metaBucket         = []byte("meta")
	attemptSequenceKey = []byte("attempt_sequence")
)

// Options configures Open.
type Options struct {
	Timeout time.Duration
	NoSync  bool
}

// Store persists jobs and attempts in one bbolt database.
type Store struct {
	db *bbolt.DB
}

type bucketSet struct {
	jobs     *bbolt.Bucket
	attempts *bbolt.Bucket
	meta     *bbolt.Bucket
}

// Open opens path with owner-only permissions and validates its schema.
func Open(path string, options Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("durable jobs bolt: empty path")
	}
	if options.Timeout < 0 {
		return nil, errors.New("durable jobs bolt: negative timeout")
	}
	initialize, err := needsInitialization(path)
	if err != nil {
		return nil, err
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: options.Timeout, NoSync: options.NoSync})
	if err != nil {
		return nil, err
	}
	if initialize {
		err = db.Update(initializeBuckets)
	} else {
		err = db.View(func(tx *bbolt.Tx) error {
			_, err := checkedBuckets(tx)
			return err
		})
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the database file lock.
func (store *Store) Close() error { return store.db.Close() }

// Create atomically persists a new job unless its ID is already in use.
func (store *Store) Create(ctx context.Context, job jobs.Job) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	var created jobs.Job
	err := store.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		bucket := buckets.jobs
		key := []byte(job.ID)
		if bucket.Get(key) != nil {
			return jobs.ErrJobExists
		}
		created = storestate.CloneJob(job)
		storestate.NormalizeJobTimes(&created)
		return put(bucket, key, created)
	})
	if err != nil {
		return jobs.Job{}, err
	}
	return storestate.CloneJob(created), nil
}

// Get returns an independently owned job snapshot.
func (store *Store) Get(ctx context.Context, id jobs.JobID) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	var job jobs.Job
	err := store.db.View(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		job, err = getJob(buckets.jobs, id)
		return err
	})
	if err != nil {
		return jobs.Job{}, err
	}
	return storestate.CloneJob(job), nil
}

// ClaimReady atomically claims the highest-priority ready job in a queue.
func (store *Store) ClaimReady(ctx context.Context, request jobs.ClaimRequest) (jobs.Attempt, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Attempt{}, err
	}
	var attempt jobs.Attempt
	err := store.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		all, err := allJobs(buckets.jobs)
		if err != nil {
			return err
		}
		now := storestate.PersistedTime(request.Now)
		var selected *jobs.Job
		for _, candidate := range all {
			if candidate.Queue != request.Queue || !storestate.Claimable(candidate, now, all) {
				continue
			}
			if selected == nil || storestate.Precedes(candidate, *selected) {
				copy := candidate
				selected = &copy
			}
		}
		if selected == nil {
			return jobs.ErrNoReadyJob
		}
		job := *selected
		job.State = jobs.StateRunning
		job.Attempt++
		storestate.AdvanceUpdatedAt(&job, now)
		sequence, err := nextAttemptSequence(buckets.meta)
		if err != nil {
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
		if err := put(buckets.jobs, []byte(job.ID), job); err != nil {
			return err
		}
		return put(buckets.attempts, []byte(attempt.ID), attempt)
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
	return store.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		attempt, job, err := currentAttempt(buckets, request.AttemptID, request.FencingToken)
		if err != nil {
			return err
		}
		attempt, job, changed, err := storestate.Heartbeat(attempt, job, request)
		if err != nil || !changed {
			return err
		}
		if err := put(buckets.attempts, []byte(attempt.ID), attempt); err != nil {
			return err
		}
		return put(buckets.jobs, []byte(job.ID), job)
	})
}

// Complete records the terminal success of a current attempt.
func (store *Store) Complete(ctx context.Context, request jobs.CompleteRequest) (jobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Job{}, err
	}
	var completed jobs.Job
	err := store.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		_, job, err := currentAttempt(buckets, request.AttemptID, request.FencingToken)
		if err != nil {
			return err
		}
		completed = storestate.Complete(job, request)
		return put(buckets.jobs, []byte(completed.ID), completed)
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
	err := store.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		_, job, err := currentAttempt(buckets, request.AttemptID, request.FencingToken)
		if err != nil {
			return err
		}
		failed = storestate.Fail(job, request)
		return put(buckets.jobs, []byte(failed.ID), failed)
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
	err := store.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		job, err := getJob(buckets.jobs, id)
		if err != nil {
			return err
		}
		canceled, err = storestate.RequestCancel(job, requestedAt)
		if err != nil {
			return err
		}
		return put(buckets.jobs, []byte(canceled.ID), canceled)
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
	err := store.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		attempt, err := getAttempt(buckets.attempts, request.AttemptID)
		if err != nil {
			return err
		}
		job, err := getJob(buckets.jobs, attempt.Job.ID)
		if err != nil {
			return err
		}
		recovered, err = storestate.RecoverAttempt(attempt, job, request)
		if err != nil {
			return err
		}
		return put(buckets.jobs, []byte(recovered.ID), recovered)
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
		return []jobs.RecoveryCandidate{}, store.db.View(func(tx *bbolt.Tx) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, err := checkedBuckets(tx)
			return err
		})
	}
	var candidates []jobs.RecoveryCandidate
	err := store.db.View(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		buckets, err := checkedBuckets(tx)
		if err != nil {
			return err
		}
		jobsByID, err := allJobs(buckets.jobs)
		if err != nil {
			return err
		}
		now := storestate.PersistedTime(observedAt)
		return buckets.attempts.ForEach(func(_, value []byte) error {
			var attempt jobs.Attempt
			if err := decode(bytes.Clone(value), &attempt); err != nil {
				return err
			}
			storestate.NormalizeAttemptTimes(&attempt)
			job, exists := jobsByID[attempt.Job.ID]
			if exists && job.State == jobs.StateRunning && attempt.Job.Attempt == job.Attempt && !attempt.HeartbeatDeadline.After(now) {
				candidates = append(candidates, jobs.RecoveryCandidate{Attempt: storestate.CloneAttempt(attempt), ObservedAt: now})
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	storestate.SortRecoveryCandidates(candidates)
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates, nil
}

func currentAttempt(buckets bucketSet, id jobs.AttemptID, token coordination.FencingToken) (jobs.Attempt, jobs.Job, error) {
	attempt, err := getAttempt(buckets.attempts, id)
	if err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	job, err := getJob(buckets.jobs, attempt.Job.ID)
	if err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	if err := storestate.CurrentAttempt(attempt, job, token); err != nil {
		return jobs.Attempt{}, jobs.Job{}, err
	}
	return attempt, job, nil
}

func needsInitialization(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		return info.Size() == 0, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	return false, err
}

func initializeBuckets(tx *bbolt.Tx) error {
	for _, name := range [][]byte{jobsBucket, attemptsBucket, metaBucket} {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	return nil
}

func checkedBuckets(tx *bbolt.Tx) (bucketSet, error) {
	buckets := bucketSet{
		jobs:     tx.Bucket(jobsBucket),
		attempts: tx.Bucket(attemptsBucket),
		meta:     tx.Bucket(metaBucket),
	}
	if buckets.jobs == nil || buckets.attempts == nil || buckets.meta == nil {
		return bucketSet{}, ErrUnsupportedFormat
	}
	return buckets, nil
}

func getJob(bucket *bbolt.Bucket, id jobs.JobID) (jobs.Job, error) {
	value := bytes.Clone(bucket.Get([]byte(id)))
	if value == nil {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	var job jobs.Job
	if err := decode(value, &job); err != nil {
		return jobs.Job{}, err
	}
	storestate.NormalizeJobTimes(&job)
	return job, nil
}

func getAttempt(bucket *bbolt.Bucket, id jobs.AttemptID) (jobs.Attempt, error) {
	value := bytes.Clone(bucket.Get([]byte(id)))
	if value == nil {
		return jobs.Attempt{}, jobs.ErrStaleAttempt
	}
	var attempt jobs.Attempt
	if err := decode(value, &attempt); err != nil {
		return jobs.Attempt{}, err
	}
	storestate.NormalizeAttemptTimes(&attempt)
	return attempt, nil
}

func allJobs(bucket *bbolt.Bucket) (map[jobs.JobID]jobs.Job, error) {
	all := make(map[jobs.JobID]jobs.Job)
	err := bucket.ForEach(func(_, value []byte) error {
		var job jobs.Job
		if err := decode(bytes.Clone(value), &job); err != nil {
			return err
		}
		storestate.NormalizeJobTimes(&job)
		all[job.ID] = job
		return nil
	})
	return all, err
}

func nextAttemptSequence(bucket *bbolt.Bucket) (uint64, error) {
	value := bytes.Clone(bucket.Get(attemptSequenceKey))
	var sequence uint64
	if value != nil {
		if err := decode(value, &sequence); err != nil {
			return 0, err
		}
	}
	sequence++
	return sequence, put(bucket, attemptSequenceKey, sequence)
}

func put(bucket *bbolt.Bucket, key []byte, value any) error {
	encoded, err := encode(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, encoded)
}

var _ jobs.Store = (*Store)(nil)
