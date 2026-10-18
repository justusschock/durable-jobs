package jobs_test

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	jobs "github.com/justusschock/go-durable-jobs"
)

// FR-JOBS-01
func TestValidateEnqueue(t *testing.T) {
	t.Parallel()

	valid := validEnqueueRequest()
	if err := jobs.ValidateEnqueue(valid); err != nil {
		t.Fatalf("ValidateEnqueue(valid) error = %v", err)
	}

	tests := map[string]func(*jobs.EnqueueRequest){
		"empty ID":             func(request *jobs.EnqueueRequest) { request.ID = "" },
		"blank ID":             func(request *jobs.EnqueueRequest) { request.ID = "  " },
		"invalid ID UTF-8":     func(request *jobs.EnqueueRequest) { request.ID = jobs.JobID([]byte{0xff}) },
		"empty queue":          func(request *jobs.EnqueueRequest) { request.Queue = "" },
		"blank kind":           func(request *jobs.EnqueueRequest) { request.Kind = "\t" },
		"zero attempts":        func(request *jobs.EnqueueRequest) { request.Retry.MaxAttempts = 0 },
		"zero initial backoff": func(request *jobs.EnqueueRequest) { request.Retry.InitialBackoff = 0 },
		"negative max backoff": func(request *jobs.EnqueueRequest) { request.Retry.MaxBackoff = -time.Second },
		"backoff order":        func(request *jobs.EnqueueRequest) { request.Retry.MaxBackoff = 500 * time.Millisecond },
		"small multiplier":     func(request *jobs.EnqueueRequest) { request.Retry.Multiplier = 0.5 },
		"NaN multiplier":       func(request *jobs.EnqueueRequest) { request.Retry.Multiplier = math.NaN() },
		"infinite multiplier":  func(request *jobs.EnqueueRequest) { request.Retry.Multiplier = math.Inf(1) },
		"empty dependency":     func(request *jobs.EnqueueRequest) { request.DependsOn = []jobs.JobID{""} },
		"self dependency":      func(request *jobs.EnqueueRequest) { request.DependsOn = []jobs.JobID{request.ID} },
		"duplicate dependency": func(request *jobs.EnqueueRequest) {
			request.DependsOn = []jobs.JobID{"parent", "parent"}
		},
	}

	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			request := valid.Clone()
			mutate(&request)
			if err := jobs.ValidateEnqueue(request); !errors.Is(err, jobs.ErrInvalidJob) {
				t.Fatalf("ValidateEnqueue() error = %v, want ErrInvalidJob", err)
			}
		})
	}
}

func TestValidateEnqueueDoesNotMutateDependencies(t *testing.T) {
	t.Parallel()

	request := validEnqueueRequest()
	request.DependsOn = []jobs.JobID{"z-parent", "a-parent"}
	want := append([]jobs.JobID(nil), request.DependsOn...)
	if err := jobs.ValidateEnqueue(request); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(request.DependsOn, want) {
		t.Fatalf("ValidateEnqueue mutated dependencies: got %v, want %v", request.DependsOn, want)
	}
}

// FR-JOBS-02
func TestJobAndRequestClonesOwnNestedState(t *testing.T) {
	t.Parallel()

	failure := &jobs.Failure{Code: "retry", Message: "try later", Retryable: true}
	job := jobs.Job{
		Payload:     []byte("request"),
		Headers:     map[string]string{"trace": "one"},
		DependsOn:   []jobs.JobID{"parent"},
		Result:      []byte("result"),
		LastFailure: failure,
	}
	clone := job.Clone()
	job.Payload[0] = 'X'
	job.Headers["trace"] = "mutated"
	job.DependsOn[0] = "mutated"
	job.Result[0] = 'X'
	job.LastFailure.Code = "mutated"
	if string(clone.Payload) != "request" || clone.Headers["trace"] != "one" || clone.DependsOn[0] != "parent" || string(clone.Result) != "result" || clone.LastFailure.Code != "retry" {
		t.Fatalf("Job.Clone() aliases input: %#v", clone)
	}

	request := validEnqueueRequest()
	requestClone := request.Clone()
	request.Payload[0] = 'X'
	request.Headers["trace"] = "mutated"
	request.DependsOn[0] = "mutated"
	if string(requestClone.Payload) != "payload" || requestClone.Headers["trace"] != "one" || requestClone.DependsOn[0] != "parent" {
		t.Fatalf("EnqueueRequest.Clone() aliases input: %#v", requestClone)
	}

	result := jobs.Result{Payload: []byte("result")}
	resultClone := result.Clone()
	result.Payload[0] = 'X'
	if string(resultClone.Payload) != "result" {
		t.Fatalf("Result.Clone() aliases input: %#v", resultClone)
	}
}

// FR-JOBS-02
func TestClonesPreserveNilState(t *testing.T) {
	t.Parallel()

	job := (jobs.Job{}).Clone()
	if job.Payload != nil || job.Headers != nil || job.DependsOn != nil || job.Result != nil || job.LastFailure != nil {
		t.Fatalf("Job.Clone() changed nil state: %#v", job)
	}
	request := (jobs.EnqueueRequest{}).Clone()
	if request.Payload != nil || request.Headers != nil || request.DependsOn != nil {
		t.Fatalf("EnqueueRequest.Clone() changed nil state: %#v", request)
	}
}

// NFR-JOBS-02
func TestJobErrorsDoNotContainPayloads(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		jobs.ErrInvalidJob,
		jobs.ErrJobExists,
		jobs.ErrJobNotFound,
		jobs.ErrNoReadyJob,
		jobs.ErrStaleAttempt,
		jobs.ErrInvalidTransition,
		jobs.ErrDependencyFailed,
	} {
		if strings.Contains(err.Error(), "raw-payload") {
			t.Fatalf("error %q contains payload text", err)
		}
	}
}

func validEnqueueRequest() jobs.EnqueueRequest {
	return jobs.EnqueueRequest{
		ID:        "job-1",
		Queue:     "alerts",
		Kind:      "deliver",
		Payload:   []byte("payload"),
		Headers:   map[string]string{"trace": "one"},
		DependsOn: []jobs.JobID{"parent"},
		Retry: jobs.RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Second,
			MaxBackoff:     time.Minute,
			Multiplier:     2,
		},
	}
}
