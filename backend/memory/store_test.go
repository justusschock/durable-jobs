package memory_test

import (
	"testing"

	jobs "github.com/justusschock/go-durable-jobs"
	"github.com/justusschock/go-durable-jobs/backend/memory"
	"github.com/justusschock/go-durable-jobs/internal/storetest"
)

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) jobs.Store {
		t.Helper()
		return memory.New()
	})
}
