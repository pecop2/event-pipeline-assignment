package unit

import (
	"event-pipeline/internal/config"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/validator"
	"event-pipeline/tests/testmocks"
	"testing"
	"time"
)

func TestWorkerRetriesOnStorageFailure(t *testing.T) {
	// storage will fail twice, succeed on 3rd attempt
	store := &testmocks.FlakyStorage{ShouldFail: 2}
	proc := &testmocks.DummyProcessor{}
	val := &validator.BasicValidator{}
	metrics := pipeline.NewMetrics()

	cfg := &config.Config{
		WorkerCount:      1,
		QueueSize:        100,
		MaxRetries:       3,
		RetryBaseBackoff: 10 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(store, proc, val, metrics, cfg)
	defer p.Shutdown()

	p.Ingest(pipeline.Event{Type: "test_event", Source: "unit"})

	// wait until processed
	deadline := time.Now().Add(2 * time.Second)
	for {
		if metrics.GetProcessed() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: received=%d processed=%d failed=%d calls=%d",
				metrics.GetReceived(), metrics.GetProcessed(), metrics.GetFailed(), store.Calls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if store.Calls != 3 {
		t.Errorf("expected 3 store attempts, got %d", store.Calls)
	}
	if metrics.GetFailed() != 0 {
		t.Errorf("expected failed=0 (final retry succeeded), got %d", metrics.GetFailed())
	}
	if metrics.GetProcessed() != 1 {
		t.Errorf("expected processed=1, got %d", metrics.GetProcessed())
	}
}
