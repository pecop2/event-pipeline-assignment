package unit

import (
	"event-pipeline/internal/config"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/validator"
	"event-pipeline/tests/testmocks"
	"testing"
	"time"
)

func TestWorkerPoolProcessing(t *testing.T) {
	metrics := pipeline.NewMetrics()
	storage := &testmocks.MockStorage{}
	processor := &testmocks.SlowProcessor{}
	val := &validator.BasicValidator{}

	cfg := &config.Config{
		WorkerCount:     4,
		QueueSize:       100,
		MaxRetries:      3,
		RetryBaseBackoff: 20 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(storage, processor, val, metrics, cfg)
	defer p.Shutdown()

	for i := 0; i < 20; i++ {
		p.Ingest(pipeline.Event{Type: "user_action", Source: "web"})
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if metrics.GetProcessed() >= 20 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 20 events, processed=%d", metrics.GetProcessed())
		}
		time.Sleep(10 * time.Millisecond)
	}

	if metrics.GetReceived() < 20 {
		t.Errorf("expected >=20 received, got %d", metrics.GetReceived())
	}
	if metrics.GetProcessed() < 20 {
		t.Errorf("expected >=20 processed, got %d", metrics.GetProcessed())
	}
	if metrics.GetFailed() != 0 {
		t.Errorf("expected 0 failed, got %d", metrics.GetFailed())
	}
	if len(storage.Events) < 20 {
		t.Errorf("expected 20 events stored, got %d", len(storage.Events))
	}
}

func TestEventValidationFailure(t *testing.T) {
	metrics := pipeline.NewMetrics()
	storage := &testmocks.MockStorage{}
	processor := &testmocks.SlowProcessor{}
	val := &validator.BasicValidator{}

	cfg := &config.Config{
		WorkerCount:     2,
		QueueSize:       100,
		MaxRetries:      3,
		RetryBaseBackoff: 20 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(storage, processor, val, metrics, cfg)
	defer p.Shutdown()

	p.Ingest(pipeline.Event{Source: "web"}) // invalid event

	time.Sleep(100 * time.Millisecond)

	if metrics.GetFailed() == 0 {
		t.Errorf("expected failed metric > 0, got %d", metrics.GetFailed())
	}
	if len(storage.Events) != 0 {
		t.Errorf("expected no events stored, got %d", len(storage.Events))
	}
}

func TestPipelineRetryAndFailureMetrics_FailAllRetries(t *testing.T) {
	store := &testmocks.FlakyStorage{ShouldFail: 5} // always fail
	proc := &testmocks.DummyProcessor{}
	val := &validator.BasicValidator{}
	metrics := pipeline.NewMetrics()

	cfg := &config.Config{
		WorkerCount:     1,
		QueueSize:       100,
		MaxRetries:      3,
		RetryBaseBackoff: 10 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(store, proc, val, metrics, cfg)
	defer p.Shutdown()

	p.Ingest(pipeline.Event{Type: "user_action", Source: "web"})

	deadline := time.Now().Add(2 * time.Second)
	for {
		if metrics.GetFailed() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting, failed=%d calls=%d",
				metrics.GetFailed(), store.Calls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if store.Calls != 3 {
		t.Errorf("expected 3 store attempts, got %d", store.Calls)
	}
	if metrics.GetProcessed() != 0 {
		t.Errorf("expected processed=0, got %d", metrics.GetProcessed())
	}
	if metrics.GetFailed() != 1 {
		t.Errorf("expected failed=1, got %d", metrics.GetFailed())
	}
}

func TestPipelineRetryAndSuccessAfterRetries(t *testing.T) {
	store := &testmocks.FlakyStorage{ShouldFail: 2} // fail twice, succeed 3rd
	proc := &testmocks.DummyProcessor{}
	val := &validator.BasicValidator{}
	metrics := pipeline.NewMetrics()

	cfg := &config.Config{
		WorkerCount:     1,
		QueueSize:       100,
		MaxRetries:      3,
		RetryBaseBackoff: 10 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(store, proc, val, metrics, cfg)
	defer p.Shutdown()

	p.Ingest(pipeline.Event{Type: "system_log", Source: "unit"})

	deadline := time.Now().Add(2 * time.Second)
	for {
		if metrics.GetProcessed() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting, processed=%d failed=%d calls=%d",
				metrics.GetProcessed(), metrics.GetFailed(), store.Calls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if store.Calls != 3 {
		t.Errorf("expected 3 attempts, got %d", store.Calls)
	}
	if metrics.GetProcessed() != 1 {
		t.Errorf("expected processed=1, got %d", metrics.GetProcessed())
	}
	if metrics.GetFailed() != 0 {
		t.Errorf("expected failed=0, got %d", metrics.GetFailed())
	}
}
