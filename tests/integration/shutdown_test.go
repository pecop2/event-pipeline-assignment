package integration

import (
	"event-pipeline/internal/config"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/validator"
	"event-pipeline/tests/testmocks"
	"testing"
	"time"
)

func TestGracefulShutdownDrainsQueue(t *testing.T) {
	storage := &testmocks.SlowStorage{}
	proc := &testmocks.FastProcessor{}
	val := &validator.BasicValidator{}
	metrics := pipeline.NewMetrics()

	cfg := config.Load()

	p := pipeline.NewEventPipeline(storage, proc, val, metrics, cfg)

	// enqueue multiple events
	total := 10
	for i := 0; i < total; i++ {
		p.Ingest(pipeline.Event{Type: "test_event", Source: "unit"})
	}

	// call shutdown (closes channel + cancels context)
	p.Shutdown()

	// wait until all events processed or timeout
	deadline := time.Now().Add(2 * time.Second)
	for {
		if int(metrics.GetProcessed()) >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: received=%d processed=%d stored=%d",
				metrics.GetReceived(), metrics.GetProcessed(), len(storage.Events))
		}
		time.Sleep(10 * time.Millisecond)
	}

	if metrics.GetReceived() != uint64(total) {
		t.Errorf("expected %d received, got %d", total, metrics.GetReceived())
	}
	if metrics.GetProcessed() != uint64(total) {
		t.Errorf("expected %d processed, got %d", total, metrics.GetProcessed())
	}
	if len(storage.Events) != total {
		t.Errorf("expected %d events stored, got %d", total, len(storage.Events))
	}

	// verify metrics report latency and EPS > 0
	if metrics.AvgLatencyMS() <= 0 {
		t.Error("expected average latency > 0")
	}
	if metrics.EPS() <= 0 {
		t.Error("expected EPS > 0")
	}
}
