package unit

import (
	"testing"
	"time"

	"event-pipeline/internal/pipeline"
)

func TestMetricsCountersAndLatency(t *testing.T) {
	m := pipeline.NewMetrics()

	// Initially everything is zero
	if m.GetReceived() != 0 {
		t.Errorf("expected received=0, got %d", m.GetReceived())
	}
	if m.GetProcessed() != 0 {
		t.Errorf("expected processed=0, got %d", m.GetProcessed())
	}
	if m.GetFailed() != 0 {
		t.Errorf("expected failed=0, got %d", m.GetFailed())
	}
	if m.AvgLatencyMS() != 0 {
		t.Errorf("expected avg latency=0, got %v", m.AvgLatencyMS())
	}
	if m.EPS() < 0 {
		t.Errorf("expected non-negative EPS, got %v", m.EPS())
	}

	// Simulate events
	m.IncReceived()
	m.IncProcessed()
	m.AddLatency(10)

	m.IncReceived()
	m.IncProcessed()
	m.AddLatency(20)

	m.IncReceived()
	m.IncFailed()

	if got := m.GetReceived(); got != 3 {
		t.Errorf("expected received=3, got %d", got)
	}
	if got := m.GetProcessed(); got != 2 {
		t.Errorf("expected processed=2, got %d", got)
	}
	if got := m.GetFailed(); got != 1 {
		t.Errorf("expected failed=1, got %d", got)
	}

	avg := m.AvgLatencyMS()
	if avg < 10 || avg > 20 {
		t.Errorf("expected avg latency between 10 and 20, got %v", avg)
	}

	// Wait a little so uptime > 0
	time.Sleep(100 * time.Millisecond)
	if m.EPS() <= 0 {
		t.Errorf("expected EPS > 0, got %v", m.EPS())
	}
	if m.StartTime().After(time.Now()) {
		t.Error("start time should be in the past")
	}
}
