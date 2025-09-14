package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"event-pipeline/internal/api"
	"event-pipeline/internal/config"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/validator"
	"event-pipeline/tests/testmocks"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- helper mocks for API tests ---

type mockStorage struct {
	stored []pipeline.ProcessedEvent
}

func (m *mockStorage) Store(_ context.Context, events []pipeline.ProcessedEvent) error {
	m.stored = append(m.stored, events...)
	return nil
}

type mockProcessor struct{}

func (p *mockProcessor) Process(ctx context.Context, e pipeline.Event) (*pipeline.ProcessedEvent, error) {
	// simulate small processing delay so latency > 0
	time.Sleep(5 * time.Millisecond)

	return &pipeline.ProcessedEvent{
		Event:            e,
		ProcessingTimeMS: 5,
		ProcessedAt:      time.Now(),
	}, nil
}

func setupTestServer() (*httptest.Server, *pipeline.EventPipeline, *mockStorage) {
	metrics := pipeline.NewMetrics()
	val := &validator.BasicValidator{}
	proc := &mockProcessor{}
	store := &mockStorage{}

	cfg := &config.Config{
		WorkerCount:      2,
		QueueSize:        100,
		MaxRetries:       3,
		RetryBaseBackoff: 20 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(store, proc, val, metrics, cfg)
	server := api.NewServer(p)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	ts := httptest.NewServer(mux)
	return ts, p, store
}

// --- Tests ---

func TestHealthEndpoint(t *testing.T) {
	ts, p, _ := setupTestServer()
	defer ts.Close()
	defer p.Shutdown()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestPostSingleEvent(t *testing.T) {
	ts, p, store := setupTestServer()
	defer ts.Close()
	defer p.Shutdown()

	payload := `{"type":"user_action","source":"web","data":{"action":"click"}}`
	resp, err := http.Post(ts.URL+"/events", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}

	time.Sleep(150 * time.Millisecond)

	if p.Metrics().GetProcessed() == 0 {
		t.Errorf("expected processed metric > 0, got %d", p.Metrics().GetProcessed())
	}
	if len(store.stored) == 0 {
		t.Errorf("expected event stored, got none")
	}
}

func TestPostBatchEvents(t *testing.T) {
	ts, p, store := setupTestServer()
	defer ts.Close()
	defer p.Shutdown()

	payload := `{"events":[
		{"type":"sensor_data","source":"iot_device","data":{"temperature":23.5}},
		{"type":"system_log","source":"system","data":{"level":"error","message":"fail"}}
	]}`
	resp, err := http.Post(ts.URL+"/events/batch", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}

	time.Sleep(200 * time.Millisecond)

	if p.Metrics().GetProcessed() < 2 {
		t.Errorf("expected >=2 processed, got %d", p.Metrics().GetProcessed())
	}
	if len(store.stored) < 2 {
		t.Errorf("expected >=2 events stored, got %d", len(store.stored))
	}
}

func TestMetricsEndpoint(t *testing.T) {
	ts, p, _ := setupTestServer()
	defer ts.Close()
	defer p.Shutdown()

	// send one event
	payload := `{"type":"user_action","source":"web","data":{"action":"login"}}`
	_, err := http.Post(ts.URL+"/events", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}

	// wait until at least 1 event processed or timeout
	deadline := time.Now().Add(2 * time.Second)
	for {
		if int(p.Metrics().GetProcessed()) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: received=%d processed=%d failed=%d",
				p.Metrics().GetReceived(), p.Metrics().GetProcessed(), p.Metrics().GetFailed())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// give some time so uptime > 0 seconds
	time.Sleep(1 * time.Second)

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	if body["events_received"].(float64) < 1 {
		t.Errorf("expected events_received >= 1, got %v", body["events_received"])
	}
	if body["events_processed"].(float64) < 1 {
		t.Errorf("expected events_processed >= 1, got %v", body["events_processed"])
	}
	if body["average_processing_latency"].(float64) <= 0 {
		t.Errorf("expected average_processing_latency > 0, got %v", body["average_processing_latency"])
	}
	if body["events_per_second"].(float64) <= 0 {
		t.Errorf("expected events_per_second > 0, got %v", body["events_per_second"])
	}
	if body["uptime_seconds"].(float64) < 1 {
		t.Errorf("expected uptime_seconds >= 1, got %v", body["uptime_seconds"])
	}
}

func TestAPIRetryAndFailureMetrics(t *testing.T) {
	store := &testmocks.FlakyStorage{ShouldFail: 5}
	proc := &testmocks.DummyProcessor{}
	val := &validator.BasicValidator{}
	metrics := pipeline.NewMetrics()

	cfg := &config.Config{
		WorkerCount:      1,
		QueueSize:        100,
		MaxRetries:       3,
		RetryBaseBackoff: 20 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(store, proc, val, metrics, cfg)
	server := api.NewServer(p)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer p.Shutdown()

	payload := `{"type":"system_log","source":"api","data":{"msg":"fail"}}`
	resp, err := http.Post(ts.URL+"/events", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if metrics.GetFailed() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting, received=%d processed=%d failed=%d calls=%d",
				metrics.GetReceived(), metrics.GetProcessed(), metrics.GetFailed(), store.Calls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	metricsResp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metricsResp.Body.Close()

	var body map[string]interface{}
	if err := json.NewDecoder(metricsResp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	if body["events_failed"].(float64) < 1 {
		t.Errorf("expected events_failed >= 1, got %v", body["events_failed"])
	}
	if body["events_processed"].(float64) != 0 {
		t.Errorf("expected events_processed=0, got %v", body["events_processed"])
	}
	if int(store.Calls) != 3 {
		t.Errorf("expected 3 attempts, got %d", store.Calls)
	}
}

func TestAPIRetryAndSuccessAfterRetries(t *testing.T) {
	store := &testmocks.FlakyStorage{ShouldFail: 2} // fail twice, succeed 3rd
	proc := &testmocks.DummyProcessor{}
	val := &validator.BasicValidator{}
	metrics := pipeline.NewMetrics()

	cfg := &config.Config{
		WorkerCount:      1,
		QueueSize:        100,
		MaxRetries:       3,
		RetryBaseBackoff: 20 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(store, proc, val, metrics, cfg)
	server := api.NewServer(p)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer p.Shutdown()

	// Send one event
	payload := `{"type":"system_log","source":"api","data":{"msg":"retry-success"}}`
	resp, err := http.Post(ts.URL+"/events", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}

	// Wait until processed
	deadline := time.Now().Add(2 * time.Second)
	for {
		if metrics.GetProcessed() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for success, received=%d processed=%d failed=%d calls=%d",
				metrics.GetReceived(), metrics.GetProcessed(), metrics.GetFailed(), store.Calls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Fetch metrics via API
	metricsResp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metricsResp.Body.Close()

	var body map[string]interface{}
	if err := json.NewDecoder(metricsResp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	// Assertions
	if body["events_processed"].(float64) != 1 {
		t.Errorf("expected events_processed=1, got %v", body["events_processed"])
	}
	if body["events_failed"].(float64) != 0 {
		t.Errorf("expected events_failed=0, got %v", body["events_failed"])
	}
	if int(store.Calls) != 3 { // 2 fails + 1 success
		t.Errorf("expected 3 store attempts, got %d", store.Calls)
	}
}

func TestAPIBatchRetryAndSuccessAfterRetries(t *testing.T) {
	// Generate valid UUIDs for events
	failID := uuid.New().String()
	succeedID := uuid.New().String()

	store := &testmocks.FlakyStorage{
		ShouldFail:    2, // applies to the non-failing event
		AlwaysFailIDs: map[string]bool{failID: true}, // this one always fails
	}
	proc := &testmocks.DummyProcessor{}
	val := &validator.BasicValidator{}
	metrics := pipeline.NewMetrics()

	cfg := &config.Config{
		WorkerCount:      2,
		QueueSize:        100,
		MaxRetries:       3,
		RetryBaseBackoff: 20 * time.Millisecond,
	}

	p := pipeline.NewEventPipeline(store, proc, val, metrics, cfg)
	server := api.NewServer(p)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer p.Shutdown()

	// Batch of 2 events with valid UUIDs
	payload := fmt.Sprintf(`{"events":[
        {"id":"%s","type":"system_log","source":"system","data":{"msg":"always fail"}},
        {"id":"%s","type":"sensor_data","source":"iot","data":{"temperature":42}}
    ]}`, failID, succeedID)

	resp, err := http.Post(ts.URL+"/events/batch", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}

	// Wait until one success and one failure are recorded
	deadline := time.Now().Add(3 * time.Second)
	for {
		if metrics.GetProcessed() >= 1 && metrics.GetFailed() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for mixed results, received=%d processed=%d failed=%d calls=%d",
				metrics.GetReceived(), metrics.GetProcessed(), metrics.GetFailed(), store.Calls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Fetch metrics via API
	metricsResp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metricsResp.Body.Close()

	var body map[string]interface{}
	if err := json.NewDecoder(metricsResp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	// Assertions
	if body["events_processed"].(float64) != 1 {
		t.Errorf("expected events_processed=1, got %v", body["events_processed"])
	}
	if body["events_failed"].(float64) != 1 {
		t.Errorf("expected events_failed=1, got %v", body["events_failed"])
	}

	// Expect retries: first event failed 3 times, second succeeded on 3rd try
	expectedCalls := 3 + 3
	if int(store.Calls) != expectedCalls {
		t.Errorf("expected %d store attempts, got %d", expectedCalls, store.Calls)
	}
}
