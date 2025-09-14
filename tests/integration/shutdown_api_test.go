package integration

import (
	"bytes"
	"encoding/json"
	"event-pipeline/internal/api"
	"event-pipeline/internal/config"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/validator"
	"event-pipeline/tests/testmocks"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)


func setupShutdownServer() (*httptest.Server, *pipeline.EventPipeline, *testmocks.SlowStorage) {
	metrics := pipeline.NewMetrics()
	val := &validator.BasicValidator{}
	proc := &testmocks.FastProcessor{}
	store := &testmocks.SlowStorage{}

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

func TestGracefulShutdownAPI(t *testing.T) {
	ts, p, store := setupShutdownServer()
	defer ts.Close()

	// Health BEFORE shutdown → should be healthy
	resp1, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp1.Body.Close()
	var body1 map[string]bool
	if err := json.NewDecoder(resp1.Body).Decode(&body1); err != nil {
		t.Fatal(err)
	}
	if !body1["healthy"] {
		t.Errorf("expected healthy=true before shutdown, got %v", body1["healthy"])
	}

	// send a batch of events
	payload := `{"events":[
		{"type":"user_action","source":"web","data":{"action":"view"}},
		{"type":"system_log","source":"system","data":{"msg":"test"}},
		{"type":"sensor_data","source":"iot","data":{"temp":42}}
	]}`
	_, err = http.Post(ts.URL+"/events/batch", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}

	// trigger shutdown (should drain queue before exit)
	p.Shutdown()

	// wait for workers to finish processing all events
	deadline := time.Now().Add(3 * time.Second)
	for {
		if int(p.Metrics().GetProcessed()) >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: received=%d processed=%d stored=%d",
				p.Metrics().GetReceived(), p.Metrics().GetProcessed(), len(store.Events))
		}
		time.Sleep(10 * time.Millisecond)
	}

	// All jobs should be stored
	if len(store.Events) != 3 {
		t.Errorf("expected 3 events stored, got %d", len(store.Events))
	}

	// Health AFTER shutdown → should be unhealthy
	resp2, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var body2 map[string]bool
	if err := json.NewDecoder(resp2.Body).Decode(&body2); err != nil {
		t.Fatal(err)
	}

	if body2["healthy"] {
		t.Errorf("expected healthy=false after shutdown, got true")
	}
}
