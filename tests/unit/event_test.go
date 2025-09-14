package unit

import (
	"event-pipeline/internal/pipeline"
	"testing"
	"time"
)

func TestNewEventGeneratesIDAndTimestamp(t *testing.T) {
	e := pipeline.Event{
		Type:   "user_action",
		Source: "web",
	}

	created := pipeline.NewEvent(e)

	// ID must be set
	if created.ID == "" {
		t.Error("expected ID to be generated, but got empty string")
	}

	// Timestamp must be set and not zero
	if created.Timestamp.IsZero() {
		t.Error("expected Timestamp to be generated, but got zero value")
	}

	// Timestamp should be recent (within 2s)
	if time.Since(created.Timestamp) > 2*time.Second {
		t.Errorf("expected recent timestamp, got %v", created.Timestamp)
	}
}

func TestNewEventKeepsExistingValues(t *testing.T) {
	now := time.Now().UTC()
	id := "custom-id"

	e := pipeline.Event{
		ID:        id,
		Type:      "sensor_data",
		Source:    "iot_device",
		Timestamp: now,
	}

	created := pipeline.NewEvent(e)

	if created.ID != id {
		t.Errorf("expected ID %s, got %s", id, created.ID)
	}
	if !created.Timestamp.Equal(now) {
		t.Errorf("expected Timestamp %v, got %v", now, created.Timestamp)
	}
}
