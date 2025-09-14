package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"event-pipeline/internal/pipeline"
	"event-pipeline/internal/storage"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMySQLStorageIntegration(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:testpass@tcp(localhost:3306)/eventdb"
	}

	store, err := storage.NewMySQLStorage(dsn)
	if err != nil {
		t.Fatalf("failed to connect to MySQL: %v", err)
	}
	defer store.Close()

	// Clean table before test
	_, _ = store.DB().Exec("DELETE FROM processed_events")

	ctx := context.Background()
	event := pipeline.ProcessedEvent{
		Event: pipeline.Event{
			ID:        uuid.New().String(),
			Type:      "system_log",
			Source:    "integration_test",
			UserID:    "u123",
			Timestamp: time.Now(),
			Data:      map[string]interface{}{"key": "value"},
		},
		ProcessingTimeMS: 42,
		ProcessedAt:      time.Now(),
	}

	if err := store.Store(ctx, []pipeline.ProcessedEvent{event}); err != nil {
		t.Fatalf("failed to store event: %v", err)
	}

	// verify stored
	var (
		gotID   string
		gotType string
		gotSrc  string
		gotUID  sql.NullString
		rawData string
		ptime   int64
	)
	row := store.DB().QueryRow(`SELECT id, type, source, user_id, processed_data, processing_time_ms 
		FROM processed_events WHERE id = ?`, event.ID)
	if err := row.Scan(&gotID, &gotType, &gotSrc, &gotUID, &rawData, &ptime); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if gotID != event.ID {
		t.Errorf("expected id %s, got %s", event.ID, gotID)
	}
	if gotType != event.Type {
		t.Errorf("expected type %s, got %s", event.Type, gotType)
	}
	if gotSrc != event.Source {
		t.Errorf("expected source %s, got %s", event.Source, gotSrc)
	}
	if gotUID.String != event.UserID {
		t.Errorf("expected user_id %s, got %s", event.UserID, gotUID.String)
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(rawData), &parsed); err != nil {
		t.Errorf("failed to unmarshal processed_data: %v", err)
	}
	if parsed["key"] != "value" {
		t.Errorf("expected processed_data.key=value, got %v", parsed["key"])
	}
	if ptime != event.ProcessingTimeMS {
		t.Errorf("expected processing_time_ms=%d, got %d", event.ProcessingTimeMS, ptime)
	}
}

func TestMySQLStorageBatchInsert(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:testpass@tcp(localhost:3306)/eventdb"
	}

	store, err := storage.NewMySQLStorage(dsn)
	if err != nil {
		t.Skipf("skipping test, mysql not available: %v", err)
	}
	defer store.Close()

	// Clean table before test
	_, _ = store.DB().Exec("DELETE FROM processed_events")

	ctx := context.Background()
	now := time.Now()

	events := []pipeline.ProcessedEvent{
		{
			Event: pipeline.Event{
				ID:        uuid.New().String(),
				Type:      "user_action",
				Source:    "integration_test",
				UserID:    "u1",
				Timestamp: now,
				Data:      map[string]interface{}{"action": "click"},
			},
			ProcessingTimeMS: 10,
			ProcessedAt:      now,
		},
		{
			Event: pipeline.Event{
				ID:        uuid.New().String(),
				Type:      "sensor_data",
				Source:    "integration_test",
				UserID:    "u2",
				Timestamp: now,
				Data:      map[string]interface{}{"temp": 25},
			},
			ProcessingTimeMS: 15,
			ProcessedAt:      now,
		},
		{
			Event: pipeline.Event{
				ID:        uuid.New().String(),
				Type:      "system_log",
				Source:    "integration_test",
				UserID:    "u3",
				Timestamp: now,
				Data:      map[string]interface{}{"msg": "hello"},
			},
			ProcessingTimeMS: 20,
			ProcessedAt:      now,
		},
	}

	if err := store.Store(ctx, events); err != nil {
		t.Fatalf("batch store failed: %v", err)
	}

	// verify count
	var count int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM processed_events`).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != len(events) {
		t.Errorf("expected %d rows, got %d", len(events), count)
	}
}
