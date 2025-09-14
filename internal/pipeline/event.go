package pipeline

import (
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Source    string                 `json:"source"`
	Timestamp time.Time              `json:"timestamp"`
	UserID    string                 `json:"user_id,omitempty"`
	Data      map[string]interface{} `json:"data"`
}

type ProcessedEvent struct {
	Event
	ProcessingTimeMS int64     `json:"processing_time_ms"`
	ProcessedAt      time.Time `json:"processed_at"`
}

// NewEvent ensures ID + timestamp are set
func NewEvent(e Event) Event {
	if e.ID == "" {
		e.ID = uuid.New().String()
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	return e
}
