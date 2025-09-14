package pipeline

import (
	"context"
	"time"
)

// JSONProcessor is a simple processor that passes events through
// without modification, while recording processing time and timestamp.
type JSONProcessor struct{}

// Process takes an Event and returns a ProcessedEvent with metadata.
func (p *JSONProcessor) Process(ctx context.Context, e Event) (*ProcessedEvent, error) {
	start := time.Now()

	processed := &ProcessedEvent{
		Event:            e,
		ProcessingTimeMS: time.Since(start).Milliseconds(),
		ProcessedAt:      time.Now(),
	}

	return processed, nil
}
