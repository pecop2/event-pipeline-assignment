package testmocks

import (
	"context"
	"errors"
	"sync"
	"time"

	"event-pipeline/internal/pipeline"
)

// --- Flaky Storage ---
// Fails the first N attempts, then succeeds.
type FlakyStorage struct {
	mu         sync.Mutex
	ShouldFail int
	attempts   map[string]int // track attempts per event.ID
	Calls      int            // total store calls across all events

	// AlwaysFailIDs holds event IDs that should *always* fail,
	// regardless of retry attempts.
	AlwaysFailIDs map[string]bool
}

func (s *FlakyStorage) Store(_ context.Context, events []pipeline.ProcessedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.attempts == nil {
		s.attempts = make(map[string]int)
	}
	if s.AlwaysFailIDs == nil {
		s.AlwaysFailIDs = make(map[string]bool)
	}

	for _, ev := range events {
		id := ev.ID
		s.Calls++

		// Forced permanent failure
		if s.AlwaysFailIDs[id] {
			return errors.New("forced permanent failure for event " + id)
		}

		// Regular fail-then-succeed logic
		s.attempts[id]++
		if s.attempts[id] <= s.ShouldFail {
			return errors.New("forced failure for event " + id)
		}
	}

	return nil
}

// --- Dummy Processor ---
// Always succeeds with a small delay.
type DummyProcessor struct{}

func (p *DummyProcessor) Process(ctx context.Context, e pipeline.Event) (*pipeline.ProcessedEvent, error) {
	time.Sleep(5 * time.Millisecond)
	return &pipeline.ProcessedEvent{
		Event:            e,
		ProcessingTimeMS: 5,
		ProcessedAt:      time.Now(),
	}, nil
}

// --- Mock Storage (always succeeds, stores events) ---
type MockStorage struct {
	mu     sync.Mutex
	Events []pipeline.ProcessedEvent
}

func (s *MockStorage) Store(_ context.Context, events []pipeline.ProcessedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, events...)
	return nil
}

// --- Mock Processor (slow, always succeeds) ---
type SlowProcessor struct{}

func (p *SlowProcessor) Process(ctx context.Context, e pipeline.Event) (*pipeline.ProcessedEvent, error) {
	time.Sleep(10 * time.Millisecond)
	return &pipeline.ProcessedEvent{
		Event:            e,
		ProcessingTimeMS: 10,
		ProcessedAt:      time.Now(),
	}, nil
}

type SlowStorage struct {
	Events []pipeline.ProcessedEvent
}

func (s *SlowStorage) Store(_ context.Context, events []pipeline.ProcessedEvent) error {
	time.Sleep(30 * time.Millisecond) // simulate I/O
	s.Events = append(s.Events, events...)
	return nil
}

type FastProcessor struct{}

func (p *FastProcessor) Process(ctx context.Context, e pipeline.Event) (*pipeline.ProcessedEvent, error) {
	return &pipeline.ProcessedEvent{
		Event:            e,
		ProcessingTimeMS: 2,
		ProcessedAt:      time.Now(),
	}, nil
}
