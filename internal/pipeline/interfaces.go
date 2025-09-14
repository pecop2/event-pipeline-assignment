package pipeline

import "context"

type Processor interface {
	Process(ctx context.Context, event Event) (*ProcessedEvent, error)
}

type Storage interface {
	Store(ctx context.Context, events []ProcessedEvent) error
}

type Validator interface {
	Validate(ctx context.Context, event Event) error
}
