package validator

import (
	"context"
	"errors"

	"event-pipeline/internal/pipeline"
	"github.com/google/uuid"
)

type BasicValidator struct{}

func (v *BasicValidator) Validate(ctx context.Context, e pipeline.Event) error {
	// Check required fields
	if e.Type == "" {
		return errors.New("missing type")
	}
	if e.Source == "" {
		return errors.New("missing source")
	}

	// Check UUID format if provided
	if e.ID != "" {
		if _, err := uuid.Parse(e.ID); err != nil {
			return errors.New("invalid UUID format")
		}
	}

	return nil
}
