package unit

import (
	"context"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/validator"
	"testing"

	"github.com/google/uuid"
)

func TestValidatorValidEvent(t *testing.T) {
	val := &validator.BasicValidator{}

	e := pipeline.NewEvent(pipeline.Event{
		Type:   "user_action",
		Source: "web",
	})

	err := val.Validate(context.Background(), e)
	if err != nil {
		t.Errorf("expected valid event, got error: %v", err)
	}
}

func TestValidatorInvalidUUID(t *testing.T) {
	val := &validator.BasicValidator{}

	e := pipeline.Event{
		ID:     "not-a-uuid",
		Type:   "user_action",
		Source: "web",
	}

	err := val.Validate(context.Background(), e)
	if err == nil {
		t.Error("expected error for invalid UUID, got nil")
	}
}

func TestValidatorMissingFields(t *testing.T) {
	val := &validator.BasicValidator{}

	e := pipeline.Event{} // missing type + source

	err := val.Validate(context.Background(), e)
	if err == nil {
		t.Error("expected error for missing required fields, got nil")
	}
}

func TestValidatorAcceptsValidUUID(t *testing.T) {
	val := &validator.BasicValidator{}

	id := uuid.New().String()
	e := pipeline.Event{
		ID:     id,
		Type:   "sensor_data",
		Source: "iot_device",
	}

	err := val.Validate(context.Background(), e)
	if err != nil {
		t.Errorf("expected valid UUID, got error: %v", err)
	}
}
