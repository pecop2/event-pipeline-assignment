package pipeline

import (
	"context"
	"sync"
	"time"

	"event-pipeline/pkg/logger"
)

type Worker struct {
	id       int
	jobChan  <-chan Event
	pipeline *EventPipeline
	wg       *sync.WaitGroup
}

func (w *Worker) Start(ctx context.Context) {
	log := logger.Get().With("worker", w.id)

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for {
			select {
			case job, ok := <-w.jobChan:
				if !ok {
					// channel closed, no more jobs
					log.Infow("worker exiting", "reason", "channel closed")
					return
				}
				w.pipeline.metrics.IncReceived()
				w.processJob(ctx, job)

			case <-ctx.Done():
				// drain remaining jobs if any
				for job := range w.jobChan {
					w.pipeline.metrics.IncReceived()
					w.processJob(ctx, job)
				}
				log.Infow("worker exiting", "reason", "context cancelled")
				return
			}
		}
	}()
}

func (w *Worker) processJob(ctx context.Context, job Event) {
	rid, _ := job.Data["request_id"].(string)
	log := logger.Get().With(
		"worker", w.id,
		"event_id", job.ID,
		"type", job.Type,
		"source", job.Source,
		"request_id", rid,
	)

	start := time.Now()

	// Validate
	if err := w.pipeline.validator.Validate(ctx, job); err != nil {
		log.Warnw("validation failed", "error", err)
		w.pipeline.metrics.IncFailed()
		return
	}

	// Process
	processed, err := w.pipeline.processor.Process(ctx, job)
	if err != nil {
		log.Errorw("processing failed", "error", err)
		w.pipeline.metrics.IncFailed()
		return
	}

	// Store with retries (from config)
	maxRetries := w.pipeline.cfg.MaxRetries
	baseBackoff := w.pipeline.cfg.RetryBaseBackoff

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = w.pipeline.storage.Store(ctx, []ProcessedEvent{*processed})
		if err == nil {
			break
		}
		log.Warnw("storage attempt failed, will retry",
			"attempt", attempt,
			"max_attempts", maxRetries,
			"error", err,
		)
		time.Sleep(time.Duration(attempt) * baseBackoff)
	}

	if err != nil {
		log.Errorw("storage permanently failed", "attempts", maxRetries, "error", err)
		w.pipeline.metrics.IncFailed()
		return
	}

	// Success
	latency := time.Since(start).Milliseconds()
	w.pipeline.metrics.AddLatency(latency)
	w.pipeline.metrics.IncProcessed()

	log.Infow("event processed", "latency_ms", latency)
}
