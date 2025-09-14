package pipeline

import (
	"context"
	"sync"
	"time"

	"event-pipeline/internal/config"
	"event-pipeline/pkg/logger"
)

type EventPipeline struct {
	ingestionChan chan Event
	workerPool    []*Worker
	storage       Storage
	processor     Processor
	validator     Validator
	metrics       *Metrics
	cfg           *config.Config
	ctx           context.Context
	cancel        context.CancelFunc
	startTime     time.Time
	wg            sync.WaitGroup
}

func NewEventPipeline(store Storage, proc Processor, val Validator, metrics *Metrics, cfg *config.Config) *EventPipeline {
	ctx, cancel := context.WithCancel(context.Background())
	p := &EventPipeline{
		ingestionChan: make(chan Event, cfg.QueueSize),
		storage:       store,
		processor:     proc,
		validator:     val,
		metrics:       metrics,
		cfg:           cfg,
		ctx:           ctx,
		cancel:        cancel,
		startTime:     time.Now(),
	}

	log := logger.Get()
	log.Infow("starting pipeline",
		"workers", cfg.WorkerCount,
		"queue_size", cfg.QueueSize,
		"max_retries", cfg.MaxRetries,
		"retry_backoff_ms", cfg.RetryBaseBackoff.Milliseconds(),
	)

	for i := 0; i < cfg.WorkerCount; i++ {
		w := &Worker{
			id:       i + 1,
			jobChan:  p.ingestionChan,
			pipeline: p,
			wg:       &p.wg,
		}
		p.workerPool = append(p.workerPool, w)
		w.Start(ctx)
		log.Infow("worker started", "worker_id", w.id)
	}

	log.Infow("pipeline started", "worker_count", cfg.WorkerCount)
	return p
}

func (p *EventPipeline) Ingest(ev Event) {
	p.ingestionChan <- NewEvent(ev)
	logger.Get().Debugw("event ingested",
		"event_id", ev.ID,
		"type", ev.Type,
		"source", ev.Source,
	)
}

func (p *EventPipeline) Shutdown() {
    log := logger.Get()
    log.Info("initiating graceful shutdown")

    // close channel → lets workers finish draining
    close(p.ingestionChan)

    // cancel context → in case workers are blocked in select
    p.cancel()

    // wait for workers
    p.wg.Wait()

    log.Info("all workers stopped, shutdown complete")
}

func (p *EventPipeline) Metrics() *Metrics {
	return p.metrics
}

func (p *EventPipeline) Queue() chan Event {
	return p.ingestionChan
}

func (p *EventPipeline) WorkerCount() int {
	return len(p.workerPool)
}

func (p *EventPipeline) StartTime() time.Time {
	return p.startTime
}

func (p *EventPipeline) Context() context.Context {
	return p.ctx
}
