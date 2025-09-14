package main

import (
	"context"
	"event-pipeline/internal/api"
	"event-pipeline/internal/config"
	"event-pipeline/internal/pipeline"
	"event-pipeline/internal/storage"
	"event-pipeline/pkg/logger"
	"event-pipeline/pkg/validator"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Initialize logger
	logger.Init(os.Getenv("LOG_MODE") == "prod")
	log := logger.Get()

	// Load configuration
	cfg := config.Load()
	log.Infow("loaded configuration",
		"db_host", cfg.DBHost,
		"db_name", cfg.DBName,
		"worker_count", cfg.WorkerCount,
		"queue_size", cfg.QueueSize,
		"max_retries", cfg.MaxRetries,
		"retry_backoff_ms", cfg.RetryBaseBackoff.Milliseconds(),
	)

	// Setup storage (MySQL)
	store, err := storage.NewMySQLStorage(cfg.DSN())
	if err != nil {
		log.Fatalw("failed to connect to MySQL", "error", err)
	}
	defer func() {
		if db := store.DB(); db != nil {
			_ = db.Close()
			log.Info("closed MySQL connection")
		}
	}()

	// Init core components
	metrics := pipeline.NewMetrics()
	processor := &pipeline.JSONProcessor{}   // replace with real processor later
	val := &validator.BasicValidator{}       // basic validation
	p := pipeline.NewEventPipeline(store, processor, val, metrics, cfg)
	defer p.Shutdown()

	// Start API server
	mux := http.NewServeMux()
	server := api.NewServer(p)
	server.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Run server
	go func() {
		log.Infow("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalw("server error", "error", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Info("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Errorw("server shutdown error", "error", err)
	}

	p.Shutdown()
	log.Info("service stopped")
}
