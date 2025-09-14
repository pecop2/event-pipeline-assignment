package api

import (
	"encoding/json"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/logger"
	"net/http"
	"time"
)

type Server struct {
	Pipeline *pipeline.EventPipeline
}

type BatchRequest struct {
	Events []pipeline.Event `json:"events"`
}

func NewServer(p *pipeline.EventPipeline) *Server {
	return &Server{Pipeline: p}
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	// Wrap all handlers with request ID middleware
	mux.Handle("/events", RequestIDMiddleware(http.HandlerFunc(s.handleSingleEvent)))
	mux.Handle("/events/batch", RequestIDMiddleware(http.HandlerFunc(s.handleBatchEvents)))
	mux.Handle("/health", RequestIDMiddleware(http.HandlerFunc(s.handleHealth)))
	mux.Handle("/metrics", RequestIDMiddleware(http.HandlerFunc(s.handleMetrics)))
}

func (s *Server) handleSingleEvent(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rid := GetRequestID(r.Context())
	log := logger.Get().With("request_id", rid)

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		log.Warnw("request rejected", "method", r.Method, "path", r.URL.Path, "status", http.StatusMethodNotAllowed)
		return
	}

	var ev pipeline.Event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		log.Warnw("invalid JSON body", "error", err, "status", http.StatusBadRequest)
		return
	}

	s.Pipeline.Ingest(ev)
	log.Infow("event accepted",
		"event_id", ev.ID,
		"type", ev.Type,
		"source", ev.Source,
	)

	w.WriteHeader(http.StatusAccepted)
	log.Infow("request completed",
		"status", http.StatusAccepted,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

func (s *Server) handleBatchEvents(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rid := GetRequestID(r.Context())
	log := logger.Get().With("request_id", rid)

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		log.Warnw("invalid method for /events/batch",
			"method", r.Method, "path", r.URL.Path, "status", http.StatusMethodNotAllowed,
		)
		return
	}

	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		log.Warnw("invalid JSON for /events/batch",
			"error", err, "status", http.StatusBadRequest,
		)
		return
	}

	if len(req.Events) > 100 {
		http.Error(w, "too many events (max 100)", http.StatusBadRequest)
		log.Warnw("batch rejected: too many events",
			"count", len(req.Events), "status", http.StatusBadRequest,
		)
		return
	}

	var ids []string
	for _, ev := range req.Events {
		ids = append(ids, ev.ID)
		s.Pipeline.Ingest(ev)
	}

	w.WriteHeader(http.StatusAccepted)
	log.Infow("batch accepted",
		"count", len(req.Events),
		"event_ids", ids,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	rid := GetRequestID(r.Context())
	log := logger.Get().With("request_id", rid)

	healthy := s.Pipeline.Context().Err() == nil
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]bool{"healthy": healthy}
	_ = json.NewEncoder(w).Encode(resp)

	log.Debugw("health check", "path", r.URL.Path, "remote_addr", r.RemoteAddr, "healthy", healthy)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	rid := GetRequestID(r.Context())
	log := logger.Get().With("request_id", rid)

	metrics := map[string]interface{}{
		"events_received":            s.Pipeline.Metrics().GetReceived(),
		"events_processed":           s.Pipeline.Metrics().GetProcessed(),
		"events_failed":              s.Pipeline.Metrics().GetFailed(),
		"average_processing_latency": s.Pipeline.Metrics().AvgLatencyMS(),
		"current_queue_depth":        len(s.Pipeline.Queue()),
		"active_workers":             s.Pipeline.WorkerCount(),
		"uptime_seconds":             int(time.Since(s.Pipeline.StartTime()).Seconds()),
		"events_per_second":          s.Pipeline.Metrics().EPS(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(metrics)

	log.Debugw("metrics requested",
		"path", r.URL.Path,
		"remote_addr", r.RemoteAddr,
		"metrics", metrics,
	)
}
