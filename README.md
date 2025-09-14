# Event Pipeline

## Project Overview and Architecture

The **Event Pipeline** is a Go-based service designed to ingest, process, validate, and store events from multiple sources (e.g., user actions, IoT devices, system logs).  
It uses a **worker pool architecture** to process events concurrently, with support for validation, retries, metrics, and MySQL storage.

### Architecture Components

- **API Layer**: Exposes HTTP endpoints for ingesting events (`/events`, `/events/batch`) and monitoring (`/health`, `/metrics`).
- **Pipeline**: Core worker pool system that validates, processes, retries on failures, and stores events.
- **Storage**: Pluggable backend (MySQL in this project).
- **Metrics**: Tracks events received, processed, failed, latency, and throughput.
- **Graceful Shutdown**: Ensures all queued events are processed before shutdown.

---

## Setup and Installation

### Prerequisites
- Go 1.21+
- Docker & Docker Compose
- Make

### Installation

Clone the repository and build:
```bash
make build
```

---

## Running the Application

Run the service locally:
```bash
make run
```

The server will listen on **http://localhost:8080**.

---

## Running Tests

- Run unit tests:
```bash
make unit-test
```

- Run integration tests (automatically starts MySQL with Docker):
```bash
make integration-test
```

- Run all tests:
```bash
make test
```

---

## Docker Compose

Start MySQL (and initialize schema):
```bash
make run-mysql
```

Stop services:
```bash
make down
```

---

## API Endpoints

### Health Check
`GET /health`  
Returns pipeline health status.

### Metrics
`GET /metrics`  
Returns JSON metrics (received, processed, failed, latency, EPS, etc).

### Single Event Ingest
`POST /events`  
Example:
```json
{
  "type": "user_action",
  "source": "web",
  "data": { "action": "click" }
}
```

### Batch Events Ingest
`POST /events/batch`  
Example:
```json
{
  "events": [
    { "type": "sensor_data", "source": "iot", "data": { "temperature": 22.5 } },
    { "type": "system_log", "source": "system", "data": { "level": "error", "message": "failed" } }
  ]
}
```

---

## Design Decisions & Trade-offs

- **Worker Pool**: Chosen for concurrency and scalability. Each worker drains jobs until shutdown.
- **Retry Logic**: Storage retries up to 3 times per event, balancing resilience vs. resource use.
- **Graceful Shutdown**: Uses context cancellation + wait groups to drain queue safely.
- **Configuration**: Managed via environment variables (`config.Config`).
- **Logging**: Structured logging with Zap for observability.
- **MySQL Schema**: Optimized with indexes on `type+created_at` and `user_id+created_at` for query performance.

---
