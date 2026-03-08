# ojs-backend-amqp

A RabbitMQ-backed implementation of the [OpenJobSpec (OJS)](https://github.com/openjobspec) specification — a standard interface for distributed job queues and workflow orchestration. This backend uses AMQP 0-9-1 direct exchanges for queue routing, dead letter exchanges for retry/DLQ, and an in-memory state store for job metadata with automatic RabbitMQ reconnection.

## Key Features

- **Full OJS compliance** — Implements the complete Backend interface (30+ methods) across all conformance levels
- **RabbitMQ native** — Uses direct exchanges, dead letter exchanges, and per-attempt TTL queues for native AMQP semantics
- **Automatic reconnection** — Connection monitor with 3-second retry loop handles transient RabbitMQ failures
- **Retry policies** — Exponential, linear, and constant backoff with jitter via dedicated retry exchanges
- **Scheduled jobs** — Delay execution until a specific time with background scheduler promotion
- **Cron scheduling** — Register recurring jobs with standard cron expressions
- **Workflows** — Chain (sequential), group (parallel), and batch execution with callbacks
- **Dead letter queue** — Inspect, retry, or delete failed jobs that have exhausted retries
- **Job deduplication** — Unique job policies with configurable conflict resolution
- **Queue management** — Pause/resume queues, view per-queue statistics
- **Batch enqueue** — Submit multiple jobs in a single request
- **Dual protocol** — HTTP (chi router) and gRPC APIs via shared backend-common handlers
- **Real-time events** — Server-Sent Events (SSE) for job state change subscriptions
- **Schema registry** — In-memory schema validation with versioning and compatibility modes
- **OpenTelemetry** — Distributed tracing with OTLP gRPC export
- **Prometheus metrics** — Request duration, throughput, and error rate metrics at `/metrics`
- **Graceful shutdown** — Clean SIGINT/SIGTERM handling with in-flight request draining

## Prerequisites

- **Go** 1.24 or later
- **RabbitMQ** 3.x with management plugin (recommended)
- **Docker** and **Docker Compose** (optional, for containerized setup)

## Quick Start

### Option 1: Docker Compose (recommended)

Start both RabbitMQ and the OJS server:

```bash
make docker-up
```

The server will be available at `http://localhost:8080`. RabbitMQ management UI at `http://localhost:15672` (guest/guest). To stop:

```bash
make docker-down
```

### Option 2: Run locally

```bash
# Start RabbitMQ (if not already running)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Build and run
make build
AMQP_URL=amqp://guest:guest@localhost:5672/ OJS_ALLOW_INSECURE_NO_AUTH=true make run
```

### Verify it's working

```bash
curl http://localhost:8080/ojs/v1/health
```

## Usage Examples

### Enqueue a job

```bash
curl -X POST http://localhost:8080/ojs/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email.send",
    "args": [{"to": "user@example.com", "subject": "Welcome"}],
    "options": {
      "queue": "emails",
      "retry": {"max_attempts": 3, "initial_interval_ms": 1000}
    }
  }'
```

### Fetch and process a job

```bash
# Fetch
JOB=$(curl -s -X POST http://localhost:8080/ojs/v1/workers/fetch \
  -H "Content-Type: application/json" \
  -d '{"queues": ["emails"], "count": 1, "worker_id": "worker-1"}')

# Acknowledge completion
JOB_ID=$(echo $JOB | jq -r '.jobs[0].id')
curl -X POST http://localhost:8080/ojs/v1/workers/ack \
  -H "Content-Type: application/json" \
  -d "{\"job_id\": \"$JOB_ID\"}"
```

### Create a workflow

```bash
curl -X POST http://localhost:8080/ojs/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "type": "chain",
    "jobs": [
      {"type": "data.extract", "args": [{"source": "s3://bucket/data.csv"}]},
      {"type": "data.transform", "args": [{"format": "parquet"}]},
      {"type": "data.load", "args": [{"destination": "warehouse"}]}
    ]
  }'
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   OJS Server                        │
│  ┌────────────────────────────────────────────────┐ │
│  │  HTTP API (chi)  │  gRPC API                   │ │
│  │  :8080           │  :9090                      │ │
│  └───────────┬──────┴──────────┬──────────────────┘ │
│              │   ojs-go-backend-common handlers     │
│  ┌───────────┴─────────────────┴──────────────────┐ │
│  │              AMQP Backend                      │ │
│  │  ┌──────────────────┐  ┌────────────────────┐  │ │
│  │  │ RabbitMQ Client   │  │ In-Memory State    │  │ │
│  │  │ • Auto-reconnect  │  │ • Jobs map         │  │ │
│  │  │ • Exchange mgmt   │  │ • Workflows map    │  │ │
│  │  │ • Queue declare   │  │ • Cron jobs map    │  │ │
│  │  │ • Publish/Consume │  │ • Workers map      │  │ │
│  │  └──────────────────┘  └────────────────────┘  │ │
│  └────────────────────────────────────────────────┘ │
│  ┌──────────────┐  ┌──────────┐  ┌──────────────┐  │
│  │  Scheduler   │  │  Events  │  │  Metrics     │  │
│  │  • Cron fire │  │  Broker  │  │  Prometheus  │  │
│  │  • Retry     │  │  (SSE)   │  │  /metrics    │  │
│  │  • Reaping   │  │          │  │              │  │
│  └──────────────┘  └──────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                    RabbitMQ                          │
│  Exchanges: ojs.direct, ojs.dlx, ojs.retry,        │
│             ojs.events                              │
│  Queues:    ojs.queue.{name}                        │
│             ojs.queue.{name}.dlq                    │
│             ojs.queue.{name}.retry.{n}              │
└─────────────────────────────────────────────────────┘
```

## AMQP Topology

Per the [OJS AMQP Binding Spec](../spec/spec/ojs-amqp-binding.md):

| Exchange | Type | Purpose |
|----------|------|---------|
| `ojs.direct` | direct | Primary job routing by queue name |
| `ojs.dlx` | direct | Dead letter exchange for failed jobs |
| `ojs.retry` | direct | Retry exchange with per-attempt TTL |
| `ojs.events` | fanout | Job lifecycle event broadcasting |

Each OJS queue maps to:
- `ojs.queue.{name}` — primary work queue bound to `ojs.direct`
- `ojs.queue.{name}.dlq` — dead letter queue bound to `ojs.dlx`
- `ojs.queue.{name}.retry.{n}` — retry delay queues with TTL, bound to `ojs.retry`

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection string |
| `OJS_PERSIST` | — | Optional SQLite path for durable state (e.g., `./ojs-amqp.db`) |
| `OJS_PORT` | `8080` | HTTP server port |
| `OJS_GRPC_PORT` | `9090` | gRPC server port |
| `OJS_API_KEY` | — | API key for authentication (required in production) |
| `OJS_ALLOW_INSECURE_NO_AUTH` | `false` | Allow running without auth (dev only) |
| `OJS_OIDC_ISSUER` | — | OIDC issuer URL for JWT authentication |
| `OJS_OIDC_CLIENT_ID` | — | OIDC client ID / audience |
| `OJS_OTEL_ENABLED` | `false` | Enable OpenTelemetry tracing |
| `OJS_OTEL_ENDPOINT` | — | OTLP gRPC endpoint (e.g., `localhost:4317`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Standard OTLP endpoint (fallback) |

## API Endpoints

### Core (Level 0)
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/ojs/v1/health` | Health check |
| `GET` | `/ojs/manifest` | Server capabilities manifest |
| `POST` | `/ojs/v1/jobs` | Enqueue a job |
| `GET` | `/ojs/v1/jobs/{id}` | Get job status |
| `DELETE` | `/ojs/v1/jobs/{id}` | Cancel a job |
| `POST` | `/ojs/v1/workers/fetch` | Fetch jobs for processing |
| `POST` | `/ojs/v1/workers/ack` | Acknowledge job completion |
| `POST` | `/ojs/v1/workers/nack` | Report job failure |
| `POST` | `/ojs/v1/workers/heartbeat` | Worker heartbeat |

### Scheduling & Queues (Levels 2–4)
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/ojs/v1/jobs/batch` | Batch enqueue |
| `GET` | `/ojs/v1/queues` | List queues |
| `GET` | `/ojs/v1/queues/{name}/stats` | Queue statistics |
| `POST` | `/ojs/v1/queues/{name}/pause` | Pause a queue |
| `POST` | `/ojs/v1/queues/{name}/resume` | Resume a queue |
| `GET` | `/ojs/v1/cron` | List cron jobs |
| `POST` | `/ojs/v1/cron` | Register cron job |
| `DELETE` | `/ojs/v1/cron/{name}` | Delete cron job |

### Workflows & Admin (Levels 3–4)
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/ojs/v1/workflows` | Create workflow |
| `GET` | `/ojs/v1/workflows/{id}` | Get workflow status |
| `DELETE` | `/ojs/v1/workflows/{id}` | Cancel workflow |
| `GET` | `/ojs/v1/dead-letter` | List dead letter jobs |
| `POST` | `/ojs/v1/dead-letter/{id}/retry` | Retry dead letter job |
| `DELETE` | `/ojs/v1/dead-letter/{id}` | Delete dead letter job |
| `GET` | `/ojs/v1/admin/stats` | Admin statistics |
| `GET` | `/ojs/v1/admin/jobs` | List all jobs |
| `GET` | `/ojs/v1/admin/workers` | List workers |

### Schema Registry & Events
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/ojs/v1/schemas` | Register schema |
| `GET` | `/ojs/v1/schemas/{jobType}` | Get latest schema |
| `GET` | `/ojs/v1/jobs/{id}/events` | SSE job events |
| `GET` | `/ojs/v1/queues/{name}/events` | SSE queue events |
| `GET` | `/ojs/v1/events` | List stored events |

## Build & Test

```bash
make build          # Build binary to bin/ojs-server
make test           # Run tests with race detection and coverage
make lint           # Run go vet
make run            # Build and run
make docker-build   # Build Docker image
make docker-up      # Start server + RabbitMQ via Docker Compose
make docker-down    # Stop Docker Compose
```

## Conformance

Target: Level 0–4 (230/230 tests).

```bash
# Run all conformance levels (requires running server)
make conformance

# Run specific level
make conformance-level-0    # Core: push, fetch, ack, nack, health, manifest
make conformance-level-1    # Reliable: retry, dead letter, heartbeat, visibility
make conformance-level-2    # Scheduled: delayed jobs, cron, TTL
make conformance-level-3    # Orchestration: chain, group, batch workflows
make conformance-level-4    # Advanced: priority, unique jobs, batch enqueue, queues
```

## Design Decisions

- **In-memory state + AMQP routing**: Job metadata lives in memory for fast access; RabbitMQ handles message delivery and ordering. This trades durability (restart loses state) for simplicity and performance. Optional SQLite persistence (`OJS_PERSIST`) restores state across restarts.
- **Shared API handlers**: HTTP/gRPC routing uses `ojs-go-backend-common/api` handlers, ensuring API consistency with all other OJS backends.
- **Auto-reconnection**: The connection monitor detects RabbitMQ disconnections and reconnects with a 3-second backoff, re-declaring exchanges and queues on reconnection.
- **Event fanout**: Job lifecycle events are published to the `ojs.events` fanout exchange, enabling SSE subscriptions and external event consumers.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

