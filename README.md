# OJS Backend — AMQP (RabbitMQ)

An [Open Job Spec](https://openjobspec.org) backend powered by RabbitMQ (AMQP 0-9-1). Uses direct exchanges for queue routing, dead letter exchanges for retry/DLQ, and an in-memory state store for job metadata.

> **Status:** 🚧 Under Development — Not yet passing conformance tests.

## Architecture

```
┌────────────────────────┐
│  OJS HTTP/gRPC API     │   ← ojs-go-backend-common/api handlers
├────────────────────────┤
│  AMQP Backend          │   ← implements core.Backend interface
│  ┌──────────────────┐  │
│  │ RabbitMQ Client   │  │   ← amqp091-go
│  │ (exchanges,       │  │
│  │  queues, bindings)│  │
│  └──────────────────┘  │
│  ┌──────────────────┐  │
│  │ State Store       │  │   ← in-memory job/workflow/cron state
│  │ (jobs, workflows, │  │
│  │  cron, workers)   │  │
│  └──────────────────┘  │
├────────────────────────┤
│  Background Scheduler  │   ← scheduled job promotion, cron, reaping
└────────────────────────┘
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
- `ojs.queue.{name}` — primary work queue
- `ojs.queue.{name}.dlq` — dead letter queue
- `ojs.queue.{name}.retry.{n}` — retry delay queues (per backoff level)

## Quick Start

```bash
# Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Build and run
make build
AMQP_URL=amqp://guest:guest@localhost:5672/ OJS_ALLOW_INSECURE_NO_AUTH=true make run
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AMQP_URL` | `amqp://localhost:5672/` | RabbitMQ connection string |
| `OJS_PORT` | `8080` | HTTP server port |
| `OJS_GRPC_PORT` | `9090` | gRPC server port |
| `OJS_API_KEY` | — | API key for authentication |
| `OJS_ALLOW_INSECURE_NO_AUTH` | `false` | Allow running without auth (dev only) |
| `OJS_OTEL_ENABLED` | `false` | Enable OpenTelemetry tracing |
| `OJS_OTEL_ENDPOINT` | — | OTLP gRPC endpoint |

## Build

```bash
make build          # Build binary to bin/ojs-server
make test           # Run tests
make lint           # Run go vet
make run            # Build and run
make docker-build   # Build Docker image
```

## Conformance

Target: Level 0–4 (230/230 tests).

```bash
make conformance    # Run conformance tests (requires ojs-conformance)
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
