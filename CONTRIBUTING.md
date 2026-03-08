# Contributing

Thanks for contributing to `ojs-backend-amqp`.

## Prerequisites

- Go 1.24+
- RabbitMQ 3.x (or Docker)
- Docker (optional, for integration tests)

## Local setup

```bash
git clone https://github.com/openjobspec/ojs-backend-amqp.git
cd ojs-backend-amqp
make build
```

Run with RabbitMQ:

```bash
# Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Run the server
AMQP_URL=amqp://guest:guest@localhost:5672/ OJS_ALLOW_INSECURE_NO_AUTH=true make run
```

## Running tests

```bash
make test           # Unit tests (no RabbitMQ required)
make lint           # go vet
```

Tests automatically fall back to in-memory mode when RabbitMQ is not available.

## Running conformance tests

```bash
# Start the server first, then:
make conformance

# Or use Docker to start everything:
make conformance-docker
```

## Code style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Add tests for new functionality
- Keep the Backend interface implementation in `internal/amqp/backend.go`
- Use the shared `ojs-go-backend-common` packages for API handlers and middleware

## Submitting changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Make your changes with tests
4. Run `make test && make lint`
5. Submit a pull request

For spec changes, see the [RFC process](https://github.com/openjobspec/spec/blob/main/CONTRIBUTING.md).
