.PHONY: build test lint run clean help docker-build docker-run conformance

BINARY_NAME := ojs-server
BUILD_DIR := bin

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## Build the server binary
	@mkdir -p $(BUILD_DIR)
	go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/ojs-server/

test: ## Run all tests
	go test ./... -race -cover

lint: ## Run linter
	go vet ./...

run: build ## Build and run the server
	./$(BUILD_DIR)/$(BINARY_NAME)

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR)

docker-build: ## Build Docker image
	docker build -t ojs-backend-amqp:latest .

docker-run: ## Run in Docker
	docker run --rm -p 8080:8080 -p 9090:9090 \
		-e AMQP_URL=amqp://guest:guest@host.docker.internal:5672/ \
		-e OJS_ALLOW_INSECURE_NO_AUTH=true \
		ojs-backend-amqp:latest

docker-up: ## Start server + RabbitMQ via Docker Compose
	docker compose -f docker/docker-compose.yml up -d

docker-down: ## Stop Docker Compose
	docker compose -f docker/docker-compose.yml down

conformance: build ## Run conformance tests (requires running server)
	@echo "Start server first, then run: cd ../ojs-conformance && go test ./runner/http/ -url http://localhost:8080"
