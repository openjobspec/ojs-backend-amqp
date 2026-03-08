.PHONY: build test lint run clean help docker-build docker-run docker-up docker-down conformance conformance-level-0 conformance-level-1 conformance-level-2 conformance-level-3 conformance-level-4 conformance-docker

BINARY_NAME := ojs-server
BUILD_DIR := bin
CONFORMANCE_RUNNER := ../ojs-conformance/runner/http
CONFORMANCE_SUITES := ../../suites
OJS_URL ?= http://localhost:8080

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

conformance: ## Run all conformance levels (requires running server at OJS_URL)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES)

conformance-level-0: ## Run conformance level 0 (Core)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 0

conformance-level-1: ## Run conformance level 1 (Reliable)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 1

conformance-level-2: ## Run conformance level 2 (Scheduled)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 2

conformance-level-3: ## Run conformance level 3 (Workflows)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 3

conformance-level-4: ## Run conformance level 4 (Advanced)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 4

conformance-docker: ## Start RabbitMQ + server via Docker, run conformance, then stop
	@echo "Starting RabbitMQ and OJS server..."
	docker compose -f docker/docker-compose.yml up -d --build --wait
	@echo "Running conformance tests..."
	cd $(CONFORMANCE_RUNNER) && go run . -url http://localhost:8080 -suites $(CONFORMANCE_SUITES) ; \
		EXIT_CODE=$$? ; \
		echo "Stopping containers..." ; \
		docker compose -f docker/docker-compose.yml down ; \
		exit $$EXIT_CODE
