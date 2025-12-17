.PHONY: help build build-ingestion build-fraud-detector run stop clean generate-mocks test integration-test logs logs-ingestion logs-fraud

help:
	@echo "Available commands:"
	@echo "  make build              - Build all Docker images"
	@echo "  make build-ingestion    - Build ingestion service only"
	@echo "  make build-fraud-detector - Build fraud-detector service only"
	@echo "  make run                - Start all services"
	@echo "  make stop               - Stop all services"
	@echo "  make clean              - Stop and remove all containers, volumes"
	@echo "  make generate-mocks 	 - Generate mocks using mockery"
	@echo "  make test               - Run all tests"
	@echo "  make test-unit          - Run unit tests only"
	@echo "  make test-coverage      - Run tests with coverage report"
	@echo "  make logs               - Tail logs from all services"
	@echo "  make logs-ingestion     - Tail logs from ingestion service"
	@echo "  make logs-fraud         - Tail logs from fraud-detector service"

build:
	docker-compose build

build-ingestion:
	docker-compose build ingestion

build-fraud-detector:
	docker-compose build fraud-detector

run:
	docker-compose up -d

stop:
	docker-compose down

clean:
	docker-compose down -v
	docker system prune -f

generate-mocks:
	@echo "Generating mocks..."
	@mockery --config .mockery.yaml

test: generate-mocks
	@echo "Running unit tests..."
	@go test -v -race ./internal/...

integration-test: generate-mocks
	@echo "Running integration tests..."
	@go test -v -race ./test/...

logs:
	docker-compose logs -f

logs-ingestion:
	docker-compose logs -f ingestion

logs-fraud:
	docker-compose logs -f fraud-detector
