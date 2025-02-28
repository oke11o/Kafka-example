.PHONY: build clean lint lint-install

BINARY_DIR=bin
GOLANGCI_LINT_VERSION=v1.63.4

build: build-consumer build-producer

build-consumer:
	@echo "Building consumer..."
	@mkdir -p $(BINARY_DIR)
	@go build -o $(BINARY_DIR)/consumer ./cmd/consumer

build-producer:
	@echo "Building producer..."
	@mkdir -p $(BINARY_DIR)
	@go build -o $(BINARY_DIR)/producer ./cmd/producer

clean:
	@echo "Cleaning up..."
	@rm -rf $(BINARY_DIR)/*

run-consumer:
	go run cmd/consumer/main.go

run-producer:
	go run cmd/producer/main.go

lint-install:
	@echo "Installing golangci-lint..."
	@mkdir -p $(BINARY_DIR)
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell pwd)/$(BINARY_DIR) $(GOLANGCI_LINT_VERSION)

lint:
	@echo "Running linters..."
	@./$(BINARY_DIR)/golangci-lint run ./...

.PHONY: test
test:
	@echo "Running tests..."
	@go test -v -race ./...

