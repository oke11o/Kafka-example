.PHONY: build clean

BINARY_DIR=bin

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

