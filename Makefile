.PHONY: build clean

BINARY_NAME=consumer
BINARY_DIR=bin

build:
	@echo "Building consumer..."
	@mkdir -p $(BINARY_DIR)
	@go build -o $(BINARY_DIR)/$(BINARY_NAME) ./cmd/consumer

clean:
	@echo "Cleaning up..."
	@rm -rf $(BINARY_DIR)/*

run:
	go run cmd/consumer/main.go

