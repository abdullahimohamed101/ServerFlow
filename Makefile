.PHONY: fmt vet lint test test-race build all mock-workers

fmt:
	gofmt -w .
	gofmt -l .

vet:
	go vet ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; skipping (install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)"; \
	fi

test:
	go test ./...

test-race:
	go test -race ./...

build:
	go build ./...

all: fmt vet lint test test-race build

# Run three mock workers side by side (simulation mode, spec section 54):
# :9001 at 100 tokens/s, :9002 at 60, :9003 at 20. Ctrl-C stops all three.
mock-workers:
	go build -o bin/mock-worker ./cmd/mock-worker
	@echo "mock workers: :9001 (100 tok/s), :9002 (60 tok/s), :9003 (20 tok/s); Ctrl-C to stop"
	@trap 'kill 0' INT TERM; \
	bin/mock-worker --addr=127.0.0.1:9001 --worker-id=mock-fast --model=mock-model --tokens-per-second=100 --ttft=100ms & \
	bin/mock-worker --addr=127.0.0.1:9002 --worker-id=mock-medium --model=mock-model --tokens-per-second=60 --ttft=150ms & \
	bin/mock-worker --addr=127.0.0.1:9003 --worker-id=mock-slow --model=mock-model --tokens-per-second=20 --ttft=300ms & \
	wait
