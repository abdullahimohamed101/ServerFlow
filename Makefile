.PHONY: fmt vet lint test test-race build all

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