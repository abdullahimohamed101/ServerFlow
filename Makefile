.PHONY: fmt vet lint test test-race build all mock-workers dev-cluster bench bench-compare

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

# A local cluster: a control plane on :9090 and three mock workers (:9001-9003 at
# 100, 60, 20 tokens/s), each with its own worker agent, and a gateway on :8080 in
# registry mode (STRATEGY=least-active make dev-cluster to change it). Ctrl-C stops everything.
# Look at it with: curl -s localhost:9090/v1/workers
dev-cluster:
	go build -o bin/control-plane ./cmd/control-plane
	go build -o bin/worker-agent ./cmd/worker-agent
	go build -o bin/mock-worker ./cmd/mock-worker
	go build -o bin/gateway ./cmd/gateway
	@echo "dev cluster: control plane :9090, workers :9001 (100 tok/s), :9002 (60), :9003 (20), gateway :8080 (registry mode, round-robin); Ctrl-C to stop"
	@trap 'kill 0' INT TERM; \
	bin/control-plane & \
	sleep 1; \
	bin/mock-worker --addr=127.0.0.1:9001 --model=mock-model --tokens-per-second=100 --ttft=100ms & \
	bin/mock-worker --addr=127.0.0.1:9002 --model=mock-model --tokens-per-second=60 --ttft=150ms & \
	bin/mock-worker --addr=127.0.0.1:9003 --model=mock-model --tokens-per-second=20 --ttft=300ms & \
	for n in 1 2 3; do \
		SERVERFLOW_WORKER_ID=mock-$$n SERVERFLOW_WORKER_MODEL=mock-model \
		SERVERFLOW_WORKER_BACKEND_URL=http://127.0.0.1:900$$n SERVERFLOW_WORKER_ADVERTISE_URL=http://127.0.0.1:900$$n \
		bin/worker-agent & \
	done; \
	SERVERFLOW_GATEWAY_WORKER_SOURCE=registry SERVERFLOW_SCHEDULER_STRATEGY=$${STRATEGY:-round-robin} bin/gateway & \
	wait

# Benchmark harness (docs/benchmarks/phase-7-harness.md). Results go to benchmark/runs/run_NNN (gitignored).
#   make bench BENCH_ARGS="--scheduler least-active --workers 4 --concurrency 12 --duration 60s --workload mixed --seed 1 --repeat 3"
#   make bench-compare A=run_001 B=run_004     (one run from each --repeat 3 group)
# Without BENCH_ARGS it runs the defaults: an embedded cluster, round-robin, 16 clients, 30s, mixed.
# Keep --concurrency to about half of workers x --mock-concurrency (8 each): more than all of it is refused unless
# --allow-overload, and more than 60% warns that the run will probably be invalid.
bench:
	go run ./cmd/benchmark run $(BENCH_ARGS)

bench-compare:
	@if [ -z "$(A)" ] || [ -z "$(B)" ]; then echo "usage: make bench-compare A=run_001 B=run_004"; exit 2; fi
	go run ./cmd/benchmark compare $(A) $(B)
