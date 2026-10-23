.PHONY: fmt vet lint test test-race build all mock-workers dev-cluster dev-postgres test-postgres dev-redis test-redis dev-kafka test-kafka dev-tracing dev-tracing-stop bench bench-compare quality quality-fast

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
# Worker size: WORKER_CONCURRENCY=8 WORKER_QUEUE=64 make dev-cluster (defaults 4 and 32). With 12 benchmark clients the
# default size overloads three workers (NO_CAPACITY); see docs/operations/observability.md.
# Look at it with: curl -s localhost:9090/v1/workers
dev-cluster:
	go build -o bin/control-plane ./cmd/control-plane
	go build -o bin/worker-agent ./cmd/worker-agent
	go build -o bin/mock-worker ./cmd/mock-worker
	go build -o bin/gateway ./cmd/gateway
	@echo "dev cluster: control plane :9090, workers :9001 (100 tok/s), :9002 (60), :9003 (20), gateway :8080 (registry mode, round-robin); Ctrl-C to stop"
	@trap 'kill 0' INT TERM; \
	WORKER_FLAGS="--max-concurrency=$${WORKER_CONCURRENCY:-4} --queue-size=$${WORKER_QUEUE:-32}"; \
	bin/control-plane & \
	sleep 1; \
	bin/mock-worker --addr=127.0.0.1:9001 --model=mock-model --tokens-per-second=100 --ttft=100ms $$WORKER_FLAGS & \
	bin/mock-worker --addr=127.0.0.1:9002 --model=mock-model --tokens-per-second=60 --ttft=150ms $$WORKER_FLAGS & \
	bin/mock-worker --addr=127.0.0.1:9003 --model=mock-model --tokens-per-second=20 --ttft=300ms $$WORKER_FLAGS & \
	for n in 1 2 3; do \
		SERVERFLOW_WORKER_ID=mock-$$n SERVERFLOW_WORKER_MODEL=mock-model \
		SERVERFLOW_WORKER_BACKEND_URL=http://127.0.0.1:900$$n SERVERFLOW_WORKER_ADVERTISE_URL=http://127.0.0.1:900$$n \
		bin/worker-agent & \
	done; \
	SERVERFLOW_GATEWAY_WORKER_SOURCE=registry SERVERFLOW_SCHEDULER_STRATEGY=$${STRATEGY:-round-robin} bin/gateway & \
	wait

# A throwaway PostgreSQL 16 on 127.0.0.1:55432 (no Docker; data in .data/, gitignored).
# Stop it with scripts/dev-postgres.sh stop, wipe it with scripts/dev-postgres.sh reset.
dev-postgres:
	scripts/dev-postgres.sh start

# The database-backed tests, against the throwaway instance (started if needed).
test-postgres:
	scripts/dev-postgres.sh start >/dev/null
	SERVERFLOW_TEST_POSTGRES_DSN="$$(scripts/dev-postgres.sh dsn)" go test -race -count=1 ./internal/auth/... ./internal/postgres/... ./internal/gateway/... ./cmd/admin/... ./tests/integration/...

# A throwaway Redis 7 on 127.0.0.1:56379 that requires a password, like CI's (Docker if its daemon runs, else a
# local redis-server). Stop it with scripts/dev-redis.sh stop.
dev-redis:
	scripts/dev-redis.sh start

# The Redis-backed tests, against the throwaway instance (started if needed).
test-redis:
	scripts/dev-redis.sh start >/dev/null
	SERVERFLOW_TEST_REDIS_ADDR="$$(scripts/dev-redis.sh addr)" SERVERFLOW_TEST_REDIS_PASSWORD="$$(scripts/dev-redis.sh password)" go test -race -count=1 ./internal/redis/... ./internal/ratelimit/... ./internal/gateway/... ./tests/integration/...

# A throwaway Kafka-compatible broker (Redpanda) on 127.0.0.1:59092 that requires SASL/SCRAM, like CI's. Stop it with
# scripts/dev-kafka.sh stop. The usage tests also need a PostgreSQL (make dev-postgres).
dev-kafka:
	scripts/dev-kafka.sh start

# The Kafka-backed tests, against the throwaway broker (started if needed). Set SERVERFLOW_TEST_POSTGRES_DSN for the
# usage tests.
test-kafka:
	scripts/dev-kafka.sh start >/dev/null
	SERVERFLOW_TEST_KAFKA_BROKERS="$$(scripts/dev-kafka.sh brokers)" SERVERFLOW_TEST_KAFKA_USER="$$(scripts/dev-kafka.sh user)" SERVERFLOW_TEST_KAFKA_PASSWORD="$$(scripts/dev-kafka.sh password)" go test -race -count=1 ./internal/kafka/... ./internal/events/... ./internal/usage/... ./tests/integration/...

# A standalone Jaeger (v2) for ServerFlow traces: OTLP/HTTP on 127.0.0.1:14318, UI on http://127.0.0.1:16686
# (TRACING_OTLP_PORT / TRACING_UI_PORT to change them). Then run scripts/trace-demo.sh. Docker is required.
dev-tracing:
	docker compose -f observability/tracing/docker-compose.yml up -d
	@echo "Jaeger UI: http://127.0.0.1:$${TRACING_UI_PORT:-16686}"
	@echo "Gateway:   SERVERFLOW_TRACING_ENABLED=true SERVERFLOW_TRACING_ENDPOINT=http://127.0.0.1:$${TRACING_OTLP_PORT:-14318}"
	@echo "Worker:    mock-worker --otlp-endpoint=http://127.0.0.1:$${TRACING_OTLP_PORT:-14318}"
	@echo "Demo:      scripts/trace-demo.sh"

dev-tracing-stop:
	docker compose -f observability/tracing/docker-compose.yml down

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

# The same checks CI runs (docs/development/ci.md). `quality` also runs the database-backed tests when
# SERVERFLOW_TEST_POSTGRES_DSN is set; `quality-fast` is lint plus unit tests.
quality:
	scripts/quality.sh full

quality-fast:
	scripts/quality.sh lint
	scripts/quality.sh unit

# Phase 10: Prometheus and Grafana beside a local cluster (docs/operations/observability.md). Needs
# observability/.env with GRAFANA_ADMIN_PASSWORD (copy observability/.env.example; the file is untracked).
.PHONY: obs-up obs-down obs-logs obs-check
obs-up:
	docker compose -f observability/docker-compose.yml up -d

obs-down:
	docker compose -f observability/docker-compose.yml down

obs-logs:
	docker compose -f observability/docker-compose.yml logs -f --tail=100

# promtool checks of the config and rules plus the dashboard validation (needs promtool or Docker).
obs-check:
	scripts/quality.sh observability

# Phase 16: the whole system in containers on one machine (docs/operations/docker-deployment.md, ADR-021).
# Needs Docker with Compose 2.24+. Configuration is .env (copied from deploy/compose/.env.example on the first run);
# secrets are generated into .dev/ (gitignored). The host-process workflow above (dev-cluster, dev-postgres, ...) is unchanged.
.PHONY: dev-up dev-down dev-reset dev-smoke dev-audit dev-ps dev-logs dev-config dev-pull dev-psql dev-redis-cli dev-rpk dev-key deploy-check
dev-up:
	scripts/dev-up.sh up

dev-down:
	scripts/dev-up.sh down

# Also removes named volumes (PERSIST=1), .dev/ (secrets, API keys) and the serverflow/*:dev images.
dev-reset:
	scripts/dev-up.sh reset

dev-smoke:
	scripts/dev-up.sh smoke

# The running containers against the promises of ADR-021: read-only, no capabilities, loopback-only ports, no anonymous volume.
dev-audit:
	scripts/dev-up.sh audit

dev-ps:
	scripts/dev-up.sh ps

# make dev-logs SERVICE=gateway
dev-logs:
	scripts/dev-up.sh logs $(SERVICE)

# The rendered compose file without any secret value (safe to paste into an issue).
dev-config:
	scripts/dev-up.sh config

dev-pull:
	scripts/dev-up.sh pull

dev-psql:
	scripts/dev-up.sh psql

dev-redis-cli:
	scripts/dev-up.sh redis-cli

dev-rpk:
	scripts/dev-up.sh rpk $(ARGS)

# Prints where the dev API key is stored (never the key itself).
dev-key:
	scripts/dev-up.sh key

# Static checks of the deployment files: no Docker daemon needed (docs/development/ci.md).
deploy-check:
	scripts/quality.sh deploy

# Phase 17: ServerFlow on a local kind cluster (docs/operations/kubernetes-deployment.md). Needs Docker with at least 4 GB of
# memory, kind and kubectl at the versions in deploy/kubernetes/tools.env (nothing is installed for you), and Compose's
# secrets generator (scripts/dev-up.sh secrets). The cluster uses a dedicated kubeconfig, .dev/k8s/kubeconfig, and the scripts
# refuse any context other than kind-serverflow. PR 2 deploys the data tier (PostgreSQL, Redis, Redpanda) and its jobs.
.PHONY: k8s-up k8s-down k8s-reset k8s-smoke k8s-roll-test k8s-audit k8s-render k8s-check
k8s-up:
	scripts/k8s-up.sh up

# Deletes the kind cluster and its kubeconfig and nothing else.
k8s-down:
	scripts/k8s-up.sh down

# Also removes .dev/k8s/ (the generated secrets in .dev/ stay; make dev-reset removes them).
k8s-reset:
	scripts/k8s-up.sh reset

k8s-smoke:
	scripts/k8s-up.sh smoke

# Not implemented yet: the rolling-update measurement arrives in PR 6, the pod audit with the workloads in PR 3.
k8s-roll-test:
	scripts/k8s-up.sh roll-test

k8s-audit:
	scripts/k8s-up.sh audit

# The rendered manifests of the kind overlay, with no Secret content (safe to paste into an issue).
k8s-render:
	scripts/k8s-up.sh render

# The static checks, no cluster: render, kubeconform, the tests/k8s policy tests, shellcheck.
k8s-check:
	scripts/quality.sh k8s
