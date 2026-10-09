# ServerFlow

A distributed LLM inference control plane and routing layer. ServerFlow
serves open-source large language models through an OpenAI-compatible API,
scheduling inference requests across GPU-backed vLLM workers while
optimizing latency, throughput, utilization, reliability, and fairness.

The product is everything between the client and the inference engine: the
gateway, scheduler, worker registry, rate limiting, event pipeline,
and observability. See `ARCHITECTURE.md` for the full picture.

> Status: Phases 0, 2, 3, 4, 5, 6, 7 and 9 are complete. The gateway serves an
> OpenAI-compatible API in front of a single configured upstream; a configurable
> mock worker (`docs/development/mock-worker.md`) stands in for that upstream
> without a GPU; and a control plane tracks workers through register, heartbeat,
> and death (`docs/architecture/worker-lifecycle.md`). With
> `gateway.worker_source: registry` the gateway chooses a worker per request with
> a configurable scheduler (`docs/architecture/scheduling.md`) and retries a
> request that fails before any output on a different worker (ADR-012). A benchmark harness (`cmd/benchmark`, Phase 7) runs reproducible load tests and compares
> runs (ADR-013). With `auth.mode: required` the gateway needs an API key kept in PostgreSQL
> (`docs/operations/postgres-and-auth.md`, ADR-014). Phase 1 is deferred
> until a GPU is available. See `docs/plans/completed/` for finished plans and
> `docs/plans/active/` for the current one.

## Repository Layout

```text
cmd/             binaries (gateway, mock-worker, worker-agent, control-plane, usage-consumer, benchmark)
internal/       shared libraries (config, telemetry, and future packages)
pkg/            public protocol types
worker/         worker runtime + vLLM integration
benchmark/    load generation, reports, analysis
schemas/       event schemas
migrations/    PostgreSQL migrations
deploy/        Docker, Kubernetes, Helm
observability/  Prometheus, Grafana, OpenTelemetry
tests/         integration, e2e, chaos, load
docs/          architecture, ADRs, benchmarks, operations, development
```

## Quick Start

Building and testing requires only Go 1.25+.

```bash
go build ./...
go vet ./...
go test ./...
```

Run the gateway against any OpenAI-compatible server (for example vLLM):

```bash
SERVERFLOW_GATEWAY_UPSTREAM_URL=http://localhost:8000 \
SERVERFLOW_GATEWAY_MODELS=my-model \
go run ./cmd/gateway
```

Endpoints: `GET /healthz`, `/readyz`, `/metrics`, `/v1/models`, and
`POST /v1/chat/completions` (streaming supported). See `internal/config` for all
`gateway.*` settings and their `SERVERFLOW_GATEWAY_*` environment variables.

Run a local cluster (a control plane plus three mock workers, each with an agent):

```bash
make dev-cluster
curl -s localhost:9090/v1/workers     # who is registered, their state and health
curl -s 'localhost:9090/v1/workers?model=mock-model&eligible=true'
```

Run a load test and compare two schedulers (an embedded simulated cluster; results in `benchmark/runs/`):

```bash
make bench BENCH_ARGS="--scheduler round-robin --workers 4 --worker-profile heterogeneous --concurrency 12 --duration 60s --workload mixed --seed 1 --repeat 3"
make bench BENCH_ARGS="--scheduler least-active --workers 4 --worker-profile heterogeneous --concurrency 12 --duration 60s --workload mixed --seed 1 --repeat 3"
make bench-compare A=run_001 B=run_004      # a run from each --repeat 3 group (run_001-003 and run_004-006)
```

Use well fewer clients than the cluster has slots (workers x `--mock-concurrency`, 8 each by default). The harness refuses
more than 100% of them (unless `--allow-overload`) and warns above 60%, because round-robin ignores load and fills a slow
worker's slots first, after which the gateway answers 503 and the run is invalid; 12 to 16 clients on 32 slots is a safe
range. Use `--repeat 3` or more so `compare` can show run-to-run spread. A run in which more than
5% of requests fail is marked invalid and exits non-zero. See `docs/benchmarks/phase-7-harness.md` for how to read the output.

On Windows (no `make`), use the existing quality gate:

```powershell
.\scripts\quality.ps1 -Mode Full
```

## Phase Status

| Phase | Description | Status |
| --- | --- | --- |
| 0 | Repository foundation | Complete |
| 1 | Single-worker vLLM baseline | Deferred (needs a GPU) |
| 2 | Gateway MVP | Complete |
| 3 | Mock worker framework | Complete |
| 4 | Worker registry | Complete |
| 5 | Scheduler framework | Complete |
| 6 | Multi-worker routing | Complete |
| 7 | Baseline benchmark harness | Complete |
| 8 | Redis integration | Not started |
| 9 | PostgreSQL | Complete |
| 10 | Prometheus + Grafana | Not started |
| 11 | OpenTelemetry | Not started |
| 12 | Kafka | Not started |
| 13 | Real vLLM worker pool | Not started |
| 14 | Workload-aware scheduler | Not started |
| 15 | Fault tolerance | Not started |
| 16 | Dockerized multi-service deployment | Not started |
| 17 | Kubernetes | Not started |
| 18 | Kubernetes GPU workers | Not started |
| 19 | Autoscaling | Not started |
| 20 | Model-aware scaling | Not started |
| 21 | Model placement controller | Not started |
| 22 | Advanced fair scheduling | Not started |
| 23 | Final performance study | Not started |
| 24 | Production polish | Not started |

## Development

See `docs/development/setup.md` for tooling requirements and Windows notes.
See `docs/plans/active/` for the current execution plan (empty between phases).