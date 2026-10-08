# ServerFlow

A distributed LLM inference control plane and routing layer. ServerFlow
serves open-source large language models through an OpenAI-compatible API,
scheduling inference requests across GPU-backed vLLM workers while
optimizing latency, throughput, utilization, reliability, and fairness.

The product is everything between the client and the inference engine: the
gateway, scheduler, worker registry, rate limiting, event pipeline,
and observability. See `ARCHITECTURE.md` for the full picture.

> Status: Phases 0, 2, 3, 4 and 5 are complete. The gateway serves an
> OpenAI-compatible API in front of a single configured upstream; a configurable
> mock worker (`docs/development/mock-worker.md`) stands in for that upstream
> without a GPU; and a control plane tracks workers through register, heartbeat,
> and death (`docs/architecture/worker-lifecycle.md`). With
> `gateway.worker_source: registry` the gateway chooses a worker per request with
> a configurable scheduler (`docs/architecture/scheduling.md`); retries come in
> Phase 6. Phase 1 is deferred
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

Building and testing requires only Go 1.24+.

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
| 6 | Multi-worker routing | Not started |
| 7 | Baseline benchmark harness | Not started |
| 8 | Redis integration | Not started |
| 9 | PostgreSQL | Not started |
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