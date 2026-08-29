# InferGrid

A distributed LLM inference control plane and routing layer. InferGrid
serves open-source large language models through an OpenAI-compatible API,
scheduling inference requests across GPU-backed vLLM workers while
optimizing latency, throughput, utilization, reliability, and fairness.

The product is everything between the client and the inference engine: the
gateway, scheduler, worker registry, rate limiting, event pipeline,
and observability. See `ARCHITECTURE.md` for the full picture.

> Status: Phase 0 (repository foundation). No inference or routing
> exists yet. See `docs/plans/active/phase-0-foundation.md` for the
> current execution plan.

## Repository Layout

```text
cmd/             binaries (gateway, worker-agent, control-plane, usage-consumer, benchmark)
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

Phase 0 requires only Go 1.24+.

```bash
go build ./...
go vet ./...
go test ./...
```

On Windows (no `make`), use the existing quality gate:

```powershell
.\scripts\quality.ps1 -Mode Full
```

## Phase Status

| Phase | Description | Status |
| --- | --- | --- |
| 0 | Repository foundation | In progress |
| 1 | Single-worker vLLM baseline | Not started |
| 2 | Gateway MVP | Not started |
| 3 | Mock worker framework | Not started |
| 4 | Worker registry | Not started |
| 5 | Scheduler framework | Not started |
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
See `docs/plans/active/` for the current execution plan.