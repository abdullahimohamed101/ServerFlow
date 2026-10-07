# Architecture

## Source of Truth

The authoritative build specification for ServerFlow is
docs/architecture/serverflow-spec.md (formerly InferGrid). This
architecture document summarizes the current implementation; the spec
defines the full phased plan, requirements, and acceptance criteria.

## System Overview

ServerFlow is a distributed inference control plane for open-source LLMs.
Clients send OpenAI-compatible chat/completion requests to an API gateway,
which authenticates, rate-limits, validates, and streams responses. A
scheduler picks the most appropriate healthy inference worker for each
request based on the requested model and current worker load. Workers run
vLLM instances on GPUs and report heartbeats, queue state, and resource
utilization to a registry. Shared services provide distributed state
(Redis), durable metadata (PostgreSQL), asynchronous lifecycle events
(Kafka), and observability (Prometheus, Grafana, OpenTelemetry).

As of Phase 3, the foundation (configuration, structured logging, CI), the
gateway MVP, and a configurable mock worker exist. The gateway proxies
OpenAI-compatible requests, including streaming, to a single statically
configured upstream; the mock worker is a GPU-free stand-in for that upstream.
There is no scheduler, worker registry, authentication, or rate limiting yet.

## Major Components

### Gateway
Purpose: OpenAI-compatible HTTP API: auth, rate limiting, validation,
streaming proxy, scheduler invocation.
Location: `cmd/gateway` + `internal/gateway`. Owns: request lifecycle,
client-facing contracts.
 Status: MVP implemented (Phase 2): `/healthz`, `/readyz`, `/metrics`,
`/v1/models`, `/v1/chat/completions` (stream and non-stream), request IDs,
structured logs, Prometheus metrics, graceful shutdown. Proxies to one
configured upstream behind the `gateway.Upstream` interface, which the worker
registry and scheduler replace in Phases 4-5. Overhead measurements are in
`docs/benchmarks/phase-2-gateway-overhead.md`. Wire types and the error model
live in `internal/api`; the vendor-neutral `InferenceRequest` lives in
`pkg/protocol` (ADR-003).

### Mock Worker
Purpose: a configurable fake vLLM (latency, tokens/s, queueing, failures) so the
platform can be built and tested without a GPU. Location: `cmd/mock-worker` +
`internal/mockworker`. Owns: simulated generation, a bounded FIFO queue, failure
injection, and provisional worker stats. It is passive: it does not register or
send heartbeats (Phase 4). See `docs/development/mock-worker.md`. Status:
implemented (Phase 3).

### Inference Router / Scheduler
Purpose: choose the best worker for each request via pluggable strategies.
Location: `internal/scheduler`. Owns: worker selection, scheduling
invariants. Status: planned (Phase 5).



### Worker Agent
Purpose: register worker, report heartbeats/metrics, proxy requests to
local vLLM. Location: `cmd/worker-agent` + `internal/worker`. Owns:
worker lifecycle, capacity reporting. Status: planned (Phase 4).



### Control Plane
Purpose: worker registry, model inventory, placement, scaling, policy.
Location: `cmd/control-plane` + `internal/registry`. Owns: desired
cluster state. Status: planned (later phases).



### Shared Services
Purpose: distributed ephemeral state (Redis), durable metadata (PostgreSQL),
async lifecycle events (Kafka), metrics (Prometheus), dashboards (Grafana),
tracing (OpenTelemetry). Location: `internal/redis`, `internal/postgres`,
`internal/events`, `observability/`. Status: skeleton only (Phase 0).



## Dependency Direction

```text
cmd/*  →  internal/*  →  pkg/protocol
```

- `cmd/*` binaries are thin: parse config, build logger, start component.
- `internal/*` packages hold all logic and may depend on `pkg/protocol` types.
- `pkg/protocol` is the public contract shared across components (and
  eventually with external tooling); it must not depend on `internal/*`.
- Components communicate over HTTP/Redis/Kafka/PostgreSQL, not via
  shared in-process state.



## Important Boundaries

- **Control plane vs data plane**: routing/streaming hot path must not
  depend on control-plane state beyond cached snapshots.

- **Kafka stays off the synchronous inference path**: a client must still
  receive output if Kafka is unavailable. Events are published
  asynchronously with bounded buffers.

- **PostgreSQL is not the heartbeat store nor the routing hot path**.
- **Redis is ephemeral shared state, never the permanent source of truth**.
- **Scheduler never routes to non-READY workers** and must explain why
  no worker is eligible when that happens.

- **No unbounded queues, no infinite retries**.



## External Systems

| System | Purpose | Phase |
| --- | --- | --- |
| vLLM | inference runtime on workers | 1 |
| Redis | rate limits, coordination, routing hints | 8 |
| PostgreSQL | tenants, API keys, model configs, usage | 9 |
| Kafka | inference/worker lifecycle events | 12 |
| Prometheus | operational metrics | 10 |
| Grafana | dashboards | 10 |
| OpenTelemetry | distributed tracing | 11 |

## Testing Architecture

- **Unit tests**: scheduler algorithms, rate-limit logic, worker state
  transitions, validation, cost estimation, retry policy, circuit breaker.
- **Integration tests**: gateway + Redis + Postgres + mock workers.
- **End-to-end tests**: full gateway → worker → (mock) vLLM path with
  OpenAI-compatible client.
- **Load tests**: benchmark harness (`cmd/benchmark`).
- **Chaos tests**: fault injection (`tests/chaos`, `make chaos-*`).

Commands:

```bash
go test ./...        # unit tests
go test -race ./...  # race detector (needs a C compiler on Windows)
.\scripts\quality.ps1 -Mode Full   # full local gate
```

## Development Commands

Build:

```bash
go build ./...
```

Test:

```bash
go test ./...
go test -race ./...
```

Lint:

```bash
go vet ./...
gofmt -l .
golangci-lint run ./...   # optional
```

Run:

```bash
go run ./cmd/gateway -config config.yaml
go run ./cmd/mock-worker --model=mock-model --addr=127.0.0.1:9001   # GPU-free upstream; see docs/development/mock-worker.md
```

## Known Architectural Risks

- **Module path is `serverflow`**; if a git remote is added later, the
  module path should be renamed via `go mod edit -module`.
- **`go test -race` requires cgo** (a C compiler); not available on
  this Windows dev machine yet. CI runs it on Linux.
- **Docker Compose skeleton is unvalidated** until Docker is installed.
- **Scheduler strategy claims** must be backed by benchmark evidence before
  any strategy is declared superior (spec section 14, 65).