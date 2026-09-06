# Architecture

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

As of Phase  ​0, only the foundation exists: configuration, structured
logging, the repository skeleton, and CI. No inference or routing is
implemented yet.

## Major Components

### Gateway
Purpose: OpenAI-compatible HTTP API: auth, rate limiting, validation,
streaming proxy, scheduler invocation.
Location: `cmd/gateway` + `internal/gateway`. Owns: request lifecycle,
client-facing contracts.
 Status: planned (Phase 2).

### Inference Router / Scheduler
Purpose: choose the best worker for each request via pluggable strategies.
Location: `internal/scheduler`. Owns: worker selection, scheduling
invariants. Status: planned (Phase 5.



### Worker Agent
Purpose: register worker, report heartbeats/metrics, proxy requests to
local vLLM. Location: `cmd/worker-agent` + `internal/worker`. Owns:
worker lifecycle, capacity reporting. Status: planned (Phase 4.



### Control Plane
Purpose: worker registry, model inventory, placement, scaling, policy.
Location: `cmd/control-plane` + `internal/registry`. Owns: desired
cluster state. Status: planned (later phases.



### Shared Services
Purpose: distributed ephemeral state (Redis,, durable metadata (PostgreSQL,,
async lifecycle events (Kafka,, metrics (Prometheus,, dashboards (Grafana,,
tracing (OpenTelemetry,. Location: `internal/redis`, `internal/postgres`,
`internal/events`, `observability/`. Status: skeleton only (Phase 0.



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
| OpenTelemetry | distributed tracing |  ​11 |

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
```

## Known Architectural Risks

- **Module path is `serverflow`**; if a git remote is added later, the
  module path should be renamed via `go mod edit -module`.
- **`go test -race` requires cgo** (a C compiler); not available on
  this Windows dev machine yet. CI runs it on Linux.
- **Docker Compose skeleton is unvalidated** until Docker is installed.
- **Scheduler strategy claims** must be backed by benchmark evidence before
  any strategy is declared superior (spec section 14, 65).