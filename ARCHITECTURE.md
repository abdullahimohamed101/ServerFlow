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

As of Phase 7 the repository contains the foundation (configuration,
structured logging, CI), the gateway MVP, a configurable mock worker, the
worker registry, a scheduler framework, and a benchmark harness. The gateway proxies OpenAI-compatible requests, including
streaming, either to a single statically configured upstream (the default) or,
in registry mode, to the worker a configured scheduler picks from the control
plane's registry (`docs/architecture/scheduling.md`). The mock worker is a
GPU-free stand-in; worker agents register workers with a control plane that
tracks their state and health. A request that fails before any output is retried on a different
worker, up to `gateway.max_attempts` (default 2; ADR-012). There is no circuit breaker, API-key authentication, or
rate limiting yet. A benchmark harness drives reproducible load against it (below).

## Major Components

### Gateway
Purpose: OpenAI-compatible HTTP API: auth, rate limiting, validation,
streaming proxy, scheduler invocation.
Location: `cmd/gateway` + `internal/gateway`. Owns: request lifecycle,
client-facing contracts.
 Status: MVP implemented (Phase 2): `/healthz`, `/readyz`, `/metrics`,
`/v1/models`, `/v1/chat/completions` (stream and non-stream), request IDs,
structured logs, Prometheus metrics, graceful shutdown. Proxies to one
configured upstream behind the `gateway.Upstream` interface; with
`gateway.worker_source: registry` (Phase 5) a router picks a worker per request
from a cached registry snapshot and sends it through a dial-guarded transport. Overhead measurements are in
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
invariants. Status: implemented (Phase 5): the section 13 interface with random,
round-robin, least-active, and least-queue strategies; the gateway's router
(`internal/gateway/router.go`) adds the in-flight overlay and snapshot cache.
See ADR-011 and `docs/architecture/scheduling.md`. Weighted and latency-aware
strategies are Phase 14.



### Benchmark Harness
Purpose: reproducible load tests and run-to-run comparison, without claiming a scheduler is better
(spec section 14). Location: `cmd/benchmark` + `internal/bench` (`workload`, `driver`, `collect`,
`embedded`, `report`, `runner`). Owns: seeded workloads, closed and open loop load, measurement
arithmetic, an in-process simulated cluster, the result schema (`benchmark/runs/run_NNN`) and the
comparison. It only talks to a gateway (and optionally a control plane) over HTTP and imports no
gateway internals except to boot the embedded cluster. Status: implemented (Phase 7). See ADR-013 and
`docs/benchmarks/phase-7-harness.md`.

### Worker Agent
Purpose: register worker, report heartbeats/metrics. It sits beside the
inference backend and is not in the data path. Location: `cmd/worker-agent` +
`internal/worker`. Owns: worker lifecycle, capacity reporting. Status:
implemented (Phase 4) with a mock-worker backend; a vLLM backend arrives in
Phase 13.



### Control Plane
Purpose: worker registry, model inventory, placement, scaling, policy.
Location: `cmd/control-plane` + `internal/registry` (core, `server`, `client`).
Owns: desired cluster state. Status: the in-memory worker registry is
implemented (Phase 4): registration, heartbeats, a state machine, health
derived from heartbeat age, eligibility, and model lookup, behind a
shared-secret bearer token. Placement and scaling are later phases. See
`docs/architecture/worker-lifecycle.md` and ADR-010. Wire types live in
`pkg/protocol`.



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
- **Load tests**: benchmark harness (`cmd/benchmark`; `make bench`).
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
go run ./cmd/control-plane                                         # worker registry on 127.0.0.1:9090
make dev-cluster                                                   # control plane + 3 mock workers + agents
```

## Known Architectural Risks

- **Module path is `serverflow`**; if a git remote is added later, the
  module path should be renamed via `go mod edit -module`.
- **`go test -race` requires cgo** (a C compiler); not available on
  this Windows dev machine yet. CI runs it on Linux.
- **Docker Compose skeleton is unvalidated** until Docker is installed.
- **Scheduler strategy claims** must be backed by benchmark evidence before
  any strategy is declared superior (spec section 14, 65).