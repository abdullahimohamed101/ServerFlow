# ADR-001: Go for the Gateway and Control Plane

Status: Accepted
Date: 2026-08-28

## Context

ServerFlow's core engineering contribution is the control plane and routing
layer between clients and inference engines: gateway, scheduler, worker
registry, rate limiting, event pipeline, and observability. These
components must handle many concurrent streaming HTTP connections, run
pluggable scheduling algorithms, and integrate with Redis, Kafka,
PostgreSQL, Prometheus, and OpenTelemetry. The project also includes a
Python-heavy inference runtime (vLLM, but that is an external system, not
code we own.

## Decision

Implement the gateway, control plane, scheduler, worker agent, and all
shared infrastructure clients in Go.

## Alternatives

- **Python**: natural for the inference ecosystem, but weaker for high-
  concurrency streaming HTTP proxies and its GIL complicates the hot path.

- **Rust**: excellent performance, but slower to iterate and a smaller
  ecosystem for the required infra clients (Kafka, Postgres, etc..
- **Node.js/TypeScript**: fine for HTTP, weaker for systems programming
  and typed infra contracts.

## Consequences

- **Positive**: strong standard library (`net/http`, `log/slog`, `context`,
  `sync`), first-class concurrency, single static binaries, mature
  clients for every required external system, easy cross-platform builds.

- **Negative**: GC pauses are a theoretical concern for the hot path, but
  acceptable for an I/O-bound proxy; team must know Go idioms.

- **vLLM remains Python** and is treated as an external black box
  accessed over HTTP. Go never replaces it; it orchestrates it.



## Status

Accepted. Recorded during Phase 0.