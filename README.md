# ServerFlow

A distributed LLM inference control plane and routing layer. ServerFlow
serves open-source large language models through an OpenAI-compatible API,
scheduling inference requests across GPU-backed vLLM workers while
optimizing latency, throughput, utilization, reliability, and fairness.

The product is everything between the client and the inference engine: the
gateway, scheduler, worker registry, rate limiting, event pipeline,
and observability. See `ARCHITECTURE.md` for the full picture.

> Status: Phases 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 and 16 are complete. The gateway serves an
> OpenAI-compatible API in front of a single configured upstream; a configurable
> mock worker (`docs/development/mock-worker.md`) stands in for that upstream
> without a GPU; and a control plane tracks workers through register, heartbeat,
> and death (`docs/architecture/worker-lifecycle.md`). With
> `gateway.worker_source: registry` the gateway chooses a worker per request with
> a configurable scheduler (`docs/architecture/scheduling.md`) and retries a
> request that fails before any output on a different worker (ADR-012). A benchmark harness (`cmd/benchmark`, Phase 7) runs reproducible load tests and compares
> runs (ADR-013). With `auth.mode: required` the gateway needs an API key kept in PostgreSQL
> (`docs/operations/postgres-and-auth.md`, ADR-014). With `rate_limit.mode: required` tenant quotas (requests, tokens, concurrency) and optional per-model
> caps are enforced across gateways through Redis (`docs/operations/redis-and-rate-limits.md`, ADR-015). With `tracing.enabled: true` one request is one trace across the gateway and the worker, retries included (`docs/operations/tracing.md`, ADR-018; try it with `make dev-tracing` and `scripts/trace-demo.sh`). Phase 1 is deferred
> until a GPU is available. See `docs/plans/completed/` for finished plans and
> `docs/plans/active/` for the current one.

## Run everything in containers

Needs only Docker with Compose 2.24 or newer, `git`, `make`, `bash` and `curl` (no Go). One command builds six small images, starts the whole system
(gateway, control plane, three mock workers with agents, PostgreSQL, Redis, Redpanda, the usage consumer, Prometheus, Grafana, Jaeger) on
a private network with only loopback ports published, and `make dev-smoke` checks it end to end:

```bash
make dev-up        # first run: copies .env, generates secrets under .dev/, builds, starts, creates two tenants and keys
make dev-smoke     # 12 of 13 checks pass and 1 is skipped; with GATEWAYS=3 in .env it ends: smoke: 13 of 13 checks passed, 0 failed, 0 skipped
export SF_KEY=$(cat .dev/api_key)
curl -s -H "Authorization: Bearer $SF_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"mock-model","messages":[{"role":"user","content":"hello"}],"max_tokens":16}' http://127.0.0.1:18080/v1/chat/completions
make dev-down      # leaves nothing behind
```

Grafana is on `http://127.0.0.1:13000` (user `admin`, password in `.dev/grafana.env`), Prometheus on `:19091`, Jaeger on `:16686`. Ports, profiles,
gateway replicas and mock-worker failure injection are set in `.env`. See `docs/operations/docker-deployment.md` and ADR-021.

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

Building and testing requires only Go 1.26+.

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

Endpoints: `GET /healthz`, `/readyz`, `/v1/models`, and
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

`quality.ps1` mirrors `scripts/quality.sh` but is not identical, and it has not been run on Windows by its authors. Differences: `observability` (promtool)
and `deploy` (hadolint, shellcheck, `docker compose config`) are skipped (run `bash scripts/quality.sh deploy` from Git Bash or WSL); `go test -race`
runs only when a C compiler (`gcc`) is on `PATH`; `govulncheck` and `golangci-lint` must be installed (the script installs nothing) and run in Full mode only;
the integration stages run only when `SERVERFLOW_TEST_POSTGRES_DSN`, `_REDIS_ADDR` and `_KAFKA_BROKERS` are set; and `./tests/integration` (its process
tests use `SIGSTOP`, undefined on Windows) is left out, with the named tests that live in it. The header of the script lists the same. If `gofmt -l`
lists every file, Git checked the sources out with CRLF line endings (the repository has no `.gitattributes`; `core.autocrlf=input` avoids it; unverified).

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
| 8 | Redis integration | Complete |
| 9 | PostgreSQL | Complete |
| 10 | Prometheus + Grafana | Complete (`docs/operations/observability.md`, ADR-017) |
| 11 | OpenTelemetry | Complete |
| 12 | Kafka | Complete |
| 13 | Real vLLM worker pool | Not started |
| 14 | Workload-aware scheduler | Not started |
| 15 | Fault tolerance | Complete (`docs/operations/resilience.md`, ADR-020, `docs/benchmarks/phase-15-chaos.md`); **on by default in registry mode, see the note below** |
| 16 | Dockerized multi-service deployment | Complete (`docs/operations/docker-deployment.md`, ADR-021) |
| 17 | Kubernetes | Not started |
| 18 | Kubernetes GPU workers | Not started |
| 19 | Autoscaling | Not started |
| 20 | Model-aware scaling | Not started |
| 21 | Model placement controller | Not started |
| 22 | Advanced fair scheduling | Not started |
| 23 | Final performance study | Not started |
| 24 | Production polish | Not started |

## Behaviour changes in Phase 15 (resilience is on by default)

From Phase 15 `resilience.mode` defaults to **`on`** (circuit breakers, retry budget, bounded admission queue with load shedding, gateway drain; ADR-020). It takes effect only with `gateway.worker_source: registry`; static-worker deployments are not affected.
An existing registry-mode deployment that sets nothing changes behaviour as listed here. `resilience.mode: off` (or `SERVERFLOW_RESILIENCE_MODE=off`, or `SF_RESILIENCE_MODE=off` in the compose `.env`) is the kill switch and restores the gateway as it was after Phase 15 PR A's three unconditional fixes (panic-safe attempts, unknown-model pre-check, second-routing metric).
Details, configuration, metrics and runbooks: `docs/operations/resilience.md`.

With the default (on, registry mode), no fault, free capacity: nothing changes except the items below. *Excluded* = made not to change; *asserted* = changes, pinned by a named test; *documented* = changes, stated in README/ADR-020/operations guide/PR text with the kill switch.

1. A request that finds every eligible worker full waits up to 3 s (bounded FIFO) before the 503; `Retry-After` reflects recent waits — asserted + documented.
2. Retries are subject to the budget; some retries that used to happen are denied and the failed worker response is relayed — asserted + documented.
3. A worker with sustained faults is ejected by its breaker (cap D9) and receives only probes; a half-open worker gets at most one request at a time — asserted + documented.
4. `admission.max_global_requests` (1000) becomes live: the 1001st concurrent request on a gateway is refused with 503 `global_limit` (1000 long streams is ordinary) — asserted + documented; the operator raises the key.
5. The first-byte deadline defaults to header + idle timeouts plus the 5 s dial timeout (185 s with the defaults), so no single attempt the old gateway served is cut — **excluded** per attempt; a request that spends a slow header phase, a retry and a late first byte beyond that total (about 250 s) was served before and now gets 504 `UPSTREAM_TIMEOUT` — documented; an explicit value below the header timeout is refused.
6. The overhead histogram excludes queue wait, so the 25 ms alert keeps its meaning — **excluded**; the wait has its own series.
7. A retried 503 now pays a body peek of at most 250 ms and only when a retry is possible — asserted (bound) + documented.
8. On SIGTERM the gateway fails `/readyz` and serves for 5 s before shutting down (a developer's Ctrl-C waits 5 s; a second signal exits); existing process tests and the embedded cluster set grace 0 — asserted + documented.
9. Log volume: one WARN per breaker open (at most one per worker per cool-down), shed INFO rate-limited to 1/s per reason — documented.
10. Tail drop is tenant-blind except for the per-tenant queue share (0.5 of depth); a shed after the quota charge is not refunded — documented.
11. New series, dashboard and alerts appear; a probe of a frozen worker can cost one client a 504 — documented.

The list is repeated verbatim in ADR-020, the operations guide, the README note and the PR text.

## Development

See `docs/development/setup.md` for tooling requirements and Windows notes.
`make quality` runs the same checks as CI (`scripts/quality.sh`; see `docs/development/ci.md`).
See `docs/plans/active/` for the current execution plan (empty between phases).