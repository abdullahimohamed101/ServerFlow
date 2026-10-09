# Phase 10 — Prometheus and Grafana

Status: Proposed (awaiting approval of the decisions below)
Owner: coding agent
Depends on: Phases 4-9 merged, and the prep plan `prep-lifecycle-observer-and-config-split.md` (the `gateway.Observer` seam and the per-feature config files). Do not start before that lands. Uses Phase 7 (`cmd/benchmark`) for the acceptance demonstration.
Spec: `docs/architecture/serverflow-spec.md` §6 (lifecycle), §28-31 (observability, metrics, dashboards, SLOs), §58 Phase 10, §63 (rule 8: instrument request stages)
Hand-offs: the prep plan gives Phase 10 `Observer` (metrics is its first implementation, already in place) and a reserved `internal/config/metrics.go`. Phase 11 (traces) and 12 (Kafka) are further observers; Phase 16 owns the full compose and consumes `observability/` unchanged.

## Outcome

Every component reports what it is doing, and an operator can watch a benchmark run live. Concretely: the gateway, control plane and mock worker each expose a Prometheus endpoint that is **not public by default**; `observability/` holds a scrape configuration, recording and alert rules with `promtool` tests, a provisioned Grafana datasource, and the four spec §30 dashboards as JSON; one command starts Prometheus and Grafana beside a local cluster; CI validates the rules, the dashboards' queries, and that every metric a dashboard uses is really exported.

```text
gateway (:8080 data, 127.0.0.1:9100 metrics) ─┐
control plane (:9090 API, 127.0.0.1:9101) ────┼─► Prometheus (:9091) ─► Grafana (:3000, dashboards from the repo)
mock workers (:9001.. same listener) ─────────┘            └─► recording + alert rules (SLOs, spec §31)
cmd/benchmark run --target ...  ───► load  ───►  dashboards move while it runs
```

Acceptance headline (spec §58): *instrument everything, dashboards, observe benchmark live.* Everything that can be checked without a running Grafana is checked in CI; the live demonstration is a documented manual run with captured evidence (see Verification Plan, honestly marked).

## Non-Goals

- No Alertmanager, paging, or notification routing: alerts are rules visible in Prometheus and Grafana. No SLO tooling beyond rules.
- No OpenTelemetry (Phase 11), no Kafka metrics (Phase 12), no real GPU totals (Phase 13: the protocol carries no `gpu_memory_total`).
- No full compose, no images for ServerFlow binaries (Phase 16), no Kubernetes ServiceMonitors (Phase 17).
- No gateway queue or admission metrics for machinery that does not exist: `inference_queue_duration_seconds` and `admission_rejections_total` wait for Phase 15 (backpressure); `scheduler_worker_score` waits for Phase 14 (no scores exist yet).
- No change to scheduling, retry, rate-limit or auth behaviour. Parsing response bodies for token counts in the gateway is out (hot path; see D3).
- No per-tenant dashboards or billing views (usage consumers, Phase 12).

## Current Architecture

- Exactly one place exports metrics: `internal/gateway/metrics.go`, a private `prometheus.Registry` per `Server` (Go and process collectors plus 10 instruments and two limiter `*Func` series), served at `GET /metrics` on the **public data listener** (`net.Listen(":<port>")`, all interfaces). `/metrics` is registered outside `authenticate`, so with `auth.mode: required` it is still world-readable. Existing series: `inference_requests_total{model,status}`, `inference_requests_active`, `inference_request_duration_seconds{model}`, `inference_ttft_seconds{model}`, `inference_attempts_total{model,outcome}`, `inference_retries_total{model,reason}`, `auth_rejections_total{status}`, `rate_limit_rejections_total{limit}`, `rate_limit_bypassed_total`, `rate_limit_decision_seconds`, `rate_limit_local_leases`, `rate_limit_dropped_releases_total`. Phase 8 deliberately left tenant labels out and left cardinality to this phase.
- Counter vectors appear only after their first increment, so many series are absent until traffic hits them (this breaks `rate()` alerts and "does the dashboard query exist" checks; see D6).
- Control plane (`cmd/control-plane`, `127.0.0.1:9090`, token-protected API), mock worker (`/health /readyz /v1/models /stats` and chat), worker agent (no HTTP server): none exports Prometheus metrics. The registry already holds per-worker state, health, heartbeat age and `protocol.Metrics` (active, queue depth, queued input tokens, tokens/s, optional GPU utilisation and memory MB); `mockworker.EngineStats` counts completed, failed, rejected, cancelled and tokens.
- `internal/redis.Client` already tracks command count, pool stats and down/backoff state; `internal/postgres` owns a `pgxpool.Pool` (its `Stat()` is unused); the auth key cache has no counters.
- `observability/prometheus/prometheus.yml` is a commented skeleton; `observability/otel/` is Phase 11's. The root `docker-compose.yml` has unvalidated `prometheus:v2.53.0` and `grafana:11.1.0` services, publishing Prometheus on host `:9090`, which collides with the control plane's default `127.0.0.1:9090`.
- Environment facts for this plan: `docker` and `promtool` were not found on the PATH of the machine this plan was written on (the user states Docker is available on their Mac; confirm before relying on it). `jq` is present. No new Go dependency is needed: `prometheus/client_golang` is already required.

## Decisions (confirm before implementation)

- **D1. Two kinds of instrument, kept apart.** *Event instruments* (counters, histograms, the active gauge) hang off `gateway.Observer`: request totals/durations/TTFT, attempts/retries, failures, rejections (auth, rate limit, no-capacity), scheduler decision count and latency, selections per worker, gateway overhead. *Collectors* (`prometheus.Collector` reading state at scrape time, no hot-path cost) cover state that is not a request event: registry/worker gauges (control plane), the gateway's registry-snapshot age and refresh failures, Redis client stats, Postgres pool stats, auth cache hit/miss, rate limiter lease stats (existing `*Func` series move here unchanged). Default: this split. Rule: nothing on the request path takes a lock that a scrape can hold.
- **D2. Prep-seam additions (additive fields only).** The metrics observer needs: `AttemptStart{SelectDuration, SinceRequestStart, Strategy, WorkerState}`; `Rejection{Kind, Reason, DecisionDuration}` with kind `capacity` carrying the no-worker reason; `Completion{ErrorCode}`. Default: if the prep PR has not merged these, they are the first commit of Phase 10 (event structs and the translation file only; no behaviour change). Open to the user: fold them into the prep PR instead.
- **D3. Series in scope (gap analysis against spec §29).** Default: add everything marked Add; defer the rest with the reason recorded in the ADR. Names are the spec's unless noted.

  | Spec group | Exists today | Add in Phase 10 | Defer / divergence |
  | --- | --- | --- | --- |
  | Gateway | `inference_requests_total`, `_active`, `_request_duration_seconds`, `_ttft_seconds`, plus attempts/retries | `inference_failures_total{model,reason}` (reason = fixed API error code set, `api/errors.go`), `inference_gateway_overhead_seconds{model}` (request start to dispatch: auth + rate limit + parse + select; the full-path overhead stays measured by the Phase 2 test) | `inference_queue_duration_seconds`, `admission_rejections_total` -> Phase 15 |
  | Scheduler | none | `scheduler_decisions_total{strategy,model,result}` (`selected|no_capacity|no_model|error`), `scheduler_decision_duration_seconds{strategy}`, `scheduler_no_capacity_total{model,reason}`, `scheduler_selections_total{model,worker_id}` (selection distribution, spec §30), `scheduler_ineligible_selections_total` (must stay 0; spec §31 "zero routing to known-unhealthy") | `scheduler_worker_score` -> Phase 14 |
  | Worker / registry | none | Control-plane collector from heartbeats: `worker_active_requests`, `worker_queue_depth`, `worker_queued_tokens`, `worker_queue_capacity`, `worker_tokens_per_second`, `worker_heartbeat_age_seconds`, `worker_health` (1 eligible else 0; state in label `state`), `registry_workers{model,state}`, `registry_registrations_total`, `registry_heartbeats_total{result}`. Mock-worker own counters/histograms: `worker_input_tokens_total`, `worker_output_tokens_total`, `worker_request_duration_seconds`, `worker_ttft_seconds`, `worker_queue_duration_seconds`, `worker_requests_total{result}` | Spec's `worker_tokens_generated_total` is the same quantity as `worker_output_tokens_total`; only the latter is exported (one name, divergence in ADR-017) |
  | Gateway view of the cluster | none | `gateway_registry_snapshot_age_seconds`, `gateway_registry_refresh_failures_total` (stale routing is spec §63 rule 10) | |
  | GPU | none | `gpu_utilization_percent{worker_id}`, `gpu_memory_used_bytes{worker_id}` exported only for workers that report them (mock workers may not; panels then show no data) | `gpu_memory_total_bytes` -> Phase 13 |
  | Rate limit | `rate_limit_rejections_total{limit}`, `_bypassed_total`, `_decision_seconds`, lease stats | none | Spec says `rate_limit_check_duration_seconds`; the shipped name `rate_limit_decision_seconds` is kept (renaming a released series is worse than a documented divergence). |
  | Auth | `auth_rejections_total{status}` | `cache_hits_total{cache}` / `cache_misses_total{cache}` with `cache="auth_key"` (spec lists them under Redis; the only cache is the key cache) | |
  | Postgres pool | none | `postgres_pool_connections{state}` (`acquired|idle|total|max`), `postgres_pool_acquires_total`, `postgres_pool_acquire_wait_seconds_total`, `postgres_pool_empty_acquires_total` (pool exhausted) | |
  | Redis client | none | `redis_up` (0 while backing off), `redis_commands_total`, `redis_errors_total{kind}` (`timeout|connection|script|other`), `redis_pool_connections{state}` | |
  | Kafka | none | none | `event_publish_*`, `consumer_lag` -> Phase 12 |
  | Process | Go/process collectors in the gateway | the same two collectors plus `serverflow_build_info{version,commit,go_version} 1` in every binary | |

  Tokens/s on dashboards comes from the worker counters (and the heartbeat gauge), not from parsing response bodies in the gateway.
- **D4. Naming and units.** snake_case; base units only (`_seconds`, `_bytes`, 0-1 ratios named `_ratio`); counters end `_total`; no unit in a gauge name that hides it. Spec-mandated exceptions: `gpu_utilization_percent`. Unprefixed names stay (they are the spec's and already shipped); the `job` label from the scrape config identifies the binary, so no `serverflow_` prefix except `serverflow_build_info`. Every divergence from the spec's names is listed in ADR-017.
- **D5. Cardinality rules, enforced rather than promised.**
  - Allowed label names (a closed list, `model, status, outcome, reason, limit, strategy, result, kind, state, cache, worker_id, direction, tenant`; `tenant` only under D5c). Never labels: request ID, attempt ID, API key or key ID, path, raw error text, client IP, user-supplied strings, trace ID.
  - (a) `model`: only validated models (configured, or registry-confirmed), else `"unknown"` (existing rule). A shared `labelGuard` caps distinct values at `metrics.max_models` (default 64); extras become `"other"`.
  - (b) `worker_id`: allowed because it is bounded by the fleet, not by requests. Collector-driven series emit only workers currently in the registry (so retired workers disappear, bounded by `control_plane.max_workers`, default 1000). Event counters (`scheduler_selections_total`) go through a `labelGuard` capped at `metrics.max_workers_label` (default 256 distinct), overflow `"other"`, so worker churn in an autoscaled fleet cannot grow a process's series without bound.
  - (c) `tenant`: **no tenant label by default**. Optional `metrics.tenant_labels: true` adds a single separate series `tenant_requests_total{tenant,outcome}` (never on the core series) with the first `metrics.max_tenants` (default 50) distinct tenant IDs kept and the rest `"other"`. First-come-first-kept is a known limitation, documented; per-tenant analysis belongs to logs and Phase 12 usage records.
  - (d) `status` is the HTTP status the gateway itself emits (a fixed small set; a test pins it). `reason`/`limit`/`outcome`/`kind` are Go constants from the enum definitions, never formatted from input.
  - Enforcement: the `labelGuard` type above; a **label allowlist test** that gathers every binary's registry and fails on any label name outside the list; and a **hostile-traffic test** (10,000 distinct bogus models, tenants, request IDs, paths and bad keys) asserting total series count stays under a stated ceiling (default 2,000 per gateway) and that overflow values appear as `other`/`unknown`. Prometheus side: `sample_limit` on scrape jobs (default 20,000) as a last line of defence.
- **D6. Series exist from the first scrape.** Instruments with bounded label sets (`outcome`, `limit`, `reason`, `result`, `status` classes we emit, `kind`) are pre-created at zero at start-up, so `rate()`, alerts and the dashboard-coverage test do not depend on traffic having reached a branch. `model`/`worker_id` series appear with traffic by nature.
- **D7. Where `/metrics` is served, and who can read it.** Default: each of the gateway and control plane serves `/metrics` on a **separate listener** configured by a new `metrics` section: `metrics.listen` (defaults: gateway `127.0.0.1:9100`, control plane `127.0.0.1:9101`; empty string disables), `metrics.token` (secret, redacted like `redis.password`), `metrics.allow_non_loopback` (default false). A non-loopback `listen` without a token and without the explicit flag is refused at start-up (the guard style of Phases 8-9). The mock worker, a test double that is never meant to be public, serves `/metrics` on its existing listener. The worker agent has no HTTP server and gets none; its health is visible through heartbeat age. **This removes `/metrics` from the gateway's public mux**, which changes a shipped surface and requires updating the tests that fetch `/metrics` from `Handler()` to use the new handler accessor (assertions about content unchanged; called out in the PR). Alternative the user may prefer: keep it on the main port when `metrics.listen` is unset (zero test churn, but keeps today's accidental exposure).
- **D8. Collectors live with what they observe, the binary wires them.** `internal/registry/metrics.go`, `internal/redis/metrics.go`, `internal/postgres/metrics.go`, the worker-engine collector in `internal/mockworker`, and an auth cache stats accessor. They depend only on `prometheus` and their own package; none imports the gateway. The gateway takes them through a new `gateway.WithCollector(c prometheus.Collector)` option (registered on its private registry); `cmd/control-plane` builds its own registry. No global `DefaultRegisterer` (spec §63 rule 7).
- **D9. Histogram buckets.** Keep existing buckets. Add: `scheduler_decision_duration_seconds` and `inference_gateway_overhead_seconds` with edges at 50 µs, 100 µs, 250 µs, 500 µs, 1, 2.5, 5, **10**, 25, 50, 100, 250 ms, 1 s (edges sit on the 10 ms and 25 ms SLO thresholds, so p95 versus target is not interpolated across a wide bucket). Worker duration/TTFT/queue histograms reuse the gateway's duration and TTFT bucket lists. Classic histograms only; no native histograms.
- **D10. Layout under `observability/`.**

  ```text
  observability/
    prometheus/prometheus.yml            global + rule_files + scrape jobs using file_sd (targets/local.yml)
    prometheus/targets/local.yml         host targets for the local stack (Phase 16 adds compose.yml, same job labels)
    prometheus/rules/recording.yml       level:metric:operation naming
    prometheus/rules/alerts.yml          SLO alerts (D12)
    prometheus/tests/*.test.yml          promtool unit tests
    grafana/provisioning/datasources/prometheus.yml   uid serverflow-prometheus, url from env
    grafana/provisioning/dashboards/serverflow.yml    file provider -> /var/lib/grafana/dashboards, read-only
    grafana/dashboards/{cluster-overview,worker,scheduler,model}.json
    docker-compose.yml                   Prometheus + Grafana only (D13)
  ```

  Dashboards are hand-maintained JSON committed with stable `uid`s, a `datasource` template variable fixed to the provisioned UID, `model` and `worker_id` multi-value variables, `schemaVersion` pinned, `editable: false`, and queries written against recording rules where a rule exists. No generator (no jsonnet/Grafonnet dependency); validation (D14) is the guard against rot. Panels follow spec §30: Cluster Overview (requests/s, tokens/s, p50/p95/p99 latency, p95 TTFT, error rate, healthy workers, GPU utilisation); Worker (queue, active, GPU, tokens/s, latency, TTFT, error rate); Scheduler (routed requests, queue imbalance = max minus min `worker_queue_depth` per model, decisions, selection distribution, no-capacity events); Model (requests/s, tokens/s, active replicas = eligible workers per model, queue depth, latency, TTFT). Panels whose source does not exist yet (GPU on mock workers) say so in their description rather than being omitted.
- **D11. Mock worker `/metrics`** adds the worker-side counters/histograms in D3 so tokens/s and per-worker latency are real in the demo. Default yes (small, same package that owns the numbers).
- **D12. Rules and SLOs (spec §31), thresholds are defaults and tunable.** Recording rules: request rate, error ratio (5xx / all; 429 and 499 are not errors), p50/p95/p99 duration, p95 TTFT, tokens/s, p95 routing decision, p95 gateway overhead, queue imbalance, eligible workers per model. Alerts (`for:` durations in the file): availability burn rate against 99.9% (two windows: 5m/1h and 30m/6h, burn 14.4 and 6); routing decision p95 above 10 ms; gateway overhead p95 above 25 ms; a worker whose heartbeat age exceeds 10 s while `worker_health` is still 1 (failed-worker detection); any increase of `scheduler_ineligible_selections_total`; any worker queue depth above its capacity ("no unbounded queues"); registry snapshot age above `registry_max_staleness`; Redis down; Postgres pool empty acquires rising. Every alert carries `summary`, `runbook` (link to a section in the new operations doc) and `severity`. No Alertmanager (Non-Goals). Each alert has a `promtool test rules` case that fires it and a case that does not.
- **D13. Local stack and hand-off to Phase 16.** `observability/docker-compose.yml` is a separate Compose project (`serverflow-observability`) with only Prometheus and Grafana, images pinned to the versions already in the root compose (`prom/prometheus:v2.53.0`, `grafana/grafana:11.1.0`), Prometheus published on host `127.0.0.1:9091` (not 9090: the control plane), Grafana on `127.0.0.1:3000`, anonymous access off, Grafana admin password from `GRAFANA_ADMIN_PASSWORD` in an untracked `.env` (compose refuses to start without it; no default password committed). It scrapes host processes through `host.docker.internal` via `targets/local.yml`. `make obs-up`, `make obs-down`, `make obs-logs`; `make dev-cluster` gains no new dependency. Hand-off: Phase 16 either `include:`s this file or copies the two services into the root compose, adding `targets/compose.yml` with service-name targets; `prometheus.yml`, rules and dashboards are unchanged. The root compose's unvalidated Prometheus/Grafana services are left alone and flagged in the PR as Phase 16's to reconcile (their host port 9090 conflicts with the control plane). Whether a loopback-bound host listener is reachable from the container through `host.docker.internal` on Docker Desktop for Mac is **unverified on this machine**; the documented fallback is `SERVERFLOW_METRICS_LISTEN=0.0.0.0:9100` plus a token for the demo.
- **D14. Validation without a running Grafana.**
  1. `promtool check config` and `promtool check rules` on the files; `promtool test rules` on `tests/`. Source of `promtool`: a pinned official release archive with a checksum in CI (a script `scripts/promtool.sh` uses a local binary, else `docker run prom/prometheus:<pin> promtool`, else fails; `scripts/quality.sh full` reports a loud skip locally if neither exists, fails in CI). New quality mode `observability`, a new CI job.
  2. Go test `internal/observability` (or `tests/observability`): parses every dashboard JSON (valid, required fields, unique uids and panel IDs, datasource uid matches provisioning, no inline datasource names, `editable: false`, templating variables resolve); extracts every PromQL expression and (i) wraps each, with Grafana variables substituted, in a temporary rule file and runs `promtool check rules` on it (syntax), (ii) extracts metric names with a documented tokenizer (strip strings, label matchers, `by/without/on/ignoring` clauses, ranges, variables; identifiers followed by `(` are functions) and requires each to be an **exported** metric family or a **recording rule** name.
  3. **Exported-by-a-running-server test:** an in-process gateway (registry mode), control plane and mock workers (`internal/bench/embedded` gives the cluster) are driven with success, stream, retry-on-failure, 401, 429, 404 and no-capacity traffic, then each registry is gathered and also scraped once over HTTP from the real metrics listener; the set of family names must cover every metric the dashboards, rules and alerts use. Redis and Postgres families are checked when the integration environment is set (same must-run pattern as Phases 8-9, added to `quality.sh integration`); without it they appear on an explicit exemption list with the reason, so a rename still fails in the integration job rather than silently.
  4. A **golden series test** for the gateway (name, type, help, labels) so a dropped label or renamed series is a reviewed diff; the prep plan's master golden file is the starting point.
  Grafana itself is not run in CI (needs Docker-in-CI and proves little beyond item 2); the manual live check below covers import errors.
- **D15. "Observe benchmark live" (acceptance demonstration).** `docs/operations/observability.md` documents: `make dev-cluster` (3 mock workers of different speeds, registry mode) in one terminal with metrics listeners on; `make obs-up`; then `go run ./cmd/benchmark run --target http://127.0.0.1:8080 --control-plane http://127.0.0.1:9090 --duration 120s --concurrency 12` (the harness's existing flags; the control-plane token via `SERVERFLOW_CONTROL_PLANE_TOKEN`); open the Cluster Overview and Scheduler dashboards. The evidence kept in `docs/benchmarks/phase-10-observability.md`: screenshots (or exported panel data) taken during the run showing request rate, p95, per-worker selection distribution skewed toward the faster workers under `least-active`, and a failure injected by stopping one mock worker so `worker_health` drops and the failed-worker alert fires within the 10 s target. The embedded `benchmark run` (no `--target`) boots its cluster in-process with no scrape endpoint and is **not** the live-demo path; no harness change in this phase.
- **D16. Overhead budget.** Instrumentation must not break the Phase 2 25 ms p95 gateway overhead nor the Phase 8 5 ms limiter budget. Phase 10's own budget: the full metrics observer adds **at most 20 µs p95 per request** and **0 extra allocations on the steady-state path** (labels resolved through preallocated child instruments or an allocation-free lookup, guard fast path read-only), `/metrics` collection takes under 50 ms with 1,000 registered workers, and `TestGatewayOverhead` plus the Phase 8 `overhead` benchmark move by less than their documented run-to-run spread. Measured with: `BenchmarkObserverRequest` (`-benchmem`, `-count=10`, compared with `benchstat` if installed, else mean/stdev recorded in the benchmark note), `BenchmarkCollectWorkers1000`, and before/after runs of the two overhead tests on the same machine, quiet, three repetitions each. Numbers go in `docs/benchmarks/phase-10-observability.md`; exceeding the budget is a finding to fix, not to document away.
- **D17. Dependencies and docs.** No new Go module. Tooling only: `promtool` (CI), Docker images (local stack). ADR-016 is taken by the prep seam, so this phase writes ADR-017 (metric names, cardinality rules, separate listener, what was deferred). Docs: `docs/operations/observability.md`, ARCHITECTURE (Shared Services and Gateway paragraphs), README phase table.

## Proposed Design

```go
// internal/config/metrics.go (reserved by the prep plan)
type MetricsConfig struct {
    Listen           string `yaml:"listen"`             // per-binary default; "" disables
    Token            string `yaml:"token"`              // secret; redacted in String/LogValue/JSON
    AllowNonLoopback bool   `yaml:"allow_non_loopback"`
    MaxModels, MaxWorkersLabel, MaxTenants int
    TenantLabels     bool   `yaml:"tenant_labels"`      // off
}

// internal/telemetry/metricsserver  (shared by cmd/gateway and cmd/control-plane)
func Serve(ctx context.Context, cfg config.MetricsConfig, g prometheus.Gatherer, log *slog.Logger) error
//   binds, applies the loopback/token guard, GET /metrics only, promhttp with a request-size cap and
//   a handler timeout, constant-time token compare, graceful shutdown with ctx.

// internal/gateway: the Observer implementation (the prep plan's `metrics` type) grows the D3 instruments;
// state is read through collectors passed with gateway.WithCollector(...).
// internal/telemetry: labelGuard (bounded distinct values, overflow "other"), shared by all binaries.
```

`labelGuard.Value(raw)` takes a read lock and a map lookup on the hot path (already-seen values); a write lock only for a new value up to the cap. Collectors implement `Describe`/`Collect` over a snapshot copied under the owner's existing lock and released before emitting. Control-plane gauges are emitted per live worker with `worker_id` and `model`; the sampler used by the benchmark harness is untouched.

## Affected Files / Components

- New: `observability/{prometheus/rules,prometheus/tests,prometheus/targets,grafana/...,docker-compose.yml}`, `internal/telemetry/{labelguard.go,metricsserver/}`, `internal/registry/metrics.go`, `internal/redis/metrics.go`, `internal/postgres/metrics.go`, `internal/config/metrics.go`, a dashboard-validation test package, `scripts/promtool.sh`, `docs/operations/observability.md`, `docs/benchmarks/phase-10-observability.md`, ADR-017.
- Changed: `internal/gateway/{metrics.go,observer files,server.go}` (instruments, `WithCollector`, `/metrics` leaves the public mux), `internal/mockworker` (metrics endpoint and collector), `internal/auth` (cache stats accessor), `cmd/gateway`, `cmd/control-plane`, `cmd/mock-worker`, `observability/prometheus/prometheus.yml`, `Makefile` (`obs-up`, `obs-down`, `obs-logs`, `obs-check`), `scripts/quality.sh` (mode `observability`), `.github/workflows/ci.yml`, `.gitignore` (`observability/.env`), tests that fetched `/metrics` from `Handler()`.
- Untouched: scheduler logic, retry, limiter, auth behaviour, benchmark harness code, `observability/otel/`, root `docker-compose.yml`.

## Acceptance Criteria

1. Every series marked Add in D3 is exported with the stated type, labels and help text, and the golden series test lists them; each deferral in D3 is recorded in ADR-017.
2. Each existing series keeps its name, labels and buckets (golden diff shows additions only), apart from the one documented move of the endpoint.
3. Cardinality: the label allowlist test passes for the gateway, control plane and mock worker; the hostile-traffic test keeps the gateway under the stated series ceiling and shows `other`/`unknown` overflow for models, tenants (when enabled) and worker IDs; with `tenant_labels` off no `tenant` label exists anywhere.
4. Endpoint security: by default `/metrics` is not served on the gateway data listener or the control plane API listener; the metrics listener binds loopback; a non-loopback `listen` without a token (or the explicit flag) fails start-up with a clear error; with a token, a request without it or with a wrong one gets 401 and the right one 200; the token never appears in logs, `String()`, JSON or error text; `metrics.listen: ""` serves nothing.
5. Bounded-series series exist before traffic (scrape of an idle gateway lists them at 0); `rate()`-based alerts are testable from a cold start.
6. Control-plane collector: with N registered workers the scrape shows N series per per-worker gauge with the heartbeat values; a deregistered or retired worker's series disappear on the next scrape; `worker_health` reflects state transitions (READY to SUSPECT to UNHEALTHY to LOST); a scrape never blocks heartbeats (test with concurrent heartbeats and scrapes under `-race`).
7. Scheduler and gateway events: tests with a recording registry prove exact counter/histogram effects for success, stream, retry on a second worker, no eligible worker, unknown model, auth refusal, rate-limit refusal and client disconnect; `scheduler_ineligible_selections_total` stays 0 across the whole suite and a deliberate ineligible selection (test-only) increments it.
8. `promtool check config`, `check rules` and `test rules` pass; every alert in `alerts.yml` has a firing and a non-firing unit test; every recording rule has at least one test sample.
9. Dashboard validation (D14.2) passes: four dashboards present with the spec §30 panels; every PromQL expression is syntactically valid; every metric or recording rule it names is exported or recorded; uids and provisioning paths consistent.
10. The exported-by-a-running-server test (D14.3) passes with the cluster traffic described; the Redis/Postgres families are verified in the integration job; the exemption list is explicit and short.
11. `docker compose -f observability/docker-compose.yml up` starts Prometheus and Grafana, Prometheus shows the gateway, control plane and mock-worker targets `up`, Grafana lists the four dashboards with no provisioning errors in its logs and a green datasource test (manual on the user's Mac; evidence recorded).
12. Live benchmark: during a 120 s `cmd/benchmark run --target` against the local cluster, the Cluster Overview request rate and p95 panels move, the Scheduler dashboard shows a selection distribution matching the benchmark report's per-worker distribution within 5 percentage points, and killing a mock worker fires the failed-worker alert in at most 10 s plus one rule evaluation interval (manual; evidence in `docs/benchmarks/phase-10-observability.md`).
13. Overhead: D16 numbers met and recorded; `TestGatewayOverhead` and the Phase 8 benchmark within their spread; collection with 1,000 workers under 50 ms.
14. No existing test is weakened; tests that moved from `Handler()` to the metrics handler keep their assertions; `scripts/quality.sh full` and the new `observability` mode pass; CI-parity holds (the observability job and the integration must-run list are in `ci.yml`).
15. Docs: ADR-017, `docs/operations/observability.md` (endpoints, security, cardinality rules, running the stack, adding a metric/panel, runbook anchors referenced by alerts), ARCHITECTURE and README updated.

## Verification Plan

- **Focused:** `go test -race -count=5 ./internal/gateway/... ./internal/registry/... ./internal/telemetry/... ./internal/mockworker/... ./internal/config/...`; the dashboard package; `go test -run 'Golden|Cardinality|Allowlist|Exported' -count=1` on the new tests.
- **Golden series:** committed per binary; regenerated only by an explicit `-update` flag and reviewed in the diff.
- **promtool:** `scripts/promtool.sh check config observability/prometheus/prometheus.yml`, `check rules`, `test rules observability/prometheus/tests/*.yml`; also run against the wrapped dashboard expressions. Cannot be run on the plan-writing machine (no `promtool`, no `docker` found); the implementer installs the pinned release or uses Docker before claiming this, and the CI job is the permanent proof.
- **Race and gate:** `go test -race ./...`, then `scripts/quality.sh full` with the Postgres and Redis test servers up, then `scripts/quality.sh observability`.
- **Overhead:** D16 procedure, recorded before and after on the same quiet machine.
- **Live check (manual, user's Mac):** acceptance 11 and 12; screenshots committed; anything not observed is stated in the PR instead of implied.
- **Independent verifier brief (read-only first):** re-derive the spec §29 list and compare with `/metrics` of a freshly started cluster, listing any gap not in ADR-017; try to blow up cardinality from outside (bogus models, long headers, many keys, 5,000 fake worker registrations via the control plane API) and report series counts; try to read `/metrics` through the data and API listeners; read the PromQL of each alert and check it can actually fire (units, label joins, `for:`); check the SLO thresholds against spec §31 wording.
- **Mutation ideas (each must make a named test fail):** drop the `model` label from `inference_requests_total`; rename `inference_ttft_seconds`; add a `request_id` label; remove the `worker_id` cap; serve `/metrics` on the data mux; skip the token check; swap `5xx` for `4xx` in the error-ratio rule; change an alert threshold or delete its `for:`; point a dashboard panel at a misspelt metric; remove a pre-created zero series; make the collector hold the registry lock while emitting (race/latency test); double-fire `RequestCompleted`; delete a dashboard variable; change the datasource uid in one file only.

## Risks

- **Cardinality blow-ups** from `worker_id` (autoscaling churn) or a future label added casually: bounded by D5's guard, allowlist test, hostile-traffic test and `sample_limit`; first-come tenant retention is lossy by design.
- **Dashboard and rule rot:** JSON and PromQL drift from code unnoticed. Mitigated by D14.2/3 (queries checked against what servers export) but panels can still be wrong in meaning; the independent verifier and the manual live check are the only semantic checks.
- **Verification that needs Docker or Grafana:** promtool, compose, provisioning and live demo cannot be proven by `go test` alone, and this plan was written on a machine without them on the PATH. CI covers promtool; compose/Grafana/live behaviour rests on a manual run and recorded evidence.
- **`host.docker.internal` and loopback-bound listeners** may not connect on Docker Desktop for Mac; fallback documented but changes the exposure for the demo.
- **Histogram bucket choice:** wrong edges silently skew p95/p99. Edges are placed on SLO thresholds; worker-side buckets reuse the gateway lists; revisit with real vLLM latencies in Phase 13 (changing buckets breaks aggregation across versions).
- **Changing a shipped surface:** removing `/metrics` from the data port may break an existing scraper or test; called out in D7, the PR and ADR-017.
- **Overhead creep:** many small instruments on the request path; guarded by the budget and benchmarks, and by keeping state-derived series in scrape-time collectors.
- **Dependence on the prep PR:** event fields (D2) may not match what is merged; checked at the start, fixed additively.
- **Alert quality without Alertmanager:** alerts only show in UIs; the SLO statements remain evidence-based, not paging guarantees.

## Implementation Steps

1. Confirm D1-D17 with the user; confirm the prep PR has merged and D2's fields exist (else add them).
2. `labelGuard`, `MetricsConfig` (with redaction and the listener guard), `metricsserver.Serve`; tests; move `/metrics` off the data mux and update the dependent tests.
3. Gateway observer instruments (failures, overhead, scheduler series) with pre-created series, golden series test, allowlist and hostile-traffic tests; overhead benchmarks started here.
4. Collectors: registry/control plane, gateway snapshot, Redis, Postgres, auth cache; `WithCollector`; wire `cmd/*`; mock worker `/metrics`.
5. `observability/` files: `prometheus.yml`, targets, recording and alert rules with promtool tests.
6. Dashboards and Grafana provisioning; dashboard validation package and the exported-by-a-running-server test; mutation pass over these.
7. Local stack compose, Makefile targets, `scripts/promtool.sh`, quality mode and CI job; run everything locally that can run; list what could not.
8. Manual live run on the user's Mac with evidence; overhead numbers; ADR-017, operations doc, ARCHITECTURE, README.
9. Independent verifier, fix round, narrower second verification, PR; stop for approval.
