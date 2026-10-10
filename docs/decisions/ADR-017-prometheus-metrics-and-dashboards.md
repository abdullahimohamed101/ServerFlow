# ADR-017: Prometheus Metrics, a Separate Metrics Listener, Bounded Cardinality, and Provisioned Dashboards

Status: Accepted (Phase 10)
Date: 2026-10-09

## Context

Before Phase 10 exactly one place exported metrics: the gateway's private registry, served at `GET /metrics` on the public data listener
(`:8080`, all interfaces) outside authentication, so with `auth.mode: required` it was still world-readable. The control plane and the mock
worker exported nothing, although the registry already held per-worker load, state and heartbeat age. There were no dashboards, no rules, and
nothing that told an operator whether the SLOs of spec section 31 were met. Phase 8 had deliberately left tenant labels out and left
cardinality policy to this phase.

## Decision

**1. Two kinds of instrument, kept apart.** *Event instruments* (counters, histograms, the in-flight gauge) are driven by `gateway.Observer`
events (ADR-016): request totals, durations, TTFT, attempts, retries, failures, rejections, scheduler decisions, selections, gateway
overhead. *Collectors* (`prometheus.Collector` implementations reading state at scrape time) cover everything that is not a request event:
the worker registry, the gateway's registry snapshot, Redis, the PostgreSQL pool, the API key cache and the mock worker's engine. Nothing on
the request path takes a lock a scrape can hold; a collector copies a snapshot under its owner's own lock, releases it, then emits. Collectors
live with what they observe (`internal/registry/metrics.go`, `internal/redis/metrics.go`, `internal/postgres/metrics.go`,
`internal/auth/metrics.go`, `internal/mockworker/metrics.go`), depend only on `prometheus` and their own package, and are attached to the
gateway with the `gateway.WithCollector` option. There is no global `DefaultRegisterer` (spec section 63, rule 7).

**2. `/metrics` moves to a listener of its own** (`internal/telemetry/metricsserver`). The gateway serves it on `metrics.listen` (default
`127.0.0.1:9100`) and the control plane on `127.0.0.1:9101`; `metrics.listen: ""` turns it off. A `listen` address that is not loopback is
refused at start-up unless `metrics.token` is set (a bearer token, at least 16 characters, compared in constant time, redacted from `String()`,
`GoString()`, `LogValue()`, JSON and every error) or `metrics.allow_non_loopback` is set explicitly. The handler serves `GET /metrics` only,
with a request limit of 4 in flight, a 10 s handler timeout and a header size cap. **This removes `/metrics` from the gateway's data
listener and from its public mux: a shipped surface changed.** The mock worker, a test double that is never public, keeps serving `/metrics`
on its existing listener. The worker agent has no HTTP server and gets none; its health is visible through the heartbeat age.

**3. Series.** Everything marked Add in the plan's gap analysis is exported; names are the spec's unless noted below.

| Group | New series |
| --- | --- |
| Gateway | `inference_failures_total{model,reason}` (reason is the API error code), `inference_gateway_overhead_seconds{model}` (request accepted to first dispatch), `gateway_registry_snapshot_age_seconds`, `gateway_registry_refresh_failures_total` |
| Scheduler | `scheduler_decisions_total{strategy,model,result}` (`selected`, `no_capacity`, `no_model`, `error`), `scheduler_decision_duration_seconds{strategy}`, `scheduler_no_capacity_total{model,reason}`, `scheduler_selections_total{model,worker_id}`, `scheduler_ineligible_selections_total` |
| Workers, from the control plane | `worker_active_requests`, `worker_queue_depth`, `worker_queued_tokens`, `worker_queue_capacity`, `worker_tokens_per_second`, `worker_heartbeat_age_seconds`, `worker_health{state}` (all with `worker_id`, `model`), `registry_workers{model,state}`, `registry_registrations_total`, `registry_heartbeats_total{result}`, `gpu_utilization_percent`, `gpu_memory_used_bytes` (only for workers that report them) |
| Mock worker | `worker_requests_total{result}`, `worker_input_tokens_total`, `worker_output_tokens_total`, `worker_request_duration_seconds`, `worker_ttft_seconds`, `worker_queue_duration_seconds` |
| Dependencies | `cache_hits_total{cache}`, `cache_misses_total{cache}` (`cache="auth_key"`), `redis_up`, `redis_commands_total`, `redis_errors_total{kind}`, `redis_pool_connections{state}`, `postgres_pool_connections{state}`, `postgres_pool_acquires_total`, `postgres_pool_acquire_wait_seconds_total`, `postgres_pool_empty_acquires_total` |
| Every binary | `serverflow_build_info{version,commit,go_version}`, the Go and process collectors |
| Optional | `tenant_requests_total{tenant,outcome}` with `metrics.tenant_labels: true` |

Every existing series keeps its name, labels and buckets (the golden series test shows additions only).

**Divergences from the spec's names** (a divergence is better documented than a rename of a shipped series):

- `rate_limit_decision_seconds` stays; the spec's `rate_limit_check_duration_seconds` is not added.
- `worker_tokens_generated_total` is the same quantity as `worker_output_tokens_total`; only the latter is exported.
- `cache_hits_total` and `cache_misses_total` are listed under Redis in the spec; the only cache is the API key cache, labelled `cache="auth_key"`.
- Unprefixed names stay; the `job` label from the scrape configuration identifies the binary. Only `serverflow_build_info` carries a prefix.
- The 99.9% availability SLO counts 5xx only; 429 (rate limited) and 499 (client left) are not errors.

**Deferred, with the reason:** `inference_queue_duration_seconds` and `admission_rejections_total` wait for Phase 15 (backpressure); there is
no queue or admission machinery to measure. `scheduler_worker_score` waits for Phase 14 (no scores exist). `gpu_memory_total_bytes` waits for
Phase 13 (the protocol carries no total). `event_publish_*` and `consumer_lag` belong to Phase 12. Tokens per second on dashboards comes from
the workers' counters and the heartbeat gauge, not from parsing response bodies in the gateway (hot path).

**4. Cardinality is enforced, not promised.**

- The label allowlist is closed: `model, status, outcome, reason, limit, strategy, result, kind, state, cache, worker_id, direction, tenant`
  (plus `le`, `quantile` and the build-info labels). Never labels: request ID, attempt ID, API key or key ID, path, raw error text, client IP,
  user-supplied strings, trace ID. A test gathers every component's registry and fails on any other label name.
- `model` is only ever a confirmed model (configured or registry-confirmed), else `unknown`; a `telemetry.LabelGuard` caps distinct values at
  `metrics.max_models` (64), later ones are `other`. `worker_id` from events is capped at `metrics.max_workers_label` (256) the same way;
  collector-driven per-worker series list only workers currently registered, bounded by `control_plane.max_workers`.
- `tenant` does not exist by default. With `metrics.tenant_labels: true` one extra series `tenant_requests_total{tenant,outcome}` keeps the first
  `metrics.max_tenants` (50) tenants and folds the rest into `other`. First-come-first-kept is a documented limitation: per-tenant analysis
  belongs to logs and the Phase 12 usage records.
- A hostile-traffic test (thousands of invented models, keys, paths and request IDs) bounds the gateway to 2,000 series; Prometheus scrape jobs
  set `sample_limit: 20000` as a last line of defence.
- Series with bounded label sets (`auth_rejections_total{status}`, `rate_limit_rejections_total{limit}`, the strategy's decision histogram, the
  registry heartbeat results, `redis_errors_total{kind}`) exist from the first scrape, so `rate()` and alerts work from a cold start. Series
  labelled by `model` or `worker_id` appear with traffic by nature.

**5. Hot-path cost is budgeted and measured.** Per-model instruments are resolved once and found by a read-locked map lookup; attempt outcomes,
HTTP statuses, decisions and per-worker counters are cached counters, so the steady-state path allocates nothing. Budget: at most 20 microseconds
per request and no extra allocation, measured by `BenchmarkMetricsObserverEvents` and `BenchmarkObserverRequest`
(`docs/benchmarks/phase-10-observability.md`). Histogram buckets for the scheduler decision and the gateway overhead put edges on the 10 ms and
25 ms SLO thresholds so the p95 is not interpolated across a wide bucket.

**6. Rules, dashboards and their validation.** `observability/` holds the Prometheus configuration (file service discovery, one scrape job per
binary), recording rules (`level:metric:operation`), SLO alerts with `promtool test rules` cases that fire them and cases that do not, a
provisioned Grafana datasource with a fixed uid, and the four dashboards of spec section 30 as hand-maintained JSON with stable uids. There is no
generator. CI (`scripts/quality.sh observability`) runs `promtool check config`, `check rules` and `test rules`, and `internal/observability`
checks that each dashboard has the spec's panels and valid structure; that every PromQL expression, variables substituted, is valid (promtool);
and that every metric a dashboard, recording rule or alert names is a family a running cluster exports over HTTP (a control plane, mock workers
and a gateway driven with success, stream, retry, 401, 429, 404, no-capacity and disconnect traffic) or a recording rule. The Redis and
PostgreSQL families are checked against the real servers in the integration job. promtool comes from the pinned official release archive with its
published SHA-256 in CI, and from `quay.io/prometheus/prometheus:v2.53.0` (the same image digest as the Docker Hub `prom/prometheus:v2.53.0`) or a
local binary elsewhere; Docker Hub is not used in CI because it rate-limits shared runners.

**7. The failed-worker alert reads the registry, not a state the plan could not produce.** The plan asked for "heartbeat age above 10 s while
`worker_health` is still 1". The control plane flips `worker_health` to 0 as soon as the worker is no longer READY and recently heard from (after
the suspect timeout), so that condition cannot occur. Two alerts replace it: `ServerFlowWorkerNotServing` (`worker_health{state=~"FAILED|UNHEALTHY|LOST"} == 0`,
an agent that reports its backend failed) and `ServerFlowWorkerHeartbeatStale` (`worker_heartbeat_age_seconds > 10`, an agent that went silent).
The control plane is scraped every 5 s and the fleet rule group is evaluated every 5 s so detection fits the 10 s target of spec section 31.

## Consequences

- An operator can watch a benchmark live, and the SLOs of spec section 31 are rules with tests. Alerts are visible in Prometheus and Grafana;
  there is no Alertmanager, so nothing pages.
- **Shipped behaviour changed:** `/metrics` is no longer on the gateway's data port. Anything that scraped `:8080/metrics` must scrape the new
  listener (default `127.0.0.1:9100`; Prometheus on another host needs a token and `metrics.listen` on a reachable address). Tests that read
  metrics through `Handler()` read them through `MetricsHandler()` instead.
- `inference_requests_active` was already changed in ADR-016 (it now includes authentication time); dashboards use it as that.
- The control plane and gateway each bind one more port; the process tests give every launched binary an ephemeral metrics port.
- Series count grows with models and workers by design, up to the caps above. Changing a histogram's buckets later breaks aggregation across
  versions; revisit the latency buckets with real vLLM numbers in Phase 13.
- Phase 16 either includes `observability/docker-compose.yml` or copies its two services, adding `targets/compose.yml` with service names; the
  Prometheus configuration, rules and dashboards stay as they are. The root `docker-compose.yml` still carries unvalidated Prometheus and Grafana
  services whose host port 9090 collides with the control plane; they are Phase 16's to reconcile.
- Known limitation: worker health series are as fresh as the last heartbeat; the registry snapshot age series makes a stale gateway view visible.
- Known limitation: a failed second routing attempt during a retry is not a rejection event, so it is not counted in `scheduler_decisions_total`
  or `scheduler_no_capacity_total`; the first attempt's response is what the client gets.
