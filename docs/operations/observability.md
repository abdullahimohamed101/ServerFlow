# Observability: Prometheus and Grafana

What the gateway, control plane and mock worker export, who may read it, how to run Prometheus and Grafana beside a local cluster, how
to add a metric or a panel, and what to do when an alert fires. The reasoning is in ADR-017.

## Endpoints and who can read them

| Component | Where `/metrics` is | Default |
| --- | --- | --- |
| Gateway | its own listener, `metrics.listen` | `127.0.0.1:9100` |
| Control plane | its own listener, `metrics.listen` | `127.0.0.1:9101` |
| Mock worker | its existing listener (a test double, never public) | the worker's `--addr` |
| Worker agent | none: it has no HTTP server; its health shows as heartbeat age on the control plane | |

**`/metrics` is not served on the gateway's data port or the control plane's API port.** Before Phase 10 the gateway served it on `:8080`,
outside authentication. Scrapers must use the metrics listener now.

```yaml
metrics:
  listen: "127.0.0.1:9100"   # unset: the binary's default; "" turns the endpoint off
  token: ""                  # bearer token Prometheus presents; required unless allow_non_loopback
  allow_non_loopback: false  # serve a non-loopback address without a token (do not, outside a private network)
  max_models: 64             # distinct model label values before "other"
  max_workers_label: 256     # distinct worker_id label values on event series before "other"
  tenant_labels: false       # tenant_requests_total{tenant,outcome}; off by default
  max_tenants: 50
```

Environment: `SERVERFLOW_METRICS_LISTEN`, `SERVERFLOW_METRICS_TOKEN`, `SERVERFLOW_METRICS_ALLOW_NON_LOOPBACK`, `SERVERFLOW_METRICS_MAX_MODELS`,
`SERVERFLOW_METRICS_MAX_WORKERS_LABEL`, `SERVERFLOW_METRICS_MAX_TENANTS`, `SERVERFLOW_METRICS_TENANT_LABELS`.

- A non-loopback `listen` (`:9100`, `0.0.0.0:9100`, a LAN address or a hostname) without `token` and without `allow_non_loopback` stops the
  binary at start-up with a clear error.
- With a token, a request without `Authorization: Bearer <token>` or with a wrong one gets `401`. The token is at least 16 characters, is compared
  in constant time, and never appears in logs, `String()`, JSON or error text. Give Prometheus the token through `authorization.credentials_file`
  in `observability/prometheus/prometheus.yml`.
- Only `GET /metrics` is served, at most 4 scrapes at once, each bounded to 10 s.

## Running the stack beside a local cluster

```sh
make dev-cluster                                   # control plane :9090, workers :9001-9003, gateway :8080 (registry mode)
cp observability/.env.example observability/.env   # then set GRAFANA_ADMIN_PASSWORD in the new file
make obs-up                                        # Prometheus http://127.0.0.1:9091, Grafana http://127.0.0.1:3000 (user admin)
go run ./cmd/benchmark run --target http://127.0.0.1:8080 --control-plane http://127.0.0.1:9090 --duration 120s --concurrency 12
make obs-logs                                      # follow both containers
make obs-down
```

- `observability/.env` is untracked and holds the Grafana admin password; compose refuses to start without it and no default password is
  committed. It can also change the host ports (`OBS_PROMETHEUS_PORT`, `OBS_GRAFANA_PORT`), the retention and the targets file
  (`OBS_TARGETS_FILE`). Both ports bind `127.0.0.1`.
- Prometheus listens on `9091`, not `9090`: the control plane owns `9090`. The root `docker-compose.yml` still has Prometheus and Grafana services
  on `9090` and `3000`; they are unvalidated and Phase 16's to reconcile. Do not run both stacks at once.
- The containers reach the host's processes through `host.docker.internal` (`observability/prometheus/targets/local.yml`). A loopback-bound
  metrics listener is reachable that way on Colima (verified for Phase 10). If it is not on your Docker, either bind the listener to a reachable
  address with a token (`SERVERFLOW_METRICS_LISTEN=0.0.0.0:9100 SERVERFLOW_METRICS_TOKEN=...` and `credentials_file` in the scrape job) or
  edit the targets.
- The metrics listener defaults are `9100` (gateway) and `9101` (control plane); the targets file lists them and the mock workers on `9001-9003`.
  Where your processes listen elsewhere, copy the targets file, edit the ports, and point `OBS_TARGETS_FILE` at it
  (`docs/benchmarks/phase-10-assets/targets.demo.yml` is an example).
- Containers cannot read bind mounts from `/tmp` on Colima; keep the repository under the home directory.
- Open **ServerFlow Cluster Overview** and **ServerFlow Scheduler** in Grafana while the benchmark runs. The embedded `benchmark run` (no
  `--target`) boots its cluster inside the benchmark process with no scrape endpoint; it is not the live-demo path.

## The dashboards

Provisioned read-only from `observability/grafana/dashboards` (uids `sf-cluster-overview`, `sf-worker`, `sf-scheduler`, `sf-model`), against the
datasource uid `serverflow-prometheus`. They cover spec section 30.

| Dashboard | Panels |
| --- | --- |
| Cluster Overview | requests/s, tokens/s, p50/p95/p99 latency, p95 TTFT, 5xx rate, healthy workers, in-flight, GPU utilisation, firing alerts |
| Worker | queue depth and capacity, active requests, GPU utilisation, tokens/s (counter and heartbeat), p95 latency, p95 TTFT, error rate, health and heartbeat age |
| Scheduler | routed requests/s, selection distribution, queue imbalance, decisions/s by result, routing decision p95, no-capacity events, retries, ineligible selections, gateway overhead p95 |
| Model | requests/s, tokens/s, active replicas, queue depth, p50/p95/p99 latency, p95 TTFT, error rate |

Mock workers report no GPU numbers, so the GPU panels show no data until a worker with a GPU does (Phase 13); the panel descriptions say so.
Per-worker latency, TTFT and error panels read the worker's own histograms, which Prometheus labels `worker_id` from the targets file.

## Series and cardinality rules

The full list and the divergences from the spec's names are in ADR-017. The rules every new series must follow:

- snake_case, base units (`_seconds`, `_bytes`, 0-1 ratios named `_ratio`), counters end `_total`.
- Label names come from a closed list: `model, status, outcome, reason, limit, strategy, result, kind, state, cache, worker_id, direction, tenant`.
  Never a request ID, attempt ID, API key or key ID, path, raw error text, client IP, user-supplied string or trace ID. A test fails on any other label.
- `model` is only a confirmed model (else `unknown`, and `other` beyond `max_models`). `worker_id` is bounded by the fleet (collectors) or
  `max_workers_label` (event series). `tenant` exists only with `tenant_labels`, capped at `max_tenants`, later tenants fold into `other`.
- `reason`, `limit`, `outcome`, `kind` and `result` are Go constants, never formatted from input.
- Series with a bounded label set are created at start-up so `rate()` and alerts work from the first scrape.

## Adding a metric or a panel

1. **A request event** (something that happens to a request): add the instrument to `internal/gateway/metrics.go` and feed it from the
   `Observer` method that sees the event; resolve the labelled child once (see `modelInst`) so the steady-state path allocates nothing; add fields to
   the event structs only when needed (`observer_events.go`). Run `go test ./internal/gateway -run MetricsSeriesGolden -update-golden`, review the
   diff (additions only unless the change is deliberate) and say so in the commit.
2. **State read at scrape time** (a pool, a cache, the registry): write a `prometheus.Collector` in the package that owns the state; attach it with
   `gateway.WithCollector` (gateway) or add it to the registry in `registry.NewMetricsRegistry` (control plane).
3. Add the name to the test expectations in `internal/observability/exported_test.go` if it is new to a component.
4. **A panel**: edit the JSON under `observability/grafana/dashboards` (keep the `${datasource}` datasource, unique panel ids, a description). Prefer a
   recording rule when the expression is reused; add it to `rules/recording.yml` with a sample in `tests/recording.test.yml`.
5. Run `scripts/quality.sh observability` (promtool and the dashboard checks need `promtool` or Docker). It fails if a dashboard or rule names a
   metric no component exports, or if an alert lacks a firing and a non-firing test.

## Alerts and runbooks

Alerts are rules in Prometheus and appear in Grafana (the "Alerts firing" panel); there is no Alertmanager, so nothing pages. Thresholds are defaults
in `observability/prometheus/rules/alerts.yml`.

<a id="runbook-availability-burn"></a>
### Availability burn (`ServerFlowAvailabilityBurnFast`, `ServerFlowAvailabilityBurnSlow`)

The 5xx share of inference requests is using up the 99.9% error budget too fast: 14.4 times (5 minute and 1 hour windows, critical) or 6 times
(30 minute and 6 hour windows, warning). Check the Cluster Overview error panel, then `inference_failures_total` by `reason`: `NO_CAPACITY` means the
workers are full or ineligible (see the Scheduler and Worker dashboards), `WORKER_UNAVAILABLE` means the gateway's registry view is stale, and
`INFERENCE_FAILED` or `UPSTREAM_TIMEOUT` points at the workers.

<a id="runbook-routing-decision-slow"></a>
### Routing decision slow (`ServerFlowRoutingDecisionSlow`)

The scheduler's p95 decision time is above 10 ms. Look at the strategy and the number of eligible workers (`registry_workers`), and at CPU on the
gateway.

<a id="runbook-gateway-overhead-high"></a>
### Gateway overhead high (`ServerFlowGatewayOverheadHigh`)

From request accepted to first dispatch the p95 is above 25 ms (authentication, rate limit, parsing, worker selection). Check
`rate_limit_decision_seconds`, `redis_up` and the PostgreSQL pool (`postgres_pool_empty_acquires_total`), and the gateway's CPU.

<a id="runbook-worker-down"></a>
### Worker not serving or silent (`ServerFlowWorkerNotServing`, `ServerFlowWorkerHeartbeatStale`)

`NotServing`: the worker's agent reports it FAILED, UNHEALTHY or LOST (its backend is down); the gateway already routes around it. `HeartbeatStale`: no
heartbeat for more than 10 s (the agent or the host is gone). Look at the agent's and the backend's logs for that `worker_id`; the registry removes a
lost worker after `worker.retention`.

<a id="runbook-ineligible-selection"></a>
### Ineligible worker selected (`ServerFlowIneligibleWorkerSelected`)

Must never fire: a request was routed to a worker the registry view did not judge eligible. This is a scheduler or snapshot bug. Keep the request IDs
from the gateway log (`worker selected`, debug level) and report it.

<a id="runbook-queue-over-capacity"></a>
### Worker queue over capacity (`ServerFlowWorkerQueueOverCapacity`)

A worker reports a queue deeper than the size it registered. Queues must be bounded; this is a worker bug or a misreported size.

<a id="runbook-registry-snapshot-stale"></a>
### Registry snapshot stale (`ServerFlowRegistrySnapshotStale`)

The gateway has not refreshed its view of the workers for more than `gateway.registry_max_staleness` (10 s by default), so it fails closed (503
`WORKER_UNAVAILABLE`). Check the control plane (`up`, its logs), the network between them and `control_plane.token`.

<a id="runbook-target-down"></a>
### Target down (`ServerFlowTargetDown`)

Prometheus cannot scrape a ServerFlow process. Check that it runs, that `metrics.listen` is where the targets file says, and the token if one is set.

<a id="runbook-redis-down"></a>
### Redis down (`ServerFlowRedisDown`)

The gateway is leaving Redis alone after a failure. Rate limits follow `redis.on_failure` (`closed`: affected requests get 503; `open`: admitted
unchecked, counted in `rate_limit_bypassed_total`). See `docs/operations/redis-and-rate-limits.md`.

<a id="runbook-postgres-pool-exhausted"></a>
### PostgreSQL pool exhausted (`ServerFlowPostgresPoolExhausted`)

Requests keep finding the pool empty. Raise `postgres.max_conns`, or find the slow queries; key lookups are capped below the pool size on purpose.
See `docs/operations/postgres-and-auth.md`.
