# Resilience (fault tolerance)

Phase 15 teaches the gateway to survive a failing worker: a per-worker circuit breaker takes it out of routing and probes it back in, a retry
budget stops retries from multiplying an outage, a bounded queue and a global cap shed overload honestly, and the gateway can drain. The design
and its reasons are in ADR-020 and `docs/plans/active/phase-15-fault-tolerance.md`. This guide grows with each of the seven pull requests; it
says at every point what is wired.

## Status (read this first)

**Nothing in this guide changes gateway behaviour yet.** Pull request C added the pure library `internal/resilience` (breakers, retry budget,
admission queue, global cap) and the configuration section below. No gateway code calls the library, so the settings are accepted, validated
and ignored. `resilience.mode` **defaults to `off`** in code until the last pull request (G) flips the shipped default to `on`; the final design
is "on in registry mode, `off` is the kill switch". `/metrics`, requests and logs are byte-identical to before.

| PR | Content | State |
| --- | --- | --- |
| A | Panic-safe attempts, unknown-model pre-check, second-routing metric, observer seam stubs | merged |
| B | Mock-worker runtime faults, chaos harness | merged |
| **C** | `internal/resilience`, `resilience.*` configuration | **this guide** |
| D | Breaker, budget, deadline and failure taxonomy wired into the gateway | not yet |
| E | Queue, load shedding, global cap | not yet |
| F | Gateway and worker drain | not yet |
| G | Default flip, dashboard, alerts, benchmark note | not yet |

## Concepts

- **Effective.** The feature takes effect only when `resilience.mode` is `on` **and** `gateway.worker_source` is `registry`. With static
  workers there is nothing to eject, queue for or drain, so `mode: on` is a valid no-op (it starts and serves; it is never an error).
- **Breaker.** One per worker registration (worker ID plus the time it registered; a worker that registers again starts closed), local to each
  gateway. It trips on `consecutive_failures` faults in a row, the first no older than `window`, or on a fault ratio of `failure_ratio` or more
  over at least `min_volume` outcomes in the sliding `window`. An unlisted 5xx (for example 500) feeds the ratio rule only; load or lifecycle
  answers (`queue_full`, `draining`, `not_ready`), a client that left, a 4xx and a panic count as neither. The verdict is taken when the
  retry decision is made (first byte), so a long stream does not hold a probe; a stream that aborts later is recorded as a late fault: it counts on a closed breaker, and a probe stream that aborts reopens the breaker.
- **States.** Closed, open, half-open, evaluated lazily when asked (no timers, no goroutines). Open lasts `cool_down` with +-20% jitter, then
  half-open admits `probe_concurrency` real requests at a time; `probe_successes` good ones close it; a failed probe reopens it with the
  cool-down doubled up to `max_cool_down` (10 s, 20 s, 40 s, 80 s, 2 min). The doubling level survives a pass-then-retrip and resets only after
  the breaker has been closed for `max_cool_down`. A probe ticket not returned within `max(2 x cool_down, upstream_header_timeout + 5 s)` is
  considered lost and released.
- **Ejection cap.** At most `floor(max_open_fraction x eligible)` breakers may be open or half-open, where `eligible` counts only workers the
  registry would route to right now (healthy, not draining, fresh heartbeat). With 0.5: 1 worker -> 0 (a single-worker model never opens),
  2 -> 1, 3 -> 1, 4 -> 2. Workers that are unhealthy, lost or draining are outside both counts, so a lost worker's open breaker never blocks
  ejecting a real failure. When the view changes (a worker becomes eligible again, or others become ineligible) and more breakers are open than the cap
  allows, the most recently ejected ones are closed again. A trip the cap holds back keeps its window and streak and happens on the worker's next outcome once there is room.
- **Retry budget.** Per model, a retry is allowed while the retries in `budget_window` are fewer than `max(budget_min, floor(budget_ratio x
  first attempts))`. First attempts are routed attempts number 1. The retry is reserved atomically before its routing and refunded if no worker
  is found. Models beyond `metrics.max_models` share one `other` budget. At low traffic the minimum dominates (up to 100% retries below about 50
  requests per window); this is intended.
- **Queue.** Per model, strictly FIFO: a request that finds waiters must enqueue behind them. A freed slot goes directly to the head waiter
  (hand-off), never to whoever routes first, so stale worker load numbers cannot lose it. Depth is `queue.max_model_depth`; one authenticated
  tenant may hold `queue.max_tenant_share` of it; a model beyond `metrics.max_models` gets no queue (the library's `MaxModels` caps the models that have waiters at once; the gateway maps untracked models to "no queue" in PR E; the budget tracks models first come, first served and maps the rest to `other`). `Retry-After` is the EWMA of recent waits,
  rounded up and clamped to 1-10 s.
- **Global cap.** `admission.max_global_requests` (an existing key, default 1000) counts requests inside the chat handler from just after
  authentication until completion, queued plus running, per gateway.

## Configuration

```yaml
resilience:
  mode: off                  # on | off. Default off until the last Phase 15 PR; the final default is on. Registry mode only.
  breaker:
    consecutive_failures: 5  # 1-1000
    failure_ratio: 0.5       # 0-1; 0 switches the ratio rule off
    min_volume: 10           # 1-100000 outcomes in the window before the ratio rule may trip
    window: 10s              # 1s-10m, sliding, one-second buckets
    cool_down: 10s           # 100ms-1h (the floor is for tests); first open period, jittered +-20%
    max_cool_down: 2m        # cool_down-1h
    probe_concurrency: 1     # 1-16 probes at a time while half-open
    probe_successes: 1       # 1-16 good probes to close
    probe_header_timeout: 0  # 0 = default, min(10s, gateway.upstream_header_timeout); else 100ms-upstream_header_timeout
    max_open_fraction: 0.5   # 0-1; 0 never ejects
  retry:
    budget_ratio: 0.2        # 0-1
    budget_min: 10           # 0-1000 retries always allowed per window
    budget_window: 10s       # 1s-10m
    first_byte_deadline: 0   # 0 = upstream_header_timeout + upstream_idle_timeout; else >= upstream_header_timeout
    min_remaining: 1s        # 0-1m; no retry starts with less than this left
  queue:
    max_model_depth: 128     # 0-100000; 0 = no queue
    max_wait: 3s             # 100ms-1m, and <= the effective first-byte deadline
    max_tenant_share: 0.5    # 0-1; 0 = off; authenticated tenants only
  drain:
    readiness_grace: 5s      # unset = default, min(5s, gateway.shutdown_timeout / 2); explicit 0-5m (0 = none), shorter than shutdown_timeout
```

Environment: `SERVERFLOW_RESILIENCE_` plus the key path in capitals with underscores: `MODE`, `BREAKER_CONSECUTIVE_FAILURES`,
`BREAKER_FAILURE_RATIO`, `BREAKER_MIN_VOLUME`, `BREAKER_WINDOW`, `BREAKER_COOL_DOWN`, `BREAKER_MAX_COOL_DOWN`, `BREAKER_PROBE_CONCURRENCY`,
`BREAKER_PROBE_SUCCESSES`, `BREAKER_PROBE_HEADER_TIMEOUT`, `BREAKER_MAX_OPEN_FRACTION`, `RETRY_BUDGET_RATIO`, `RETRY_BUDGET_MIN`,
`RETRY_BUDGET_WINDOW`, `RETRY_FIRST_BYTE_DEADLINE`, `RETRY_MIN_REMAINING`, `QUEUE_MAX_MODEL_DEPTH`, `QUEUE_MAX_WAIT`,
`QUEUE_MAX_TENANT_SHARE`, `DRAIN_READINESS_GRACE`.

Validation:

- `mode` is always checked. The **ranges and cross-checks** of everything else are checked only when the feature is effective (`on` and registry), so an
  out-of-range value does not stop a gateway that does not use it. A value of the wrong **type** (`cool_down: banana`, `consecutive_failures: abc`) is a
  YAML parse error and stops the gateway whether or not the feature is effective.
- Two defaults follow the gateway: an unset `probe_header_timeout` is `min(10s, upstream_header_timeout)` and an unset `readiness_grace` is
  `min(5s, shutdown_timeout / 2)`, so configurations that are valid today stay valid. Only explicit values are cross-validated.
- Cross-checks: `queue.max_wait` must not exceed the effective first-byte deadline; an explicit `first_byte_deadline` below
  `gateway.upstream_header_timeout` is refused (it would cut requests the gateway serves today); `probe_header_timeout` must not exceed the
  gateway's header timeout; `drain.readiness_grace` must be shorter than `gateway.shutdown_timeout`; `max_cool_down` is at least `cool_down`.
- Environment values that do not parse (a number, a duration) are ignored and leave the default, like every other setting. The one exception is
  `SERVERFLOW_RESILIENCE_MODE`: a value other than `on` or `off` is kept and fails validation, so a misspelled kill switch cannot silently leave
  the feature in its default state.

## What is not wired yet

Everything above. In particular there are no breaker, budget or queue metrics yet (`circuit_breaker_*`, `retries_denied_total`,
`admission_rejections_total`, `inference_queue_duration_seconds` arrive with PR D and E), no `Retry-After` shedding, no drain, and the
`overload` rejection kind and the other observer additions are stubs that are never fired. Runbooks, the dashboard, the alerts and the list of
behaviour changes that the default brings (decision D25) land with PR G.
