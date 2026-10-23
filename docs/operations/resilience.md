# Resilience (fault tolerance)

Phase 15 teaches the gateway to survive a failing worker: a per-worker circuit breaker takes it out of routing and probes it back in, a retry
budget stops retries from multiplying an outage, a bounded queue and a global cap shed overload honestly, and the gateway can drain. The design
and its reasons are in ADR-020 and `docs/plans/completed/phase-15-fault-tolerance.md`; the measurements are in `docs/benchmarks/phase-15-chaos.md`.

## Status (read this first)

**Everything is wired, and since the last pull request (G) `resilience.mode` defaults to `on`.** The feature takes effect only with `gateway.worker_source: registry`; with static workers
`mode: on` is a valid no-op. **This is a behaviour change for every existing registry-mode deployment that sets nothing**, listed below. `resilience.mode: off` is the kill switch: with it (and with static workers) requests, headers, bodies, logs and `/metrics`
are identical to the gateway after PR A's three unconditional fixes (panic-safe attempts, unknown-model pre-check, second-routing metric; master 16dda6b already contains them). A committed differential against master checks 39 request shapes.

| PR | Content | State |
| --- | --- | --- |
| A | Panic-safe attempts, unknown-model pre-check, second-routing metric, observer seam stubs | merged |
| B | Mock-worker runtime faults, chaos harness | merged |
| C | `internal/resilience`, `resilience.*` configuration | merged |
| D | Breaker, budget, deadline and failure taxonomy wired into the gateway | done |
| E | Queue, hand-off, load shedding, global cap | done |
| F | Gateway and worker drain | done |
| **G** | Default flip, dashboard, alerts, benchmark note | **this guide** |

### Behaviour changes with the default on (decision D25)

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

**Additional visible details** (not in the list above; found by the differential): attempt log lines gain `verdict` (`ok`, `fault`, `neutral`) and, when they apply, `probe` and `retry_denied`; the refusal of a full model is the usual
`NO_CAPACITY` body with another message and no `eligible_workers` field, and it is not counted as a scheduling decision (`scheduler_decisions_total{result="no_capacity"}` no longer rises for it, `admission_rejections_total` does); the registry refresh, key-usage writes and
lease renewal keep running through the readiness grace and the drain; the ejection cap is per model.

Also visible with the default on: responses carry `Connection: close` while the gateway is draining (item 8); traces gain the span events `queue.wait` and `retry.denied` and the attributes `serverflow.queue.wait_ms`, `serverflow.queue.result`, `serverflow.retry.denied` and `serverflow.breaker.probe`; Kafka events can carry attempt `failure_class` values `panic` and `deadline` and a shed is a `failed` event with `failure_class=no_capacity`. **The differential does not cover traces or events.**

**How far the D25 evidence goes.** `TestEveryD25ItemNamesATestThatExists` only checks that the tests named for each item still exist; it does not prove they test the item. Items 5, 6, 9 and 10 have no evidence tests of their own: 5 and 6 are exclusion items (the deadline default and the overhead histogram are covered by `TestTheDerivedDeadlineNeverCutsASlowButServedAttempt` and `TestTheOverheadHistogramExcludesTheQueueWait`, not named in the table) and 9 and 10 are documentation items. `ServerFlowCircuitOpen` can fire for a worker that is already ejected for another reason (an ejected worker whose breaker takes no room under the cap); this is harmless.

**Kill switch.** `resilience.mode: off` in the configuration file, `SERVERFLOW_RESILIENCE_MODE=off` in the environment, or `SF_RESILIENCE_MODE=off` in the compose `.env` (then `make dev-up` again); restart the gateway. A misspelled value (`of`) is refused at start-up rather than ignored,
so a mistyped kill switch cannot silently leave the feature on.

**Before you upgrade a registry-mode gateway, check:** (1) your longest concurrent load against `admission.max_global_requests` (1000 by default; each streaming request counts until its last byte); (2) that your balancer's readiness probe notices a failing `/readyz` within
`resilience.drain.readiness_grace` (5 s by default) and that `gateway.shutdown_timeout` covers the grace plus your longest stream; (3) that clients treat `503 NO_CAPACITY` with `Retry-After` as retryable; (4) that your log processing accepts the new fields; (5) the new dashboard and alerts (`docs/operations/observability.md`).

### Turning it off or on explicitly

```yaml
gateway:
  worker_source: registry
resilience:
  mode: off       # or on (the default); SERVERFLOW_RESILIENCE_MODE=off|on
```

Every other key keeps its default.

### How it works, in detail (decisions of PR D and PR E)

1. **A worker with sustained faults is ejected.** After 5 faults in a row (or a 50% fault ratio over at least 10 outcomes in 10 s) its breaker opens and the worker receives no traffic;
   after about 10 s (+-20%) one live request at a time is sent to it as a probe, and a good probe closes the breaker, a failed one reopens it with the cool-down doubled up to 2 min.
   At most half of the registry-eligible workers of a model can be ejected (a model with one worker is never ejected). If every eligible worker is ejected the gateway answers
   `503 NO_CAPACITY` at once with `Retry-After` the seconds to the first probe. Look for the WARN line `circuit breaker changed state` and `circuit_breaker_state`.
2. **Retries draw on a budget.** Per model, a retry is allowed while the retries in the last 10 s are fewer than `max(10, 20% of first attempts)`. A denied retry means the client gets the
   failed worker's own response (a 502/503) instead of a retried success; `retries_denied_total{reason="budget"}` counts them and the attempt line says `retry_denied`.
3. **A retried 503 is peeked.** To tell a busy or draining worker (`queue_full`, `draining`, `not_ready`: not a fault) from a broken one, the gateway reads at most 1 KiB of the 503's body
   for at most 250 ms before retrying. It is skipped when no retry is possible, and the bytes are put back if the response is relayed.
4. **First-byte deadline.** The retryable phase (all attempts until the first byte reaches the client) is bounded by `retry.first_byte_deadline`, by default
   `upstream_header_timeout + upstream_idle_timeout` plus the 5 s dial timeout, so no request the old gateway served is cut. Expiry before any byte is `504 UPSTREAM_TIMEOUT`
   (`inference_deadline_exceeded_total`); it never applies after the first byte. No retry starts with less than `retry.min_remaining` (1 s) left.
5. **Probes can cost a client.** A probe of a frozen worker waits for the probe header timeout (`min(10 s, upstream_header_timeout)` by default) and is not retried: that client gets a 504.
6. **New series, logs and span attributes** (below). Kafka events and the schema are unchanged; an attempt's `failure_class` can be `deadline`.
7. **A request that finds every eligible worker full waits.** Up to `queue.max_wait` (default `min(3 s, first-byte deadline)`) in a bounded, strictly first-in-first-out queue per model. The slot the next
   finishing request frees goes straight to the request at the head of the queue (hand-off), so a stale load figure from a worker's last heartbeat cannot hide it. A request that arrives while others wait queues behind them.
   Only a first routing waits: a retry never does, and neither does a request for a model nobody serves or with no eligible worker.
8. **Overload is refused with `503 NO_CAPACITY` and `Retry-After`, never 429.** Reasons (the `reason` of `admission_rejections_total`): `global_limit`, `queue_full` (the model's queue is at `queue.max_model_depth`, or one authenticated tenant
   already holds `queue.max_tenant_share` of it), `queue_timeout` (waited the maximum) and `draining` (the gateway is shutting down). `429 RATE_LIMITED` stays the tenant's own quota (Phase 8). `Retry-After` is the average of recent
   queue waits rounded up (1-10 s) for the queue reasons and 1 s for `global_limit` and `draining`. The body is the usual `NO_CAPACITY` body; the message names the reason.
9. **`admission.max_global_requests` is live.** It counts the requests inside one gateway from just after authentication (the body read included) until they finish, queued and running, streams until their last byte. This is not the same
   number as `inference_requests_active`, which starts before authentication. The 1001st concurrent request with the default 1000 is refused `global_limit` without its body being read. If 1000 long streams are ordinary for you, raise the key (`admission.max_global_requests`, or `SERVERFLOW_ADMISSION_MAX_GLOBAL_REQUESTS`; it must be above 0).
10. **Quota and shedding.** Refusals made before the rate limiter (`global_limit`, a full queue, a used tenant share) spend none of the tenant's quota. A request that waited and timed out was charged before it queued and is **not refunded**
    (Phase 8 has no refunds). A queued request holds its concurrency lease until it leaves the queue.
11. **The gateway overhead histogram excludes the queue wait**, so `ServerFlowGatewayOverheadHigh` keeps meaning "the gateway's own work"; the wait has its own histogram.

How a failure is judged (the taxonomy): connect errors, resets before headers, an empty first chunk, 502, 504, header timeouts, stalls and a 503 with any other code are **faults**; a 503 with
`queue_full`/`draining`/`not_ready`, a 4xx, a client that left, an expired deadline and a panic are **neutral**; a 500 (or any other 5xx) counts **only in the ratio rule** and is never retried.
A stream that breaks after its first byte is not retried; its ok (counted at the first byte) is converted into a fault. A 503 that is not peeked (no retry possible) counts as a fault.

Metrics (present only when the feature is effective; no new label names): `circuit_breaker_state{worker_id,model}` (1 half-open, 2 open; only non-closed breakers have a series),
`circuit_breakers_open{model}`, `circuit_breaker_transitions_total{model,state}`, `circuit_breaker_probes_total{model,result}`, `retries_denied_total{model,reason}`,
`breaker_trips_suppressed_total{model}`, `inference_deadline_exceeded_total{model}`. Granted retries are the existing `inference_retries_total`. From PR E:
`admission_rejections_total{model,reason}` (a `global_limit` shed is counted under `model="unknown"`: it is refused before the body is read), `inference_queue_duration_seconds{model}` (only requests that waited, whatever the outcome)
and `admission_queue_depth{model}` (every served model, 0 when empty).

Logs from PR E: `request shed` (INFO, at most one per second per reason, with `suppressed` counting what was held back; the request line of the shed request is logged as always, at WARN for a 5xx).
Traces: a request that waited has a `queue.wait` event on its root span with `serverflow.queue.wait_ms` and `serverflow.queue.result` (`served`, `timeout`, `client_gone`, `drained`).
Kafka events are unchanged: a shed after admission is one `failed` event with `failure_class=no_capacity`; a `global_limit` shed (before admission) emits none.

### Runbook: everything is being shed (PR E)

1. `sum by (reason) (rate(admission_rejections_total[1m]))` says which door is closing. `global_limit`: the gateway holds more concurrent requests than `admission.max_global_requests`; look at `inference_requests_active`
   and the length of your streams, then raise the key or add gateways. `queue_full` and `queue_timeout`: the workers are full for longer than `queue.max_wait`; `admission_queue_depth` against `queue.max_model_depth` and
   `histogram_quantile(0.95, rate(inference_queue_duration_seconds_bucket[5m]))` against the wait say how deep the shortage is. More workers, or a lower client load, is the fix; a longer `max_wait` only moves the failure later.
2. If only one tenant is shed with `queue_full` while the queue is not full, that tenant holds its `queue.max_tenant_share`; it is working as designed (raise the share, or the depth, if you want it to take more).
3. `draining` during a restart is normal.
4. A queue that is deep while the workers have free slots means the registry's counts are stale or the workers are ejected: check `circuit_breakers_open` and the worker heartbeat age. Waiters are re-offered capacity at every registry refresh.
5. The kill switch is `resilience.mode: off`: the gateway then answers a full cluster with an immediate `503 NO_CAPACITY` as before.

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
  tenant may hold `queue.max_tenant_share` of it; a model beyond `metrics.max_models` gets no queue and is refused as it always was (immediate `NO_CAPACITY`, counted as a scheduling decision), as does
  every model with `queue.max_model_depth: 0`. `Retry-After` is the EWMA of recent waits, rounded up and clamped to 1-10 s. On shutdown the queue closes and whoever still waits 250 ms before the shutdown timeout is
  answered `draining`.
- **Global cap.** `admission.max_global_requests` (an existing key, default 1000) counts requests inside the chat handler from just after
  authentication until completion, queued plus running, per gateway.

## Configuration

```yaml
resilience:
  mode: on                   # on | off. Default on; off is the kill switch. Effective in registry mode only.
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
    max_wait: 0              # 0 = default, min(3s, effective first-byte deadline); else 100ms-1m and <= that deadline
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
- Three defaults follow the gateway: an unset `probe_header_timeout` is `min(10s, upstream_header_timeout)`, an unset `queue.max_wait` is `min(3s, first-byte deadline)` and an unset `readiness_grace` is
  `min(5s, shutdown_timeout / 2)`, so configurations that are valid today stay valid. Only explicit values are cross-validated.
- Cross-checks: `queue.max_wait` must not exceed the effective first-byte deadline; an explicit `first_byte_deadline` below
  `gateway.upstream_header_timeout` is refused (it would cut requests the gateway serves today); `probe_header_timeout` must not exceed the
  gateway's header timeout; `drain.readiness_grace` must be shorter than `gateway.shutdown_timeout`; `max_cool_down` is at least `cool_down`.
- Environment values that do not parse (a number, a duration) are ignored and leave the default, like every other setting. The one exception is
  `SERVERFLOW_RESILIENCE_MODE`: a value other than `on` or `off` is kept and fails validation, so a misspelled kill switch cannot silently leave
  the feature in its default state.

## Runbooks

**A worker keeps getting ejected.** `circuit_breaker_state` (the Resilience dashboard, "Breaker state by worker") names it and the gateway's WARN `circuit breaker changed state` gives the reason (`consecutive_failures` or `failure_ratio`). The worker is failing for real (check its own logs and
`inference_failures_total` by `reason`), or the gateway is misjudging it: a worker that answers `503` without the codes `queue_full`, `draining` or `not_ready` is judged faulty (a real vLLM sidecar's vocabulary is unverified; Phase 13), and a worker that is merely slow
beyond `upstream_header_timeout` is ejected by design. Each gateway decides alone, so another gateway may still use the worker. If ejection is wrong for your workers, raise `resilience.breaker.consecutive_failures` or lower `max_open_fraction` (0 never ejects) before turning the feature off.

**The breaker will not close.** After the cool-down (10 s, doubling on each failed probe up to 2 min) one live request at a time probes the worker; a good probe closes it. If it never closes: the probes fail (`circuit_breaker_probes_total{result="fault"}` rising and "Breaker transitions" cycling open and half-open),
or no request ever reaches it (traffic too low to probe, a model with one worker that is the only candidate, or the worker is ineligible in the registry, which takes it out of the cap and out of routing anyway). A probe of a frozen worker can cost one client a 504 after `breaker.probe_header_timeout`. Restarting the gateway closes every breaker.

**Everything is being shed.** See "Runbook: everything is being shed (PR E)" above; the alert is `ServerFlowShedding`.

**A restart dropped requests.** See "Drain and rolling restarts (PR F)" below.

## Drain and rolling restarts (PR F)

**What happens when a gateway gets SIGTERM** (resilience `on`, registry workers; with `off` or static workers the old behaviour is unchanged: the listener closes at once and in-flight requests finish within `gateway.shutdown_timeout`):

| time | gateway |
| --- | --- |
| signal | `/readyz` answers `503` at once (the 2 s probe cache is bypassed; same body as an overload shed, "shutting down", `Retry-After: 1`), `gateway_draining` is 1, every response to a request that starts after the signal says `Connection: close` (one already in flight keeps its keep-alive header; `Shutdown` closes idle connections anyway). `/healthz` stays `200`; `/metrics` keeps answering. Log: `gateway draining: not ready, still serving`. |
| for `drain.readiness_grace` | the listener stays open and requests are served (including queueing) so a balancer that has not noticed yet loses nothing. |
| end of grace | the queue closes (waiters finish, are served or hit `queue.max_wait`; arrivals that would have to wait are shed `draining`), the listener closes, in-flight requests, streams included, finish. Whoever is still queued 250 ms before the end of the budget is shed `draining`. |
| `gateway.shutdown_timeout` after the signal | whatever is left is cut and the process exits non-zero (as before). The timeout covers the grace. |
| second signal, any time | the process exits at once (queued and in-flight requests get no answer). |

**Settings to choose together:** `readiness_grace` longer than your balancer's detection time (probe period times failure threshold, plus propagation to the data path); `gateway.shutdown_timeout` at least `readiness_grace` plus your longest
stream; the orchestrator's termination grace period at least `shutdown_timeout` plus a margin. The default grace is `min(5s, shutdown_timeout/2)`; a developer's Ctrl-C therefore waits up to 5 s (press it again to quit at once).
Set `SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE=0` to drain without a grace (the listener then closes at the signal, as with the feature off, but `/readyz` still fails and the queue still closes).

**Rolling workers.** The mock worker (`--drain-announce`, `--drain-timeout`) and the agent implement the worker half: on SIGTERM the backend reports `draining`, keeps serving for the announce period, refuses new work (a neutral `503 draining`) and finishes what is in flight;
the agent reports `DRAINING` (the registry stops offering the worker within the heartbeat interval plus the gateway's registry refresh) and deregisters when the backend exits. Choose the announce above the heartbeat interval plus `gateway.registry_refresh`
(1 s + 0.1 s in the benchmark cluster, plus margin). With an announce of 0 the requests routed from a stale view reach a refusing worker and are retried on another worker: the client sees nothing, but the retry budget is spent. Signal the agent first or the backend first;
both end with the worker deregistered.

**Runbook: a restart dropped requests.** (1) `connection refused` or resets at the balancer right after a gateway restart: the balancer sent requests after the listener closed. Lengthen `readiness_grace` beyond its detection time. (2) Streams cut at the end of a restart:
`shutdown_timeout` minus the grace is shorter than the stream; raise `shutdown_timeout` and the orchestrator's termination period. (3) `503 NO_CAPACITY` "shutting down" from a gateway: it was draining, the client should retry (another instance); `admission_rejections_total{reason="draining"}`
counts them. (4) A worker restart shows retries (`inference_retries_total`) and `503 draining` from the worker: the announce is shorter than the heartbeat plus refresh.

**Metrics.** `gateway_draining` (gauge, only with the feature on): 1 from the first signal. Alert on a gateway that stays 1 for longer than `shutdown_timeout` (it has not exited).

**Verified with mock workers and loopback only.** Process tests send real signals to the gateway, mock worker, agent and control plane binaries (`tests/integration/drain_process_test.go`); the results are in `docs/benchmarks/phase-15-chaos.md`. Real balancers, Kubernetes
preStop and termination grace periods, and vLLM's own drain behaviour are Phase 13, 17 and 18 and have not been tested.

## Known limits of the queue (PR E)

- A retry's routing bypasses the queue: it may take a free slot ahead of waiters (its failed attempt's own slot goes to the head of the queue).
- A handed-off request runs on the worker that freed the slot, not on the scheduler's choice.
- A queued request holds its rate-limit concurrency lease, so a tenant's own `max_concurrent` counts its waiters.
- Each gateway has its own queue, EWMA and global cap. With N gateways the cluster admits N times the cap.
- A worker that becomes admissible only because its breaker's cool-down ended frees nobody before the next registry refresh (at most `gateway.registry_refresh`).
- With authentication off there is no tenant share: one client can fill a model's queue (the depth bound and the global cap still hold).
- Under sustained overload every queued request waits `queue.max_wait` and then fails: queueing helps bursts, not shortage.
- The global cap is taken before the body is read and the body read deadline is 30 s (`defaultBodyReadTimeout`). A client that authenticates and then dribbles its body can hold a place for up to 30 s, so enough slow
  connections keep `global_limit` firing for everyone; with authentication off anyone can do it. Before this PR the gateway had no cap, so this is a new availability trade (the price of refusing a flood without reading its
  bodies). The cap is live since PR G flipped the default; the mitigations are a larger cap, a shorter body read timeout at the edge (load balancer) and authentication.
- A `global_limit` refusal sets `Connection: close` with the request body unread. A client that is still uploading a large body can see a connection reset or a broken pipe instead of the 503 and its `Retry-After`; clients should treat
  a reset on upload as retryable.

## Known limits of the breaker (measured while building PR D)

- The ratio rule looks at a sliding window that still contains the worker's healthy history, so a worker that starts failing every request needs as many faults as it had successes in the window
  before the ratio trips; a hard failure (503s, resets, timeouts) is ejected by the consecutive rule within 5 faults. A worker that fails exactly `failure_ratio` of its requests is on the threshold and
  is ejected only by chance. An always-500 worker is visible to the ratio rule only.
- A probe whose stream breaks after its first byte has passed (its verdict is taken at the first byte so a long stream does not hold the probe slot); the re-closed breaker has an empty window and needs the faults to
  reach half of it again, and the window holds the oks of the streams still in flight. The cost of a cool-down cycle therefore scales with the number of streams concurrently in flight on the worker (stream length times
  request rate), not with `min_volume`. A worker that aborts every stream builds no consecutive streak (each stream's first byte is an ok); the ratio rule ejects it once its faults equal its healthy outcomes in the window. Simulation results, not production data (one worker aborting every stream, 10 requests/s per worker, 10 minutes, 50 seeds): about 38 / 186 / 690 aborted streams per cool-down cycle for streams of 2 s / 10 s / 60 s, and a median time to the first ejection of an always-aborting worker of 3.8 s / 19 s / 69 s (it was ejected in all 50 runs). A worker that aborts 20-30% of its streams, or alternates ok and abort, is never ejected, consistent with the 50% ratio threshold.
- The 503 peek also runs for a model with a single worker (up to 250 ms, usually the time to read a small body), because whether another worker exists is known only after the retry is routed.
- A worker whose breaker has just closed can wait up to about one heartbeat before it is picked again: the router uses the larger of its own in-flight count and the worker-reported load, and a stale high report keeps the least-active strategies away from it until the next heartbeat. This is a property of the scheduler's inputs, not of the breaker; the chaos tests allow for it.
- A slow `BreakerTransition` observer delays the one request that delivers the transition; queued transitions are flushed at shutdown.
- `Retry-After` for an all-ejected refusal is computed over all models' breakers.
- A lying worker can answer `queue_full` to stay in rotation; registration is the trust boundary.
