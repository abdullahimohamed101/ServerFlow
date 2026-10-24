# Scheduling and Routing (Phase 5)

How the gateway chooses a worker in `registry` mode. Decisions and reasoning are in
`docs/decisions/ADR-011-scheduler-and-routing.md`; the spec sections are 13 to 15 and 51.

```text
client -> gateway handler -> router.Route
                               |-- snapshot cache  <-- poll GET /v1/workers (control plane)
                               |-- in-flight overlay (this gateway's open requests per worker)
                               |-- scheduler.SelectWorker (random | round-robin | least-active | least-queue)
                               `-- guarded transport -> worker
```

## Selection, step by step

1. The handler parses the request. In registry mode the model is not checked against a static
   list; the registry decides.
2. `router.Route` takes the cached workers for the model. No snapshot, or one older than
   `gateway.registry_max_staleness`: 503 `WORKER_UNAVAILABLE`.
3. Each worker's heartbeat age is advanced by the snapshot's age. A worker that has reached
   `worker.suspect_timeout` is treated as not eligible.
4. `ActiveRequests` becomes `max(reported, requests this gateway has in flight)`.
5. The scheduler's shared filter keeps eligible, READY, same-model workers below `MaxConcurrency`.
   If none are left: no worker serves the model at all gives 404 `MODEL_NOT_FOUND`; otherwise
   503 `NO_CAPACITY` with `Retry-After: 1` (or `WORKER_UNAVAILABLE` if registry refreshes have been
   failing for more than two intervals).
   `eligible_workers` in the body counts workers the registry offers, before the concurrency check.
6. The strategy picks one; the router reserves a slot on it (in the same critical section) and
   hands the handler a `release` that runs when the request ends, however it ends.
7. The request is sent to the worker's address through the guarded transport.

## Strategies

| Name | Picks | Ties |
| --- | --- | --- |
| `random` | uniformly among candidates (seedable in tests) | n/a |
| `round-robin` | the next worker by ID after the last one used for the model | n/a |
| `least-active` | lowest `ActiveRequests` | take turns in ID order |
| `least-queue` | lowest `QueueDepth` | take turns in ID order |
| `least-tokens` (Phase 14, opt-in) | fewest estimated tokens of work ahead of the request | take turns in ID order |
| `least-work` (Phase 14, opt-in) | smallest predicted wait, `(work ahead + this request's cost) / the worker's estimated token rate` | take turns in ID order |

Because full workers are never selected and a worker normally queues only once full, `least-queue`
mostly sees equal queue depths on the mock worker and so behaves like round-robin; it differs when
queue values are stale or a backend queues earlier (real vLLM). See ADR-011.

`least-tokens` and `least-work` are the work-aware strategies of Phase 14 (below, ADR-022); a latency-aware
strategy is a later, optional pull request and `scheduler.strategy: latency-aware` is refused at startup.

## How stale can the view be?

A worker that dies can still be picked until the first of: its backend reports FAILED and a
refresh sees it (about one heartbeat plus `registry_refresh`), or its cached heartbeat age
reaches the suspect threshold (default 5s, measured from its last real heartbeat). With the
defaults a silent worker is therefore trusted for at most about 5 seconds. A request sent to a
worker that is already dead is retried on another worker (see Retries and attempts below).

During a control plane outage the gateway serves from its last snapshot until workers age past
the suspect threshold (they cannot heartbeat either), then answers `WORKER_UNAVAILABLE` (the
registry refresh is failing, so the cause is not capacity), as it also does once the snapshot is
older than `registry_max_staleness`. When the control
plane returns, traffic resumes on the next successful refresh.

## Retries and attempts (Phase 6)

The full policy and its reasons are in `docs/decisions/ADR-012-retries-and-attempts.md`. In short, in
registry mode a request may try up to `gateway.max_attempts` workers (default 2), always a different one,
and only while nothing has been sent to the client:

- retried: connection failures (including the dial guard refusing an address), a connection reset or closed
  before headers, worker statuses 502 and 503 (`gateway.retry_statuses`), and a 200 whose body fails or ends
  before its first byte;
- not retried: any 2xx/3xx/4xx, a 500 or 504 (unless listed), a timeout before headers, a stalled stream, an
  any status outside 200-599 (a gateway 502), a 3xx (also a gateway 502), and anything after the first byte (the stream ends with an error event);
- if no second worker is selectable, or the budget is spent, the last worker's own response is relayed.

Response headers are held until the first body byte arrives (streams and plain responses alike). Each attempt gets its own `attempt_id`
(sent to the worker as `X-Attempt-ID`, with the shared `X-Request-ID`), one `attempt finished` log line
(debug when it succeeded, info otherwise), and counts in `inference_attempts_total` and
`inference_retries_total`. A client sees `X-ServerFlow-Attempts: N` only when N is greater than 1.

Two behaviours to know about when reading distribution numbers: a retry takes a turn in the rotation, and a
worker that fails instantly can look idle to `least-active` and `least-queue` (see ADR-012 and
`docs/benchmarks/phase-6-distribution.md`).

## What the gateway will and will not connect to

The worker address comes from a registration, so it is untrusted. At connect time (after DNS
resolution) the gateway refuses unspecified, link-local (including 169.254.169.254), multicast,
and broadcast addresses, the Azure, Alibaba, and AWS-IPv6 metadata endpoints, its own listener, and
IPv4-mapped, NAT64, and 6to4 forms of forbidden IPv4 addresses. If `gateway.worker_networks` lists
CIDRs, only addresses inside them are dialed. Loopback and private ranges are otherwise allowed, so registration is the trust boundary and
`worker_networks` is the control to use in any shared deployment (the gateway warns at startup when
it is empty and the control plane is not on loopback).
Proxies are disabled, redirects are never followed, and the client's `Authorization` header is
never sent to a worker. Refusals are logged with the worker ID; the address is never put in an
API response.

## Limits worth knowing

- **Capacity is the gateway's own accounting.** A slot is freed when the gateway's handler returns. If a
  client cancels, the worker may still be finishing that request, so for a moment the worker can run more
  than `MaxConcurrency` requests. With several gateways each only sees its own load (Phase 8).
- **`/readyz` lags by up to 2 seconds** (its result is cached), while `/v1/models` follows the snapshot at once.
- **The registry's numbers are checked, not trusted.** At each refresh a worker whose heartbeat age is
  negative, NaN or infinite, whose concurrency is below 1, whose load is negative, or whose address would
  not pass registration is marked ineligible. A control plane response that is empty or has no `workers`
  list is an error and keeps the previous snapshot, not an empty registry.
- **The scheduler also requires `health` to be healthy or suspect**, so it does not rely on the `eligible`
  flag alone.
- **`worker.suspect_timeout` must be at least `gateway.registry_refresh` plus `worker.heartbeat_interval`**
  (config validation enforces it); otherwise every cached worker would age to suspect between refreshes.
- A wrong control plane token does not stop the gateway: it logs the rejected refresh and fails closed
  (`WORKER_UNAVAILABLE`) until the token is fixed.

## Settings

| Key (env `SERVERFLOW_` + uppercase, dots as underscores) | Default | Meaning |
| --- | --- | --- |
| `gateway.worker_source` | `static` | `static` keeps the single configured upstream; `registry` uses the scheduler |
| `gateway.control_plane_url` | `http://127.0.0.1:9090` | where to poll; token is `control_plane.token` |
| `gateway.registry_refresh` | `1s` | poll interval |
| `gateway.registry_max_staleness` | `10s` | oldest snapshot still trusted; at least twice the refresh |
| `gateway.worker_networks` | empty | optional CIDR allow-list for worker addresses |
| `gateway.max_attempts` | `2` | tries per request, 1 to 5 (1 disables retries; each extra attempt multiplies load during an outage, so keep it low) |
| `gateway.retry_statuses` | `[502, 503]` | worker statuses that may be retried on another worker (5xx only, at most 10) |
| `resilience.mode` | `on` | `on` (registry mode only) adds per-worker circuit breakers, a retry budget on top of `max_attempts`, a bounded admission queue with shedding and gateway drain; `off` is the kill switch. All `resilience.*` keys: `docs/operations/resilience.md` |
| `scheduler.strategy` | `round-robin` | random, round-robin, least-active, least-queue, least-tokens, least-work (the last two need `scheduler.load_overlay: ledger`, below) |
| `scheduler.load_overlay` | `max` | how the gateway combines a worker's reported active count with its own in-flight count: `max` (the larger of the two; the original rule) or `ledger` (opt-in, see below) |
| `scheduler.cost.output_fraction` | `1.0` | work-aware strategies only: the share of the output budget (`max_tokens`, or `gateway.max_tokens_limit`) counted as work, in (0, 1] |
| `scheduler.cost.input_weight` | `1.0` | work-aware strategies only: weight of the input tokens in the work of a request, 0 to 100 |
| `scheduler.signals.capacity_halflife` | `60s` | work-aware strategies only: how fast a worker's capacity peak decays toward the fleet prior, 1s to 10m |
| `scheduler.signals.capacity_floor` | `0.25` | work-aware strategies only: the lowest a decayed capacity goes, as a fraction of the fleet prior, in (0, 1] |
| `worker.suspect_timeout` | `5s` | the gateway shares this with the control plane |

The four `scheduler.cost.*` and `scheduler.signals.*` keys are read, and validated, only when `scheduler.strategy` is `least-tokens` or `least-work` in registry mode; with any other strategy (and in static mode) they
are inert. Environment names are `SERVERFLOW_SCHEDULER_COST_OUTPUT_FRACTION`, `..._COST_INPUT_WEIGHT`, `..._SIGNALS_CAPACITY_HALFLIFE` and `..._SIGNALS_CAPACITY_FLOOR`; a value that does not parse is ignored.

## The load overlay: `max` and `ledger`

A worker's reported active count is up to one heartbeat old (plus one registry refresh). The gateway therefore overlays what it knows itself: the requests it has in flight on the worker.

- `max` (default, unchanged behaviour): effective load is `max(reported, local)`. A stale **low** report is covered by the local count. A stale **high** report is not: after this gateway
  finishes or abandons everything it sent to a worker, the worker still looks full until its next heartbeat, so it can sit unpicked for about a heartbeat (recorded in
  `docs/operations/resilience.md`; measured as experiment E0 below).
- `ledger` (opt-in, Phase 14, ADR-022): effective load is `max(local, reported + started_since_report - finished_since_report)`. The gateway keeps a short, bucketed (100 ms) history of when it started and
  finished requests per worker, and measures "since the report" from the gateway-local time the report was made (snapshot fetch time minus the worker's heartbeat age). It never returns less than the local
  count. When the history cannot answer (the gateway has not followed the worker since before the report, the report is older than `registry_max_staleness` plus one heartbeat interval, the control plane's
  heartbeat time for the worker went backwards, or more than 4,096 workers are tracked) it uses `max` for that worker. The report time is known only to about a bucket, so starts and finishes in the report's own bucket are
  classified alike (as after the report): a request that starts and finishes inside that bucket nets to zero. For the requests in that one bucket the classification is ambiguous, and the error is bounded: with 100 ms buckets the result can differ from an exact (1 ms) classification by at most the number of in-flight requests that start or finish in the report's own 100 ms bucket. An independent verifier measured that against a 1 ms-bucket reference over 28 million simulated samples: 71.3% identical, 19.1% off by one in either direction, 9.6% off by two or more (worst -11 and +9, at 32 slots with short requests). The floor `local` protects only this gateway's own count. With several gateways the ledger can say "free" where `max` would have said "full" while the worker is truly full, because it subtracts this gateway's own finishes while the other gateways' requests are invisible between reports: in the verifier's multi-gateway simulation that happened in a mean 4.8% of the truly-full samples (up to 14.5% at 32 slots with three gateways and 200 ms to 3 s requests); with 1 ms buckets the rate is about the same (4.6%), so the exposure comes from subtracting own finishes, not from the boundary rule. Overall the ledger is still better than `max` there (wrong-full 11.5% against 13.4%; oversubscription time 278 against 391, summed over 44 uncoordinated configurations; one gateway: wrong-free 0.0%). These are simulator-validated, not hardware-validated results from the independent verifier's simulation (not part of the repository); the repository guards the relative claim in `signals/multigateway_test.go` (ledger within 3 points of `max` for wrong-free and 5 for wrong-full, fixed seeds). The embedded cluster has a single gateway, so none of this multi-gateway exposure appears in the H1 measurement (0.00 pp rise in refusals below saturation).
  This replaces an earlier rule that guaranteed an over-count and, at short request lengths, made workers look full for the whole heartbeat interval. Simulator results (one worker, Poisson
  arrivals, the real tracker; share of requests for which the estimator says "full" while the gateway's own count has room, ledger against max, heartbeat 1 s): 8 slots, 20 ms requests, 30% load: 4.0% against 0.9%;
  8 slots, 100 ms, 70%: 12.6% against 12.7%; 32 slots, 100 ms, 70%: 4.5% against 1.6%; 8 slots, 500 ms, 70%: 5.0% against 8.8%. They are guarded by `overcount_test.go`.
- The same function answers the scheduler's capacity check (`reserve`) and the admission queue's free-slot count (`freeSlots`), each with the instant of its own snapshot view, so the two never disagree about
  whether a worker has room. The overlay changes `ActiveRequests` for **all** strategies.
- With `ledger` on, the gateway also keeps a ledger of the estimated token cost (`input + max_tokens`, the same estimate the rate limiter uses) of its in-flight requests per worker and puts it, with the
  effective active count, on the private snapshot copy the strategies see (`WorkerSnapshot.Observed`, never serialised). The strategies of Phase 5 ignore it; the token-aware strategies of a later change read it.
  A slot handed to a queued request by the admission queue carries no cost until the request that receives it adds its own; for those microseconds the ledger undercounts by one request (admission uses slot counts, so
  nothing is over-admitted).
- E0, the recovered-worker experiment (`internal/bench/embedded/recovered_worker_test.go`): two workers, the stuck requests of one abandoned by the client, a probe sent at once. Embedded cluster
  (heartbeat 1 s, refresh 100 ms): the probe waits 0.71 s (median; 0.70 to 0.80 s over 20 runs) under `max` and is served in 12 ms under `ledger` (20 runs). Simulator-validated, not hardware-validated: embedded mock workers on an idle machine; the real-time test discards repetitions a busy machine invalidates and asserts on medians.

## The work-aware strategies: `least-tokens` and `least-work` (Phase 14, opt-in)

Both read the gateway's own view of the work on each worker (`WorkerSnapshot.Observed`, ADR-022), so they need the in-flight ledger: choosing either with the default `scheduler.load_overlay: max` fails at
startup ("needs scheduler.load_overlay: ledger"). Nothing else changes with the defaults: `round-robin` stays the default strategy and `max` the default overlay.

- **Work ahead** of a request on a worker is the estimated cost of this gateway's in-flight requests there (the ledger) plus the load other gateways and clients appear to add (the reported active count minus
  this gateway's own, priced at the model's mean request cost) plus the worker's reported queued input tokens times `scheduler.cost.input_weight`.
- **Cost of a request** is `ceil(input_weight x estimated input tokens) + ceil(output_fraction x output budget)`, from the same estimate as the rate limiter (ADR-015), which is still charged the unweighted sum.
- `least-tokens` picks the smallest work ahead. `least-work` picks the smallest predicted wait: (work ahead + the request's own cost) divided by the worker's token rate, so a fast worker with more work ahead can beat
  a slow idle one. Ties (within a relative 1e-9) take turns in ID order.
- **Token rate** of a worker is a decayed peak of its reported `recent_tokens_per_second`, sampled only while it had work, decaying toward the median of its model's peaks (never below `capacity_floor` times it).
  A worker with no sample, or a re-registered one, uses that median; with no sample anywhere `least-work` scores like `least-tokens`.
- Missing or invalid signals never exile a worker and never choose an ineligible one: see ADR-022 for the exact fallbacks. `scheduler_signal_fallbacks_total{model,reason}` and
  `scheduler_predicted_wait_seconds{model}` exist only for these two strategies.
- **What is not shown.** These are simulator-validated, not hardware-validated: no experiment has compared them with the others yet (PR J), the mock worker has no batching (its tokens per second grows with its load), the
  cost of a request is an upper-bound estimate, and with several gateways each sees only its own requests.

## Operating notes

- Worker responses are relayed with `X-Content-Type-Options: nosniff` in registry mode.
- Invalid duration values in `SERVERFLOW_GATEWAY_REGISTRY_*` environment variables are ignored and the
  default is kept (the same convention as other numeric overrides); check the startup log line.
- `registry_refresh` must be at least 10ms and `registry_max_staleness` at most 5 minutes.
- Run the gateway with the same `worker.suspect_timeout` as the control plane.
- `make dev-cluster` starts a control plane, three mock workers with agents, and a gateway in
  registry mode on `:8080` (`STRATEGY=least-active make dev-cluster` to change the strategy).
- Selection is logged at debug level (`worker selected`: request, attempt, strategy, worker).
  Rejections (`no worker selected`) log at info, and at debug for an unknown model, which a client
  can trigger at will; a failed request to a worker (`worker request failed`) logs at warn.
- The control plane client reads at most 4 MiB per response, roughly 7,000 workers; beyond that
  refreshes fail and the gateway fails closed.
- An empty registry (for example right after a control plane restart, before agents re-register)
  answers 404 `MODEL_NOT_FOUND` for every model.
