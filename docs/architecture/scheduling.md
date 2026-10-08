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

Because full workers are never selected and a worker normally queues only once full, `least-queue`
mostly sees equal queue depths on the mock worker and so behaves like round-robin; it differs when
queue values are stale or a backend queues earlier (real vLLM). See ADR-011.

`least-work` and latency-aware strategies are Phase 14. Setting `scheduler.strategy` to
`least-work` with `gateway.worker_source: registry` fails at startup with a clear message.

## How stale can the view be?

A worker that dies can still be picked until the first of: its backend reports FAILED and a
refresh sees it (about one heartbeat plus `registry_refresh`), or its cached heartbeat age
reaches the suspect threshold (default 5s, measured from its last real heartbeat). With the
defaults a silent worker is therefore trusted for at most about 5 seconds. A request sent to a
worker that is already dead fails (503 `WORKER_UNAVAILABLE`); retrying elsewhere is Phase 6.

During a control plane outage the gateway serves from its last snapshot until workers age past
the suspect threshold (they cannot heartbeat either), then answers `WORKER_UNAVAILABLE` (the
registry refresh is failing, so the cause is not capacity), as it also does once the snapshot is
older than `registry_max_staleness`. When the control
plane returns, traffic resumes on the next successful refresh.

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
| `scheduler.strategy` | `round-robin` | random, round-robin, least-active, least-queue |
| `worker.suspect_timeout` | `5s` | the gateway shares this with the control plane |

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
