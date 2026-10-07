# Worker Lifecycle

How a worker joins, is judged, and leaves the registry (spec sections 10 to 12,
39, 40). Decision record: ADR-010.

## Components

```text
inference backend  <-- /stats --  worker-agent  -- register / heartbeat / deregister -->  control plane (registry)
(mock-worker, later vLLM)         (sidecar)                                                      |
                                                                          GET /v1/workers?model=&eligible=true
```

The gateway still talks to the backend directly; the agent is not in the data path.

## States

A worker reports its own state in each heartbeat. The registry overlays UNHEALTHY
and LOST when heartbeats stop.

| State | Who sets it | Meaning |
| --- | --- | --- |
| REGISTERING | worker | just registered, no heartbeat yet |
| LOADING_MODEL | worker | the backend is starting or loading its model |
| WARMING | worker | loaded, warming up |
| READY | worker | accepting work |
| DRAINING | worker | finishing in-flight work, accepting none |
| FAILED | worker | the backend is unreachable or broken |
| UNHEALTHY | registry | no heartbeat for more than the unhealthy timeout |
| LOST | registry | no heartbeat for more than the lost timeout |
| TERMINATED | (deregistration) | the worker left gracefully; its record is removed |

Legal reported transitions: from REGISTERING anywhere; LOADING_MODEL onward to
WARMING, READY, DRAINING, or FAILED; WARMING to READY, DRAINING, or FAILED; READY to
DRAINING or FAILED; DRAINING only to FAILED; FAILED back to LOADING_MODEL, WARMING,
READY, or DRAINING (the backend came back). FAILED is reachable from every reported
state. **A drain is one-way for the whole incarnation, even through FAILED**: a worker
that has reported DRAINING can afterwards only report DRAINING or FAILED, never serve
again; to serve it must register again as a fresh incarnation. An illegal transition is
refused with 409 `illegal_transition` and changes nothing: it does not count as proof of
life, and does not overwrite the stored state, metrics, or reason.

## Health and eligibility

Health is judged from heartbeat age, measured by the registry when the heartbeat
arrives (worker clocks are never used) and computed on read. Defaults:

| Heartbeat age | Health | Effective state | Eligible |
| --- | --- | --- | --- |
| under 5s | healthy | as reported | yes, if READY |
| 5s up to and including 10s | suspect | as reported | no (configurable) |
| over 10s | unhealthy | UNHEALTHY | never |
| over 30s | lost | LOST | never; removed after the retention period |

A worker is **eligible** only when READY and healthy. The scheduler (Phase 5) reads
eligible workers per model and must never route to anything else.

## Walkthrough: a worker dies

Measured with the real binaries and thresholds of 0.5s, 1s, and 3s (heartbeat 200ms):

| Time after `kill -9` of the agent | Registry view |
| --- | --- |
| 0.00s | READY, healthy, eligible |
| 0.47s | READY, **suspect**, not eligible |
| 0.99s | **UNHEALTHY**, not eligible |
| 3.02s | **LOST**, not eligible |

If only the **backend** dies and the agent survives, the agent reports FAILED within
one heartbeat (0.16s measured), so the worker is withdrawn immediately instead of
waiting out the timeout.

## Joining, restarting, leaving

- **Join**: the agent waits until its backend answers `/stats`, then registers with the
  backend's real capacity. Until then nothing is registered. The first heartbeat follows
  at once.
- **Restart**: registering again under the same worker ID replaces the old record with a
  new incarnation. The old process's heartbeats (if it is somehow still running) get 409
  `stale_registration`, and **an agent that receives that stops**: it logs an error, exits
  non-zero, and does not register again (registering again would steal the identity back
  and the two processes would fight forever). Two live agents sharing one worker ID is a
  misconfiguration; the newer one wins and the older one exits.
- **Control plane restart**: the registry starts empty; each agent's next heartbeat gets
  404, and it registers again within about one heartbeat interval.
- **Graceful stop**: on SIGINT or SIGTERM the agent reports DRAINING, deregisters, and
  exits 0. A second signal force-quits. If the backend itself drains and then exits, the
  agent treats that as a clean end, deregisters, and waits for the backend to return.
- **Crash**: nothing is sent; the worker walks suspect, unhealthy, lost, and is evicted
  after the retention period.

## API

All `/v1` endpoints require `Authorization: Bearer <token>` when a token is configured.

```text
POST   /v1/workers/register          WorkerInfo -> 201 {registration_id, heartbeat_interval_seconds}
                                     (bodies must be Content-Type: application/json, else 415)
POST   /v1/workers/{id}/heartbeat    Heartbeat  -> 204 | 404 unknown_worker | 409 stale_registration / illegal_transition
DELETE /v1/workers/{id}              header X-Registration-ID -> 204
GET    /v1/workers                   ?model= &state= &eligible=true -> {"workers": [WorkerSnapshot]}
GET    /v1/workers/{id}              -> WorkerSnapshot
GET    /v1/models                    -> {"models": [{model, workers, eligible}]}
GET    /healthz  /readyz             open, no token
```

A snapshot never includes the registration ID.

## Agent behavior worth knowing

- **Cadence**: heartbeats are scheduled from the start of each cycle, so probe and
  network latency do not stretch the period beyond one interval, and each probe or call
  is bounded to half an interval (with a floor), so a hung backend cannot stall a
  heartbeat. The control plane's suggested interval is accepted only within
  [max(10ms, configured/10), 10 x configured]; anything else is clamped and logged.
- **Bad data**: a backend report the control plane could not accept (negative or absurd
  counters, out-of-range capacity) is reported as FAILED with a reason, not retried until
  the worker decays to LOST. A heartbeat the control plane rejects as invalid is logged
  as an error.
- **Failures**: a control plane that is unreachable is retried at the normal heartbeat
  cadence (backing off would let health decay); registration failures and an absent
  backend back off up to ten intervals.

## Configuration

Shared YAML or `SERVERFLOW_*` environment (see `internal/config`).

| Setting | Default | Meaning |
| --- | --- | --- |
| `worker.heartbeat_interval` | 2s | how often agents heartbeat (at least 10ms); the control plane's value wins, within bounds |
| `worker.suspect_timeout` | 5s | heartbeat age at which a worker is suspect (at least 2x the interval) |
| `worker.unhealthy_timeout` | 10s | age beyond which it is UNHEALTHY |
| `worker.lost_timeout` | 30s | age beyond which it is LOST |
| `worker.retention` | 5m | how long a LOST worker is kept before eviction |
| `control_plane.addr` | `127.0.0.1:9090` | listen address; anything but loopback needs a token |
| `control_plane.token` | none | shared secret (at least 16 characters), also used by agents |
| `control_plane.max_workers` | 1000 | registry size cap |
| `worker.id`, `model`, `backend_url`, `advertise_url`, `control_plane_url` | | the agent's identity and addresses |

## Security

The registry decides where prompts will be sent, so registration is sensitive. Until
real authentication (Phase 9):

- **Token**: the control plane listens on loopback by default and refuses (in the config
  check and again when serving) any non-loopback address without a token. The token
  (16 characters or more) is compared in constant time, accepted with a case-insensitive
  `Bearer` scheme, never logged, and failures are logged at a limited rate.
- **Browsers**: a web page can reach a loopback service, so every `/v1` request that
  carries an `Origin` header is refused (403), bodies must be `application/json` (415;
  a cross-site `text/plain` or form POST needs no preflight, JSON does), and without a
  token the `Host` must be a loopback name (421), which defeats DNS rebinding. Without a
  token, `curl` needs `-H 'Content-Type: application/json'`, and `0.0.0.0` or a machine
  hostname as the Host is refused; use `127.0.0.1` or `localhost`. An empty
  `SERVERFLOW_CONTROL_PLANE_TOKEN=` overrides a configured token to none (the startup log
  says `auth_required=false`); off-loopback that is refused. A non-numeric
  `SERVERFLOW_CONTROL_PLANE_MAX_WORKERS` is ignored, as other numeric overrides are.
- **Registered addresses** must be http or https, with no credentials, query, fragment,
  or `..` path segment. The host must be a canonical IP literal or a strict ASCII DNS name
  (no fullwidth digits, decimal/hex/octal integers, zone ids, or trailing dot), and an IP
  may not be unspecified, link-local (which includes the cloud metadata address),
  multicast, or broadcast. A hostname that resolves to such an address is not caught here. Text fields (model, reason)
  reject control and invisible formatting characters. **Trust model**: a registered worker
  is trusted to name its own address, so only parties holding the token may register;
  the control plane never connects to that address itself, but the gateway will (Phase
  5/6), so the gateway must check the address it actually dials.
- **Transport**: the control plane speaks plain HTTP, so run it behind TLS when it is
  reachable over a network. An agent refuses to send the token over cleartext `http` to
  a non-loopback control plane (`https` or loopback is required when a token is set).
  The client never follows redirects, so the token cannot be replayed to another host.
- **Limits**: bodies are capped, requests have read and write timeouts (so a slow body
  cannot hold a connection), and the registry holds at most `max_workers`. There is no
  per-client connection cap or pagination yet.

## Limits

- The registry is in memory and single-process; there is no failover (ADR-010).
- A worker that went lost (not yet evicted) and then reports in again resumes at once,
  without a probation period (a healed partition is treated as alive).
- An agent registers the backend's capacity once; if the backend's limits change, the
  registry keeps the old figures until the worker registers again.
- `GET /v1/workers` returns every worker with no pagination (about 20 MB at 50,000).
- Only a mock-worker backend exists; a vLLM backend arrives with Phase 13.
- Transitions are logged, not published; Kafka events arrive with Phase 12.
- Registered load figures are as fresh as the last heartbeat.
