# Phase 4 — Worker Registry

Status: Completed (implemented, independently verified twice and reviewed once; merged with its PR)
Owner: coding agent
Depends on: Phase 3 (mock worker; PR #4). Phase 1 remains deferred (needs a GPU).
Spec: `docs/architecture/serverflow-spec.md` §4, §10–12, §13, §15, §31, §39–40, §44, §58 Phase 4, §63

## Outcome

Workers can register with a control plane, heartbeat, and be found by model. The
control plane tracks each worker through the spec's state machine, derives health from
heartbeat age, notices a dead worker within the configured timeout, and lets a restarted
worker come back. The scheduler (Phase 5) will consume the result as `WorkerSnapshot`s.

```text
mock-worker  <--stats/readyz--  worker-agent  --register/heartbeat-->  control-plane (registry)
     ^                                                                       |
     |                                                          GET /v1/workers?model=  (Phase 5 consumes)
  gateway  (data path unchanged this phase: still one static upstream)
```

Acceptance headline (spec): *test worker death*. Kill a worker; the registry shows it
suspect, then unhealthy, then lost within the configured thresholds, never offers it as
eligible, and offers it again once it re-registers.

## Non-Goals

- No scheduler and no change to the gateway's data path (Phases 5–6). The gateway keeps
  its single static upstream.
- No Redis, Postgres, or Kafka. The registry is in memory (Phases 8, 9, 12).
- No request proxying by the agent; the agent is not in the data path.
- No real vLLM backend or GPU metrics (Phase 13); the agent ships a mock-worker backend
  and a `Backend` interface.
- No circuit breakers or retries (Phase 15), no autoscaling, no API-key auth (Phase 9).
- No Prometheus metrics (Phase 10); transitions are logged.

## Current Architecture

- `cmd/control-plane` and `cmd/worker-agent` are Phase 0 stubs (config plus a log line).
- `internal/config` already has `worker.heartbeat_interval` (2s) and
  `worker.unhealthy_timeout` (10s); there is no suspect or lost threshold and no
  control-plane section.
- The mock worker exposes `/readyz` (`ready`, `starting`, `draining`) and a provisional
  `/stats` with the spec §10 fields, but not its concurrency or queue limits.
- `ARCHITECTURE.md` reserves `internal/registry` (control plane) and `internal/worker`
  (agent), and states the boundary "routing hot path must not depend on control-plane
  state beyond cached snapshots".
- Spec §11 state machine, §12 heartbeat rules (<5s healthy, 5–10s suspect, >10s unhealthy,
  thresholds configurable), §15 invariants, §63 rule 10 ("never assume worker health").

## Decisions (confirm before implementation)

- **D1 In-memory registry** in the control-plane process. Not durable: after a control
  plane restart, workers notice (heartbeat answered 404) and re-register, so it
  self-heals within one heartbeat. Redis (Phase 8) and Postgres (Phase 9, durable worker
  config only) come later.
- **D2 The agent is a sidecar for the control-plane protocol only.** It registers,
  heartbeats, and reports readiness; the gateway still talks to the worker directly. Keeps
  the hot path free of an extra hop (spec §4).
- **D3 Health comes from heartbeat age measured by the registry at receipt time**, never
  from worker clocks: healthy under 5s, suspect 5–10s, unhealthy over 10s, lost over 30s,
  evicted after a retention period. All configurable. State is computed from age on read,
  so it needs no timers and tests need no sleeps.
- **D4 Suspect workers are not eligible by default** (strict, per §63 rule 10);
  `Eligible()` takes an option so the scheduler can choose otherwise.
- **D5 Registration incarnations.** `register` returns a `registration_id`. A worker that
  restarts under the same `worker_id` supersedes the old incarnation; heartbeats carrying a
  stale `registration_id` are rejected (409), and unknown workers get 404, which tells the
  agent to re-register.
- **D6 Shared-secret bearer token for the control-plane API** (security-sensitive, needs
  explicit review per AGENTS.md). Without it any process could register as any worker and
  steer prompts to an arbitrary URL once routing exists. The control plane binds loopback
  by default and refuses a non-loopback address without a token. Real API-key auth is
  Phase 9.
- **D7 Reported states** come from the agent: `REGISTERING` at registration, then
  `LOADING_MODEL`, `WARMING`, `READY`, `DRAINING`, plus `FAILED` when the backend is
  unreachable. The registry overlays `UNHEALTHY` and `LOST` from heartbeat age. A dead
  backend with a live agent reports `FAILED` at once instead of waiting for the timeout.
- **D8 Contract types live in `pkg/protocol`**: `WorkerState`, `WorkerInfo`, `Heartbeat`,
  `WorkerSnapshot` (spec §13), with GPU fields optional so the mock simply omits them.
- **D9 A background sweeper** logs transitions as they are observed and evicts workers
  lost for longer than the retention period, bounding memory. The registry also caps the
  number of workers.
- **D10 Agent identity and addresses come from `internal/config`** (the agent is a
  platform component, unlike the dev-only mock worker). New `control_plane` section; new
  worker settings for suspect, lost, and retention thresholds.
- **D11 The mock worker's `/stats` gains `max_concurrency` and `queue_size`** (additive),
  so a registration can state capacity, which the scheduler needs (§15).
- **D12 Layering:** `internal/registry` has no HTTP dependency in its core;
  `registry/server` and `registry/client` wrap it. The agent depends on the client, never
  on registry internals.
- **D13 Transition events** (Kafka `worker.lifecycle`) are deferred to Phase 12; the state
  machine is written so an observer can be attached later without redesign.

## Proposed Design

### Packages

```text
pkg/protocol/worker.go      WorkerState, WorkerInfo, Heartbeat, WorkerSnapshot, ID rules
internal/registry/          Registry (core, clock-injected), thresholds, sweeper
internal/registry/server    HTTP API for the control plane
internal/registry/client    Go client used by the agent (and later the gateway)
internal/worker/            Agent loop, Backend interface, mock-worker backend
cmd/control-plane/          config, auth, server, graceful shutdown
cmd/worker-agent/           config, agent, graceful deregistration
docs/architecture/worker-lifecycle.md   states, thresholds, death walkthrough (spec §57)
docs/decisions/ADR-010-worker-registry.md
tests/integration/          control plane + agents + mock workers, including process kills
```

### State machine

Reported by the worker: `REGISTERING → LOADING_MODEL → WARMING → READY → DRAINING →
TERMINATED`, and `FAILED`. Overlaid by the registry from heartbeat age: `UNHEALTHY` and
`LOST`. A superseded or stale incarnation is rejected, never merged. Only `READY` plus
healthy workers are eligible; everything else is never offered (spec §11, §15).

| Heartbeat age | Health | Eligible |
| --- | --- | --- |
| under 5s | healthy | yes if READY |
| 5s to 10s | suspect | no (configurable) |
| over 10s | unhealthy (state UNHEALTHY) | never |
| over 30s | lost (state LOST) | never; evicted after retention |

### API (control plane, `/v1`, bearer token when configured)

```text
POST   /v1/workers/register          WorkerInfo -> 201 {registration_id, heartbeat_interval}
POST   /v1/workers/{id}/heartbeat    Heartbeat  -> 204 | 404 unknown | 409 stale incarnation
DELETE /v1/workers/{id}              graceful deregistration (needs registration_id)
GET    /v1/workers                   ?model= &state= &eligible=true  -> []WorkerSnapshot
GET    /v1/workers/{id}              -> WorkerSnapshot
GET    /v1/models                    model inventory with worker counts
GET    /healthz  /readyz             liveness and readiness
```

Validation at the boundary: worker ID charset and length, model name, advertised URL
(`http`/`https` with a host, no credentials, query, or fragment), capacity bounds, body
size limit, and a cap on registered workers. Errors use the OpenAI-shaped error body for
consistency.

### Agent loop

1. Probe the backend until it answers; register with the control plane (state
   `REGISTERING`), keeping the `registration_id`.
2. Every `heartbeat_interval`: probe the backend (`/readyz` and `/stats`), map it to a
   state and metrics, send a heartbeat. A backend that cannot be reached reports `FAILED`.
3. On 404 or 409, register again. On control-plane downtime, keep retrying with backoff;
   never crash.
4. On SIGINT/SIGTERM: report `DRAINING`, deregister, exit 0; a second signal force-quits.

### Config

`control_plane.addr`, `control_plane.token`, `control_plane.max_workers`, and for the
agent `worker.id`, `worker.model`, `worker.backend_url`, `worker.advertise_url`,
`worker.control_plane_url`, `worker.suspect_timeout`, `worker.lost_timeout`,
`worker.retention`. Validated at startup; defaults keep every other binary valid.

## Affected Files / Components

New: the packages above, ADR-010, `worker-lifecycle.md`, `tests/integration` registry
tests. Changed: `internal/config`, `cmd/control-plane`, `cmd/worker-agent`, the mock
worker's `/stats` (additive), `Makefile` (a `dev-cluster` target), README, ARCHITECTURE.

## Acceptance Criteria

1. Registration: a valid `WorkerInfo` is accepted with a `registration_id`; invalid ID,
   model, URL (including credentials/query/fragment), or capacity is rejected with 400;
   the worker cap is enforced; a body over the limit is refused.
2. Heartbeat updates state and metrics. Using the injected clock: age under 5s is healthy,
   5–10s suspect, over 10s UNHEALTHY, over 30s LOST, with exact boundary behavior; worker
   clocks are ignored.
3. The state machine accepts the legal reported transitions and rejects illegal ones
   (for example `READY` back to `LOADING_MODEL` after `DRAINING`); `FAILED` is reportable
   from any state.
4. Lookup: `List` by model and state, `Eligible(model)` returns only READY and healthy
   workers (and never suspect, unhealthy, lost, draining, or failed ones), `Models()` is
   accurate, and results are consistent snapshots (no data races).
5. Re-registration: a restarted worker under the same ID supersedes the old incarnation;
   a stale `registration_id` gets 409; an unknown worker gets 404 and the agent re-registers.
6. Death test, in process with real timers (short thresholds): kill the worker; the
   registry reports suspect, then UNHEALTHY, then LOST within the thresholds, and it is
   never in `Eligible` from the moment it turns suspect; restarting it returns it to
   READY and eligible.
7. Death test with real binaries: `kill -9` the mock worker (agent alive) gives `FAILED`
   within one heartbeat; `kill -9` the agent gives suspect, UNHEALTHY, LOST; restarting
   either recovers.
8. A control-plane restart is survived: agents re-register within one heartbeat interval.
9. Graceful shutdown: SIGTERM on a worker makes it `DRAINING` then deregistered and
   absent from `Eligible`; SIGTERM on the control plane drains its requests and exits 0.
10. Auth: with a token configured every endpoint returns 401 without it and works with it
    (constant-time comparison); a non-loopback address without a token fails to start.
11. Eviction and bounds: workers lost beyond retention are removed; memory stays bounded
    under a flood of registrations.
12. `gofmt`, `go vet`, `go build`, `go test -race ./...`, `golangci-lint run ./...` pass;
    CI green; no new dependencies.

## Verification Plan

- Unit tests of the registry with a fake clock: every threshold boundary, every transition,
  eligibility, incarnation handling, eviction, caps, and a randomized model test against a
  brute-force oracle.
- HTTP tests of the server (validation, auth, status codes) and the client.
- Agent tests against a fake backend and a fake control plane (re-register on 404/409,
  backoff, drain on signal).
- Integration tests with the real control plane, agents, and mock workers in process,
  including the death scenarios with short real thresholds.
- Process tests of the built binaries: `kill -9` of the worker and of the agent, control
  plane restart, SIGTERM exit codes and a forced second signal.
- `-race -count=10` and under heavy CPU load; thresholds in tests are wide relative to
  scheduler jitter and assert lower bounds hard and upper bounds loosely.
- Independent `verify-change` (twice, as in Phases 2 and 3) and `review-change`, with
  mutation testing of the new tests, then `harden-change`, then `prepare-pr` and stop
  for approval.

## Implementation Notes (deviations and additions)

- **Packages follow the plan**: `pkg/protocol/worker.go`, `internal/registry` (core),
  `internal/registry/server`, `internal/registry/client`, `internal/worker` (agent and
  mock-worker backend), `cmd/control-plane`, `cmd/worker-agent`. `registry.NewWithClock`
  is exported so other packages' tests can use a fake clock.
- **Config**: new `control_plane` section; `worker.suspect_timeout`, `lost_timeout`,
  `retention`, and the agent identity fields. `suspect_timeout` must be at least twice
  `heartbeat_interval` (an existing Phase 0 test used a 3s interval and was updated to
  2.5s). Settings only one component needs are validated by that component
  (`ValidateAgent`, `ValidateServe`) so other binaries' defaults stay valid. A token must
  be at least 16 characters; no error or log ever contains it or a URL.
- **Eligibility check on the heartbeat path**: a rejected heartbeat (stale, illegal, or
  invalid) does not refresh the worker's age, so a misbehaving process cannot keep a dead
  worker alive.
- **Resuming workers**: a worker that had gone suspect, unhealthy, or even lost resumes
  without re-registering if its own incarnation reports in again (a healed partition).
- **Agent**: driven by a deterministic `Step` returning the delay to the next cycle, so
  every behavior (wait for backend, backoff 1x, 2x, 4x up to 10x, re-register on 404 or
  409 with a guard against spinning, drain then deregister, FAILED on a dead backend) is
  unit tested without sleeping. `Run` never deregisters (cancelling it simulates a crash);
  `Shutdown` is the graceful path. A backend that drained and then exited is a clean end,
  not a failure.
- **Mock worker `/stats`** gained `max_concurrency` and `queue_size` (D11).
- **`make dev-cluster`** starts a control plane, three mock workers and three agents.
- **Second-signal handling** (force quit) is in both new binaries, as in the mock worker.

## Independent Review and Verification (round 1) and What Changed

A fresh read-only reviewer and verifier (the review ran with the security-sensitive
mandate) found, and these are fixed with tests that failed first:

- **P1: two agents with one worker ID fought forever.** A 409 made the agent register
  again, which superseded the other process, which did the same: the registry flapped and
  each side registered over and over. Now a `stale_registration` response means "another
  process owns this ID", and the agent stops (exit 1) instead of taking it back; an
  illegal transition or an unknown worker still re-registers. End-to-end tests with two
  real agents assert exactly one supersede and a stable registration.
- **P1: a web page could register a rogue worker** against the default tokenless
  loopback control plane (a `text/plain` cross-site POST with a foreign `Host`). Now bodies
  must be `application/json` (415), requests with an `Origin` header are refused (403),
  and without a token the `Host` must be loopback (421). Verified on the real binary.
- **Drain is one-way**, even through FAILED (a worker could re-enter rotation through
  DRAINING, FAILED, READY). The registry remembers that an incarnation drained.
- **Control plane hardening**: read and write timeouts (a stalled body can no longer hold
  a connection or block shutdown), `Serve` refuses a tokenless non-loopback listener,
  case-insensitive `Bearer`, rate-limited auth-failure logging.
- **Agent hardening**: the control plane's suggested heartbeat interval is clamped to
  [max(10ms, configured/10), 10 x configured]; every probe and call has a timeout of half
  an interval; heartbeats are scheduled from the start of each cycle so latency cannot
  drift the period; backend data the control plane would reject is reported as FAILED with a
  reason; a heartbeat rejected as invalid is logged as an error.
- **Config**: heartbeat interval at least 10ms; an agent refuses to send the token over
  cleartext http to a non-loopback control plane.
- **Validation**: text fields reject control and invisible formatting characters
  (zero-width, bidi, soft hyphen); addresses reject link-local (cloud metadata),
  unspecified, and multicast hosts and `..` path segments; validation errors do not echo
  client text; the registry logs after releasing its lock.
- **Test coverage**: the verifier's mutation testing (217 mutants, 62 survivors) showed
  real gaps (prefix model matching, snapshot fields, exact limits, client query handling,
  agent state flags, second-signal handling, `main` wiring). All are now covered, and a
  follow-up pass of 61 targeted mutants is fully killed.

## Second Verification (round 2) and What Changed

Everything fixed in round 1 was confirmed on the real binaries (two agents with one ID, browser
attacks, one-way drain, timeouts, hostile control plane and backend). Findings and fixes:

- **Address policy bypass (security)**: `net.ParseIP` did not understand fullwidth digits, zone
  ids, or decimal/hex/octal integers, yet the dialer resolved them (the verifier reached
  `169.254.169.254` and a local listener). The host must now be a canonical IP literal or a strict
  ASCII DNS name; broadcast, `0.x.x.x`, bad ports, and spaces or control characters in the path are
  also refused. Tests list each bypass spelling plus legitimate forms that must keep working.
  A hostname that resolves to a forbidden address is still the gateway's job to catch at dial time.
- An empty `Origin` header slipped past `Header.Get`; the check now looks for the header itself.
- An unknown backend status was labelled "backend unreachable"; it is now "unusable data".
- Surviving mutants closed: registry log content, level, and eviction logs; auth-limiter
  accounting (and a test whose key assertion could never run); production default timeouts, the
  header-size limit, the forced close on shutdown, loopback Host forms; the call-timeout floor,
  backoff not being shortened by probe time, capacity exactly at its caps, and a draining backend
  returning garbage; the configured heartbeat interval reaching the agent. A process test that
  hung when validation was skipped now fails within 10 seconds.
- **Accepted gap**: the control plane's exit code when `Serve` returns an error cannot be
  provoked from outside (the listener is already bound and the token check runs earlier).

## Known Limitations / Follow-ups

- The registry is in memory and single-process; there is no failover (ADR-010).
- The gateway does not read the registry yet; the scheduler (Phase 5) connects them and
  must serve from cached snapshots when the control plane is down.
- Only a mock-worker backend exists; the vLLM backend and GPU metrics are Phase 13.
- Transitions are logged, not published (Kafka, Phase 12); no Prometheus metrics (Phase 10).
- One shared token protects the API; real authentication is Phase 9. The control plane
  serves plain HTTP (run it behind TLS off-loopback) and has no per-client connection cap.
- A lost worker that reports in again resumes at once (no probation), and an agent
  registers its backend's capacity only once.
- `GET /v1/workers` has no pagination (about 20 MB at 50,000 workers).
- `make dev-cluster` kills the control plane and agents together on Ctrl-C, so agents log
  harmless "deregistration failed" warnings.
- `cmd/gateway` still swallows a second signal during shutdown.
- Without a token, `curl` needs a JSON Content-Type and a loopback Host (`0.0.0.0` is refused);
  an empty token env var turns auth off on loopback; a non-numeric `MAX_WORKERS` is ignored.
- The gateway must re-check the address it dials; the registry cannot resolve hostnames.

## Risks

- **Unauthenticated registration** is a steering hole once routing exists (D6). Loopback
  default plus a required token off-loopback; real auth in Phase 9.
- **Flaky timing tests** for the death scenarios. Fake clock for logic; real-timer tests
  use wide margins.
- **Stale or duplicate workers**, split brain on restarts. Incarnations and exact
  supersede rules (D5), plus a randomized model test.
- **Unbounded growth** from registrations or lost workers: capped and evicted.
- **Scope creep** into scheduling, Redis, or events. Kept out by the non-goals.
- **The mock's `/stats` becomes the de facto agent contract.** It stays provisional; the
  `Backend` interface isolates it so Phase 13 can add a vLLM implementation.
- **Control-plane single point of failure.** Acceptable now; the gateway must keep serving
  from cached snapshots (a Phase 5 requirement, recorded here).
- **Clock handling:** registry time uses the monotonic clock; a wall-clock step must not
  mark every worker lost (lesson from Phase 3).

## Implementation Steps

1. `pkg/protocol/worker.go` (states, info, heartbeat, snapshot, validation), with tests.
2. `internal/config`: control-plane and worker settings, validation, env overrides.
3. `internal/registry` core: states, thresholds, register/heartbeat/deregister, lookup,
   eligibility, incarnations, sweeper, caps, with fake-clock and model tests.
4. `internal/registry/server` and `client`: API, validation, auth, with tests.
5. `internal/worker`: `Backend` interface, mock-worker backend, agent loop, with tests.
6. `cmd/control-plane` and `cmd/worker-agent`: wiring, signals, exit codes.
7. Mock worker `/stats` additions.
8. Integration and process tests, including the death scenarios.
9. ADR-010, `worker-lifecycle.md`, `Makefile` `dev-cluster`, README and ARCHITECTURE.
10. Full gate, independent verify (twice) and review, harden, then `prepare-pr` and stop
    for approval. Move this plan to `docs/plans/completed/` on completion.
