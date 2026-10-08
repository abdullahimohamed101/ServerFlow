# Phase 5 — Scheduler Framework

Status: Completed (implemented, independently verified twice and reviewed once; merged with its PR)
Owner: coding agent
Depends on: Phase 4 (worker registry; PR #5). Phase 1 remains deferred (needs a GPU).
Spec: `docs/architecture/serverflow-spec.md` §4, §13–15, §16 (separate concern), §51, §52, §58 Phase 5, §63
Hand-off from Phase 4: `docs/plans/completed/phase-4-worker-registry.md` ("Inputs for the Phase 5 plan")

## Outcome

The gateway can choose a worker for each request instead of forwarding to one static
upstream. A `Scheduler` interface (spec §13) with four strategies (random, round-robin,
least-active, least-queue) picks among the workers the registry says are eligible for the
requested model. The gateway reads the registry through a cached snapshot, so a control
plane outage does not stop traffic immediately, and it never dials an address that could
reach the cloud metadata service or another forbidden target.

```text
client -> gateway -> Router -> Scheduler (strategy from config)
                       |  ^
                       |  +-- snapshot cache <-- poll GET /v1/workers?eligible  -- control-plane (registry)
                       +----- guarded dialer ---------------------------------->  worker (mock-worker)
```

Acceptance headline: *with three mock workers registered, a request for the model goes to
a worker chosen by the configured strategy; kill one and it stops receiving traffic; stop
the control plane and traffic continues for a bounded time, then fails closed.*

This phase makes a single choice per request. Retrying on another worker, recording
request attempts, and the 1000-request distribution acceptance belong to Phase 6.

## Non-Goals

- No retries, no second attempt on failure, no circuit breaker (Phase 6 and Phase 15). A
  chosen worker that fails is a failed request, reported as today.
- No admission control or rate limiting (spec §16; Phases 8–9). Selecting a worker and
  deciding whether to accept a request stay separate concerns.
- No weighted-least-work or latency-aware strategies (Phase 14), and no benchmark claims
  about which strategy is better (spec §14, §63).
- No Redis, Postgres, or Kafka. No Prometheus scheduler metrics (Phase 10); selections are
  logged.
- No change to the control plane API or the agent.
- No real vLLM or GPU awareness (Phase 13).

## Current Architecture

- `internal/gateway` forwards every chat request to one static `Upstream` (`Do`, `Probe`),
  built from `gateway.upstream_url`. `/v1/models` serves the static `gateway.models` list.
  `info.attemptID` already exists per request, anticipating attempts.
- `internal/config` has `scheduler.strategy` (valid: `random`, `round-robin`,
  `least-active`, `least-queue`, `least-work`; default `least-work`), unused so far.
- `ARCHITECTURE.md` reserves `internal/scheduler` and states "routing hot path must not
  depend on control-plane state beyond cached snapshots".
- `internal/registry/client` provides `Workers(Query{Model, State, EligibleOnly})` and
  `Models()`; the registry computes `Eligible` (READY and healthy; suspect only if
  configured). `protocol.WorkerSnapshot` carries state, health, `Eligible`, capacity
  (`MaxConcurrency`, `QueueSize`), and `Metrics` (`ActiveRequests`, `QueueDepth`, ...).
- Spec §13 fixes the interface; §15 lists what the scheduler must never do: pick an
  unhealthy, wrong-model, draining, or at-capacity worker, silently drop a request, or
  block indefinitely, and it must say why when nothing is eligible.
- Worker-reported metrics are only as fresh as the last heartbeat (up to one interval, 2s
  by default), so a burst can pile onto one "idle-looking" worker.

## Decisions (confirm before implementation)

- **D1 Scope: library plus wiring for one attempt.** Build `internal/scheduler`, a snapshot
  source, a guarded dialer, and a gateway `registry` mode that makes one selection per
  request. The alternative, a library only, would leave the Phase 4 hand-off items (dial
  check, cached snapshots) with no consumer and nothing tested end to end. Phase 6 then
  adds retries and attempts on top.
- **D2 Static mode stays the default.** `gateway.worker_source: static` (default) keeps
  Phase 2–3 behavior byte for byte; `registry` turns the new path on. Existing tests and
  the overhead benchmark are unaffected.
- **D3 The `Scheduler` interface is exactly spec §13**:
  `SelectWorker(ctx, *InferenceRequest, []WorkerSnapshot) (*WorkerSnapshot, error)`. Schedulers
  are pure functions of their inputs (plus an injected random source and a round-robin
  cursor), so unit tests need no clock, network, or sleeps.
- **D4 Four strategies.** `random` (seedable), `round-robin` (a cursor per model, over
  workers sorted by ID so the order is stable), `least-active` (minimum
  `ActiveRequests`), `least-queue` (minimum `QueueDepth`). Ties go to the next worker in the
  round-robin order among the tied ones, so ties neither favor the lowest ID nor starve it.
- **D5 Filtering is the scheduler's job and is tested as invariants (§15).** Before any
  strategy runs, drop snapshots that are not `Eligible`, serve a different model, are not
  READY, or are at hard capacity. Strategies only ever see survivors. If none survive the
  scheduler returns a typed error carrying the reason and the counts.
- **D6 Capacity uses the larger of reported and local in-flight.** The router counts the
  requests it has sent to each worker that have not finished, and presents each snapshot to
  the scheduler with `ActiveRequests = max(reported, local)`. The max avoids double counting
  the same requests (the worker's report already includes earlier ones) while still
  protecting against stale reports during a burst. A worker at `MaxConcurrency` is excluded.
  Schedulers stay pure; the overlay happens in the router. Per-gateway only: several
  gateways do not see each other (Phase 8).
- **D7 Errors.** Unknown model (no worker registered for it, eligible or not): 404
  `model_not_found`, as today. Known model, nothing selectable: 503 `no_capacity` with a
  `Retry-After: 1` header and a body naming the model and the counts (spec §15 shows
  `capacity_exhausted`; spec §51 names the code `NO_CAPACITY`, so I propose using the §51
  name for the API code and keeping the §15 fields). Snapshot unavailable or too stale:
  503 `worker_unavailable`. Nothing is dropped silently and nothing blocks.
- **D8 Snapshot source.** A background loop polls the control plane's `GET /v1/workers`
  (all workers, not only eligible ones, so "unknown model" and "no capacity" can be told
  apart) every `gateway.registry_refresh` (default 1s), keeping the last good result.
  Requests read it lock-free (atomic pointer swap). A failed refresh keeps the old
  snapshot and logs once per state change.
- **D9 Bounded staleness, then fail closed.** Each snapshot records when it was fetched.
  Selection uses it only while `age <= gateway.registry_max_staleness` (default 10s, the
  registry's unhealthy threshold, so a dead worker is never trusted longer than it would be
  at the registry itself). Within that bound, the cached `heartbeat_age_seconds` is advanced
  by the snapshot's age, and a worker whose advanced age exceeds the suspect threshold is
  treated as not eligible. Past the bound the gateway returns `worker_unavailable` (spec
  §63 rule 10: never assume worker health). This is the second Phase 4 hand-off item.
- **D10 Dial-time address guard** (the first Phase 4 hand-off item; security-sensitive).
  The worker transport uses a dialer whose `Control` hook inspects the IP actually being
  connected to, after DNS resolution and for every redirect-free request, and refuses
  unspecified, link-local (includes 169.254.169.254), multicast, and broadcast addresses.
  Checking at connect time, not at resolution time, defeats DNS rebinding and odd spellings.
  Loopback and private ranges stay allowed because workers and the dev cluster live there.
  An optional `gateway.worker_networks` allow-list of CIDRs narrows it further for
  deployments; empty means "anything not forbidden". Proxies are disabled, redirects are
  never followed (as in the static upstream).
- **D11 Control plane credentials.** The gateway reads the control plane URL from
  `gateway.control_plane_url` (default `http://127.0.0.1:9090`) and the token from the
  existing `control_plane.token` / `SERVERFLOW_CONTROL_PLANE_TOKEN`. It reuses
  `internal/registry/client` and the same rule as the agent: refuse to send the token over
  cleartext http to a non-loopback control plane. The token is never logged.
- **D12 Strategy availability.** `least-work` stays a valid config value (Phase 14) but the
  gateway refuses to start in `registry` mode with it, with a clear message, and the default
  becomes `round-robin` so the out-of-the-box configuration works. Unknown names are
  already rejected by config validation.
- **D13 `/v1/models` and `/readyz` in registry mode.** `/v1/models` lists the models that
  have at least one eligible worker in the snapshot; `/readyz` is ready when a snapshot
  within the staleness bound has at least one eligible worker. In static mode both are
  unchanged.
- **D14 Selection is logged, not metered.** Each request logs `request_id`, `attempt_id`,
  `model`, `strategy`, and `worker_id` at debug level (the candidate count was dropped); rejections
  log at info (debug for an unknown model, which a client can trigger at will) and worker request failures at warn. Prometheus scheduler metrics wait for Phase 10 (worker IDs as labels need a
  cardinality decision there).

## Proposed Design

**Package `internal/scheduler`** (no network, no gateway imports):

```go
type Scheduler interface {
    SelectWorker(ctx context.Context, req *protocol.InferenceRequest,
        workers []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error)
}
func New(strategy string, opts ...Option) (Scheduler, error)   // WithSeed(n) makes random reproducible
func Strategies() []string                                      // the implemented names
type NoWorkerError struct{ Model string; Serving, Eligible, AtCapacity int } // errors.As-able
func Candidates(req, workers) (survivors []WorkerSnapshot, why NoWorkerError) // the D5 filter, shared by all strategies
```

**Router in `internal/gateway`** (`router.go`): owns the snapshot cache (D8–D9), the in-flight
counters (D6), and the guarded transport (D10). `Route(ctx, ireq) (target, release, error)`:
read the snapshot, advance ages, overlay in-flight, call the scheduler, bump the chosen
worker's counter, and return a `release` the handler defers. The handler then sends the
body to `target.Address + path` with the existing header allow-list, the same idle-timeout
and streaming code, and the same error mapping. In static mode the old `Upstream` path is
used unchanged.

**Config** (additions, all with defaults and validation, env overrides in the existing
style): `gateway.worker_source` (`static`|`registry`), `gateway.control_plane_url`,
`gateway.registry_refresh` (1s), `gateway.registry_max_staleness` (10s, must be at least
2x refresh), `gateway.worker_networks` (CIDR list, optional); `scheduler.strategy` default
`round-robin`.

**Errors** (`internal/api`): add `no_capacity` (503, with `Retry-After`) and a body that
carries `model` and `eligible_workers`; reuse `model_not_found` and `worker_unavailable`.

**Docs**: ADR-011 (scheduler interface, in-flight overlay, snapshot staleness, dial guard),
`docs/architecture/scheduling.md`, README and ARCHITECTURE updates.

## Affected Files / Components

New: `internal/scheduler` (interface, four strategies, filter, tests), `internal/gateway/router.go`
and a guarded dialer (`internal/gateway/dialguard.go`), integration tests under
`tests/integration`, ADR-011, `docs/architecture/scheduling.md`.
Changed: `internal/gateway` (server, handlers, readiness for registry mode),
`internal/api` (new error), `internal/config` (new gateway fields, default strategy),
`cmd/gateway`, `Makefile` (`dev-cluster` can start a gateway in registry mode), README,
ARCHITECTURE.

## Acceptance Criteria

1. Interface: every strategy implements spec §13; `New` rejects unknown names.
2. Invariants (property-style tests over randomized snapshot sets, for every strategy): never
   selects a non-eligible, wrong-model, non-READY, draining, failed, unhealthy, or at-capacity
   worker; never returns a worker when none qualifies; never blocks (honors a cancelled
   context); never mutates its input.
3. Determinism: with a seeded source, `random` produces the same sequence; `round-robin`
   cycles A, B, C, A, B, C over three equal workers and keeps its order when a worker
   disappears and returns; `least-active` and `least-queue` pick the minimum, and ties rotate.
4. No-worker reasons: unknown model gives 404 `model_not_found`; a known model with every
   worker ineligible or full gives 503 `no_capacity` with `Retry-After` and correct counts.
5. In-flight overlay: a burst of N concurrent requests across 3 workers, with stale reported
   metrics, spreads rather than piling on one worker, and never exceeds a worker's
   `MaxConcurrency`; counters return to zero after completion, cancellation, and error.
6. Snapshot cache: a failed refresh keeps the last snapshot; once older than the staleness
   bound the gateway returns `worker_unavailable`; a cached worker whose advanced heartbeat
   age passes the suspect threshold is not selected; recovery after the control plane returns.
7. Dial guard: connections to 169.254.169.254, `0.0.0.0`, multicast, and broadcast are
   refused even when reached through a hostname or a DNS answer that changes between the
   registration check and the dial; loopback and private addresses still work; with
   `worker_networks` set, addresses outside it are refused. Redirects are never followed.
8. End to end with real components (in process): three mock workers behind agents and a
   control plane, a gateway in registry mode; requests spread by strategy; killing one
   worker's backend stops traffic to it within the heartbeat and cache bounds; stopping the
   control plane keeps traffic flowing until the staleness bound, then fails closed;
   restarting it recovers.
9. Real binaries: the same death and control-plane-outage scenarios with `kill -9`, plus
   `least-work` refused at startup, a bad token giving a clear error, and a cleartext token to
   a non-loopback control plane refused.
10. Static mode is unchanged: all existing gateway tests pass untouched, and the overhead
    benchmark stays within its Phase 2 budget (p95 < 25ms added).
11. `gofmt`, `go vet`, `go build`, `go test -race ./...`, `golangci-lint run ./...` pass; CI
    green; no new dependencies.

## Verification Plan

- Scheduler unit tests with no clocks or sleeps: table tests per strategy, the §15
  invariants as randomized property tests with a brute-force oracle, and a concurrency
  test of the round-robin cursor under `-race`.
- Router tests with a fake registry client and fake clock for cache refresh, staleness,
  age advancing, and the in-flight overlay.
- Dial guard tests against a local listener, a resolver stub returning forbidden then
  allowed addresses, and the spellings the Phase 4 verifier used (fullwidth digits, integer
  forms, zone ids).
- Gateway handler tests in registry mode with fake workers: status codes, headers
  (`Retry-After`), the error body, header allow-list, streaming and mid-stream failure
  behavior unchanged.
- Integration and process tests as in criteria 8–9, reusing the Phase 4 cluster helpers.
- `-race -count=10`, a run under heavy CPU load, and the overhead benchmark.
- Independent `verify-change` (twice) and `review-change` with mutation testing, `harden-change`,
  then `prepare-pr` and stop for approval. The dial guard and token handling are
  security-sensitive and get an explicit security review (AGENTS.md).

## Implementation Notes (deviations and additions)

- **The gateway shares `worker.suspect_timeout`** (the open detail from the plan review): the
  advanced-age rule uses `cfg.Worker.SuspectTimeout`, so the gateway and control plane must be
  configured alike. No new setting.
- **Outage tolerance is shorter than the staleness bound.** Because cached heartbeat ages keep
  advancing, a cached worker stops being eligible about `suspect_timeout` after its last real
  heartbeat (5s by default). Once refreshes have been failing for more than two intervals the
  gateway answers `WORKER_UNAVAILABLE` throughout (the cause is the registry, not capacity; review
  round 1 changed this from `NO_CAPACITY`), and past `registry_max_staleness` it stops using the
  snapshot altogether. This matches the registry, which would also call silent workers suspect. Acceptance 8 reads "keeps serving
  briefly, then fails closed", which is what is tested.
- **Schedulers work on indices.** `scheduler.Candidates` stays public (copying), but the
  strategies use an internal index-based filter so only the chosen worker is copied; the first
  benchmark showed 300 microseconds and 1.8 MB per route at 1000 workers, now 54 microseconds
  and 263 KB. The snapshot keeps each model's workers sorted by ID.
- **Model labels.** In registry mode `info.model` (a Prometheus label) is set only after the
  registry confirms the model, so client-chosen names cannot create unbounded label values;
  a test guards it.
- **`protocol.ForbiddenAddr`** is exported and shared by registration validation and the dial
  guard, so there is one definition of a forbidden address.
- `api.Limits.AnyModel`, `api.Error` optional detail fields, and `readiness` taking a `prober`
  are small additive changes.
- `startMockProc` in the process tests now uses `--tokens-per-second=1000 --output-tokens=8`
  so each request takes milliseconds, not seconds (the integration suite went from ~75s to ~33s).

## Review round 1 and what changed

The independent review found no blockers. Fixed: a registry outage now reads `WORKER_UNAVAILABLE`
instead of `NO_CAPACITY`; `eligible_workers` counts registry-eligible workers before the concurrency
check (it was always 0); the router unlocks with `defer`; a client that leaves during selection is
recorded as a closed client, not a 500; rejection logs are Info (Debug for unknown models); the snapshot
is loaded once per view; the dial guard also refuses Azure, Alibaba and AWS-IPv6 metadata endpoints,
NAT64 and 6to4 wrappers, and the gateway's own listener; `nosniff` on relayed worker responses; the
control plane client no longer uses environment proxies; refresh and staleness have bounds; `main` builds
one server; a startup warning when `worker_networks` is empty with a remote control plane; docs corrected
(strategies are stateful, `Candidates` is the copying form, the error body shape, `least-queue` caveat).
Verification round 1 added: snapshot numbers and addresses are validated at refresh and heartbeat ages are
compared as float seconds (a huge value wrapped on amd64); the scheduler also requires `health` healthy or
suspect; an empty or list-less control plane answer is an error; config requires `suspect_timeout >=
registry_refresh + heartbeat_interval`; tests added for DNS rebinding (a fake resolver whose answer changes),
the no-proxy transport, overlay max-versus-sum, trailing slashes, a wrong token, and a killed agent;
documented the 2-second `/readyz` cache and slot overlap after client cancels.
Verification round 2 added: the self-loop guard covers every loopback and interface address of a
wildcard listener (and only the listener's own address for a specific one); zoned forms of metadata
addresses, IPv4-compatible (`::a.b.c.d`), SIIT (`::ffff:0:a.b.c.d`) and local-use NAT64 spellings are
refused; "degraded" now means refreshes failing for more than two intervals, so one missed refresh no
longer turns a real capacity shortage into `WORKER_UNAVAILABLE`; tests for the client-left-while-choosing
path and for log levels. Not fixed, documented: the registry client reads at most 4 MiB, about 7,000
workers, after which refreshes fail and the gateway fails closed; an empty registry answers 404.
Not changed: the per-model round-robin map is not pruned (a client cannot grow it; only registered models
do), and strict 'require worker_networks off-loopback' was left as a warning.

## Risks

- **Thundering herd on a stale snapshot.** Mitigated by the in-flight overlay (D6), but only
  per gateway. Documented; multi-gateway coordination is Phase 8.
- **Stale health.** Up to refresh plus heartbeat interval can pass between a worker dying and
  the gateway noticing; the staleness bound and the advanced-age rule cap the exposure, and
  Phase 6 retries cover the remainder. The budget is documented in `scheduling.md`.
- **Dial guard gaps.** The guard blocks metadata, unspecified, multicast, and broadcast, but
  private networks stay reachable by design. Deployments that need more use `worker_networks`.
  IPv4-mapped and zone-id forms are tested explicitly.
- **Configuration drift.** The default strategy changes from `least-work` to `round-robin`;
  config tests that assert the old default must be updated, not weakened, and the change is
  recorded in the ADR.
- **Hot path cost.** One atomic load, a filter, and a small sort per request; the overhead
  benchmark gates it.
- **Strategy claims.** No claim that one strategy beats another until Phase 7 produces evidence.

## Implementation Steps

1. `internal/scheduler`: interface, `NoWorkerError`, the filter, the four strategies, and
   tests including the invariants. (Independent, no other package needed.)
2. Config fields, defaults, validation, env overrides, and tests; default strategy change.
3. `internal/api`: the `no_capacity` error and tests.
4. Dial guard and tests.
5. Snapshot cache and tests (fake client and clock).
6. Router (selection, in-flight overlay, release) and tests.
7. Gateway wiring for registry mode: handlers, `/v1/models`, `/readyz`, `cmd/gateway`.
8. Integration and process tests; `dev-cluster` gateway.
9. ADR-011, `scheduling.md`, README, ARCHITECTURE; run the gates; independent verification
   and review; fix findings; second verification; harden; commit in small logical commits;
   prepare the PR and stop for approval.

Steps 1, 2–3, and 4 can proceed independently; 5–7 depend on them.
