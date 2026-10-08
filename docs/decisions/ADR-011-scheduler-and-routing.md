# ADR-011: Scheduler Interface, In-Flight Overlay, Bounded Snapshots, Dial Guard

Status: Accepted
Date: 2026-10-07

## Context

Phase 4 gave the control plane a registry of workers. Phase 5 lets the gateway
choose among them. Four questions needed a decision: what a scheduler looks like,
how fresh its view of the workers must be, what the gateway may connect to, and how
a burst is kept from piling onto one worker whose last heartbeat said it was idle.

## Decision

- **Interface.** Spec section 13, unchanged: `SelectWorker(ctx, req, workers)`.
  Strategies do no I/O and read no clock; their only state is a seedable random source and a
  per-model round-robin position, so tests need no clock, network, or sleeps. Four
  strategies ship: random, round-robin, least-active, least-queue. Ties among the
  least loaded take turns in worker-ID order, so they neither favor the lowest ID nor
  starve anyone.
- **Invariants live in one function.** The shared filter (`Candidates` is its copying form;
  the strategies use an index-based twin so the hot path copies one worker) drops every
  worker that is not eligible, serves another model, is not READY, or is at its concurrency
  limit, before any strategy runs. Strategies cannot violate spec section 15 because
  they never see a violating worker. When nothing remains, a typed error says why.
- **In-flight overlay.** Worker metrics are up to one heartbeat old. The router counts
  the requests this gateway has in flight per worker and gives the scheduler
  `ActiveRequests = max(reported, local)`. The maximum, not the sum: the worker's report
  already includes requests sent earlier, and adding would count them twice. Selection and
  reservation happen under one lock so two requests cannot both take the last slot.
  It is per gateway; other gateways are invisible until Phase 8.
- **Bounded snapshots, then fail closed.** The gateway polls `GET /v1/workers` and routes
  from the last good answer. A snapshot older than `registry_max_staleness` (default 10s)
  is not used, and within that bound each worker's heartbeat age is advanced by the
  snapshot's age: once it would have reached `worker.suspect_timeout` the worker is no
  longer eligible. During a control plane outage the workers cannot heartbeat either, so
  silence is not health. In practice a cached worker is trusted for at most about the
  suspect threshold (default 5s) after its last heartbeat, which is shorter than the
  staleness bound. The snapshot's age is measured from when the fetch began, the
  conservative end.
- **Dial guard.** The registry validates how an address is spelled; it cannot see what a
  hostname resolves to. The gateway's transport therefore checks the IP it is about to
  connect to (a `net.Dialer.Control` hook, after name resolution), and refuses
  unspecified, link-local (includes the cloud metadata address), multicast, and broadcast
  addresses, with IPv4-mapped forms judged as the IPv4 address. Well-known cloud metadata
  endpoints outside the link-local block (Azure 168.63.129.16, Alibaba 100.100.100.200, AWS IPv6
  fd00:ec2::254) and NAT64 or 6to4 forms wrapping a forbidden IPv4 address are refused too. The
  gateway also refuses its own listener address, so a worker cannot loop requests back through it.
  Loopback and private ranges stay allowed by default because workers live there, which means
  registration is the trust boundary: anyone who may register can point the gateway at an internal
  address. `gateway.worker_networks` is the real control, and the gateway warns at startup when it is
  empty and the control plane is not on loopback. `gateway.worker_networks` narrows it to
  listed CIDRs. A listed network never makes a forbidden address dialable. Proxies are off
  and redirects are never followed.
- **Errors.** Unknown model: 404 `MODEL_NOT_FOUND`. A served model with no selectable worker:
  503 `NO_CAPACITY` with `Retry-After: 1` and the model and the number of workers the
  registry calls eligible (before the concurrency check, so "all full" and "all dead" read
  differently). The body is OpenAI-shaped (`error.code`, `error.model`,
  `error.eligible_workers`), not the flat example in spec section 15; the fields are the same.
  If registry refreshes have been failing for more than two refresh intervals, the answer is
  `WORKER_UNAVAILABLE` instead, since the cause is the registry, not capacity. No usable snapshot, or an unreachable worker: 503 `WORKER_UNAVAILABLE`.
- **`least-queue` and the concurrency cap.** A worker at `MaxConcurrency` is never selected (spec
  section 15), and a worker normally queues only once it is at that limit, so among selectable workers
  `QueueDepth` is usually 0 and `least-queue` behaves like round-robin until reports are stale or
  queues form for other reasons. The strategy exists for backends whose queue forms earlier (real
  vLLM; Phase 13), and Phase 7 will show whether it matters.
- **Metrics labels.** The model name becomes a Prometheus label only after the registry
  confirmed the model exists, so client-chosen names cannot create unbounded label values.
- **Static mode stays the default** (`gateway.worker_source: static`), so Phases 2-3 behave
  exactly as before. The default strategy becomes `round-robin`; `least-work` remains a valid
  setting for Phase 14 but the gateway refuses to start with it in registry mode.

## Alternatives

- **Library only, no gateway wiring**: smaller, but the dial guard and cache would have no
  consumer and nothing would be tested end to end.
- **Query the control plane per request**: always fresh, but puts the control plane on the
  hot path and makes every outage a total outage.
- **Sum reported and local load**: simpler, but double counts and wastes capacity.
- **Resolve and check addresses at registration**: a hostname can resolve differently later
  (rebinding), so only the connect-time address is trustworthy.
- **Pass in-flight counts to schedulers as a separate argument**: breaks the section 13
  signature; overlaying them on the snapshots keeps strategies pure.

## Consequences

- **Positive**: strategies are trivially testable and comparable; a dead worker stops
  receiving traffic within a refresh plus the suspect threshold even if the registry itself
  is unreachable; metadata-service access through a worker address is closed at the point
  of connection.
- **Negative**: the selection lock serializes picks within one gateway. Choosing a worker costs
  about 1 microsecond for 3 workers, 4 for 50, and 54 for 1000 serving one model
  (`BenchmarkRoute`, parallel, Apple M5 Pro), against a 25 ms gateway overhead budget; per-gateway counters do not see other gateways;
  during a control plane outage the gateway turns traffic away after a few seconds even
  though workers may be fine; a worker that fails mid-request is a failed request until
  Phase 6 adds retries.
- **Follow-ups**: retries and request attempts (Phase 6); coordination across gateways
  (Phase 8); scheduler metrics (Phase 10); weighted and latency-aware strategies (Phase 14);
  no strategy is claimed better than another until Phase 7 produces evidence.
