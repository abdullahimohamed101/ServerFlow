# ADR-020: Fault Tolerance (Circuit Breakers, Retry Budget, Bounded Admission, Drain)

Status: Accepted in part. Phase 15 is delivered as seven pull requests (plan: `docs/plans/active/phase-15-fault-tolerance.md`, decisions D1 to D27). This record
is extended by each of them. Sections are marked **[PR A]**, **[PR C]** (written; PR C is library and configuration only, nothing is wired) or **[later PR]** (placeholder: the decision is in the plan, the text lands with
the pull request that implements it).
Date: 2026-10-11

## Context

ADR-012 left four things to Phase 15: circuit breakers, a retry budget, backpressure and the panic gap named in ADR-016. ADR-015 and ADR-017 left
two small defects that belong to the same code: an unknown model was charged to the tenant's quota, and a retry whose second routing failed was not counted.
PR A closes the three defects that need no configuration and adds the seam the rest of the phase builds on.

## Decision

### [PR A] Panic-safe attempts (D2, debt 1)

`forwardRegistry` used to register its cleanup after the send loop, so a panic in the request recorder, in the upstream call or between two attempts skipped
`AttemptEnded` and left the worker slot taken, forever, with the worker's overlay count one too high.

- `routeAttempt` is split into `reserveAttempt` (the router picks a worker and takes its slot; no observer, no record) and `startAttempt` (record, `AttemptStarted`,
  the attempt's context and idle clock). A run is tracked by an `attemptGuard` the moment its slot is taken.
- One deferred cleanup is installed right after the first reservation. When a retry replaces an attempt, the next run is tracked and made current **before** the old
  one is closed, so the guard always reaches both; a panic while closing the old one (for example in closing its response) cannot leak the next slot.
- `abandonAttempt` is idempotent and defers each of its steps (stop the idle clock, cancel, close the response, release the slot, end the attempt), so one step
  panicking does not skip the others.
- Contract: **every `AttemptStarted` is followed by exactly one `AttemptEnded`, including on panic, and never for an attempt that was not started.** A panic ends
  the open attempts `failed` with class `panic`. The panic is re-raised unchanged; `withRequest` still answers 500. `http.ErrAbortHandler` is not classed a panic.
- Alternative rejected: recover and convert the panic into an error in the attempt code. The request has already lost its normal flow and `withRequest` owns the 500.
- Not changed: static mode already ended its single attempt in a deferred recover (`endStaticAttempt`) and keeps its behaviour.

### [PR A] Model pre-check before the rate limiter (D3, debt 2)

In registry mode `snapshotCache.Serves(model)` (no copy; true when any worker, eligible or not, serves the model) runs before the limiter. A model nobody serves is
answered `404 MODEL_NOT_FOUND` with the sequence `RequestStarted, RequestAdmitted, RequestRejected(model), RequestCompleted` and **no request, token or concurrency
quota taken**. The refusal is logged and counted as before (`no worker selected` at debug, `scheduler_decisions_total{result="no_model"}`); the time the check took
is reported as the decision duration. With no usable snapshot nothing is known and the request reaches the limiter as before (it is then refused 503 later). A model
whose workers are all lost is "served" and still charged. A model that vanishes between the check and routing 404s after the charge (rare, accepted).

Consequences visible to operators: `rate_limit_decision_seconds` no longer observes unknown-model requests (no decision is made for them), and the
`Admission` event of such a request has `RateLimitChecked=false` and `EstimatedCost=0`. The request log line of an unknown-model 404 no longer carries `est_cost` (about 4124 on the previous gateway), because the request is refused before the cost is estimated and the limiter is consulted.

### [PR A] A retry whose second routing fails is counted (debt 3)

The first routing's refusal reaches the metrics observer as a `Rejection`; a retry's does not (the client gets the first attempt's response). `AttemptEnd` now carries
`NextRouteResult` (`no_capacity`, `worker_unavailable`, `error`) and the metrics observer counts it in `scheduler_decisions_total` and `scheduler_no_capacity_total`
with the same result and reason a first routing's refusal gets. It is empty when the client left during the second routing. No new series are created
(`scheduler_decision_duration_seconds` is not observed for it).

### [PR A] The typed routing result

`router.Route` returns `*routeError`, which embeds the `*api.Error` to send and carries the scheduler's counts (`Serving`, `Eligible`, `AtCapacity`) that were lost
at the router boundary. Later PRs need `AtCapacity > 0 && Eligible == AtCapacity` to decide that a request may wait. It is internal; no behaviour depends on it yet.

### [PR A] Observer seam additions (D13; amends ADR-016)

Stubs only; see the ADR-016 amendment and `docs/development/observers.md`. `AttemptEnd.NextRouteResult` (live, above), `AttemptEnd.RetryDenied`,
`AttemptStart.Probe`, `AttemptStart.QueueWait`, `Rejection` kind `overload` with its four reasons, and the methods `QueueWaited` and `BreakerTransition`
(`NopObserver` defaults, panic-contained fan-out). They are never fired until the features land, which is pinned by tests. The metrics observer ignores `overload`
explicitly: shedding is admission, not scheduling.

### [PR A] 503 versus 429 for overload (research recorded for PR E)

Overload will be refused `503 NO_CAPACITY` with `Retry-After`; `429` stays reserved for a tenant exceeding its own quota (`RATE_LIMITED`, Phase 8). RFC 9110 section
15.6.4 defines 503 as the server being unable to handle the request because of overload or maintenance, with `Retry-After` as the estimated recovery; section 15.5.30
and RFC 6585 define 429 as a user sending too many requests. OpenAI's error documentation uses 503 ("engine currently overloaded") for capacity and 429 for account
limits, but developers report 429 with overload bodies, which makes 429 ambiguous for clients; Anthropic uses the non-standard 529. Clients tell the two apart by status
and body code. No 529. The body shape (`error.message/type/code` plus `model` and `eligible_workers`) is unchanged.

### [PR C] The resilience library and its configuration (D1, D7-D9, D11; unwired)

`internal/resilience` holds the building blocks as a pure library: standard library plus `internal/config`, no gateway import (a test enforces both),
no goroutines or timers, an injectable clock and jitter source, every state change evaluated lazily when a method is called. `resilience.mode` is added to
the configuration and **defaults to `off` in code until PR G**; the section is validated only when the feature is effective (`mode: on` and registry
workers), so `on` with static workers is a valid no-op. Nothing in the gateway reads either yet.

**Breakers** (`Breakers`: `Sync`, `Admissible`, `Acquire`, `Record`, `RecordLate`, `Release`, `Prune`, `Snapshot`, `Counts`, `RetryAfter`).
- Keyed by worker ID plus registration time. The gateway reports the registry view with `Sync` (workers gone are pruned, a new registration starts closed,
  eligibility is updated); a worker not in the last `Sync` has no breaker and is admissible. Changes are delivered to a hook after the lock is released.
- Verdicts: `OK`, `Fault`, `RatioFault` (an unlisted 5xx; feeds the ratio rule and never the consecutive rule, and neither extends nor breaks a streak),
  `Neutral` (nothing is recorded, a probe ticket is returned).
- Consecutive rule: the last `consecutive_failures` faults with no OK between, the oldest no older than `window` (inclusive). Ratio rule: counted outcomes
  in the sliding window (one-second buckets, `ceil(window / 1 s)` of them) reach `min_volume` with faults / outcomes >= `failure_ratio`; evaluated after
  every counted outcome, so an OK that completes the volume can trip it. `failure_ratio: 0` switches the rule off (a deviation: the plan said bounds 0-1
  without a meaning for 0).
- Lazy states and tickets as in D8. A probe verdict is accepted only for a ticket still outstanding in the current half-open period; a stale, released or
  lost ticket is ignored. A verdict that arrives for a non-probe ticket after the breaker opened is ignored; a late mid-stream fault (`RecordLate(ticket, v)`) counts
  like any fault while the breaker is closed; for a probe ticket of the current half-open period it reopens the breaker with a doubled cool-down, so an
  aborted probe stream is not a success when `probe_successes` > 1; otherwise it is ignored (the breaker already acted).
- Cool-down: `cool_down x 2^level` clamped to `max_cool_down`, then jittered by a factor in [0.8, 1.2) and clamped again, so the cap is never exceeded
  (deviation: the plan did not say whether the jitter could push above the cap). `level` grows on a failed probe only. On a trip from closed it is kept
  unless the breaker has been closed for at least `max_cool_down`, in which case it resets; so a worker that passes a probe and fails again keeps its grown
  cool-down but does not grow it further.
- Ejection cap: `floor(max_open_fraction x eligible)` (with a small epsilon so 0.29 x 100 is 29), counted over registry-eligible workers only, numerator open or
  half-open. A trip the cap forbids keeps the window and streak, is reported once per episode (`Change.Suppressed`, `Suppressed()`), and happens on a later
  outcome once there is room. A worker that is itself not eligible is outside the cap: its own trip takes no room (it cannot be selected anyway). Because that can leave more breakers open than the cap allows once the worker is eligible again (or once healthy workers turn unhealthy and the cap shrinks), `Sync` and `Prune` enforce the cap: the most recently ejected breakers (ties: the larger worker ID) are closed (`Change.Reason` `ejection_cap`, fresh window, cool-down level kept) until open + half-open <= `floor(fraction x eligible)`. The longest-standing evidence is kept; moving the extra breaker to half-open instead would still count against the cap and not help.
- Alternative rejected: counting the cap over all registered workers. A lost or unhealthy worker with an open breaker would then block ejecting a real
  failure (test: 4 workers with 2 unhealthy, and a lost worker's open breaker).

**Budget** (`Budget`: `NoteFirst`, `Reserve`, `Refund`, `Usage`, `Prune`). The limit is `max(min, floor(ratio x first attempts))` over a sliding window of
one-second buckets, so a ratio of 0.2 never rounds up past 20%. `Reserve` checks and increments in one critical section (no overshoot under concurrency) and
returns a `Reservation`; `Refund` takes it back from the bucket it was taken in and does nothing once that bucket left the window. Models beyond
`metrics.max_models` share the `other` window.

**Queue** (`Queue`: `Enqueue`, `Busy`, `Full`, `Handoff`, `Wake`, `Park`, `Done`, `Leave`, `Wait`, `Flush`, `Close`, `RetryAfter`) and **global cap**
(`Admission`).
- Strict FIFO with direct hand-off. `Busy(model)` is true while any waiter exists, including a waiter that is routing after a wake-up, and an arrival that finds
  the model busy must enqueue. `Handoff(model, value)` removes the head waiter and gives it `value` in one step under the queue lock; the caller holds its own
  lock around it (the router's), so the lock order is caller then queue and the queue never calls out.
- A refresh wakes waiters with `Wake(model, n)`: they receive `Reroute`, stay in place, route through the normal path, and call `Done` (served) or `Park`
  (still full). A slot released while the head is routing cannot be handed over (`Handoff` declines and the caller frees it normally); it becomes one unit of
  **credit** on the model's queue. Credit is spent at once on the first waiter, in queue order, that is not routing (it receives `Reroute`); if every waiter is
  routing it waits, and is spent when a routing waiter stops: `Park` returns true (route again), `Done` and `Leave` pass it on, a new waiter takes it at
  `Enqueue`, and a waiter that leaves without having used its wake-up (timeout, cancel) gives that wake-up back as credit. Invariant: credit is outstanding only
  while every waiter is routing, so a freed slot is never left unused while a waiter that could take it is idle. A unit of credit can be spurious (the routing
  waiter took the slot itself); the cost is one failed routing attempt, and every unit is spent once, so nothing loops or ping-pongs. Hand-offs stay strictly FIFO;
  the order in which concurrently woken waiters win slots is not guaranteed. `Flush` ends all waiters and settles nothing.
- `Leave` removes a waiter at once and, if a hand-off had already been delivered, returns it so the slot is never dropped; `Wait` applies the same rule when the
  timer or the request context fires. `Flush` hands every waiter `Flushed` (gateway drain); `Close` makes `Enqueue` fail with `ErrDraining`.
- Refusals: depth (`ErrQueueFull`), tenant share (`ErrTenantShare`), no queue for the model (`ErrNoQueue`: depth 0, or more distinct queued models than
  `metrics.max_models`, so client text cannot create queues), draining; `ShedReason` maps them to `queue_full` or `draining`. The tenant cap is
  `max(1, floor(share x depth))` and never applies to unauthenticated requests. Per-tenant occupancy therefore lives in `Queue`, not in `Admission`
  (a deviation from the plan's wording: it is a share of a model's depth).
- `Admission` is an atomic counter with idempotent `Slot.Release`; `admission.max_global_requests` is its cap (0 would mean unlimited in the library, config
  validation keeps the key above 0).

**Configuration** (`internal/config/resilience.go`). Defaults, bounds and cross-checks are those of D1, except that two defaults follow the gateway's own settings so a configuration that is valid today stays valid when the default flips to `on`: an unset `probe_header_timeout` is min(10s, `upstream_header_timeout`) and an unset `readiness_grace` is min(5s, `shutdown_timeout`/2); only explicit values are cross-validated (`EffectiveProbeHeaderTimeout`, `EffectiveReadinessGrace`). A suppressed consecutive trip happens on the next counted fault of either kind. An unset `queue.max_wait` is min(3s, the effective first-byte deadline) (`EffectiveMaxWait`), so a configuration with small upstream timeouts stays valid. Tickets carry the breaker's epoch (the number of times it has opened): a verdict or late fault from a request that started before the breaker last opened is ignored in every state. `Reservation` is a pointer and `Refund` idempotent. The environment overlay covers every key
(`SERVERFLOW_RESILIENCE_*`), ignores values that do not parse or are not finite, and keeps a mode other than `on`/`off` so that it fails validation
(deviation: a silently ignored typo in the kill switch would leave the default, which becomes `on`, in force).

Verification: table tests with a fake clock and scripted jitter for every transition, a randomized model check of the breaker against a naive reference
that rescans an event list (4 parameter sets, 60 seeds, 400 operations each, with random re-registration, eligibility changes, pending tickets and clock
steps; the run must reach every kind of transition), the 1/2/3/4-worker cap table and the ineligible cases, budget window edges, queue tests and a `-race`
stress (400 concurrent requests: capacity never exceeded, no waiter lost, no double hand-off, hand-offs in enqueue order, goroutines back to baseline;
10,000 cancellations leave nothing behind).

### [later PR] Circuit breaker wiring, retry deadline and failure taxonomy (PR D)

Decisions D4 to D10 as wired into the gateway; the library above implements D7 to D9. Text lands with PR D.

### [later PR] Admission queue, hand-off, shedding and the global cap (PR E)

Decisions D11 and D12. Text lands with PR E.

### [later PR] Drain contract (PR F)

Decisions D15 and D16. Text lands with PR F.

### [later PR] Defaults, the default-on consequences and the D25 list (PR G)

Text lands with PR G, with `docs/operations/resilience.md`.

## Consequences (PR C)

- A new unused package and an inert configuration section. `resilience.mode` is `off` and nothing reads it; behaviour, metrics and logs are unchanged.
- `resilience.*` keys are accepted in files and the environment from now on, so a configuration prepared for PR D already loads.

## Consequences (PR A)

- A panic in a registry-mode request no longer leaks a worker slot, and every started attempt is observable as ended. The tracing and event observers see a
  `failed` attempt with class `panic` where they previously saw nothing; the event's per-attempt `failure_class` can therefore be `panic`.
- Unknown models stop costing quota in registry mode. Tenants that relied on the old charge as a crude 404 throttle lose it.
- `scheduler_decisions_total` and `scheduler_no_capacity_total` can rise on traffic with retries where only one worker serves the model; they now describe every
  scheduling decision.
- Everything else is byte-identical to the previous gateway (verified by a master-versus-branch differential recorded in the plan's implementation progress).
- Risk: a custom `Observer` implemented without embedding `NopObserver` stops compiling until it adds the two methods.
