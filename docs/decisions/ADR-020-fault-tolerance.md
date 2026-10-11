# ADR-020: Fault Tolerance (Circuit Breakers, Retry Budget, Bounded Admission, Drain)

Status: Accepted in part. Phase 15 is delivered as seven pull requests (plan: `docs/plans/active/phase-15-fault-tolerance.md`, decisions D1 to D27). This record
is extended by each of them. Sections are marked **[PR A]** (written, in force) or **[later PR]** (placeholder: the decision is in the plan, the text lands with
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

### [later PR] Circuit breaker, retry budget, deadline and taxonomy (PR C, D)

Decisions D4 to D10 of the plan. Text lands with PR D.

### [later PR] Admission queue, hand-off, shedding and the global cap (PR E)

Decisions D11 and D12. Text lands with PR E.

### [later PR] Drain contract (PR F)

Decisions D15 and D16. Text lands with PR F.

### [later PR] Defaults, the default-on consequences and the D25 list (PR G)

Text lands with PR G, with `docs/operations/resilience.md`.

## Consequences (PR A)

- A panic in a registry-mode request no longer leaks a worker slot, and every started attempt is observable as ended. The tracing and event observers see a
  `failed` attempt with class `panic` where they previously saw nothing; the event's per-attempt `failure_class` can therefore be `panic`.
- Unknown models stop costing quota in registry mode. Tenants that relied on the old charge as a crude 404 throttle lose it.
- `scheduler_decisions_total` and `scheduler_no_capacity_total` can rise on traffic with retries where only one worker serves the model; they now describe every
  scheduling decision.
- Everything else is byte-identical to the previous gateway (verified by a master-versus-branch differential recorded in the plan's implementation progress).
- Risk: a custom `Observer` implemented without embedding `NopObserver` stops compiling until it adds the two methods.
