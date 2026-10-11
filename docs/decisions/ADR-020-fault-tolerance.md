# ADR-020: Fault Tolerance (Circuit Breakers, Retry Budget, Bounded Admission, Drain)

Status: Accepted in part. Phase 15 is delivered as seven pull requests (plan: `docs/plans/active/phase-15-fault-tolerance.md`, decisions D1 to D27). This record
is extended by each of them. Sections are marked **[PR A]**, **[PR C]** (library and configuration only), **[PR D]** (breaker, budget, deadline and taxonomy wired) or **[later PR]** (placeholder: the decision is in the plan, the text lands with
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
  routing it waits, and is spent when a routing waiter stops: `Park` returns true (route again), `Done` and `Leave` pass it on, a waiter
  that arrives meanwhile does not take it (it stays behind the earlier waiter that is still routing, whose `Park`, `Done` or `Leave` spends the credit on the first
  idle waiter in queue order), and a waiter that leaves without having used its wake-up (timeout, cancel) gives that wake-up back as credit. Invariant: credit is outstanding only
  while a waiter is routing and every waiter ahead of the first idle one is routing, so a freed slot is never left unused while an idle waiter has nobody
  routing ahead of it. A unit of credit can be spurious (the routing
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

### [PR D] Circuit breakers, retry budget, first-byte deadline and failure taxonomy, wired (D4-D10, D13, D14)

With `resilience.mode: on` in registry mode the gateway now uses the library of PR C. With `mode: off` (the code default until PR G) or with static workers
`Server.res` is nil, every call site returns before touching it, the series are not registered, and requests, headers, bodies, logs and `/metrics` are byte-identical
to the previous gateway (master-versus-branch differential on 40 request shapes, below). Files: `internal/gateway/resilience.go` (state, breaker-change dispatch),
`resilience_attempt.go` (taxonomy, peek, budget and deadline checks, tickets), `metrics_resilience.go`, and small edits to `attempt.go`, `router.go`, `snapshot.go`.

**Failure taxonomy and the verdict point (D4, D7).** At the retry-decision point (`sendAttempt` has returned: headers plus, for a 200, the first byte) each attempt gets one
verdict. Connect error, reset or EOF before headers, an empty first chunk, 502, any 503 whose peeked code is not `queue_full`/`draining`/`not_ready`, a header timeout, a stalled
first byte, 504 and any unexpected transport error are faults; a 503 with one of the three codes, a 4xx, a client that left, a first-byte deadline expiry and a panic are neutral;
a 2xx is ok; an unlisted 5xx (500, 501, 505..) and a 3xx feed the ratio rule only (`RatioFault`: never extend or break a streak). Retry eligibility is unchanged from ADR-012.
A 503 whose body is not peeked (no retry possible: attempts used up, deadline or budget denied) counts as a fault, the default row of the table.
**Deviation (D7): the success of a closed breaker's attempt is counted when its response completes, not at the first byte.** A stream that aborts after its first byte
would otherwise add an ok at the first byte (resetting the consecutive streak and diluting the ratio to exactly one half) and then a fault, so a worker that aborts every stream
could not trip the consecutive rule and could trip the ratio rule only once its healthy history left the window. A closed breaker's ticket holds nothing, so there is nothing to
return at the first byte; the probe's verdict is still taken at the first byte (its ticket must not be held for the length of a stream), a stream that aborts after the first byte
is `RecordLate` with the attempt's own ticket (a fault, or a reopening for a probe), and a client that leaves proves nothing either way. `AttemptEnd.Verdict` reports `ok` for
such an attempt until it fails late, then `fault`.

**The 503 peek (D4).** When a retry is possible (attempts left, `retry.min_remaining` and the budget allowing), the first 1 KiB of a held retryable 503's body is read for at most
`min(250 ms, time left for the first byte)` on a separate goroutine, and `error.code` is extracted with a streaming JSON scan that tolerates a cut-off body. A read that times out,
ends short, holds no code or holds any other code is a fault (the retry still goes ahead). The peeked bytes are put back in front of the body; if the wait ran out, a response that is
relayed after all (no other worker) is joined with the still-running read first, which the idle timeout bounds, and a response that is abandoned ends the read by closing the body. The
body is never logged, never a label and never an event field. A worker can lie to dodge the breaker (trust boundary, unchanged from the plan's risk list). The peek runs before the
retry is routed, so a retry for which no other worker exists also pays it (bounded) although its verdict could not matter; the plan's list of skip cases is attempts exhausted, budget
denied and deadline denied.

**Retry budget and deadline order (D5, D6).** `NoteFirst(model)` is called for every routed attempt number 1. When a retry is wanted the deadline (`retry.min_remaining`) is checked first
(a denial reserves nothing), then the budget is reserved atomically, both **before** the retry's routing; a missing worker refunds the reservation; a denial relays the held response
and sets `AttemptEnd.RetryDenied` (`budget`, `deadline`) on the first attempt, with no `AttemptStarted` for the retry and no attempts header. The budget key is the registry-confirmed
model with the metrics model cap (`other` beyond it); it is pruned with each snapshot.
The first-byte deadline is a timer per attempt that cancels the attempt's context and is disarmed when the retryable phase ends (the first byte arrived, or a final response was
received): a context deadline cannot be lifted, and the attempt's context also governs the body stream that follows, so "no effect after the first byte" needs a timer (deviation from
"a context deadline"). Expiry before any byte is a 504 `UPSTREAM_TIMEOUT` with attempt class `deadline` (a new class value, neutral for the breaker). The derived default is
`upstream_header_timeout + upstream_idle_timeout + 5 s` (the dial timeout): the transport's header timeout starts after the connection is made, so without the dial allowance the
derived deadline could cut an attempt the previous gateway served (deviation from "header + idle"; an explicit value is used as given). A probe attempt has its own header timeout
(`breaker.probe_header_timeout`, applied only when below the gateway's) as a second timer that is disarmed when the headers arrive.

**Breakers in routing (D7-D10).** `snapshotCache` reports the view to the breakers after every successful refresh and, between refreshes, whenever aging has turned an eligible worker
suspect (`nextAge`, one comparison per `View`; the aged eligible set is what the ejection cap divides by). `Route` marks workers whose breaker is not admissible ineligible in its private copy
before the scheduler (so `eligible_workers` stays truthful), then the chosen worker calls `Acquire` inside the routing critical section; a lost ticket race drops that candidate from the
copy and the scheduler chooses again inside `Route`, before any observer fires and without a slot taken. The `Admissible` pass is skipped while no breaker is believed open (an atomic
count maintained from the change hook and corrected at every sync), and `Acquire` stays authoritative, so a stale zero costs one re-select. `/readyz` does not consult breakers. When
every eligible worker is ejected the refusal is the usual `503 NO_CAPACITY` with `Retry-After` the time to the earliest half-open breaker (1-30 s; the library's value covers all models).
The ticket is settled exactly once: by the verdict at the decision point, or by the attempt's cleanup (`returnTicket`: panic, client gone, deadline, abandoned) -- the D2 cleanup of PR A.

**Transitions (D13).** The breakers' change hook runs after the library's lock but can run while the router's lock is held (`Acquire` is called inside the routing section), so it only
appends to a bounded queue (4096; the newest beyond that is dropped and counted). `flush` delivers the queued changes to the observers in order, from the request goroutine that finds
them, after a routing, after a verdict, after a registry refresh and after a sync; one goroutine delivers at a time and picks up what others queued. No goroutine of its own, nothing
delivered under a gateway lock (a test calls back into the router from `BreakerTransition`). A trip held back by the ejection cap arrives as `To: "closed"`, `Reason: "suppressed"`.

**Seam (D13; amends ADR-016).** Fired now: `BreakerTransition`, `AttemptStart.Probe`, `AttemptEnd.RetryDenied`. `AttemptEnd` also gains `Probe` and `Verdict` (`ok`, `fault`, `neutral`, or
empty without resilience) -- a deviation from "fields exactly as stated": the probe counter needs the verdict and the observer is the one path events take. `QueueWaited`,
`AttemptStart.QueueWait` and the `overload` rejection stay unfired (PR E).

**Series, spans, logs (D14).** Registered only when effective: `circuit_breaker_transitions_total{model,state}`, `circuit_breaker_probes_total{model,result}`,
`retries_denied_total{model,reason}`, `breaker_trips_suppressed_total{model}`, `inference_deadline_exceeded_total{model}` (new; the plan named a deadline counter only implicitly), and the
scrape-time `circuit_breaker_state{worker_id,model}` (1 half-open, 2 open; non-closed breakers only; worker labels capped like the others, later workers folded into `other` with the
highest state) and `circuit_breakers_open{model}` (open or half-open breakers per served model, 0 when none). No new label names. The granted retries are the existing
`inference_retries_total`. With 1,000 workers and 1,000 models the new families hold 1,009 series (test: ceiling 1,500; the gateway's own ceiling is unchanged). Spans: `serverflow.breaker.probe` on the
attempt span and a `retry.denied` span event with `serverflow.retry.denied` (budget, deadline, else `other`); both keys are on the allow-list. Logs: one `circuit breaker changed state` line per
transition (WARN on open, INFO otherwise), a line for a suppressed trip, DEBUG lines for denied retries, and `verdict`, `probe` and `retry_denied` fields on the attempt line when set.
Kafka events are unchanged (schema v1): the attempt-level `failure_class` can now be `deadline`; a deadline expiry is a `timeout` terminal; nothing else changes (tests).

**Limits found while implementing (kept, reported).**
- The ratio rule works over the sliding window, which still holds the worker's healthy history: a worker that fails every request needs as many faults as it had successes in the window
  (up to `window`) before the ratio trips. A fault rate equal to `failure_ratio` (50% of the default) sits on the threshold and trips by chance. The consecutive rule is what ejects a hard failure quickly.
- A probe whose stream breaks after its first byte has already passed (the verdict is taken at the first byte): the breaker closes, lets the next requests through and opens again after
  `consecutive_failures` further aborts, so each cool-down cycle can cost that many failed streams for a worker that always breaks mid-stream. Taking the probe's verdict at the end of the stream
  would hold its ticket for the length of the stream, which the plan rules out.
- `breakers.RetryAfter()` is computed over every model's breakers, so `Retry-After` can be shorter than the model's own earliest half-open.

**Alternatives rejected.** Recording the mid-stream abort as a fault after an ok at the first byte (diluted the ratio, reset the streak); delivering transitions under the router lock (a callback
that touches the router would deadlock); a goroutine for transition delivery (more lifecycle for events that are a handful per minute); peeking after routing (it would make the verdict of a
retry whose routing fails depend on a guess).

### [later PR] Admission queue, hand-off, shedding and the global cap (PR E)

Decisions D11 and D12. Text lands with PR E.

### [later PR] Drain contract (PR F)

Decisions D15 and D16. Text lands with PR F.

### [later PR] Defaults, the default-on consequences and the D25 list (PR G)

Text lands with PR G, with `docs/operations/resilience.md`.

## Consequences (PR D)

- With `resilience.mode: on` (registry mode) a worker with sustained faults is ejected by its breaker (`consecutive_failures` 5 in a row, or the ratio rule) up to the 50% cap and receives only probes
  (one live request at a time) until a probe passes; retries are subject to the budget (`max(10, 20% of first attempts)` per model per 10 s): some retries that used to happen are denied and the failed
  worker's response is relayed (the client sees the worker's 502/503 instead of a retried success); a retried 503 first waits at most 250 ms for its body; a request that sees no output within the
  first-byte deadline gets 504 `UPSTREAM_TIMEOUT` (never shorter than one attempt today). A probe of a frozen worker can cost one client a 504 after the probe header timeout.
  New series appear (list above). Logs gain one WARN per breaker open and `verdict`/`probe`/`retry_denied` fields on attempt lines.
- With `mode: off` (the code default until PR G) and with static workers nothing changes: byte-identical requests, headers, bodies, logs and `/metrics`; the golden series files are unchanged.
- Risk: misclassifying overload as a fault removes capacity when it is scarce. The cap (50%), the neutral codes, the peek and the 10 s cool-down limit it; a real vLLM sidecar's 503 vocabulary is unknown
  (the code list is `neutral503Codes`). A terminal 503 that is not peeked counts as a fault.
- Risk: a lying worker can answer `queue_full` to stay in rotation (trust boundary: registration).

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
