# ADR-020: Fault Tolerance (Circuit Breakers, Retry Budget, Bounded Admission, Drain)

Status: Accepted. Phase 15 was delivered as seven pull requests (plan: `docs/plans/completed/phase-15-fault-tolerance.md`, decisions D1 to D27). This record
was extended by each of them. Sections are marked **[PR A]**, **[PR C]** (library and configuration only), **[PR D]** (breaker, budget, deadline and taxonomy wired), **[PR E]** (admission queue, hand-off, shedding and the global cap wired), **[PR F]** (gateway and worker drain) or **[PR G]** (the default flip, the D25 list, observability and the benchmark note).
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
the configuration and **defaulted to `off` in code until PR G, which flipped it to `on`**; the section is validated only when the feature is effective (`mode: on` and registry
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

With `resilience.mode: on` in registry mode the gateway now uses the library of PR C. With `mode: off` (the code default until PR G; now the kill switch) or with static workers
`Server.res` is nil, every call site returns before touching it, the series are not registered, and requests, headers, bodies, logs and `/metrics` are byte-identical
to the previous gateway (master-versus-branch differential on 40 request shapes, below). Files: `internal/gateway/resilience.go` (state, breaker-change dispatch),
`resilience_attempt.go` (taxonomy, peek, budget and deadline checks, tickets), `metrics_resilience.go`, and small edits to `attempt.go`, `router.go`, `snapshot.go`.

**Failure taxonomy and the verdict point (D4, D7).** At the retry-decision point (`sendAttempt` has returned: headers plus, for a 200, the first byte) each attempt gets one
verdict. Connect error, reset or EOF before headers, an empty first chunk, 502, any 503 whose peeked code is not `queue_full`/`draining`/`not_ready`, a header timeout, a stalled
first byte, 504 and any unexpected transport error are faults; a 503 with one of the three codes, a 4xx, a client that left, a first-byte deadline expiry and a panic are neutral;
a 2xx is ok; an unlisted 5xx (500, 501, 505..) and a 3xx feed the ratio rule only (`RatioFault`: never extend or break a streak). Retry eligibility is unchanged from ADR-012.
A 503 whose body is not peeked (no retry possible: attempts used up, deadline or budget denied) counts as a fault, the default row of the table.
**The ok is counted at the first byte and a later abort converts it (D7; refined after verification).** Every outcome is counted at the retry-decision point: faults and oks alike, for the streak and the
ratio, so successes are never counted later than faults. A stream that breaks after its first byte is *converted*: `Breakers.RecordAbort(ticket, okAt)` takes the ok out of the sliding window (when its
bucket is still in it) and counts a fault in its place, so each request counts once with its final outcome; for a probe ticket it is `RecordLate` (the breaker reopens). A client that leaves proves nothing
either way. The library change is additive (`RecordAbort`; its model check and unit tests are in `internal/resilience`); epochs still ignore stale reports. An earlier cut of this PR counted a closed breaker's
success only when the response completed. The verifier showed why that is wrong: while long streams are in flight, faults (which are known at once) pile up with no success to reset the streak or balance the ratio, and
a mostly healthy worker is ejected. The permanent model test `TestSuccessLagDoesNotEjectAMostlyHealthyWorker` (one worker of two, 10 requests a second, a 1,200-request window, 200 seeded runs per cell) gives, as
trips out of 200:

| fault rate | success counted at completion: stream length 0 s / 10 s / 60 s | success counted at the first byte (shipped): 0 s / 10 s / 60 s |
| --- | --- | --- |
| 2% | 0 / 10 / 106 | 0 / 0 / 0 |
| 5% | 0 / 119 / 200 | 0 / 0 / 0 |
| 10% | 1 / 196 / 200 | 1 / 1 / 1 |

Cost of the shipped design: a stream that aborts after its first byte is an ok (resetting the consecutive streak) and then its conversion (a fault), so a worker that aborts every stream does not build a consecutive
streak (the next stream's first byte resets it); the **ratio rule** is what ejects it, once its faults equal its healthy outcomes in the window (with 20 healthy outcomes in the window, after 20 aborts). A re-closed
breaker has an empty window, so after a probe that passed at its first byte, a worker that keeps aborting needs its faults to reach half of the window again, which costs as many streams as are in flight (see the limits below for the simulated scaling). `AttemptEnd.Verdict` reports `ok` for such an attempt until it fails late,
then `fault`.

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
"a context deadline"). The timer and the arrival of the response are decided under one lock when the response arrives (headers, and for a 200 the first byte): if the arrival came first the timer is inert from then on, however long the caller takes to reach the decision point (test with a hook that lets three deadlines pass in that gap). Expiry before any byte is a 504 `UPSTREAM_TIMEOUT` with attempt class `deadline` (a new class value, neutral for the breaker). The derived default is
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
- A probe whose stream breaks after its first byte has already passed (the verdict is taken at the first byte): the breaker closes, lets the next requests through and, its window being empty, needs the faults to reach half
  of the window again before it opens. The window also holds the oks of the streams still in flight, so the cost of a cool-down cycle scales with the number of streams concurrently in flight on the worker (their
  length times the request rate), not with `min_volume`; `min_volume` is only the floor. A worker that aborts every stream builds no consecutive streak (the ratio rule ejects it, after as many aborts as it had healthy
  outcomes in the window). Simulation results, not production data (one worker aborting every stream, 10 requests/s per worker, 10 minutes, 50 seeds): about 38 / 186 / 690 aborted streams per cool-down cycle for streams of 2 s / 10 s / 60 s, and a median time to the first ejection of an always-aborting worker of 3.8 s / 19 s / 69 s (it was ejected in all 50 runs). A worker that aborts 20-30% of its streams, or alternates ok and abort, is never ejected, consistent with the 50% ratio threshold.
- (F3) The 503 peek also runs when the model has only one worker: whether another worker exists is known only after the retry has been routed, and the peek decides the verdict before that, so a single-worker model's
  retryable 503 pays up to 250 ms (usually the time to read a small body) for a verdict that cannot matter (a single-worker model never opens). Skipping it would need the routing result first and would make the verdict of a
  retry whose routing fails a guess; the cost is bounded and tracked here as a known cost. Not changed.
- (F6) Breaker transitions are delivered by the request goroutine that finds them queued, so a slow `BreakerTransition` observer delays that one request (other requests are not held: delivery is single-file and they leave their
  changes to it). At shutdown the queue is flushed once more; a queue that overflowed (4096) drops the newest and logs the count. Taking the probe's verdict at the end of the stream
  would hold its ticket for the length of the stream, which the plan rules out.
- `breakers.RetryAfter()` is computed over every model's breakers, so `Retry-After` can be shorter than the model's own earliest half-open.

**Alternatives rejected.** Recording the mid-stream abort as a fault after an ok at the first byte (diluted the ratio, reset the streak); delivering transitions under the router lock (a callback
that touches the router would deadlock); a goroutine for transition delivery (more lifecycle for events that are a handful per minute); peeking after routing (it would make the verdict of a
retry whose routing fails depend on a guess).

### [PR E] Admission queue, hand-off, load shedding and the global cap, wired (D11, D12, D13, D14)

With `resilience.mode: on` in registry mode the gateway now uses the queue and the counter of PR C. With `mode: off` or static workers `Server.res` is nil, every call site returns before touching anything,
the queue series are not registered, and requests, headers, bodies, logs and `/metrics` are byte-identical to master (master-versus-branch differential on 39 request shapes, below). Files: `resilience_queue.go`
(global cap, pre-limiter check, shed, first routing with the queue, the wait, the shutdown flush), `router.go` (enqueue and hand-off inside the router's critical section), `metrics_resilience.go` (series),
and small edits to `attempt.go`, `handlers.go`, `snapshot.go`, `server.go`, `observer_translate.go`, `metrics.go`.

**Where a request waits (D11).** Only on its first routing, and only when the model is served and every eligible worker is at its concurrency limit (`routeError.full()`: `Eligible > 0 && Eligible == AtCapacity`), or when
other requests are already waiting for the model and at least one worker is schedulable. The decision and the enqueue happen **inside `router.reserve`, under `router.mu`**, which is also what a release holds while it hands a
slot over: there is no instant between "everything is full" and "I am a waiter" in which a slot can be freed with nobody to receive it. A retry's routing and a woken waiter's routing never enqueue. No eligible worker, an unknown
model and an unusable view are immediate refusals as before. The queue key is the registry-confirmed model name; a model beyond `metrics.max_models` gets no queue (`ErrNoQueue`), so client text cannot create one (test with 20 hostile names).

**Strict FIFO with direct hand-off.** An arrival that finds the model's queue non-empty enqueues behind it even when a worker has room. When a request's attempt ends, `routed.release` (under `router.mu`) asks the queue for the head
waiter: if there is one it receives a `routed` for the **same worker**, `inflight[worker]` stays incremented, and the scheduler is not consulted. The hand-off first looks the worker up in the current (aged) registry view, requires it
to be schedulable and takes a **breaker ticket** (`Acquire`), so ejection and draining cannot be bypassed; otherwise the slot is released normally and one waiter is woken (`Wake(model, 1)`) to route through the normal path. If the
head is itself routing after a wake-up, the library banks the slot as credit (PR C). After every successful registry refresh `wakeWaiters` computes the free slots of each queued model from the new view (workers the registry offers and
the breakers admit, load `max(worker-reported, local)`, as the router overlays it) and wakes up to that many head waiters: this is when a stale reported count stops hiding capacity. A woken waiter routes with the normal path, and if
everything is still full parks in place (or, on any other routing failure, also keeps waiting until `max_wait`). Hand-off bypasses the scheduler strategy for the queued request (it inherits the freeing worker); that is a stated
distortion of the strategy's distribution under queueing.

**Waiting.** `awaitSlot` waits on the waiter's channel, the request context and a timer of `queue.max_wait` (default `min(3 s, first-byte deadline)`). Outcomes: hand-off or successful re-route (served), `max_wait` (shed
`queue_timeout`), the client leaving (the waiter leaves at once; recorded as a client that left, 499, not a shed), a flush at shutdown (shed `draining`). `QueueWaited(served|timeout|client_gone|drained)` fires exactly once for a request that
waited, before anything that follows from it; a deferred cleanup removes the waiter and returns a slot that was handed to it if anything else unwinds the request. The wait is measured in real time (a timer decides it). The first-byte
deadline (D6) now starts when the request reaches `forwardRegistry`, so **queue wait counts against it**; `queue.max_wait` is validated not to exceed the effective deadline, so a request that waits the maximum still has
the rest of its deadline for its attempt (test: a request that waited most of a 2 s deadline is cut with 504 when its worker is slow).

**Shedding (D11).** All overload is `503 NO_CAPACITY` with `Retry-After` and the existing OpenAI-shaped body (`error.message/type/code`, plus `model` and `eligible_workers` when known; `api.ErrOverloaded`); the message carries the reason.
429 stays reserved for the tenant's own quota (`RATE_LIMITED`); a test asserts no overload path produces one. Reasons and `Retry-After`:

| reason | when | before the limiter | `Retry-After` |
| --- | --- | --- | --- |
| `global_limit` | `admission.max_global_requests` places are taken | yes, **before the body is read** | 1 |
| `queue_full` | the model's queue is at `max_model_depth`, or the tenant holds its share | yes (only if the queue already has waiters) | EWMA of recent waits, rounded up, 1-10 |
| `queue_timeout` | a waiter passed `max_wait` | no: after the charge, **not refunded** | same |
| `draining` | the gateway is shutting down and the request would have to wait, or a waiter is flushed | queue closed: at enqueue; flush: after the charge | 1 |

The global cap counts from the top of the chat handler (authentication is behind it) until the request ends: body read, queue wait and the whole response, streams included. It is not `inference_requests_active`, which starts before
authentication. Over the cap the arrival is refused without reading its body (`Connection: close`), so a flood costs almost nothing; the model is unknown at that point, so the shed is counted under `model="unknown"`.
The tenant share is `max(1, floor(share x depth))` of a model's queue per authenticated tenant (not applied when authentication is off); it stops one tenant from filling the queue and is not fairness (Phase 22).
The EWMA is fed by every served and timed-out wait of the gateway, not per model.

**Phase 8 order.** The cheap checks run before the limiter so those refusals spend no quota: the global cap, and a model queue that already has waiters and is full for the request (depth reached or tenant share used). A request that
finds an empty queue goes to the limiter and then routes; if it then has to wait it holds its **concurrency lease** until it leaves, whichever way (tested against a fake limiter: charges, releases and leases). A request that passed
the pre-check can still find the queue full at enqueue time (a race) and is then shed after its charge.

**Seam (D13).** Now fired: `QueueWaited`, `Rejection{Kind: overload}` with the four reasons, `AttemptStart.QueueWait` (the first attempt only: the wait precedes it). `RequestAdmitted` precedes a pre-limiter queue shed, as it does the D3 pre-check
(the Admission says no rate limit decision was made), so the Kafka stream sees `received` then one `failed` event with `failure_class=no_capacity`; a `global_limit` shed happens before admission and emits no event (nothing is known about
the request). Sequences, pinned by `reqSeq` tests: served `start admit queue:served attempt+1 attempt-1 done`; timeout `start admit queue:timeout reject:overload:queue_timeout done:503`; shed without waiting `start admit reject:overload:queue_full done:503`; global
`start reject:overload:global_limit done:503`; client gone `start admit queue:client_gone done:499`.

**Series, spans, logs (D14).** Registered only when the feature is effective: `admission_rejections_total{model,reason}`, `inference_queue_duration_seconds{model}` (requests that waited, whatever the outcome; buckets 5 ms to 60 s) and, read at
scrape time, `admission_queue_depth{model}` (0 for every served model). No new label names. **The overhead histogram excludes the wait**: `inference_gateway_overhead_seconds` observes `SinceRequestStart - QueueWait`, so
`ServerFlowGatewayOverheadHigh` keeps its meaning (test: a request that waited 300 ms adds 0.3 s to the queue histogram and nothing like it to the overhead). A shed is never a scheduling decision (it does not feed
`scheduler_decisions_total` or `scheduler_no_capacity_total`). Span: a `queue.wait` **event** on the request's root span with `serverflow.queue.wait_ms` and `serverflow.queue.result` (a closed set), both on the allow-list; it is an event,
not a span, because `queue_wait` is already the name of the worker-side span. Log: one `request shed` line (INFO) per reason at most once a second, with the count held back; it carries the reason, the model when confirmed, the tenant
IDs as every request line does, and never a body, prompt, key or worker text. The metrics observer's queue and rejection methods allocate nothing in the steady state (test and benchmark).

**Shutdown.** When `Serve`'s context ends the queue is closed (arrivals that would have to wait are shed `draining`; arrivals a worker can take are served) and a timer flushes whoever is still waiting 250 ms before the shutdown timeout
(`draining` sheds), so a waiter is answered rather than cut. Waiters that finish earlier (served or `max_wait`) are unaffected. The readiness grace, `Connection: close` and the second-signal behaviour of D16 are PR F.

**Deviations and decisions made while implementing PR E** (numbered for the plan's progress notes):
1. `ErrNoQueue` (depth 0, or a model beyond the model cap) keeps the classic refusal: `NO_CAPACITY`, `Retry-After: 1`, counted in `scheduler_decisions_total{result="no_capacity"}` and `scheduler_no_capacity_total`. It is not an overload shed: with `max_model_depth: 0` the gateway behaves as master.
2. The global cap refuses before the body is read and so has no model, no `RequestAdmitted`, no Kafka event and `model="unknown"` in `admission_rejections_total` (the plan said "from just after authentication (body read included)" and "refused at once").
3. The pre-limiter queue check runs only when the queue already has waiters (`Busy`); the authoritative refusal is the enqueue.
4. A woken waiter that cannot be routed for any reason keeps waiting until `max_wait`; the "no eligible worker is an immediate refusal" rule applies to arrivals only. An arrival that finds a busy queue but no schedulable worker is refused at once, not queued.
5. The shed body omits `eligible_workers` when the count is not known (a busy queue, a timeout) instead of saying 0.
6. The first-byte deadline starts at `forwardRegistry`, before the queue (it was set after the first routing); with an empty queue this is the same instant.
7. `AttemptStart.QueueWait` is set on attempt 1 only.
8. Tracing: an event, not a span (above). 9. `gateway_draining` and the readiness behaviour are PR F's. 10. Tenant occupancy lives in `Queue`, as in PR C.
11. Library change (additive): `Queue.Refusal(model, tenant) error` (the error `Enqueue` would return), used by the pre-limiter check.
12. Harness: the chaos windows gain `gateway_queued`, `gateway_shed`, `queue_wait_mean_ms` and `queue_depth_max` (omitted when zero, so the committed baselines keep their keys), and the `overload` scenario's evidence accepts queueing as well as refusals.

**Alternatives rejected.** Waking and re-routing on release (the first draft: a stale reported count makes the woken waiter fail deterministically, and arrivals can jump the queue); enqueueing after the routing returned (a slot freed in
the gap is lost until the next refresh); a goroutine per queue for timeouts (the caller already has the timer and the context); sharing one queue across overflow models (they would block each other); refusing the global cap
after parsing the body (costs a body read per refused request at the moment the gateway is least able to afford it); returning the shed as 429 or 529 (above).

**Limits found while implementing (kept, reported).**
- Strict FIFO means a request that arrives while others wait queues behind them even if its worker would have room (the point), and a retry's routing bypasses the queue (it may take a free slot ahead of waiters; its own
  failed attempt's slot is handed to the head).
- Hand-off gives the queued request the worker that freed the slot, not the scheduler's best choice.
- A queued request holds its rate-limit concurrency lease: a tenant's own `max_concurrent` quota counts its waiters.
- The queue is per gateway; N gateways have N queues, N EWMAs and N global caps.
- A refresh wakes waiters only when the new view has free slots; a worker that becomes admissible only through a cool-down ending frees nobody until the next refresh (at most `registry_refresh`, one second by default).
- Unauthenticated deployments have no tenant share: one client can fill the queue (the depth bound and the global cap still hold).
- The global cap is taken before the body is read and the body read deadline is 30 s (`defaultBodyReadTimeout`). A client that authenticates and then dribbles its body can hold a place for up to 30 s, so enough slow
  connections keep `global_limit` firing for everyone; with authentication off anyone can do it. Before this PR the gateway had no cap, so this is a new availability trade (the price of refusing a flood without reading its
  bodies). The cap became live when PR G flipped the default; the mitigations are a larger cap, a shorter body read timeout at the edge (load balancer) and authentication.
- A `global_limit` refusal sets `Connection: close` with the request body unread. A client that is still uploading a large body can see a connection reset or a broken pipe instead of the 503 and its `Retry-After`; clients should treat
  a reset on upload as retryable.

### [PR F] Drain contract (D15, D16)

**Worker drain (D15).** *Announce, then refuse, then finish.* A mock worker given `--drain-announce` reports `draining` on SIGTERM but keeps serving for the announce period, then refuses new work
(`503 draining`, a neutral answer) and finishes what is in flight up to `--drain-timeout`; its agent sees the draining state in the next probe and reports `DRAINING`, so the registry stops offering the worker; the agent
deregisters when the backend has exited. A SIGTERM to the agent itself reports `DRAINING` and deregisters at once (the agent is not in the data path; the backend keeps serving what it has). The announce must exceed the heartbeat
interval plus the gateway's registry refresh; without it the requests routed from a view that has not caught up reach a refusing worker and are retried on another (the retry costs nothing the client sees, but the retry budget
is spent). **The agent was not changed**: PR B (announce, `embedded.DrainWorker`, the `drain-roll` scenario) already provided the worker half; PR F proves it with real processes and measures it.

**Gateway drain (D16),** when the feature is effective (`resilience.mode: on`, registry workers). On the first SIGTERM or SIGINT, in this order:

1. The drain flag is set. `/readyz` answers `503` at once, **bypassing the 2 s probe cache** (the body is the usual `NO_CAPACITY` shed body, message "the gateway is shutting down; retry on another instance", `Retry-After: 1`; no
   new error code), every response to a request that starts after the signal carries `Connection: close` (a request already in flight keeps its keep-alive header, which is harmless because `Shutdown` closes idle connections), and `gateway_draining` is 1. `/healthz` (liveness) stays `200`, and the metrics listener keeps answering until the process is about to exit.
2. For `resilience.drain.readiness_grace` (default `min(5s, shutdown_timeout/2)`) the listener stays open and requests are served normally, including queueing: a load balancer that has not yet noticed loses nothing.
3. At the end of the grace the queue is closed (arrivals that would have to wait are shed `draining`; waiters already queued finish, are served or time out by `max_wait`), the listener is closed and `http.Server.Shutdown` waits for in-flight requests.
   Whoever is still queued 250 ms before the end of the shutdown budget is flushed with a `draining` shed.
4. **One budget:** `gateway.shutdown_timeout` is measured from the first signal and covers the grace and the drain (hence `readiness_grace < shutdown_timeout`). A drain that does not finish in the budget closes the remaining connections and exits non-zero, as before.
5. A **second signal** ends the process at once, in the grace and in the drain (the default signal disposition is restored after the first signal, `cmd/gateway/main.go`; the queue is dropped silently). No code was added: process tests pin it.

Background work (registry refresh, key usage writer, rate-limit lease renewal, request recorder) used to stop at the signal. With the feature effective it runs until `Serve` returns, because the grace and the drain serve
requests from the registry view, which would otherwise go stale (and fail closed after `registry_max_staleness`) in the middle of a drain, and leases of requests still running would not be renewed. With the feature off it still stops at the signal.

**With `mode: off` or static workers nothing changes:** `Serve` takes the old path (no drain flag, no grace, `/readyz` unchanged, no `Connection: close` of our own, background work stops at the signal), the series are absent,
`/metrics` is byte-identical and the existing signal process tests (static mode) pass unchanged. The embedded benchmark cluster sets `drain.readiness_grace: 0` (its gateway is stopped by cancelling a context, with nobody to notice a failing probe).

*Why a flag and a timer and not a goroutine per phase:* `Serve` already owns the shutdown sequence; the new state is one atomic flag read by the readiness handler and the request wrapper, and one timer.
*Why `Connection: close` and not only a failing `/readyz`:* keep-alive clients that do not go through a balancer would otherwise stay on this process until the listener closes; the header moves them at their next request.
*Why not stop accepting at the first signal:* the grace exists so that a balancer that polls every few seconds still has somewhere to send the requests it routes before its next poll.

**Operator contract** (what Phase 17-18 will rely on): `readiness_grace` must be longer than the balancer's detection time (probe period times failure threshold, plus propagation); `shutdown_timeout` must be at least the grace plus your longest
stream; the orchestrator's termination period must be at least `shutdown_timeout` plus a margin; send SIGTERM, not SIGKILL. Real preStop and load balancer behaviour are Phase 17-18 and are not verified here.

**Deviations and decisions made while implementing PR F** (numbered for the plan's progress notes):
1. The shutdown timeout covers the grace (one budget from the first signal); the queue flush timer is set to what remains.
2. Background work keeps running through the grace and the drain when the feature is effective (above); unchanged when off.
3. `/readyz` while draining reuses the overload shed body (`NO_CAPACITY`, "shutting down") instead of a new error code; the status is what balancers read.
4. The agent and the mock worker were not changed (PR B had them). Given SIGTERM, the agent reports `DRAINING` and deregisters at once (also when the backend gets the signal at the same moment, which then only shortens the window); with
   the backend alone it reports `DRAINING` until the backend exits. The agent-alone and backend-alone cases are process-tested (both signalled together is the first case and is not a separate test).
5. The queue stays open through the grace (D16 step 3 is "at the end of the grace"); PR E closed it at the signal. With grace 0 the two are the same.
6. New series `gateway_draining` (gauge) appears with the other Phase 15 series, only when the feature is effective; the registry-mode golden additions file gains one line.
7. Harness: `benchmark chaos --scenario drain-roll --no-drain-announce` drains with no announce period (the comparison of acceptance 18); `drain_announce` is recorded for every roll, including `0s`.
8. The `drain-roll` scenario is the embedded harness's in-process drain (mock drain plus `agent.Shutdown`, the registry told at once); process-level rolls (worker and gateway behind a test balancer) are in `tests/integration/drain_process_test.go`.

**Limits found (kept, reported).**
- A request that arrives at the very end of the grace, or after a balancer routes it from a stale poll, can still reach a closed listener (`connection refused`); the grace makes this unlikely, not impossible. With grace 0 the gateway roll behind the test balancer
  failed 1-5 requests per roll (see the benchmark note); with a 1 s grace and a 50 ms poll it failed none.
- A second signal drops queued waiters and in-flight requests without an answer. Observation from the verification, not a PR F change: two SIGTERMs sent with zero gap were swallowed in 5 of 15 trials (probably the kernel coalescing identical pending signals); gaps of 10-20 ms always worked.
- Streams longer than `shutdown_timeout` minus the grace are cut at the end of the budget, as before.
- Only mock workers: a real vLLM sidecar's drain vocabulary and a real orchestrator's sequence are not verified (Phase 13, 17).

### [PR G] The default flip, the D25 list, observability and the benchmark note (D1, D14, D23-D26)

**Decision (D1, user decision): `resilience.mode` ships defaulting to `on`.** The change is one line, `defaultResilience().Mode = on` in `internal/config/resilience.go`, made in the last pull request after every part
existed and had been measured. The feature takes effect only when `gateway.worker_source` is `registry`: static workers are never affected and `mode: on` with them starts and serves. `resilience.mode: off` (or
`SERVERFLOW_RESILIENCE_MODE=off`, or `SF_RESILIENCE_MODE=off` in the Phase 16 compose `.env`) is the kill switch; with it requests, headers, bodies, logs and `/metrics` are byte-identical to the gateway before Phase 15
(differential below). **This is a behaviour change for every existing registry-mode deployment that sets nothing**: an upgraded gateway ejects failing workers, holds a request for a full model, can refuse with a live global cap, and drains on
SIGTERM, with no configuration change. The list of what changes is decision D25, repeated verbatim here, in `docs/operations/resilience.md`, in the README note and in the pull request text
(a test, `TestD25ListIsRepeatedVerbatimInTheDocuments`, compares them with the plan):

- **D25 Default-on behaviour changes: each item decided.** With the default (on, registry mode), no fault, free capacity: nothing changes except the items below. *Excluded* = made not to change; *asserted* = changes, pinned by a named test; *documented* = changes, stated in README/ADR-020/operations guide/PR text with the kill switch.
  1. A request that finds every eligible worker full waits up to 3 s (bounded FIFO) before the 503; `Retry-After` reflects recent waits — asserted + documented.
  2. Retries are subject to the budget; some retries that used to happen are denied and the failed worker response is relayed — asserted + documented.
  3. A worker with sustained faults is ejected by its breaker (cap D9) and receives only probes; a half-open worker gets at most one request at a time — asserted + documented.
  4. `admission.max_global_requests` (1000) becomes live: the 1001st concurrent request on a gateway is refused with 503 `global_limit` (1000 long streams is ordinary) — asserted + documented; the operator raises the key.
  5. The first-byte deadline defaults to header + idle timeouts, so no request the old gateway served is cut — **excluded**; an explicit value below the header timeout is refused.
  6. The overhead histogram excludes queue wait, so the 25 ms alert keeps its meaning — **excluded**; the wait has its own series.
  7. A retried 503 now pays a body peek of at most 250 ms and only when a retry is possible — asserted (bound) + documented.
  8. On SIGTERM the gateway fails `/readyz` and serves for 5 s before shutting down (a developer's Ctrl-C waits 5 s; a second signal exits); existing process tests and the embedded cluster set grace 0 — asserted + documented.
  9. Log volume: one WARN per breaker open (at most one per worker per cool-down), shed INFO rate-limited to 1/s per reason — documented.
  10. Tail drop is tenant-blind except for the per-tenant queue share (0.5 of depth); a shed after the quota charge is not refunded — documented.
  11. New series, dashboard and alerts appear; a probe of a frozen worker can cost one client a 504 — documented.
  The list is repeated verbatim in ADR-020, the operations guide, the README note and the PR text.

**Additional visible details (not in the plan's D25 list; found by the differential and documented rather than hidden).**
- (a) Attempt log lines gain `verdict` (`ok`, `fault`, `neutral`) and, when they apply, `probe` and `retry_denied` fields (PR D). Log parsers that reject unknown fields need the new ones.
- (b) The body of a refusal because the model is full is the usual `NO_CAPACITY` body with another message ("... within the queue wait limit") and without the `eligible_workers` field, and its `Retry-After` is the recent queue wait (item 1); a shed is not a scheduling
  decision, so `scheduler_decisions_total{result="no_capacity"}`, `scheduler_no_capacity_total` and the decision-duration histogram no longer count a refusal of a full model (the `admission_rejections_total` series does) (PR E).
- (c) The ejection cap is per model (see the correction below); a model with one worker keeps it however many workers other models have.
- (d) The registry refresh, key-usage writes and lease renewal keep running through the readiness grace and the drain (PR F). With the feature off they stop at the signal as before.

**Differential (acceptance 4, 24).** `internal/gateway/differential_test.go` drives 39 request shapes (35 registry, 4 static: success, stream, unknown model, bad input, retry on reset/502/503/empty stream, `queue_full`, both workers 503, a single 503, a full
cluster, draining, 500, 504, 4xx, mid-stream failures, header timeout, first-byte stall, client disconnect, redirect, `/v1/models`, `/healthz`, `/readyz`, wrong method and path, oversize body, no snapshot, no workers, `max_attempts` 1 and 3, mixed models, a reported-full
worker, a lost model) through a gateway in process against a fake control plane and fake workers, recording every status, header and body, the normalised log lines and every `/metrics` series with its value. The outputs of master (16dda6b, which contains
Phase 15 PR A) are committed under `internal/gateway/testdata/differential/master` and were produced by the same file run on that tree (recording command in the file header; two recordings were identical after one shape, a body just over the limit, was
made deterministic). Results: **`mode: off`: 39 of 39 identical to master**; static workers with the default (on): identical; **default on, registry workers: 38 of 39 identical to master apart from the new series (D25 item 11, the names of the
resilience golden file) and the attempt-line fields (a)**, and the one shape that differs is the full model (item 1). A table in the test names the shapes allowed to differ; an entry that does not differ fails the test (the list cannot go stale), and a shape not in the
table that differs fails it. Items 2-4, 7, 8 and 11 are not visible in fault-free shapes; each is pinned by a named test, and `TestEveryD25ItemNamesATestThatExists` fails when one of them is renamed or removed.

**Observability (D14, D23, D24).** One dashboard, *ServerFlow Resilience* (`sf-resilience`, `observability/grafana/dashboards/resilience.json`, 11 panels): breakers open and the state of each ejected worker, transitions, probes and trips held back by the cap, retries made and denied,
admission queue depth and wait, requests shed per second by reason, gateways and workers draining, requests shed while draining. A test starts a gateway with the shipped defaults in front of mock workers, makes every event happen (a breaker opens, probes fail, a second trip is held back, a retry is
denied, a request waits and is shed) and requires every series the dashboard and the new alerts name to be exported by it, with the label names they use (the Phase 10 exported-by-a-running-server check, extended). Alerts: `ServerFlowCircuitOpen` (`circuit_breakers_open > 0` for 2 minutes, warning),
`ServerFlowShedding` (more than one `NO_CAPACITY` refusal per 20 s for 2 minutes, `draining` excluded, warning) and, for D23, `ServerFlowWorkerHeartbeatLate` (warning at a heartbeat age above 5 s, the suspect threshold at which the gateway stops routing to the worker); `ServerFlowWorkerHeartbeatStale`
stays critical above 10 s, so one late heartbeat never pages. The plan (D23) asks for a **new warning at 5 s with the critical alert staying at 10 s**, and that is what was implemented (the critical threshold did not move). Each alert has a firing and a non-firing promtool case and
a runbook section. D23's repeated kill experiment and its numbers are in `docs/benchmarks/phase-15-chaos.md`: the gateway stops routing to a silent worker about 4 s after its agent dies (control-plane eligibility at the suspect age), the warning alert fires about 15 s after the kill and the critical one about 20 s after
(Prometheus scrape plus rule evaluation on top of the age), which is still above 10 s; the plan said to report it as it is.

**Deviations and decisions made while implementing PR G.**
1. **The ejection cap is per model (a correction to PR C/D, plan D9).** The library divided by every eligible worker of the gateway, so in a fleet with several models the sole worker of one model could be ejected (and the model refused until the cool-down ended) while other models had room. The plan's table
   says a single-worker model never opens. `resilience.Member` gains `Model`; the cap's denominator, numerator and the relaxation on a view change are counted per model (`TestEjectionCapIsPerModel*` in the library, `TestTheEjectionCapIsPerModel` in the gateway, both red on the previous code). The randomized model check still uses one group (it leaves `Model` empty).
2. **The default flip changes tests that assumed the default off, and only those:** the config defaults test (now asserts on), the env-overlay case for `MODE` (now `off`, the value that changes something), the harness tests that record the build's default (`on`), the chaos `TestRealScenarios` (asks for `off` explicitly, because its
   expectations describe a gateway without the feature), and `TestProcessGatewayRetriesAroundAFlakyWorker` (asks for `off`: its distribution is the old retry behaviour; a new test, `TestProcessGatewayEjectsAFlakyWorkerByDefault`, asserts the default's behaviour). No assertion was loosened.
3. **The compose stack** gets `SF_RESILIENCE_MODE` (default `on`, validated `on|off`, passed as `SERVERFLOW_RESILIENCE_MODE`) so the kill switch is visible in `.env`. `make dev-up` and `make dev-smoke` pass with the flipped default (12 of 13 checks, the scale check skipped with one gateway); smoke S8 now checks the five dashboards.
4. The dashboard has no panel for `inference_deadline_exceeded_total` (the series stays documented): triggering a first-byte deadline in the test cluster is racy against the header timeout, and the dashboard test requires every series it names to be produced by a running cluster.
5. The ADR-017 ceilings are unchanged by PR G (no series added).

## Consequences (PR G)

- Every registry-mode deployment now runs with the resilience feature unless it sets `mode: off`. The D25 list above is the complete statement of what changes; the first thing an operator may notice is a request for a full model waiting up to 3 s before the 503, a worker missing from routing after
  repeated failures (`circuit breaker changed state` WARN), and a 5 s wait after Ctrl-C or SIGTERM. The kill switch needs a restart.
- Static-worker deployments and `mode: off` are unchanged (differential: 39 of 39 byte-identical to master).
- Risk: the global cap (1000) is live. A gateway that holds more than 1000 concurrent streams starts refusing with `global_limit`; raise `admission.max_global_requests` before upgrading if that is ordinary.
- Risk: N gateways each decide alone (N times the budget, N times the cap, breakers that disagree); accepted in the plan.
- Risk: only mock workers and loopback were measured (`docs/benchmarks/phase-15-chaos.md` states what was not).

## Consequences (PR F)

- With `resilience.mode: on` (registry mode) a SIGTERM or SIGINT makes the gateway fail `/readyz` at once, answer with `Connection: close`, keep serving for `drain.readiness_grace` (default `min(5s, shutdown_timeout/2)`), and then drain
  in the rest of `gateway.shutdown_timeout`. A developer's Ctrl-C therefore waits for the grace; a second Ctrl-C exits at once. A rolling replacement of workers with a suitable announce costs the clients no failure and no retry.
- With `mode: off` and with static workers nothing changes (process tests; the signal tests of `signals_process_test.go` pass unchanged).
- Security review items: none that touch authentication, tokens or the data plane. The readiness and shed bodies carry no worker, key or tenant text. The drain flag is process-local. The one behaviour to read twice is that background
  work (key usage writes, lease renewal) now outlives the signal when the feature is on, so a drain can still write to PostgreSQL and Redis until `Serve` returns (bounded by the shutdown budget).
- Risk: a grace shorter than the balancer's detection time loses requests; a shutdown timeout shorter than the grace plus the longest stream cuts streams; both are configuration the operator owns (operator contract above).

## Consequences (PR E)

- With `resilience.mode: on` (registry mode) a request that finds every eligible worker full waits up to `queue.max_wait` (default 3 s) in a bounded per-model FIFO instead of getting an immediate 503; what exceeds the depth (128),
  the tenant share (half of it, per authenticated tenant) or the wait is refused `503 NO_CAPACITY` with `Retry-After`, never 429. `admission.max_global_requests` (default 1000) becomes live: the 1001st concurrent request on a gateway
  is refused `global_limit` (long streams count until they end; raise the key if 1000 is ordinary). A shed after the quota charge is not refunded. New series: `admission_rejections_total`, `inference_queue_duration_seconds`,
  `admission_queue_depth`; the overhead histogram no longer includes waiting. Logs gain `request shed` (at most 1/s per reason).
- With `mode: off` and with static workers nothing changes (differential; golden series files unchanged).
- Security review items: the tenant share (authenticated tenant ID as a queue key, bounded by the depth), the pre-body global cap (a flood is refused without reading its bodies), the interplay of sheds with the limiter (which
  refusals spend quota), the queue key (registry-confirmed models only) and the hand-off consulting the breaker.
- Risk: queueing hides overload for up to `max_wait`: under sustained overload every queued request waits the maximum and then fails (worse latency for failures); the depth bound limits how many.
- Risk: the hand-off and wake paths are the most intricate concurrency in the gateway; they are covered by a randomized model test, `-race -count` runs, the library's stress tests and hand mutation (plan, PR E notes).

## Consequences (PR D)

- With `resilience.mode: on` (registry mode) a worker with sustained faults is ejected by its breaker (`consecutive_failures` 5 in a row, or the ratio rule) up to the 50% cap and receives only probes
  (one live request at a time) until a probe passes; retries are subject to the budget (`max(10, 20% of first attempts)` per model per 10 s): some retries that used to happen are denied and the failed
  worker's response is relayed (the client sees the worker's 502/503 instead of a retried success); a retried 503 first waits at most 250 ms for its body; a request that sees no output within the
  first-byte deadline gets 504 `UPSTREAM_TIMEOUT` (never shorter than one attempt today). A probe of a frozen worker can cost one client a 504 after the probe header timeout.
  New series appear (list above). Logs gain one WARN per breaker open and `verdict`/`probe`/`retry_denied` fields on attempt lines.
- With `mode: off` (the code default until PR G, now the kill switch) and with static workers nothing changes: byte-identical requests, headers, bodies, logs and `/metrics`; the golden series files are unchanged.
- Risk: misclassifying overload as a fault removes capacity when it is scarce. The cap (50%), the neutral codes, the peek and the 10 s cool-down limit it; a real vLLM sidecar's 503 vocabulary is unknown
  (the code list is `neutral503Codes`). A terminal 503 that is not peeked counts as a fault.
- Risk: a lying worker can answer `queue_full` to stay in rotation (trust boundary: registration).

## Consequences (PR C)

- A new unused package and an inert configuration section. `resilience.mode` was `off` and nothing read it; behaviour, metrics and logs were unchanged.
- `resilience.*` keys are accepted in files and the environment from now on, so a configuration prepared for PR D already loads.

## Consequences (PR A)

- A panic in a registry-mode request no longer leaks a worker slot, and every started attempt is observable as ended. The tracing and event observers see a
  `failed` attempt with class `panic` where they previously saw nothing; the event's per-attempt `failure_class` can therefore be `panic`.
- Unknown models stop costing quota in registry mode. Tenants that relied on the old charge as a crude 404 throttle lose it.
- `scheduler_decisions_total` and `scheduler_no_capacity_total` can rise on traffic with retries where only one worker serves the model; they now describe every
  scheduling decision.
- Everything else is byte-identical to the previous gateway (verified by a master-versus-branch differential recorded in the plan's implementation progress).
- Risk: a custom `Observer` implemented without embedding `NopObserver` stops compiling until it adds the two methods.
