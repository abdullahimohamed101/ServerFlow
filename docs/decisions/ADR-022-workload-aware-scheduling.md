# ADR-022: Workload-Aware Scheduling (Signals, Ledger Overlay, Simulator Validation)

Status: Proposed, delivered in parts. Phase 14 is a sequence of pull requests (plan: `docs/plans/active/phase-14-workload-aware-scheduler.md`, decisions D1 to D29).
This record grows with each of them. Sections are marked **[PR A]** (delivered: the seam and the estimator extraction, no behaviour change), **[PR B]** (the signals library, the ledger and the opt-in overlay v2) or
**[later PR]** (placeholder: the decision is in the plan, the text lands with the pull request that implements it).
Date: 2026-10-22

## Context

The schedulers of Phase 5 choose by request count. Spec section 14 also names queued tokens, throughput, time to first token and GPU pressure, and ADR-011 left
"weighted and latency-aware strategies" to Phase 14. Phase 13 (a real vLLM pool) is not done and cannot be done without a GPU, so this phase proceeds without hardware:
it records that deviation from the spec's order and labels every performance claim it will ever make "simulator-validated, not hardware-validated".

PR A changes no behaviour. It prepares the two places later pull requests build on, so they can be reviewed on their own.

## Decision

### [PR A] One cost estimator in a pure package (D3)

The Phase 8 estimator (`input tokens + max_tokens`, ADR-015) moves unchanged from `internal/ratelimit` to the new pure package `internal/cost`, because the scheduler must
not import the rate limiter and its Redis dependency. `internal/ratelimit` keeps `EstimateCost`, `EstimateRequestCost` and `MaxCost` as thin delegates. Billing must not move: tests keep
a verbatim copy of the old implementation and compare the delegates with it over a corpus and 100,000 randomised requests.

`cost.Estimate` returns the same number split in two, the estimated input tokens and the output budget (`max_tokens`, or `gateway.max_tokens_limit` when the client set none), which always sum to
`EstimateRequestCost` (the `MaxCost` bound applies to the sum). The gateway handler now computes the estimate once per request after parsing, with or without a limiter, and stores the parts
on `InferenceRequest.EstInputTokens` and `EstOutputTokens` (Go-only, `json:"-"`, never on any wire). The limiter is charged the sum, as before. `info.cost`, the `est_cost` log field and the
event's `EstimatedCost` still report 0 when rate limiting is off, so logs, events and metrics are byte-identical.

### [PR A] Gateway-local observations travel on the snapshot copy (D2)

`protocol.WorkerSnapshot` gains one field, `Observed ObservedLoad`, tagged `json:"-"`: it is never serialised and never deserialised. Consequences, each pinned by a test: the registry wire
format is byte-identical to master's (a golden recorded from master's code), and a control plane cannot supply the value (a forged `Observed`/`observed` key is ignored). `ObservedLoad` is a plain
struct of gateway-computed numbers (in-flight requests, work ahead in tokens, capacity estimate, time to first token, effective active count, quality flags). Nothing fills it in PR A; the router will fill it
on its private copy inside the existing critical section, and strategies will do arithmetic on it and read no clock, so the `Scheduler` interface (spec section 13) and strategy purity (ADR-011) are unchanged.

Rejected, as in the plan: context values (hidden, untestable by signature); a second `Scheduler` method; overloading `Metrics.QueuedInputTokens`; changing `SelectWorker`'s signature; strategies that own state or a clock.

A known cost, accepted: `routed.worker` is a snapshot copy and so carries `Observed` into code that already receives it (observers, logs). It is a struct of numbers; the hostile-input test of a later PR
asserts it never reaches a label, a log line or a span.

### [PR A] Evidence that nothing changed

A differential test replays a fixed request script through seven gateway set-ups (static mode with and without a limiter; registry mode with round-robin with and without a limiter, least-active and least-queue over several workers, and random over a single worker, so its choice is deterministic) and compares status, body, headers, what the workers
received, what the limiter was charged, the structured logs and the Prometheus series with fixtures recorded once from unmodified master (`scripts/record-differential.sh`; the fixtures were committed before any
production code changed). Go version, build info, IDs, ports and timing are normalised so CI on another toolchain does not differ.

### [PR B] The in-flight ledger and the `ledger` load overlay (D4, D14, D17, D18)

`internal/scheduler/signals` (pure: standard library and `pkg/protocol` only, no clock read, no goroutine, enforced by a test) keeps per worker incarnation (worker ID and `RegisteredAt`) the number of this gateway's in-flight
requests, their summed estimated token cost, and a bounded history of starts and finishes in 100 ms buckets (cumulative counters per non-empty bucket, so a query is a binary search whatever the rate). The `Tracker` has
its own lock; the router calls it while holding `r.mu` (lock order router, then tracker; the tracker never calls out). Everything is opt-in: `scheduler.load_overlay` is `max` by default, the router then holds no tracker,
runs none of this code and the differential fixtures of PR A pass unchanged.

- **Ledger.** `reserve` calls `Begin` (count and history start) and `AddCost` next to `inflight[id]++`; the lease (token and cost) travels with `routed`; `release`, inside its `sync.Once`, calls `End` with exactly the cost
  that was added, so every path that releases a slot (success, error, retry, client cancel, panic, abandoned waiter) returns the ledger to zero. The token carries the entry's generation, so a request that outlives its
  worker's re-registration, or a pruned and re-created entry, cannot subtract from the new entry. Entries are pruned at each registry refresh when the worker (or its incarnation) leaves the snapshot, and capped at 4,096
  workers (the others are untracked: counted, logged once, and use the `max` rule).
- **Hand-off (the likeliest bug site).** `handoffLocked` runs under `r.mu`, knows only the model, and `internal/resilience` is untouched, so the router cannot know the head waiter's request. The `routed` created by a hand-off
  has cost 0 (its count and history start are recorded under `r.mu`, the finished request's own entry is removed by its own `End` just before); the waiter adds its request's cost when it receives the slot
  (`router.charge`, in `awaitSlot`, outside `r.mu`). In that window the ledger undercounts by one request and the slot count is exact, so nothing is over-admitted. A hand-off the waiter never consumes comes back through
  `release` with cost 0 and subtracts nothing. A declined hand-off ends the token it took with nothing to subtract.
- **Overlay v2.** `effective = max(local, reported + started_since_report - finished_since_report)`, one function (`Tracker.Overlay`) used by both `reserve` and `freeSlots`, each with the instant of its own snapshot view
  (`snapshotCache.viewAt`), so `AtCapacity` and the queue's wake-ups cannot diverge (a randomised test fails if they do). The report time is `view time - aged heartbeat age`, which equals the snapshot's fetch time minus the
  heartbeat age at the registry: one clock domain, no registry clock involved. Classification at the report's bucket `b`: starts and finishes in buckets `>= b` both count as after the report. The report time
  is only known to about a bucket (round trip, registry processing, the worker's sampling delay), so for the requests in that one bucket the classification can be wrong in either direction: the result can be a request or two too high
  or too low for up to one heartbeat. The floor `local` bounds the low side (the gateway never admits to a worker it knows is full; a test asserts zero such admissions). **This replaces the plan's D4(a) rule**
  (boundary always classified as an over-count: starts from `b`, finishes from `b+2`): the independent verification showed it keeps every request that starts and finishes around the report as a permanent +1 until the next
  report, so with short requests a worker looked full for the whole heartbeat interval (simulated wrong-full 99.7% against 0.9% for `max` at 8 slots, 20 ms requests, 30% load; 84-94% refusals in the embedded cluster). With the symmetric
  rule the same cases are 4.0% against 0.9%, 12.6% against 12.7%, 4.5% against 1.6% and 5.0% against 8.8% (simulator results, guarded by `internal/scheduler/signals/overcount_test.go`). The guarantee "always an over-count" is given up deliberately.
- **Fallback to `max`, never below local.** The rule gives way to `max(local, reported)` for a worker the tracker does not follow, for a report made before the tracker began following the worker (a new gateway, a new
  incarnation, a reset history), for a report older than the window (`registry_max_staleness` plus one heartbeat interval), and when the history no longer reaches back. A registry restart that keeps `RegisteredAt` shows as the
  worker's `LastHeartbeat` going backwards; the worker's history (not its ledger: its requests are still in flight) is then reset like a re-registration.
- **Observed.** With the overlay on, the router fills `WorkerSnapshot.Observed` on its private copy (in-flight requests, work ahead in tokens from the ledger, effective active, a cold-start flag when the history fell back).
  The capacity estimate, TTFT and the queued-token term are not filled yet (PR C1 and later); the strategies of Phase 5 ignore the field.
- **Deviations from the plan text, recorded.** (1) No new Prometheus series in this change: the plan's minimum observability (`scheduler_signal_fallbacks_total`) lands with the first strategy that reads the signals (PR C1);
  the tracker exposes the counters (`Stats`) and the router logs once when the worker cap is reached. (2) The slow terms the plan computes "at refresh time into an immutable per-worker struct" (capacity peak, TTFT, medians) do not exist yet;
  what exists per refresh is `Sync` (entries, pruning, restart detection), and the per-request work is the overlay lookup. (3) E0 resets the abandoned requests by a client cancel rather than through a breaker; the mechanism
  (a stale-high report) is the same and a breaker adds timing the experiment does not need. (4) An invalid `SERVERFLOW_SCHEDULER_LOAD_OVERLAY` fails startup (validated with the rest, like `SERVERFLOW_SCHEDULER_STRATEGY`)
  where plan criterion 9 says invalid env values are ignored; an enumeration is not a numeric override, and silently running the other overlay would hide a typo. (5) The boundary rule above (D4(a)).
- **For PR C1's strategies.** A worker that flaps out of the snapshot and back loses its ledger entry while its requests are in flight (the entry is pruned at the refresh, and the old requests' tokens go stale), so the ledger's
  in-flight count can be below the slot count (`r.inflight`, which is keyed by worker ID and survives). That is the safe direction (the overlay falls back to `max`, admission uses slot counts), but a strategy that reads
  `Observed.WorkAheadTokens` must treat it as a lower bound. The signals package is 32-bit safe (counts are clamped to `MaxInt32`); the repository as a whole does not build for 386 on master (`internal/ratelimit`).

### [later PR] Strategies, observability, simulator, experiments (D5 to D28)

`least-tokens` and `least-work` (PR C1), the simulator (PR E), the experiment framework (G-lite), the pre-registered predictions (PR I)
and the results (PR J) are recorded here as they land. Until then the plan is the source of truth. The defaults do not change: `scheduler.strategy` stays `round-robin` and the overlay stays `max`.

## Consequences

- **Positive**: one estimator for billing and scheduling; the seam is reviewed without any behaviour in it; the differential fixtures remain as a regression guard for later pull requests of the phase.
- **Negative**: `WorkerSnapshot` carries a field that is invisible on the wire, which a reader of the struct could miss (the tag and a test say so); the handler now does one estimate per request even without a limiter
  (about 30 microseconds for a 100 KB body, benchmarked, against a 25 ms overhead budget).
- **Follow-ups**: the rest of the plan; flipping any default needs the pre-registered criteria of D26 and an explicit decision.
