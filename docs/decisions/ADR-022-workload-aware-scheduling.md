# ADR-022: Workload-Aware Scheduling (Signals, Ledger Overlay, Simulator Validation)

Status: Proposed, delivered in parts. Phase 14 is a sequence of pull requests (plan: `docs/plans/active/phase-14-workload-aware-scheduler.md`, decisions D1 to D29).
This record grows with each of them. Sections are marked **[PR A]** (delivered: the seam and the estimator extraction, no behaviour change), **[PR B]** (the signals library, the ledger and the opt-in overlay v2),
**[PR C1]** (the `least-tokens` and `least-work` strategies and their configuration) or
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
  is only known to about a bucket (round trip, registry processing, the worker's sampling delay), so the requests in that one bucket are ambiguous: the classification error is bounded: with 100 ms buckets the result can differ from an exact (1 ms) classification by at most the number of in-flight requests that start or finish in the report's own 100 ms bucket. An independent verifier measured that against a 1 ms-bucket reference over 28 million simulated samples: 71.3% identical, 19.1% off by one in either direction, 9.6% off by two or more (worst -11 and +9, at 32 slots with short requests). The floor `local` protects only this gateway's own count. With several gateways the ledger can say "free" where `max` would have said "full" while the worker is truly full, because it subtracts this gateway's own finishes while the other gateways' requests are invisible between reports: in the verifier's multi-gateway simulation that happened in a mean 4.8% of the truly-full samples (up to 14.5% at 32 slots with three gateways and 200 ms to 3 s requests); with 1 ms buckets the rate is about the same (4.6%), so the exposure comes from subtracting own finishes, not from the boundary rule. Overall the ledger is still better than `max` there (wrong-full 11.5% against 13.4%; oversubscription time 278 against 391, summed over 44 uncoordinated configurations; one gateway: wrong-free 0.0%). These are simulator-validated, not hardware-validated results from the independent verifier's simulation (not part of the repository); the repository guards the relative claim in `signals/multigateway_test.go` (ledger within 3 points of `max` for wrong-free and 5 for wrong-full, fixed seeds). The embedded cluster has a single gateway, so none of this multi-gateway exposure appears in the H1 measurement (0.00 pp rise in refusals below saturation). **This replaces the plan's D4(a) rule**
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
- **The H1 refusal guard is a median of five paired runs, and false-refusal episodes exist under both overlays.** `TestLedgerRefusalsAndOversubscriptionUnderLoad` once failed in CI (24 clients on 4 x 8 slots, resilience off: max 0.00%, ledger 1.04%, 0.5 pp limit). Investigation (idle machine, `-race`, embedded cluster, 4 s cells, 250 runs of each overlay): a cell either has no refusals or one episode lasting one heartbeat interval (1 s, starting about 0.5 s into a second) in which 0.8% to 4.4% of the requests get 503 NO_CAPACITY while the workers' own counts show free slots (sampled at the first refusal: 3, 7, 0 and 7 of 8 active). Ledger: 2 of 250 cells, `max`: 3 of 250, so the two cannot be told apart at this sample size. A report that makes a worker look full stays in force until the next heartbeat: for `max` it is a stale-high report, for the ledger a bucket-boundary misclassification (see the 4.0% against 0.9% "says full with room" figures above, which are the same effect at short requests). Machine slowness did not reproduce it (30 runs with 15 CPU hogs and GOMAXPROCS=2: no refusals in either overlay, throughput a third of CI's), so it is not a slow-machine artifact; faster machines issue more requests per bucket. A single paired run therefore fails about 1% of the time with a correct ledger; the guard now takes the median of the paired differences over five runs (an episode must hit three of five pairs to move it), keeping the 0.5 pp limit. Mutation check: an over-count of 3 or more slots (`eff + 3`, `eff + 4`, `eff * 2`) fails the guard (median differences 5.9, 19.7, 18.7 pp); an over-count of 1 or 2 slots or 5% does not (about 0.2 pp or less on average), nor reliably did the single-run guard: at 24 clients on 32 slots the cell can only see offsets of about 3 slots. The product effect (a ledger or `max` report keeping a worker out of rotation for up to a heartbeat) is unchanged and not fixed here. Simulator results from an idle machine, not hardware results.
- **For PR C1's strategies.** A worker that flaps out of the snapshot and back loses its ledger entry while its requests are in flight (the entry is pruned at the refresh, and the old requests' tokens go stale), so the ledger's
  in-flight count can be below the slot count (`r.inflight`, which is keyed by worker ID and survives). That is the safe direction (the overlay falls back to `max`, admission uses slot counts), but a strategy that reads
  `Observed.WorkAheadTokens` must treat it as a lower bound. The signals package is 32-bit safe (counts are clamped to `MaxInt32`); the repository as a whole does not build for 386 on master (`internal/ratelimit`).

### [PR C1] `least-tokens`, `least-work` and the capacity estimate (D5, D6, D19)

Two strategies join `scheduler.Strategies()`: `least-tokens` (fewest estimated tokens of work ahead of the request) and `least-work` (the smallest predicted wait). Both are pure arithmetic over the snapshot
copy, start from the shared `eligible()` filter (spec section 15 holds unchanged), read no clock and start no goroutine (a source test pins that), and share the Phase 5 rotor for ties. `scheduler.strategy` stays
`round-robin` by default and nothing below is active unless one of the two is chosen.

**What each strategy minimises.** With `c` the request's own work cost and `R_w` the capacity estimate of worker `w` in tokens per second:

| Strategy | Score |
| --- | --- |
| `least-tokens` | `WorkAhead_w` |
| `least-work` | `(WorkAhead_w + c) / R_w`, a predicted wait in seconds |

`WorkAhead_w = L_w + F_w + Q_w`: `L_w` is this gateway's ledger (the weighted cost of its in-flight requests on the worker); `F_w` is the load other gateways and clients appear to add, `max(0, effective_active - local slots)`
priced at the model's mean request cost (an EWMA, alpha 0.1, of the cost of the requests this gateway routed; the request's own cost until there is one); `Q_w` is the worker's reported `queued_input_tokens` (clamped
to `[0, 2^31)`) times `input_weight`. `Q_w` can double count requests this gateway also holds in its ledger, only on a worker that is queueing, which is the direction wanted; it is not de-duplicated.
`c = ceil(input_weight x EstInputTokens) + ceil(output_fraction x EstOutputTokens)`, at least 1 (`cost.Work`). The weights apply to the scheduler's accounting only: the rate limiter is still charged the
unweighted `EstInputTokens + EstOutputTokens`, so billing does not move. A score within a relative 1e-9 of the minimum is a tie; tied workers take turns in worker-ID order through the rotor.

**Selecting them requires `scheduler.load_overlay: ledger` (decision, not in the plan text).** The strategies read the gateway's own view of the work on each worker, which exists only with the ledger. The plan
leaves open whether choosing them should imply the overlay. It does not: the `ledger` overlay changes `ActiveRequests` for every strategy and for the queue's free-slot count, it is an opt-in behaviour change
that the plan reserves for experiment E0 and the optional default decision (PR L), and silently turning it on because of a strategy name would be hidden magic and would confound the comparisons the experiments make
(two strategies compared under different overlays). So the combination is explicit, and the incomplete one is refused at startup with the setting to use ("scheduler.strategy "least-work" reads the gateway's
in-flight ledger and needs scheduler.load_overlay: ledger"), both by `Config.Validate` (registry mode; static mode has no scheduler and ignores every scheduling setting) and by `gateway.NewRegistry` for callers
that build a `Config` by hand. The benchmark harness (`internal/bench/embedded`) sets the overlay for these two strategies before its `GatewayMutate` hook runs, so `benchmark run --scheduler least-work` works;
a result must record the overlay (PR G-lite).

**When a signal is missing** (all of it deterministic, and each is flagged on the chosen copy so the router can count it):
- no candidate carries a ledger observation (a snapshot that did not come through the ledger path): the choice is least-active's on the effective active count, flagged `ObservedDegraded`;
- `least-work` and a candidate has no usable rate (0, NaN, infinite, negative): the median rate of the candidates that have one stands in; the router has normally supplied the fleet prior already (below). If no
  candidate has a rate the score is `WorkAhead` alone, i.e. `least-tokens`, flagged `ObservedDegraded` ("when no worker has a rate, the strategy degenerates to `least-tokens`");
- a candidate's work ahead or score is NaN, infinite, negative or overflows, or it has no ledger observation while others do: it scores the median of the valid scores (garbage neither wins nor is exiled) and
  is flagged `ObservedInvalid` if chosen; if every candidate is garbage, all tie and take turns.

**The capacity estimate `R_w` (D6), computed at snapshot-refresh time.** The tracker's `Sync` takes, per refresh, each worker's model, reported active count, reported `recent_tokens_per_second` and whether
the registry offers it (`Fresh`). A report is sampled once (per heartbeat), only while the worker had work (`active >= 1`), only when fresh, and only if the rate is a finite number of at least 1e-3 tokens/s
(a larger one is clamped to 1e7). A worker can lie about its own metrics, and 1e7 alone does not help: against honest workers at 100 tokens/s a claim of 1e9 would become 1e7 and win every `least-work`
decision. So every stored peak is also cut to **K = 10** (`peerClamp`) times the median of the model's *other* workers' peaks, all bounds taken from the peaks before any is cut (the result does not depend on the
order of the snapshot), and re-applied at every refresh, so a lie stored while the worker was alone is cut as soon as a peer has a sample, and a re-registered liar is cut again. The residual bound is stated honestly: a lie
inflates a worker's capacity by at most K times the honest median (a worker that claims 10x still wins `least-work` against an honest peer of equal load), a liar also lifts the model's decay target and so the floor
of the honest workers, a majority of liars moves the median itself, a fleet that is really more than K times heterogeneous has its fastest workers under-estimated, and a **cold fleet is unprotected**: with no
other peak (a lone worker, or no peer has been sampled yet) only the 1e7 bound applies. The estimate is a decayed **peak**, not the latest
value: an idle or lightly loaded worker reports little and must not look slow. The peak decays toward the median of the model's stored peaks with the half-life `scheduler.signals.capacity_halflife` (default
60 s), continuously (no snap), and never below `capacity_floor` (default 0.25) times that median, so no worker is estimated zero or infinitely slow. After four half-lives an estimate that is not refreshed is within 10% of the median only if its peak was
at most about 2.6 times the median; otherwise the remaining difference is (peak - median) / 16 (a test pins the 2.6x case), and the floor holds it up from below. A worker with no sample, or a re-registered one, takes the **fleet prior**, the median of the model's sampled estimates, flagged `ObservedPriorCapacity`; with one sampled worker its
estimate is its own peak. The per-request work (the ledger terms, `F`, `Q`, the overlay) is evaluated inside `r.mu` by `Tracker.Load`, one tracker lock per candidate; the slow terms were computed at refresh time.

**Configuration (D19), checked only when one of the two strategies is selected in registry mode:**

| Key | Env (`SERVERFLOW_` +) | Default | Bounds |
| --- | --- | --- | --- |
| `scheduler.cost.output_fraction` | `SCHEDULER_COST_OUTPUT_FRACTION` | 1.0 | (0, 1] |
| `scheduler.cost.input_weight` | `SCHEDULER_COST_INPUT_WEIGHT` | 1.0 | [0, 100] |
| `scheduler.signals.capacity_halflife` | `SCHEDULER_SIGNALS_CAPACITY_HALFLIFE` | 60s | 1s to 10m |
| `scheduler.signals.capacity_floor` | `SCHEDULER_SIGNALS_CAPACITY_FLOOR` | 0.25 | (0, 1] |

An env value that does not parse is ignored (the repository convention); one that parses is validated like the file's. With any other strategy the keys are inert and a bad value in them is not an error.
`scheduler_signal_fallbacks_total{model,reason}` (`cold_start`, `untracked`, `invalid`) and `scheduler_predicted_wait_seconds{model}` (the chosen worker's predicted wait at decision time, `(WorkAhead + c) / R`,
for decisions that have a capacity estimate) are registered only for these two strategies (the plan's minimum observability); label names are the existing `model` and `reason`. The rest of D24 (prediction error, the
in-flight gauge, span attributes) is not built here. The `least-work` startup refusal and its four pinning tests are gone: they now pin that an unknown or still-unimplemented strategy (`latency-aware`) is refused with the
supported list, that the two strategies are refused without the ledger overlay and with out-of-range weights, and that they are accepted with it (`internal/config/gateway_test.go`, `internal/gateway/router_test.go`,
`tests/integration/gateway_process_test.go`, `internal/scheduler/scheduler_test.go`).

**Deviations from the plan text, recorded.** (1) The ledger overlay is required, not implied (above). (2) The plan's "peak window of 60 s" and "half-life" are one parameter here: the half-life; there is no separate
window after which a worker snaps to the prior, because a hard window contradicts "the decay is continuous". (3) The decay target is the median of the stored (undecayed) peaks and the prior is the median of the decayed
estimates, so the definition is not circular. (4) `scheduler.signals.ewma_alpha`, `ttft_halflife`, `min_samples` and `latency_aware.*` belong to PR C2 and are not added. (5) The model mean cost uses a fixed alpha of 0.1
(not a key). (6) The strategy flags the copy it returns (`ObservedDegraded`, `ObservedInvalid`) as the only pure channel back to the router for the fallback counter. (7) The `Observed.Flags` byte is now full (eight flags); a
later flag needs a wider type. (8) `signals` may import `math` (the purity test's allow-list gained it; the clock, goroutine and I/O bans are unchanged). (9) Plan criterion 9 asks for a startup log line when an env value is ignored; the config
package has no logger and no other numeric override logs, so none is added. (10) `internal/gateway/attempt.go` changes by two lines the plan's wording does not spell out: `startAttempt` builds the `AttemptStart` event into a variable and sets `started.Signals = schedulerSignals(target)`
(the observer-seam field that carries the cold-start, untracked and invalid flags to the metrics) before passing it to the observers.

**Cost of a decision (D18), benchmarked on this machine (Apple silicon, battery), `BenchmarkRouteWorkAware`, 3,000 iterations per cell, one thread / 15 threads:** `least-tokens` 0.8 / 1.0 us at 3 workers,
5.5 / 6.7 us at 50, 118 / 96 us at 1,000; `least-work` 0.8 / 1.0, 5.6 / 6.8, 113 / 101 us. The ledger path with `least-active` costs the same (5.8 / 7.1 us at 50). The budget is 25 us at 50 workers and 400 us at 1,000.
The strategy alone (`BenchmarkSelect`) is 0.3 to 0.5 us at 3 to 50 workers and 3.4 us at 1,000 with three allocations whatever the fleet size; most of the router's time is the private copy of the view.

**Honest limits.** Everything here is simulator-validated, not hardware-validated, and no scheduler comparison has been run: hypotheses H3 and H4 (the confirmatory primaries) are evaluated only in PR J, after the
predictions freeze (PR I). In particular: (a) the mock worker counts words as tokens, its `recent_tokens_per_second` is the sum of its active generations (it grows with load, so the peak measures the highest
demand seen, not a hardware limit), and it has no batching: on it `least-work`'s capacity term is a demand signal, which is exactly why PR E builds the richer simulator; (b) the cost of a request is an estimate
(`max_tokens` is an upper bound on output, the input estimate is low for dense text, ADR-015) and a request counts its full cost until it ends; (c) other gateways' load is visible only through the reported active count and priced at
the mean; with several gateways each gateway's ledger sees its own share (Phase 8); (d) a worker that flaps out of the snapshot loses its ledger entry while its requests run, so `WorkAhead` is a lower bound
then; (e) a hand-off (Phase 15) bypasses the scheduler, so under queueing the strategies matter less (H10); (f) the capacity floor and the prior keep every worker finite and pickable, but a worker the strategy never
picks is refreshed only by the decay, so a persistently slower worker is re-learned each time it decays back toward the prior; (h) a worker beyond the tracked-worker cap (4,096) is untracked: its ledger reads empty and it has no capacity, so with `WorkAhead = 0` it would look the lightest worker of all. The strategies therefore score it
like an invalid candidate, the median of the valid scores: it is neither favoured nor exiled (it takes its turn whenever it ties the best), it is counted as `untracked` (not also as `invalid`), and when every
candidate is untracked the choice is `least-active`'s, flagged degraded; (i) capacity claims are bounded only relative to peers (K = 10, above): a cold fleet, a majority of liars and a lie within K times the median
are not caught; (g) the TTFT signal and the probe-priority rule (D7, D11) are PR C2's, so a half-open
breaker's worker is probed by the existing Phase 15 mechanism only.

### [later PR] Observability beyond the minimum, simulator, experiments (D24 to D28)

`latency-aware` and the TTFT signal (PR C2), the rest of D24, the simulator (PR E), the experiment framework (G-lite), the pre-registered predictions (PR I)
and the results (PR J) are recorded here as they land. Until then the plan is the source of truth. The defaults do not change: `scheduler.strategy` stays `round-robin` and the overlay stays `max`.

## Consequences

- **Positive**: one estimator for billing and scheduling; the seam is reviewed without any behaviour in it; the differential fixtures remain as a regression guard for later pull requests of the phase.
- **Negative**: `WorkerSnapshot` carries a field that is invisible on the wire, which a reader of the struct could miss (the tag and a test say so); the handler now does one estimate per request even without a limiter
  (about 30 microseconds for a 100 KB body, benchmarked, against a 25 ms overhead budget).
- **Follow-ups**: the rest of the plan; flipping any default needs the pre-registered criteria of D26 and an explicit decision.
