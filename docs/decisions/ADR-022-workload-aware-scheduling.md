# ADR-022: Workload-Aware Scheduling (Signals, Ledger Overlay, Simulator Validation)

Status: Proposed, delivered in parts. Phase 14 is a sequence of pull requests (plan: `docs/plans/active/phase-14-workload-aware-scheduler.md`, decisions D1 to D29).
This record grows with each of them. Sections are marked **[PR A]** (delivered: the seam and the estimator extraction, no behaviour change) or
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

### [later PR] Ledger, overlay v2, signals, strategies, observability, simulator, experiments (D4 to D28)

The in-flight token ledger and the opt-in `load_overlay: ledger` (PR B), `least-tokens` and `least-work` (PR C1), the simulator (PR E), the experiment framework (G-lite), the pre-registered predictions (PR I)
and the results (PR J) are recorded here as they land. Until then the plan is the source of truth. The defaults do not change: `scheduler.strategy` stays `round-robin` and the overlay stays `max`.

## Consequences

- **Positive**: one estimator for billing and scheduling; the seam is reviewed without any behaviour in it; the differential fixtures remain as a regression guard for later pull requests of the phase.
- **Negative**: `WorkerSnapshot` carries a field that is invisible on the wire, which a reader of the struct could miss (the tag and a test say so); the handler now does one estimate per request even without a limiter
  (about 30 microseconds for a 100 KB body, benchmarked, against a 25 ms overhead budget).
- **Follow-ups**: the rest of the plan; flipping any default needs the pre-registered criteria of D26 and an explicit decision.
