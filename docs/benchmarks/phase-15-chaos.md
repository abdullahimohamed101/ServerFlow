# Phase 15 — Chaos benchmark, PR D (circuit breakers, retry budget, first-byte deadline, failure taxonomy)

Part of the seven-PR delivery of Phase 15 (plan `docs/plans/active/phase-15-fault-tolerance.md`, decisions D18 to D20). This section is **PR D**: the breaker, budget, deadline and taxonomy are wired
(ADR-020), and the PR B chaos harness is run on the branch with `resilience.mode` **off** and **on** against the PR B baseline of master (`docs/benchmarks/phase-15-baseline/`, resilience `absent`, commit
92c5488). The queue, shedding, drain and the overload and drain-roll scenarios belong to PRs E and F; worker-crash is a registry-plus-retry baseline and was not repeated.
Every number describes this machine on this day (Apple M5 Pro, 15 CPUs, loopback, mock workers); none is a capacity claim, and the plan defines no success target, so none is asserted.

**Harness caveat (from PR C):** before this PR `--resilience on` recorded `on` without any effect because the gateway did not read the section. Groups produced earlier than this PR with `on` must not be used; the
`on` groups below were all produced by the binary built from this branch.

## How the runs were made

- `benchmark chaos --scenario S --repeat 3 --resilience off|on`, scale 1, defaults otherwise (4 workers, 14 closed-loop clients, round-robin, seed 1, workload `uniform-short`, half streaming, fault window 20 s,
  header timeout lowered by the scenario where noted), binary built with `go build` from the clean committed branch, one scenario at a time, `off` then `on` per scenario, no full-suite lock held (checked before every
  group), `caffeinate` on. Breaker settings are the defaults (5 consecutive faults or 50% over 10 outcomes, 10 s cool-down, one probe, 50% cap); budget 20% / 10 per 10 s. The 3-repeat spread rule of Phase 7 applies:
  an effect is claimed only when the min-max ranges of two groups do not overlap.
- The machine **slept during the first batch** (a network cut-off of the session): 9 runs were invalid under the validity gate ("no sleep or clock step") and were **never compared**; the affected groups were
  re-run cleanly in a second batch (`error-burst-unavailable-partial` on, `error-burst-500` off and on, `midstream` off). The `midstream` `on` group comes from the first batch (3 valid runs). Invalid runs: see the end.
- The load average is the 1-minute value the harness prints at the start of each repeat; it was 1.3-7.7, mostly 2.5-5 (a laptop on battery with other work running). Groups ran in different load: a difference
  between groups is only read where ranges are disjoint and the cause is visible in the mechanism.
- Primary measures (pre-registered): success rate in the fault window, attempts per request in it. Everything else is descriptive.

## Results (fault window; min / median / max of 3 valid runs)

#### worker-freeze

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 93.45 / 93.45 / 93.45 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 2.69, 3.18, 3.43 |
| off (branch) | 3 | 93.45 / 93.45 / 93.45 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.07, 3.52, 2.65 |
| on (branch) | 3 | 99.44 / 99.44 / 99.44 | 1.000 / 1.000 / 1.000 | 12.0 / 12.0 / 14.0 | 1.0 / 1.0 / 1.0 | 164 / 164 / 164 | 3.39, 2.85, 2.52 |
- success rate: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### slow-worker-header-timeout-below-delay

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 96.15 / 96.15 / 96.15 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 164 / 164 / 165 | 2.87, 2.84, 3.43 |
| off (branch) | 3 | 96.15 / 96.15 / 96.15 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 164 / 164 / 165 | 2.42, 2.46, 2.19 |
| on (branch) | 3 | 99.59 / 99.59 / 99.59 | 1.000 / 1.000 / 1.000 | 12.0 / 12.0 / 14.0 | 1.0 / 1.0 / 1.0 | 164 / 164 / 164 | 3.34, 4.16, 4.83 |
- success rate: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### slow-worker-header-timeout-above-delay

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 167 / 167 / 167 | 3.35, 3.58, 3.02 |
| off (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 167 / 167 / 167 | 4.93, 3.86, 3.93 |
| on (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 167 / 167 / 167 | 4.66, 3.90, 4.11 |
- success rate: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### error-burst-unavailable-100

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 100.00 / 100.00 / 100.00 | 1.333 / 1.333 / 1.333 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 3.59, 4.56, 4.21 |
| off (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.333 / 1.333 / 1.333 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.60, 4.76, 5.00 |
| on (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.005 / 1.005 / 1.005 | 1.0 / 1.0 / 1.0 | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 5.20, 4.61, 4.85 |
- success rate: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference
- attempts per request: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs

#### error-burst-unavailable-50

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 100.00 / 100.00 / 100.00 | 1.143 / 1.143 / 1.143 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.83, 5.80, 6.19 |
| off (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.143 / 1.143 / 1.143 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 5.26, 5.24, 5.79 |
| on (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.081 / 1.081 / 1.082 | 12.0 / 12.0 / 12.0 | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 2.82, 2.17, 3.75 |
- success rate: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference
- attempts per request: baseline vs off: differs; off vs on: differs; baseline vs on: differs

#### error-burst-error-100

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 74.99 / 74.99 / 74.99 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 6.03, 5.99, 5.28 |
| off (branch) | 3 | 74.99 / 74.99 / 74.99 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 3.80, 2.68, 2.96 |
| on (branch) | 3 | 93.45 / 93.60 / 93.71 | 1.000 / 1.000 / 1.000 | 5.0 / 5.0 / 5.0 | 1.0 / 1.0 / 1.0 | 164 / 164 / 164 | 3.02, 3.03, 2.47 |
- success rate: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### midstream

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 87.57 / 87.58 / 87.60 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 5.09, 6.24, 5.15 |
| off (branch) | 3 | 87.57 / 87.58 / 87.58 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 2.11, 1.41, 1.32 |
| on (branch) | 3 | 94.49 / 94.49 / 94.49 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 7.65, 5.03, 4.55 |
- success rate: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### conn-reset

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 100.00 / 100.00 / 100.00 | 1.332 / 1.333 / 1.334 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.63, 3.95, 3.55 |
| off (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.333 / 1.333 / 1.333 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.47, 6.18, 3.82 |
| on (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.005 / 1.005 / 1.005 | 1.0 / 1.0 / 1.0 | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 2.67, 2.21, 1.82 |
- success rate: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference
- attempts per request: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs

The comparison lines use the spread rule on the two primary measures. Two caveats on reading them: a "differs" on attempts per request for `error-burst-unavailable-50` between master and `off` is a gap of 0.0001 (1.1427-1.1429
against 1.14286): the seeded fault makes the runs nearly identical and the gap is noise, not a difference; and the `off` group is the behavioural check -- it agrees with master within spread on both primary measures in every scenario.

## What the numbers say, and do not say

- **Off is master.** In all eight scenarios `mode: off` agrees with the baseline (resilience `absent`) on both primary measures within spread (apart from the 0.0001 above).
- **Hard faults are isolated quickly and cost retries nothing.** Against a worker answering 503 `mock_failure` or resetting connections, `on` cuts the attempts per request in the fault window from 1.333 to 1.005
  (the retries stop because the worker stops being chosen) with 100% success in both; the harness' time to isolate is 1.0 s. With half of the requests failing the attempts per request fall from 1.143 to 1.081 and the
  worker is isolated at 12 s: the breaker trips on the ratio rule, which has to outweigh the worker's healthy history in the 10 s window (ADR-020, limits).
- **Faults retries cannot absorb get better, not fixed.** A frozen worker (header timeout 2 s) raised the fault-window success rate from 93.45% to 99.44%, a slow worker beyond the timeout from 96.15% to 99.59%, an
  always-500 worker from 74.99% to 93.45-93.71% (the 500s before the ratio trips are relayed, never retried) and mid-stream aborts from 87.58% to 94.49%. In each the remaining failures are those before ejection and one
  per probe; the plan sets no target and none is claimed beyond "the ranges do not overlap".
- **Time to isolate is not the breaker's time for freeze and slow (12-14 s).** The harness counts selections from `scheduler_selections_total` (at the start of an attempt) and subtracts probes from
  `circuit_breaker_probes_total` (counted at the end of the attempt). A probe of a frozen worker lasts one header timeout, so its selection and its probe count land in different one-second buckets. The per-second
  series of run_001 (freeze, on) shows the frozen worker's selections stop after second 15 (the fault began at 13) with its failures at seconds 15 and 17, then a single probe selection at second 26 that is counted as a
  probe at second 28 (and cost one client a 504). The spec's bound of 10 s for freeze is therefore met by the breaker (about 2-4 s here) but not by the harness' descriptive figure; that figure is not asserted.
- **Slow but successful is not ejected.** With the header timeout above the delay (`slow-worker-header-timeout-above-delay`) `on` and `off` and master are identical: 100%, 1.000, no isolation. The false-positive check passes.
- **Mid-stream aborts are only partly handled.** Success rises (87.58% to 94.49%) but the breaker never reaches "isolated": a probe whose stream breaks after its first byte has already passed, the breaker closes and lets
  five more through (ADR-020, limits). This is a measured property of verdicts taken at the first byte, reported as such.
- **No scenario got worse** in the two primary measures. Latency of successes (p95 164-167 ms) is the same in every group.
- **What this does not show:** crash (the registry is faster), overload and drain (PRs E and F), real networks, real GPUs, other fault rates, `max_attempts` above 2, more than one gateway, or the default cool-down
  against a fault window longer than 20 s.

## Invalid runs (never compared)

| run | scenario | reason |
| --- | --- | --- |
| r1off/run_016 (off) | error-burst-error-100 | no sleep or clock step: the wall clock moved 14.612s against the monotonic clock during the run... |
| r1off/run_017 (off) | error-burst-error-100 | no sleep or clock step: the wall clock moved 1h40m1.938s against the monotonic clock during the... |
| r1off/run_018 (off) | error-burst-error-100 | no sleep or clock step: the wall clock moved 1h16m42.667s against the monotonic clock during th... |
| r1off/run_019 (off) | midstream | no sleep or clock step: the wall clock moved 26m56.68s against the monotonic clock during the r... |
| r1on/run_014 (on) | error-burst-unavailable-50 | no sleep or clock step: the wall clock moved 17.313s against the monotonic clock during the run... |
| r1on/run_015 (on) | error-burst-unavailable-50 | no sleep or clock step: the wall clock moved 13.13s against the monotonic clock during the run ... |
| r1on/run_016 (on) | error-burst-error-100 | no sleep or clock step: the wall clock moved 59m57.084s against the monotonic clock during the ... |
| r1on/run_017 (on) | error-burst-error-100 | no sleep or clock step: the wall clock moved 59m57.196s against the monotonic clock during the ... |
| r1on/run_018 (on) | error-burst-error-100 | no sleep or clock step: the wall clock moved 59m57.078s against the monotonic clock during the ... |

Raw results: `/tmp/p15d-runs*` (not committed); the aggregation is mechanical (valid runs only, min/median/max, disjoint ranges). The master baseline files are the ones committed in PR B.
