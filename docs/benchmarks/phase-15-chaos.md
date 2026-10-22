# Phase 15 — Final benchmark note (PR G): defaults on against off, and what the whole phase measured

**Simulator results, not production results.** Everything here ran on one Apple M5 Pro laptop (15 CPUs, loopback, `caffeinate`), in the embedded benchmark cluster with **mock workers** (no GPU, no batching, no real network), or against the Phase 16 compose stack with mock workers. They show shapes, not capacity. The plan defines no success target, so none is asserted, and an effect is claimed only where the min-max ranges of the 3 repeats do not overlap (the Phase 7 spread rule).

## Defaults on against off (PR G, the shipped default)

Three groups per scenario, 3 repeats each, same seed, binaries built from `git archive` of master 16dda6b (**absent**: it has no resilience section, `--resilience off` is a no-op and the result records `absent`) and of this branch (**off**: `--resilience off`; **default = on**: no flag, so the shipped default is measured). All 39 chaos runs and 9 Run A runs of the batch are in the tables; every chaos run was valid under the gate (battery 98% falling to 76% while discharging, clock drift 0.0000 s over 5 s before the batch, no sleep or clock step, `--allow-dirty` because the archives carry no VCS stamp). Fault window, min / median / max:

| scenario | group | success % (primary) | attempts per request (primary) | time to isolate s | success p95 / p99 ms | failed requests |
|---|---|---|---|---|---|---|
| worker-freeze (header timeout 2 s) | absent | 93.45 / 93.45 / 93.45 | 1.000 | not reached | 165 / 169 | 72 |
| | off | 93.45 / 93.45 / 93.45 | 1.000 | not reached | 165 / 169-170 | 72 |
| | **default** | **99.44 / 99.44 / 99.44** | 1.000 | 14.0 / 14.0 / 14.0 | 164 / 168-169 | 12 |
| midstream | absent | 87.57 / 87.58 / 87.58 | 1.000 | not reached | 165 / 169 | 323 |
| | off | 87.58 / 87.58 / 87.58 | 1.000 | not reached | 165 / 169 | 323 |
| | **default** | **91.89 / 92.01 / 92.39** | 1.000 | 12.0 / 13.0 / 13.0 | 164-165 / 168-169 | 190-203 |
| overload (48 clients on 32 slots) | absent | 67.58 / 67.90 / 67.95 | 0.675-0.678 | n/a | 163-164 / 168 | 1280-1296 |
| | off | 62.73 / 67.97 / 68.00 | 0.626-0.679 | n/a | 163-164 / 169 | 1280-1377 |
| | **default** | **100 / 100 / 100** | 1.000 | n/a | 227-228 / 238-239 | 0 (5379-5384 queued, deepest queue 16 of 128) |
| drain-roll (announce 1.5 s) | absent / off / default | 100 / 100 / 100 in all three | 1.000 in all three | n/a | 164-165 / 168-169 | 0 |

By the spread rule: **off agrees with master within spread on both primary measures in all four scenarios** (the behavioural check of the kill switch). Default differs from both on success in freeze, midstream and overload (ranges disjoint), on attempts per request in overload only, and not at all in drain-roll. The scenario-level reading is in the sections below; in short: a frozen worker costs 12 failures instead of 72 (the in-flight requests at the moment of the freeze, then ejection by the breaker 14 s after the fault began, mostly the 2 s header timeouts plus the consecutive-fault rule), mid-stream aborts are only partly handled (the symmetric accounting makes the ratio rule slow, ADR-020), and the surplus load of the overload scenario waits in the queue instead of getting 503, at the price of about 60 ms more at p95. The overload comparison offers different loads to the groups (closed-loop clients that get a 503 back off), see the PR E caveat below.

**Run A (decision D21)**: 3 workers x concurrency 4 (12 slots), 12 closed-loop clients, least-active, 30 s window, seed 1 (`benchmark run ... --allow-overload`; master and off with `--allow-errors` so the repeats run):

| group | valid runs | error rate % | requests/s | latency p95 ms |
|---|---|---|---|---|
| absent (master) | 0 of 3 | 19.33 / 19.62 / 19.67 | 49.1 / 49.7 / 49.9 | 164.5-164.8 |
| off (branch) | 0 of 3 | 17.99 / 18.86 / 21.16 | 46.4 / 50.2 / 52.1 | 165.3-165.4 |
| default = on (branch) | 3 of 3 | 0 / 0 / 0 | 88.5 / 89.1 / 90.3 | 183.3-187.7 |

`resilience.mode: off` **reproduces the Phase 10 Run A failure** (about 19% `NO_CAPACITY`; Phase 10 measured 19.44% with 8 clients on real worker processes) and agrees with master within spread; the default produces none, because the hand-off gives a freed slot straight to the head waiter, so the stale-load shortage no longer turns into 503. The Run A estimator problem itself is untouched (Phase 14). The load average printed at the start of the last Run A group was high (36.7, a leftover of the batch's own workers; the groups ran back to back); that group was valid and its numbers are in line with the PR E group (89-90 requests/s, 0% errors).

### What is NOT shown here
- Real workers, GPUs, batching, vLLM's real 503 vocabulary and drain behaviour; real packet loss, partitions, balancers or Kubernetes (Phase 13, 17, 18). Linux behaviour (TCP resets differ; CI only).
- More than one gateway (each decides alone: N times the budget, N breakers, N caps), open-loop overload, a real crowd at the default cap of 1000 (no run reached it), `max_attempts` above 2, fault windows longer than 20 s against the default 10 s cool-down.
- `worker-crash` was not repeated in this batch (the registry removes the worker before any breaker could act; the PR B baseline showed 100% success and 1.009 attempts per request and PR D changed nothing there, see below), and neither were `slow-worker`, `error-burst-*` and `conn-reset` (PR D's table below holds them, with the same off-equals-master result).
- Time to isolate for freeze and midstream is the harness' descriptive figure and is inflated by how it counts probes (PR D section); the spec's 10 s bound is met by the breaker itself, not demonstrated by that figure. It is not asserted.
- Latency of failures, goroutine and memory returns to baseline after chaos runs were checked in the PR D-F verification, not re-measured here.

## Silent-agent detection (decision D23): the Phase 10 kill experiment, repeated

Phase 10 killed a worker's agent and saw the critical alert fire 16.3 s later. Repeated on the Phase 16 compose stack (`make dev-up`, default on, one gateway, about 4 requests/s of load, Prometheus scraping the control plane every 5 s and evaluating the fleet rules every 5 s), script and raw output in `docs/benchmarks/phase-15-assets/`:

| event, seconds after SIGKILL of the agent container (2 runs) | run 1 | run 2 |
|---|---|---|
| control plane stops offering the worker (heartbeat age passes the 5 s suspect threshold; the gateway then stops routing to it) | 4.2 | 4.1 |
| `ServerFlowWorkerHeartbeatLate` (new warning, age above 5 s) firing | 15.4 | 15.8 |
| `ServerFlowWorkerHeartbeatStale` (critical, age above 10 s) and `ServerFlowWorkerNotServing` firing | 20.5 | 20.9 |
| UNHEALTHY / LOST in the registry | 9.3 / 29.1 | 9.2 / 29.0 |

158 of 158 requests after the kill succeeded in both runs (none needed a second attempt: the worker was already out of routing and its backend kept answering what it had). **Isolation is fast (about 4 s plus the gateway's refresh), the alerts are slow: about 11 s of scrape, evaluation and Prometheus lag sit on top of the age they test**, so the critical alert is still above the 10 s target of spec §31, as the plan said it might be; this run is 4 s slower than Phase 10's 16.3 s on a different setup (containers rather than host processes), and the cause of the extra seconds was not isolated. The warning at 5 s was added as planned (the critical alert stays at 10 s so one late heartbeat never pages; promtool cases prove it).
With the backend frozen instead (`docker pause`, its agent still running): the agent reported FAILED 2.6 s after the pause (the control plane stopped offering the worker), `ServerFlowWorkerNotServing` fired at 10.8 s, 395 requests were sent and 5 failed: the ones already in flight on the frozen worker, which stalled until the gateway's header timeout (not retried by design; their faults later opened the breaker of the already-ejected worker). Embedded freeze (above) is the case where the agent keeps saying READY and the breaker is the only detector.

## What the whole phase measured (PR B to PR F, collected; all simulator)

| PR / scenario | absent (master) | default on | note |
|---|---|---|---|
| B baseline `worker-crash` | 100% success, 1.009 attempts/request | not repeated | registry-plus-retry, the breaker is not the detector |
| D `worker-freeze` | 93.45% | 99.44% | repeated in this note |
| D slow worker beyond the header timeout | 96.15% | 99.59% | |
| D slow worker below the header timeout | 100%, 1.000 | identical | the false-positive check: no breaker opens |
| D `error-burst-unavailable` 100% / 50% | attempts 1.333 / 1.143 | 1.005 / 1.019-1.056 | success 100% in both |
| D always-500 worker | 74.99% | 93.40-93.78% | via the ratio rule; 500s are never retried |
| D `midstream` | 87.58% | 91.61-91.82% (this note: 91.89-92.39%) | partly handled, ADR-020 limits |
| D `conn-reset` | attempts 1.333 | 1.005 | |
| E `overload` | 63.7-68.4% | 100% | p95 164 to 227 ms; closed-loop clients offered more load |
| E Run A | 19% errors | 0% | |
| F `drain-roll` | 100%, 1.000 | 100%, 1.000 (no announce: 1.002-1.003) | process-level rolls: worker roll 0 failed; gateway roll behind a polling balancer 0 failed with a 1 s grace, 1-5 failed with none |

No scenario got worse on the two pre-registered measures. Where the feature costs something it is stated: p95 of successes rises about 60 ms in `overload` (waiting), a probe of a frozen worker can cost one client a 504, and a worker aborting 20-30% of its streams is never ejected. Invalid runs of the earlier batches are listed at the end of the PR D section; the PR G batch had none.

---

# Phase 15 — Chaos benchmark, PR F (drain): drain-roll and process-level rolls

**Simulator results.** Embedded cluster on one Apple M5 Pro laptop (loopback, `caffeinate`, mock workers, no GPU), plus process tests on the same machine with real binaries and a test balancer. They show the shape of the drain, not production behaviour; the plan defines no success target. Battery 88% and charging, clock drift 0.0000 s over 5 s before the runs, all runs valid under the gate, load average 2.4-3.5. Raw results are in the scratchpad (not committed).

## drain-roll (`benchmark chaos --scenario drain-roll --repeat 3`): fault window, min / median / max of 3 valid runs

4 workers x 8 slots, 14 closed-loop clients, round-robin, seed 1, half streaming; every worker is drained and replaced in turn (mock announces draining and keeps serving 1.5 s, then refuses; the agent deregisters; a fresh worker starts). **absent** = master 16dda6b (no resilience section, no drain), **off** / **on** = this branch (`resilience.mode` off / on; the embedded gateway has no readiness grace), **on, no announce** = on with `--no-drain-announce` (the mock refuses at the moment it drains).

| group | fault-window success % (primary) | attempts per request (primary) | time to recover s | success p95 / p99 ms |
|---|---|---|---|---|
| absent (master) | 100 / 100 / 100 | 1.000 / 1.000 / 1.000 | 1.0 | 165 / 165-169 |
| off (branch) | 100 / 100 / 100 | 1.000 / 1.000 / 1.000 | 1.0 | 165 / 168-169 |
| on (branch), announce 1.5 s | 100 / 100 / 100 | 1.000 / 1.000 / 1.000 | 1.0 | 165 / 168-169 |
| on (branch), no announce | 100 / 100 / 100 | 1.002 / 1.003 / 1.003 | 1.0 | 165 / 168 |

- With an announce above the heartbeat plus the registry refresh the roll costs no failure and no retry (attempts per request 1.000 in every group). Without the announce no client failed either, but about 0.2-0.3% more attempts were made: the retries covering the requests routed to a worker that had begun refusing. The ranges of the announce groups and of the no-announce group do not overlap on attempts per request (1.000 against 1.002-1.003): that is the one distinguishable effect, and it is the cost the announce avoids. Success is 100% everywhere, so there is nothing to compare there. off agrees with absent within spread.
- Not shown: the benchmark drains workers in process (the registry is told at once), so it cannot show the heartbeat-propagation window of a real SIGTERM; the process tests below do.

## Process-level rolls (`tests/integration/drain_process_test.go`, real binaries, one run each from the 30-run loop; counts vary by run)

| test | setup | result |
|---|---|---|
| worker roll, announce 1 s | 2 mock workers + agents, gateway (off), 2 closed-loop clients, both mocks SIGTERMed in turn and replaced | 110 requests, 0 failed, 0 retries |
| worker roll, announce 0 | same | 42 requests, 0 failed, 3 retries (the cost of no announce) |
| gateway roll, grace 1 s | two gateways (on) behind a test balancer that polls `/readyz` every 50 ms and does not retry; both SIGTERMed in turn and replaced | 108-216 requests, 0 failed; a stream in flight at the first SIGTERM completed |
| gateway roll, grace 0 | same, `drain.readiness_grace: 0` | 1-5 failed requests per roll (`connection refused` from the balancer's detection gap) of 47-100: reported, not asserted |

The grace-0 failures are what the readiness grace is for: the balancer needs a poll to notice. Repeatability: the whole process-test set passed 30 times in a row (`-count=30`, 1,843 s), and the in-process tests `-race -count=30` (gateway) and `-count=10` (chaos).

## What this does not show

Real load balancers, Kubernetes preStop and termination periods, vLLM's own drain, several gateways each with a different poll cadence, signals on Linux, and drains longer than the shutdown timeout. The numbers are this machine on this day; a range that overlaps another is not a difference.

---

# Phase 15 — Chaos benchmark, PR E (admission queue, hand-off, load shedding, global cap): overload and Run A

**Simulator results.** Embedded cluster on one Apple M5 Pro laptop (15 CPUs, battery, `caffeinate`, loopback), mock workers, no GPU. They show the shape of the queue's effect, not capacity, and the plan defines no success target, so none is asserted.
Three groups per experiment, 3 repeats each, same seed: **absent** = master 16dda6b (PR A+B; `--resilience off` is a no-op there and the result records `absent`), **off** = this branch (PR E on PR D, f653c00 plus later test-only commits) with `resilience.mode: off`, **on** = this branch with `mode: on` (all queue defaults: depth 128, `max_wait` 3 s derived down to `min(3 s, first-byte deadline)`, share 0.5, global cap 1000).
Binaries were built from `git archive` copies of the two trees (no VCS stamp, so chaos runs used `--allow-dirty`). Battery 96%, clock drift 0.0000 s over 5 s before the runs, all chaos runs valid under the gate; raw results are in `/tmp/p15e-runs` (not committed). Load average at the start of the runs was 1.4-3.3.

## Overload (`benchmark chaos --scenario overload`): fault window, min / median / max of 3 valid runs

4 workers x 8 slots = 32 slots; 19 base closed-loop clients, 29 more added for the 20 s fault window (48 clients, 1.5 x the slots); `uniform-short`, half streaming, round-robin.

| group | fault-window success % (primary) | attempts per request (primary) | requests sent | failed | queued | shed | gateway no-capacity | deepest queue | mean wait ms | success p95 / p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| absent (master) | 63.72 / 68.32 / 68.36 | 0.64 / 0.68 / 0.68 | 3768 / 4039 / 4043 | 1278 / 1281 / 1367 | 0 | 0 | 1278 / 1281 / 1367 | 0 | - | 163-164 / 168 |
| off (branch) | 67.18 / 68.37 / 68.41 | 0.67 / 0.68 / 0.68 | 3958 / 4021 / 4027 | 1272 / 1272 / 1299 | 0 | 0 | 1272 / 1272 / 1299 | 0 | - | 163-164 / 168-169 |
| on (branch) | 100 / 100 / 100 | 1.00 / 1.00 / 1.00 | 5413 / 5414 / 5414 | 0 | 5384 / 5384 / 5385 | 0 | 0 | 16 / 16 / 16 | 59.3 / 59.3 / 59.3 | 227 / 239 |

- Primary measures by the spread rule: off agrees with master within spread (success 67.18-68.41 against 63.72-68.36: the ranges overlap, no distinguishable difference; attempts per request 0.67-0.68 against 0.64-0.68: overlap). **on differs from both on both primary measures** (ranges do not overlap): 100% against 64-68%.
- What happened: the 48 clients keep 16 requests more than the 32 slots in flight; with the queue the surplus waits (5,384 requests queued in the fault window, mean wait 59 ms, the queue never deeper than 16, far under its bound of 128) and every request is served; without it the same surplus is answered 503 at once (1,278-1,367 failures). The price is latency: success p95 rises from 164 to 227 ms (the wait), and the whole-run TTFT p95 rises from about 20 ms (master and off: 20.3-20.4 ms) to about 94 ms (on: 93.4-94.2 ms; p50 20.2 to about 63 ms), because the first token waits behind the queue (raw `result.json` in `/tmp/p15e-runs/overload-*`).
- **Caveats, stated:** closed-loop clients back off after a 503, so the master/off groups offered *less* load than the on group, and closed-loop clients in the on group offered more load because they never backed off (3,768-4,043 against 5,413 requests): the comparison is of what the clients experienced, not of equal offered load. Nothing was shed in the on group because 48 closed-loop clients can never fill a 128-deep queue: the shed path is exercised by `tests/chaos/overload_test.go` (depth 6, 16 clients on 4 slots) and the unit tests, not by this table. An open-loop crowd and real GPU contention are not modelled. The hand-off's contribution is not separable here from the refresh wake-up.

## Run A reproduction (decision D21): 3 workers x concurrency 4 (12 slots), 12 closed-loop clients, least-active, 30 s window

`benchmark run --workers 3 --mock-concurrency 4 --mock-queue 32 --concurrency 12 --allow-overload --scheduler least-active --workload uniform-short --duration 30s --repeat 3 --seed 1` (master and off groups with `--allow-errors` so the three repeats run; they are marked invalid by the harness' 5% rule, as Phase 10 run A was). The second block adds `--worker-profile heterogeneous` (fast, medium and slow in turn).

| profile | group | valid runs | error rate % (min / median / max) | requests/s | latency p95 ms |
|---|---|---|---|---|---|
| identical | absent (master) | 0 of 3 | 18.14 / 19.34 / 19.38 | 49.9 / 50.2 / 51.7 | 164.3 / 164.5 / 164.6 |
| identical | off (branch) | 0 of 3 | 18.86 / 18.87 / 19.39 | 49.9 / 50.3 / 50.5 | 164.4 / 164.6 / 165.2 |
| identical | on (branch) | 3 of 3 | 0 / 0 / 0 | 89.3 / 90.0 / 90.4 | 182.6 / 184.5 / 187.1 |
| heterogeneous | absent (master) | 0 of 3 | 27.88 / 28.11 / 29.10 | 28.9 / 29.7 / 29.8 | 625 / 635 / 636 |
| heterogeneous | off (branch) | 0 of 3 | 27.90 / 28.03 / 28.17 | 29.3 / 29.7 / 29.8 | 621 / 626 / 641 |
| heterogeneous | on (branch) | 3 of 3 | 0 / 0 / 0 | 54.4 / 54.6 / 54.9 | 619 / 635 / 647 |

- `mode: off` reproduces the Phase 10 run A failure (about 19% `NO_CAPACITY` with the identical profile; the Phase 10 number was 19.44% with 8 clients and the real workers), and agrees with master within spread.
- With the default on, the same load produces no failures: the stale-count shortage that made workers look full right after their requests finished is absorbed by the hand-off (a freed slot goes straight to the head waiter) and by the wake-up at each refresh. The throughput difference (50 against 90 requests/s) is mostly the closed-loop clients that no longer back off after a 503. In the heterogeneous profile the p95 is unchanged within spread (619-647 against 621-641 ms), the failures are gone.
- This does not "fix" Phase 14's estimator problem (the scheduler still trusts a stale reported count); it makes a request wait for a slot instead of failing while the count is wrong.

## Differential: `mode: off` is master

39 request shapes (registry: success JSON/stream, unknown model, bad JSON, missing fields, retry on reset/502/503/503 `queue_full`/empty stream, single and both workers 503, all workers reported full, draining only, 500, 504, 400, 404/429, mid-stream failure (stream and JSON), header timeout, first-byte stall, client disconnect, redirect, `/v1/models`, `/healthz`, `/readyz`, 405/404, body too large, no snapshot, no workers, `max_attempts` 1 and 3, mixed models, one worker reported full, a failed worker; static: success, 503, unknown model, mid-stream) against a fake control plane and fake workers, one gateway per shape; status, headers (minus date and request ID), bodies, normalised logs and `/metrics` series (names, labels, counter and gauge values, histogram counts; Go and process collectors excluded) compared between the master build and the branch build:

| comparison | shapes that differ |
|---|---|
| master against branch, default configuration | 0 of 39 |
| master against branch, `resilience.mode: off` set explicitly | 0 of 39 |
| master against branch, `resilience.mode: on` (the harness is sensitive) | 33 of 39 (new series and the `verdict` log field; responses differ only in the all-workers-full shape: a 3 s wait, then `503 NO_CAPACITY` with `Retry-After: 3` and the queue-timeout message, instead of an immediate 503 with `Retry-After: 1`) |

The golden series files for static and registry mode are unchanged by this PR.

## What this does not show

Open-loop overload, the shed path under a realistic crowd, several gateways each with their own queue, real GPU queueing, and the default cap of 1000 (no run reached it). Drain is PR F. The numbers above are the machine's on this day; a range that overlaps another is not a difference.

---

# Phase 15 — Chaos benchmark, PR D (circuit breakers, retry budget, first-byte deadline, failure taxonomy)

Part of the seven-PR delivery of Phase 15 (plan `docs/plans/completed/phase-15-fault-tolerance.md`, decisions D18 to D20). This section is **PR D**: the breaker, budget, deadline and taxonomy are wired
(ADR-020), and the PR B chaos harness is run on the branch with `resilience.mode` **off** and **on** against the PR B baseline of master (`docs/benchmarks/phase-15-baseline/`, resilience `absent`, commit
92c5488). The queue, shedding, drain and the overload and drain-roll scenarios belong to PRs E and F; worker-crash is a registry-plus-retry baseline and was not repeated.
Every number describes this machine on this day (Apple M5 Pro, 15 CPUs, loopback, mock workers); none is a capacity claim, and the plan defines no success target, so none is asserted.

**Second batch (after the independent verification, F1 change: an ok is counted at the first byte and converted by a later abort).** All `on` groups were re-run with the new binary (`/tmp/p15d-runs3`); `off` groups are unchanged code paths and keep their numbers. `conn-reset` `on` could not be re-run validly (5 invalid runs: the wall clock moved 13-43 s against the monotonic clock, the machine kept sleeping or being stepped); its `on` group is the first batch's, whose code path (a pre-first-byte fault) the F1 change does not touch.

**Harness caveat (from PR C):** before this PR `--resilience on` recorded `on` without any effect because the gateway did not read the section. Groups produced earlier than this PR with `on` must not be used; the
`on` groups below were all produced by the binary built from this branch.

## How the runs were made

- `benchmark chaos --scenario S --repeat 3 --resilience off|on`, scale 1, defaults otherwise (4 workers, 14 closed-loop clients, round-robin, seed 1, workload `uniform-short`, half streaming, fault window 20 s,
  header timeout lowered by the scenario where noted), binary built with `go build` from the clean committed branch, one scenario at a time, `off` then `on` per scenario, no full-suite lock held (checked before every
  group), `caffeinate` on. Breaker settings are the defaults (5 consecutive faults or 50% over 10 outcomes, 10 s cool-down, one probe, 50% cap); budget 20% / 10 per 10 s. The 3-repeat spread rule of Phase 7 applies:
  an effect is claimed only when the min-max ranges of two groups do not overlap.
- The machine **slept during the first batch** (a network cut-off of the session): 9 runs were invalid under the validity gate ("no sleep or clock step") and were **never compared**; the affected groups were
  re-run cleanly in a second batch (`error-burst-unavailable-partial` on, `error-burst-500` off and on, `midstream` off). The `midstream` `on` group comes from the first batch (3 valid runs). Invalid runs: see the end.
- The load average is the 1-minute value the harness prints at the start of each repeat; it was 1.3-7.7 in the first batches and up to 14.8 in the last one (after the F1 change; a laptop on battery with other work running), so the load column is part of every table. Groups ran in different load: a difference
  between groups is only read where ranges are disjoint and the cause is visible in the mechanism.
- Primary measures (pre-registered): success rate in the fault window, attempts per request in it. Everything else is descriptive.

## Results (fault window; min / median / max of 3 valid runs)

#### worker-freeze

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 93.45 / 93.45 / 93.45 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 2.69, 3.18, 3.43 |
| off (branch) | 3 | 93.45 / 93.45 / 93.45 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.07, 3.52, 2.65 |
| on (branch) | 3 | 99.44 / 99.44 / 99.44 | 1.000 / 1.000 / 1.000 | 12.0 / 13.0 / 14.0 | 1.0 / 1.0 / 1.0 | 164 / 165 / 165 | 3.42, 3.92, 5.65 |
- success rate: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### slow-worker-header-timeout-below-delay

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 96.15 / 96.15 / 96.15 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 164 / 164 / 165 | 2.87, 2.84, 3.43 |
| off (branch) | 3 | 96.15 / 96.15 / 96.15 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 164 / 164 / 165 | 2.42, 2.46, 2.19 |
| on (branch) | 3 | 99.59 / 99.59 / 99.59 | 1.000 / 1.000 / 1.000 | 12.0 / 13.0 / 14.0 | 1.0 / 1.0 / 1.0 | 164 / 164 / 165 | 3.75, 4.35, 4.86 |
- success rate: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### slow-worker-header-timeout-above-delay

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 167 / 167 / 167 | 3.35, 3.58, 3.02 |
| off (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 167 / 167 / 167 | 4.93, 3.86, 3.93 |
| on (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 167 / 167 / 167 | 10.49, 11.67, 14.83 |
- success rate: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### error-burst-unavailable-100

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 100.00 / 100.00 / 100.00 | 1.333 / 1.333 / 1.333 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 3.59, 4.56, 4.21 |
| off (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.333 / 1.333 / 1.333 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.60, 4.76, 5.00 |
| on (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.005 / 1.005 / 1.005 | 1.0 / 1.0 / 1.0 | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 10.05, 8.89, 5.49 |
- success rate: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference
- attempts per request: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs

#### error-burst-unavailable-50

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 100.00 / 100.00 / 100.00 | 1.143 / 1.143 / 1.143 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 4.83, 5.80, 6.19 |
| off (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.143 / 1.143 / 1.143 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 5.26, 5.24, 5.79 |
| on (branch) | 3 | 100.00 / 100.00 / 100.00 | 1.019 / 1.019 / 1.056 | 12.0 / 14.0 / 14.0 | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 8.86, 9.50, 5.52 |
- success rate: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference
- attempts per request: baseline vs off: differs; off vs on: differs; baseline vs on: differs

#### error-burst-error-100

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 74.99 / 74.99 / 74.99 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 6.03, 5.99, 5.28 |
| off (branch) | 3 | 74.99 / 74.99 / 74.99 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 3.80, 2.68, 2.96 |
| on (branch) | 3 | 93.40 / 93.49 / 93.78 | 1.000 / 1.000 / 1.000 | 4.0 / 5.0 / 5.0 | 1.0 / 1.0 / 1.0 | 164 / 165 / 165 | 12.01, 10.19, 6.91 |
- success rate: baseline vs off: no distinguishable difference; off vs on: differs; baseline vs on: differs
- attempts per request: baseline vs off: no distinguishable difference; off vs on: no distinguishable difference; baseline vs on: no distinguishable difference

#### midstream

| group | runs | fault-window success % (min/med/max) | attempts per request | time to isolate s | time to recover s | success p95 ms | load average (1m) at start of each repeat |
|---|---|---|---|---|---|---|---|
| absent (master) | 3 | 87.57 / 87.58 / 87.60 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 5.09, 6.24, 5.15 |
| off (branch) | 3 | 87.57 / 87.58 / 87.58 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 | 2.11, 1.41, 1.32 |
| on (branch) | 3 | 91.61 / 91.74 / 91.82 | 1.000 / 1.000 / 1.000 | 13.0 / 13.0 / 14.0 | 1.0 / 1.0 / 1.0 | 164 / 165 / 165 | 4.99, 4.13, 3.35 |
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
  (the retries stop because the worker stops being chosen) with 100% success in both; the harness' time to isolate is 1.0 s. With half of the requests failing the attempts per request fall from 1.143 to 1.019-1.056 and the
  worker is isolated at 12-14 s: the breaker trips on the ratio rule, which has to outweigh the worker's healthy history in the 10 s window (ADR-020, limits).
- **Faults retries cannot absorb get better, not fixed.** A frozen worker (header timeout 2 s) raised the fault-window success rate from 93.45% to 99.44%, a slow worker beyond the timeout from 96.15% to 99.59%, an
  always-500 worker from 74.99% to 93.40-93.78% (the 500s before the ratio trips are relayed, never retried) and mid-stream aborts from 87.58% to 91.61-91.82%. In each the remaining failures are those before ejection and one
  per probe; the plan sets no target and none is claimed beyond "the ranges do not overlap".
- **Time to isolate is not the breaker's time for freeze and slow (12-14 s).** The harness counts selections from `scheduler_selections_total` (at the start of an attempt) and subtracts probes from
  `circuit_breaker_probes_total` (counted at the end of the attempt). A probe of a frozen worker lasts one header timeout, so its selection and its probe count land in different one-second buckets. The per-second
  series of run_001 (freeze, on) shows the frozen worker's selections stop after second 15 (the fault began at 13) with its failures at seconds 15 and 17, then a single probe selection at second 26 that is counted as a
  probe at second 28 (and cost one client a 504). The spec's bound of 10 s for freeze is therefore met by the breaker (about 2-4 s here) but not by the harness' descriptive figure; that figure is not asserted.
- **Slow but successful is not ejected.** With the header timeout above the delay (`slow-worker-header-timeout-above-delay`) `on` and `off` and master are identical: 100%, 1.000, no isolation. The false-positive check passes.
- **Mid-stream aborts are only partly handled, and less well than in the first cut of this PR.** Success rises (87.58% to 91.61-91.82%; the first cut, which counted a success only when a stream completed, reached 94.49% but ejected
  mostly healthy workers on long streams, see ADR-020) and the harness' time to isolate is 13-14 s: an aborted stream is an ok converted to a fault, builds no consecutive streak, and the ratio rule opens the breaker once
  the faults equal the worker's healthy outcomes in the window; a probe whose stream breaks after its first byte has already passed, the breaker closes with an empty window and lets more through, as many as the streams in flight plus what the ratio needs (ADR-020, limits, with the simulated scaling).
  The cost of the symmetric accounting is therefore visible here, and it is reported as such.
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
| r3on/run_023 (on) | conn-reset | no sleep or clock step: the wall clock moved 43.444s against the monotonic clock during the run... |
| r3on/run_024 (on) | conn-reset | no sleep or clock step: the wall clock moved 13.124s against the monotonic clock during the run... |
| r4on/run_001 (on) | conn-reset | no sleep or clock step: the wall clock moved 13.11s against the monotonic clock during the run ... |
| r4on/run_002 (on) | conn-reset | no sleep or clock step: the wall clock moved 12.967s against the monotonic clock during the run... |
| r4on/run_003 (on) | conn-reset | no sleep or clock step: the wall clock moved 25.327s against the monotonic clock during the run... |

Raw results: `/tmp/p15d-runs*` (not committed); the aggregation is mechanical (valid runs only, min/median/max, disjoint ranges). The master baseline files are the ones committed in PR B.
