# Phase 14 predictions: pre-registered hypotheses for the scheduler experiments

Status: **frozen** (PR I, ADR-022, plan D25/D26/D28 step 3). The freeze record is `docs/benchmarks/phase-14-freeze.json`; it pins the text of this file **above the `## Amendments` heading** by SHA-256. Evaluate with `make bench-verdict` (tool and rules: `docs/benchmarks/phase-14-experiment-framework.md`).
Every claim below is about simulated worlds: **simulator-validated, not hardware-validated**. Nothing here is a result. No scheduler comparison run exists in this repository at the time of the freeze (D28 step 1); the only runs ever made were sub-second pipeline runs inside unit tests, whose numbers were asserted on structure and discarded.

## What is registered and what is not

Registered here (parsed by the tool): H1 (guard), H2, H3, H4, H5 (the tuned range and the held-out gain), H11. They are what the real-time embedded cluster can measure with the metrics `benchmark verdict` reads (`latency_p95`, ...) and with the strategies that exist (`round-robin`, `least-active`, `least-tokens`, `least-work`).

Not registered, and why (each would need its own predictions file, frozen before its runs; an amendment below cannot add a verdict because amendments are never parsed):

| Plan row | Not registered because |
| --- | --- |
| H6 (output fraction) | needs a world where true output length is a fraction of `max_tokens`; the embedded harness does not set the engine's `output_fraction` |
| H7 (`latency-aware`, TTFT) | `latency-aware` and the TTFT signal (PR C2) do not exist |
| H8 (`w_gpu`) | the GPU term is not built |
| H9 (worker added mid-run) | the harness has no mid-run membership change; not a metric the verdict tool reads |
| H10 (120% load, queue on) | not required for the slice; the queue-on cell is a separate, later registration |
| H12 (decision cost) | a Go benchmark with a budget (`internal/scheduler`), not a result-file hypothesis |
| H13 (rank stability under +-30% perturbation) | needs parameter files for the perturbed worlds and an independent choice of signs (D25); not built |

H1's first half (time from breaker close to first pick, `max` against `ledger`) is **not a prediction either**: it is the E0 experiment, already measured in `internal/bench/embedded/recovered_worker_test.go` (0.71 s median against 12 ms) before this freeze, and the verdict tool has no metric for it. What is registered as H1 is the other half, a guard: the ledger overlay must not change `least-active` p95 latency in a healthy fleet.
The earlier ADR-022 "H1 refusal guard" (refusals rise by less than 0.5 pp) stays a repository test; refusals cannot be a verdict metric because the baseline error rate is zero (a relative difference to a zero baseline is INVALID), so a run with a high error rate is handled by the invalid-run rule below.

## The single-developer caveat (D25 item 7)

Measurements exist already and are not hidden: E0 (above), the ledger refusal guard recorded in `docs/architecture/scheduling.md` (median rise in refusals below saturation 0.00 pp over five paired runs; ledger and `max` alike show occasional heartbeat-long false-refusal episodes), that page's independent multi-gateway simulation of the ledger, and the load probes in the Load section. None compares `least-tokens` or `least-work` against another strategy. One person wrote the simulator, the strategies, these predictions and the verdict tool, and has seen the pipeline run in tests. **This pre-registration is not blind.** The freeze is a norm backed by `git log` (this file and the freeze record are committed before any experiment result file exists), not a guard against bias. Mitigations: the draft table committed at `1a1841a` is the audit anchor and the thresholds that moved since are listed below; the held-out seeds are from D23's held-out range and the independent verifier reviews them in this PR; the verifier designs one workload and one world variant blind later (PR J); the tool never edits a prediction.

## A deliberate change to the plan: at least 8 pairs for H3 and H4

The plan (D26) uses 5 repeats per side for real-time cells. The verdict tool floors every p-value at the exact sign-test minimum `2/2^n` (n = non-zero pairs), because a bootstrap p-value is anti-conservative at small n (ADR-022, G-lite decision 11; `docs/benchmarks/phase-14-experiment-framework.md`, "How a verdict is computed"). At alpha = 0.05 a superiority claim therefore **cannot be CONFIRMED from 5 pairs** (smallest p 0.0625), needs 6 alone and 7 when two confirmatory claims share a Holm family of 2.
So, as a deliberate departure from D26 recorded here and in the PR text: **H3 and H4 cells use at least 8 pairs** (8 distinct seeds, one repeat each: 101 to 108). With 8 non-zero pairs the floor is 0.0078, and the three confirmatory blocks below (H3, H4a, H4b: a family of 3) each have a floored p of at least 0.0078, and Holm's running maximum makes the best case 0.0234 for each of the three, below 0.05. Equivalence claims (H1, H2, H11) and refutation do not use the p-value and keep 6 pairs (6 seeds, one repeat each; the tool's minimum is 5).
More pairs are allowed, fewer are INVALID. Distinct seeds (different request streams) are used instead of repeats of one seed, because repeats of one deterministic seed share a request stream and are not independent samples. This change does not alter any threshold or the claims; it only makes H3/H4 confirmable at all with the real-time cluster. A confirmed superiority from 8 pairs is still a statement about 8 seeds on one machine.

## Worlds, workloads, strategies, cells

Worlds are the embedded cluster with 3 workers and 8 concurrent requests each (24 slots), heartbeat 1 s and registry refresh 100 ms (the embedded constants), one gateway, resilience at the build default.

| Name | World label (as recorded) | How it is run | Plan row |
| --- | --- | --- | --- |
| W0 | `mock:identical` | constant-speed mock workers, `--worker-profile identical` | W0 |
| W1 | `sim:default` | simulator engine, `--sim-profile default`: batching, homogeneous | W1 |
| W2 | `sim:fast,medium,slow` | `--sim-profile fast,medium,slow`: three hardware generations, heterogeneous | W2 |
| W3 | `sim:kv-tight` | `--sim-profile kv-tight`: KV budget 6144 tokens (a prompt of 6144 or more tokens is refused) | W3, **not used** (see below) |
| W4 | `sim:noisy` | `--sim-profile noisy`: step and prefill noise CV 0.3, homogeneous | W4 (homogeneous only) |

All simulator profiles are `assumed` (order-of-magnitude shapes, not fitted to any engine; PR H, the calibration, may not land before the experiments). Their parameter hashes are recorded in every result (`parameter_hash`, `sim_params_hash`) and the tool refuses to pair runs with different hashes, so a change of the parameters after this freeze makes the affected verdicts INVALID rather than silently different.
Deviations from D23: W3 is not used because every long prompt (2000 to 8000 tokens) above the 6143-token limit would be refused, which would make `mixed`, `heavy-tail` and `prefill-heavy` cells invalid; and W4 is the built-in homogeneous noisy profile, not a noisy heterogeneous one.

Workloads: `uniform-short`, `mixed`, `heavy-tail`, `prefill-heavy` (shapes pinned by digest in `internal/bench/workload`).
Load: closed loop at **14 clients** in the main cells (14 of 24 slots, 58%) and **7 clients** in the negative controls (29%). This replaces the plan's "80% of capacity" and "50%": the harness calls more than 60% of the slots (`runner.SafeSlotFraction`, 14 of 24) likely to be invalid, and 14 is the highest load inside that limit. The earlier idea that high load causes the `round-robin` errors was wrong; the diagnosis (an independent investigation before this freeze) is that the errors are **504 UPSTREAM_TIMEOUT** from the embedded gateway's fixed 30 s upstream header timeout: a long non-streaming request (the `long` class, 500 to 1500 output tokens) sends its headers only when generation completes, so on a medium or slow simulated worker it exceeds 30 s under any strategy, in a homogeneous world as well. The cause is request size, not slot count. The fix is the harness flag `--upstream-header-timeout` (PR #75, `fix/bench-upstream-header-timeout`), which these cells set to **120 s**. **This file depends on PR #75 being merged; a cell run without the flag (30 s) is invalid**, and the verifier audits it: the verdict selectors cannot match it, but a non-default value is recorded in each result as `upstream_header_timeout_ms` (120000 expected) in the experiment options.
A second, real effect is expected and reportable, not an error to remove: `round-robin` sends the slow, KV-poor worker its share of long requests regardless of its state, and the simulator's admission is strict first-in-first-out, so a head request that does not fit the KV budget blocks everything behind it (queue waits of 10 to 30 s were observed in a probe). That is a finding about round-robin in W2 and is part of H4b's evidence.
Why 14 and not fewer: with the cause explained there is no reason to run light, and more load separates the strategies; an earlier draft ran 8 clients only because the errors had been misread as a load problem. Read-only probes before this freeze (not scheduler comparisons; outputs discarded, none in the repository): `round-robin`, W2, `mixed`, 14 clients, 60 s, `--upstream-header-timeout 120s`, seeds 101, 102 and 103 (each run takes several minutes and the remaining seeds were not finished when the load was chosen): all three runs valid with 0 failed requests of 85, 97 and 99 sent, and the result recorded `upstream_header_timeout_ms` 120000. Before the fix (30 s), the same cell at 14 clients and 30 s had 1 invalid run of 8 (up to 8.7% errors) and at 10 and 8 clients similar errors, which is how the timeout was found. The probe says nothing about the other schedulers or seeds 104 to 108.
Each run is 60 s (`--duration 60s`), one repeat, the seed of the pair. The harness's default 5% error-rate validity gate stays for every cell and every arm, with no exemption: an UPSTREAM_TIMEOUT (or any other failure) counts toward it like any other. The results record the error codes per cell and report the error rate of every arm beside each verdict. One gateway behaviour is disclosed and not changed here: a 504 from the upstream timeout is not retried (`attempts: 1`), and why is unexplained. The load is part of every selector (`concurrency`), so a run at another load matches no hypothesis.
Schedulers and overlay: `round-robin` and `least-active` run with the default overlay `max`; `least-tokens` and `least-work` run with `ledger` (the harness switches it on); H1's treatment arm is `least-active` with `--load-overlay ledger`. `input_weight` and `output_fraction` stay at their defaults (1 and 1) except in H5; the selectors of `least-tokens` and `least-work` pin `input_weight` 1.0, so tuned-weight runs can never be mixed into another cell.

A cell is one hypothesis arm: (scheduler, overlay, workload, world, concurrency) over its registered seeds. A pair is the same seed in both arms. Command for one run (change the flags per cell):

```text
make bench BENCH_ARGS="--scheduler least-tokens --workers 3 --sim-profile default --workload heavy-tail --seed 101 --concurrency 14 --duration 60s --upstream-header-timeout 120s --repeat 1"
```

For W0 use `--worker-profile identical` and no `--sim-profile`. Arms of a pair are run back to back on an otherwise idle machine, in an order alternated by seed; a run the harness marks invalid is not dropped and makes its cell INVALID. If PR J reruns it, the invalid attempt is moved out of the runs directory first (two runs for one seed and repeat make the pairing ambiguous, which is itself INVALID) and both attempts are reported in PR J. The verifier also audits `worker_count` (3) and `duration_seconds` (60) in every result, as well as `upstream_header_timeout_ms` (120000), because the selectors cannot match them.

## Statistics rules (D26, fixed here)

- Effect: per pair `(b - a) / a` of the registered metric; the summary is the median; the interval is the percentile bootstrap, 95%, 10,000 resamples, seed 20260101 (the defaults; no hypothesis overrides them).
- Superiority is CONFIRMED when the median improvement reaches `confirm_min_effect` and the interval excludes zero, and for confirmatory claims the Holm-adjusted floored p-value is below 0.05. REFUTED when the whole interval lies below `refute_max_effect`; an interval that includes zero is INCONCLUSIVE (not refuted). Effects are fractions: 0.10 is 10%.
- Equivalence is CONFIRMED only when the whole 95% interval lies strictly inside the margin; REFUTED when entirely outside the refute margin (default the margin); INCONCLUSIVE when it straddles. H11 sets `invalid_margin` 0.05: a strategy entirely outside +-5% in a control means the harness is suspect and the verdict is INVALID, not a finding about the strategy.
- **Confirmatory primaries are H3 and H4 only.** H4 is the roll-up of two superiority blocks (H4a, H4b), both marked confirmatory, so the Holm family has three members: H3, H4a, H4b. Everything else is exploratory or a guard and is labelled so in the results. A confirmatory block with no data counts with p = 1.
- Roll-ups (`all_of`): H2 = H2a and H2b; H4 = H4a and H4b; H11 = its six parts. A roll-up reads REFUTED if any part is REFUTED even when another part is INVALID; the results read the parts.
- H4b encodes "round-robin is at least 25% worse than least-work" as least-work at least 20% lower than round-robin (RR = 1.25 x LW means LW = 0.8 x RR), because the tool measures the effect as a fraction of the baseline arm.
- Zero spread is INCONCLUSIVE; fewer than 3 distinct seeds warns; a missing arm, a missing registered seed, fewer than 5 pairs, an invalid run, a run without an experiment record, differing parameter hashes or a non-positive baseline is INVALID (tool rules).
- Run validity includes the harness's default 5% error-rate limit, with no exemption for any error code (see Load); a cell with a run above it is INVALID and reported, which is how a large rise in refusals or timeouts is caught.

## Tuning and held-out data (D23, D26, D28)

- **Tuning set** (the only data any weight may be adjusted on): worlds W1 and W2, workloads `mixed` and `heavy-tail`, seeds 1 to 10, 14 clients, 60 s, `--upstream-header-timeout 120s`.
- **Held-out** (never used to adjust anything): seeds 101 to 108, W4 and the workload `prefill-heavy`, every cell registered above. The H3 and H4 cells use worlds and workloads of the tuning set but **seeds outside it**.
- **One tuning pass**, objective written before it: for `least-work` sweep `input_weight` over {0.1, 0.2, 0.3, 0.5, 0.75, 1.0} (`output_fraction` 1) on the four tuning cells; for each cell take the median `latency_p95` over the 10 seeds at each weight and divide it by that cell's value at weight 1.0; the tuned `input_weight` is the grid value with the lowest mean of those four ratios (ties: the larger value). The file is `{"tuned_at": ..., "values": {"input_weight": x}}`; its hash and time go into the freeze record (`tuned_values_sha256`, `tuned_values_frozen_at`) **before** any held-out run (frozen_at <= tuned_at <= tuned_values_frozen_at, enforced by the tool). The grid includes values outside [0.1, 0.5] so H5a can fail.
- The freeze record in this PR holds only the predictions hash and `frozen_at`; the tuned-values fields do not exist yet and are added by a later change that does not touch this file's frozen text.
- H5b compares `least-work` at `input_weight` 1 against `least-work` at the tuned value on held-out data. If the tuned value is 1.0 the arms are the same runs and the cell is INCONCLUSIVE (zero spread); that cannot happen if H5a is CONFIRMED, since 1.0 is outside [0.1, 0.5].
- Deviation from the plan's H5 draft: the draft tuned on `prefill-heavy` in W1, but D23 lists `prefill-heavy` as held-out; D23 governs here, and the tuning workloads are `mixed` and `heavy-tail`.

## Hypotheses

Confidence is the author's prior that the prediction will be CONFIRMED; several are expected to fail, and that is a result.

| ID | Claim | World, workload, load | Confirmed if | Refuted if | Role | Conf. |
| --- | --- | --- | --- | --- | --- | --- |
| H1 | the ledger overlay leaves `least-active` p95 latency unchanged in a healthy fleet | W0, `uniform-short`, 14 clients (58%), `max` against `ledger`, 6 seeds | interval inside +-5% | interval outside +-5% | guard | 0.85 |
| H2 | `least-tokens` and `least-work` equal `least-active` on p95 with identical workers (nothing to be aware of) | W0, `uniform-short`, 14 clients (58%), 6 seeds | each interval inside +-5% | either entirely outside +-5% | equivalence | 0.85 |
| H3 | `least-tokens` cuts p95 latency against `least-active` by at least 10%: count-based balancing treats a 7k-token and a 100-token request alike | W1, `heavy-tail`, 14 clients (58%), 8 seeds | median effect >= 10%, interval excludes 0, Holm p < 0.05 | interval entirely below 5% | **confirmatory** | 0.70 |
| H4a | `least-work` cuts p95 against `least-tokens` by at least 15% (speed-aware) | W2, `mixed`, 14 clients (58%), 8 seeds | median >= 15%, interval excludes 0, Holm p < 0.05 | interval entirely below 5% | **confirmatory** | 0.65 |
| H4b | `round-robin` is at least 25% worse than `least-work` (least-work at least 20% lower) | W2, `mixed`, 14 clients (58%), 8 seeds | median >= 20%, interval excludes 0, Holm p < 0.05 | interval entirely below 10% | **confirmatory** | 0.65 |
| H5a | the tuned `input_weight` lies in [0.1, 0.5] (prefill tokens are cheaper than decode tokens, so 1:1 misranks) | tuning set | tuned value in the range | tuned value outside it | exploratory | 0.60 |
| H5b | on held-out data the tuned weight cuts `least-work` p95 by at least 5% against `input_weight` 1 | W4, `prefill-heavy`, 14 clients (58%), 6 seeds | median >= 5%, interval excludes 0 | interval entirely below 3% | exploratory | 0.50 |
| H11 | no strategy differs from `least-active` in the controls: `round-robin`, `least-tokens`, `least-work`, in W0 and W1 | `uniform-short`, 7 clients (29%), 6 seeds | all six intervals inside +-3% | any entirely outside +-3% (INVALID if entirely outside +-5%) | negative control | 0.90 |

The plan's 0.65 for H4 is given to each of its two parts (so the prior for both together is lower). The priors for H1 (a different claim from the plan's H1) and H5b (the plan's 0.60 covered both halves of H5) are new.
Mechanism, in a sentence each: H3 and H4 follow ADR-022 and `docs/architecture/scheduling.md` (work-aware strategies use the gateway's in-flight token ledger and a capacity estimate, so they separate a long request from a short one and a slow worker from a fast one, which a request count cannot); H2 and H11 are the null (identical workers and light load leave nothing to be aware of); H1 follows from the ledger removing the stale-count rule without changing steady-state routing.

## Verdict blocks

The blocks below are the machine-readable form of the table. Selectors pin the world, the overlay, the load and (H5b) the weights; seeds are exactly the seeds that will be run.

```verdict
{"id": "H1", "kind": "equivalence", "claim": "the ledger overlay does not change least-active p95 latency in a healthy fleet (guard)", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "max", "concurrency": 14}, "b": {"scheduler": "least-active", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "ledger", "concurrency": 14}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.05}
```

```verdict
{"id": "H2a", "kind": "equivalence", "claim": "least-tokens is equivalent to least-active on p95 latency with identical workers", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "max", "concurrency": 14}, "b": {"scheduler": "least-tokens", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "ledger", "concurrency": 14, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.05}
```

```verdict
{"id": "H2b", "kind": "equivalence", "claim": "least-work is equivalent to least-active on p95 latency with identical workers", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "max", "concurrency": 14}, "b": {"scheduler": "least-work", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "ledger", "concurrency": 14, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.05}
```

```verdict
{"id": "H2", "kind": "all_of", "claim": "H2: both work-aware strategies are equivalent to least-active on identical workers", "parts": ["H2a", "H2b"]}
```

```verdict
{"id": "H3", "kind": "superiority", "claim": "least-tokens cuts p95 latency against least-active on heavy-tail in a homogeneous batching world", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "heavy-tail", "world": "sim:default", "load_overlay": "max", "concurrency": 14}, "b": {"scheduler": "least-tokens", "workload": "heavy-tail", "world": "sim:default", "load_overlay": "ledger", "concurrency": 14, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106, 107, 108], "direction": "lower", "confirmatory": true, "confirm_min_effect": 0.1, "refute_max_effect": 0.05}
```

```verdict
{"id": "H4a", "kind": "superiority", "claim": "least-work cuts p95 latency against least-tokens on mixed in a heterogeneous batching world", "metric": "latency_p95", "a": {"scheduler": "least-tokens", "workload": "mixed", "world": "sim:fast,medium,slow", "load_overlay": "ledger", "concurrency": 14, "input_weight": 1.0}, "b": {"scheduler": "least-work", "workload": "mixed", "world": "sim:fast,medium,slow", "load_overlay": "ledger", "concurrency": 14, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106, 107, 108], "direction": "lower", "confirmatory": true, "confirm_min_effect": 0.15, "refute_max_effect": 0.05}
```

```verdict
{"id": "H4b", "kind": "superiority", "claim": "round-robin is at least 25 percent worse than least-work on p95 latency (least-work at least 20 percent lower) on mixed in a heterogeneous batching world", "metric": "latency_p95", "a": {"scheduler": "round-robin", "workload": "mixed", "world": "sim:fast,medium,slow", "load_overlay": "max", "concurrency": 14}, "b": {"scheduler": "least-work", "workload": "mixed", "world": "sim:fast,medium,slow", "load_overlay": "ledger", "concurrency": 14, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106, 107, 108], "direction": "lower", "confirmatory": true, "confirm_min_effect": 0.2, "refute_max_effect": 0.1}
```

```verdict
{"id": "H4", "kind": "all_of", "claim": "H4: least-work beats least-tokens and round-robin by the registered margins", "parts": ["H4a", "H4b"]}
```

```verdict
{"id": "H5a", "kind": "tuned_range", "claim": "the input_weight that minimises p95 on the tuning set lies in [0.1, 0.5]", "value_key": "input_weight", "min": 0.1, "max": 0.5}
```

```verdict
{"id": "H5b", "kind": "superiority", "claim": "on held-out prefill-heavy the tuned input_weight cuts least-work p95 latency against input_weight 1 (exploratory)", "metric": "latency_p95", "a": {"scheduler": "least-work", "workload": "prefill-heavy", "world": "sim:noisy", "load_overlay": "ledger", "input_weight": 1.0, "concurrency": 14}, "b": {"scheduler": "least-work", "workload": "prefill-heavy", "world": "sim:noisy", "load_overlay": "ledger", "input_weight_tuned": "input_weight", "concurrency": 14}, "seeds": [101, 102, 103, 104, 105, 106], "direction": "lower", "confirm_min_effect": 0.05, "refute_max_effect": 0.03}
```

Controls (H11): `invalid_margin` 0.05 makes an interval entirely outside +-5% INVALID; the refute margin is the default, the margin 0.03.

```verdict
{"id": "H11-W0-rr", "kind": "equivalence", "claim": "round-robin is equivalent to least-active on p95 latency in the W0 control (uniform-short, 7 clients)", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "max", "concurrency": 7}, "b": {"scheduler": "round-robin", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "max", "concurrency": 7}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.03, "invalid_margin": 0.05}
```

```verdict
{"id": "H11-W0-lt", "kind": "equivalence", "claim": "least-tokens is equivalent to least-active on p95 latency in the W0 control (uniform-short, 7 clients)", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "max", "concurrency": 7}, "b": {"scheduler": "least-tokens", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "ledger", "concurrency": 7, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.03, "invalid_margin": 0.05}
```

```verdict
{"id": "H11-W0-lw", "kind": "equivalence", "claim": "least-work is equivalent to least-active on p95 latency in the W0 control (uniform-short, 7 clients)", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "max", "concurrency": 7}, "b": {"scheduler": "least-work", "workload": "uniform-short", "world": "mock:identical", "load_overlay": "ledger", "concurrency": 7, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.03, "invalid_margin": 0.05}
```

```verdict
{"id": "H11-W1-rr", "kind": "equivalence", "claim": "round-robin is equivalent to least-active on p95 latency in the W1 control (uniform-short, 7 clients)", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "sim:default", "load_overlay": "max", "concurrency": 7}, "b": {"scheduler": "round-robin", "workload": "uniform-short", "world": "sim:default", "load_overlay": "max", "concurrency": 7}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.03, "invalid_margin": 0.05}
```

```verdict
{"id": "H11-W1-lt", "kind": "equivalence", "claim": "least-tokens is equivalent to least-active on p95 latency in the W1 control (uniform-short, 7 clients)", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "sim:default", "load_overlay": "max", "concurrency": 7}, "b": {"scheduler": "least-tokens", "workload": "uniform-short", "world": "sim:default", "load_overlay": "ledger", "concurrency": 7, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.03, "invalid_margin": 0.05}
```

```verdict
{"id": "H11-W1-lw", "kind": "equivalence", "claim": "least-work is equivalent to least-active on p95 latency in the W1 control (uniform-short, 7 clients)", "metric": "latency_p95", "a": {"scheduler": "least-active", "workload": "uniform-short", "world": "sim:default", "load_overlay": "max", "concurrency": 7}, "b": {"scheduler": "least-work", "workload": "uniform-short", "world": "sim:default", "load_overlay": "ledger", "concurrency": 7, "input_weight": 1.0}, "seeds": [101, 102, 103, 104, 105, 106], "margin": 0.03, "invalid_margin": 0.05}
```

```verdict
{"id": "H11", "kind": "all_of", "claim": "H11: no strategy differs from least-active in the negative controls; a difference outside 5 percent invalidates the harness", "parts": ["H11-W0-rr", "H11-W0-lt", "H11-W0-lw", "H11-W1-rr", "H11-W1-lt", "H11-W1-lw"]}
```

## What this file does not let anyone claim

Nothing about multiple gateways, absolute latency, a default flip from a simulator run, `latency-aware`, or that a simulator calibration transfers to a GPU engine (D26). A CONFIRMED verdict says what the registered seeds showed in the recorded worlds. The decision criteria for changing a default stay D26's and are not touched here.

## Thresholds moved since the draft table at `1a1841a`

| Row | Draft at `1a1841a` | Frozen | Reason |
| --- | --- | --- | --- |
| load | 80% and 50% of capacity | 14 clients (58%) and 7 clients (29%), 60 s, `--upstream-header-timeout 120s` | the harness's safe limit is 60%; 504 upstream timeouts diagnosed and removed by PR #75 (see Load) |
| H1 | heartbeat 2 s, time-to-first-pick below 0.3 s | a latency equivalence guard; the E0 timing is a repository test | the embedded heartbeat is 1 s (review F2); the verdict tool has no metric for E0 |
| H2 | within 5% "with non-overlapping interval" | interval entirely inside +-5% | TOST rule (review F8); `latency-aware` removed (not built) |
| H3 | 10% / 5%, 5 real-time repeats | 10% / 5%, **8 pairs** | the p-value floor, above |
| H4 | 15% and 25%, one block | H4a 15% / 5%, H4b 20% / 10%, 8 pairs | composite expressed as two blocks; 25% worse expressed as 20% lower |
| H5 | default-weight worse than tuned-weight least-tokens by 5% | range [0.1, 0.5] and a held-out 5% gain | ratio prediction (review F9); tuning workloads follow D23 |
| H11 | within 3%, 5% apart non-overlapping | inside +-3%, INVALID outside +-5% | TOST rule; the harness-suspect rule made explicit |
| H6 to H10, H12, H13 | drafted | not registered | see "What is registered and what is not" |

Seeds and worlds are new in this file (the draft left them open). H3 and H11 confidences are unchanged.

## Amendments

Dated amendments go below this heading. They are prose, are not parsed and are not part of the hash: an amendment can explain, but it cannot change a verdict and never applies to a run it would have rescued.
