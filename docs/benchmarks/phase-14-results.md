# Phase 14 results: the pre-registered scheduler experiments (PR J)

Status: results of the experiments registered in `docs/benchmarks/phase-14-predictions.md` (frozen in PR I; the freeze check prints "predictions match the freeze"). The verdicts below are the verdict tool's output, copied verbatim (`docs/benchmarks/phase-14-runs/verdict-final.txt`). Nothing here was reinterpreted: CONFIRMED, REFUTED, INCONCLUSIVE and INVALID stand as printed.
Every number is about simulated worlds: **simulator-validated, not hardware-validated**. Single machine (Apple M5 Pro, 15 CPUs, darwin/arm64), one gateway, 3 workers, closed-loop load, **non-blind pre-registration** (one person wrote the simulator, the strategies, the predictions and the tool; see the predictions file).

## Verdicts

Tool summary: 9 confirmed, 0 refuted, 8 inconclusive, 0 invalid of 17 (after the two reruns described under "Anomalies"; the first complete evaluation, kept in `verdict-first-attempt.txt`, had H3 INVALID and so H3 was the only difference).

| ID | Role | Verdict | Median effect and 95% interval (as printed) | Pairs |
| --- | --- | --- | --- | --- |
| H1 | guard | CONFIRMED | +0.02% [-0.01%, +0.08%] (relative difference, ledger against max) | 6 |
| H2a | equivalence | CONFIRMED | +0.00% [-0.02%, +0.09%] | 6 |
| H2b | equivalence | CONFIRMED | +0.00% [-0.01%, +0.02%] | 6 |
| H2 | roll-up | CONFIRMED | H2a and H2b | |
| **H3** | **confirmatory** | **INCONCLUSIVE** | +0.20% [-4.58%, +11.17%] (improvement), Holm-adjusted p 0.8555 | 8 |
| **H4a** | **confirmatory** | **INCONCLUSIVE** | +13.14% [+1.63%, +22.43%], p 0.0172, Holm-adjusted 0.0344 (median below the 15% threshold) | 8 |
| **H4b** | **confirmatory** | **CONFIRMED** | +51.05% [+25.76%, +67.67%], Holm-adjusted p 0.02344 | 8 |
| H4 | roll-up | INCONCLUSIVE | H4a INCONCLUSIVE, H4b CONFIRMED | |
| H5a | exploratory | CONFIRMED | tuned input_weight = 0.3, inside [0.1, 0.5] | tuning set |
| H5b | exploratory | INCONCLUSIVE | +2.68% [-5.25%, +5.23%], interval includes zero | 6 |
| H11-W0-rr / -lt / -lw | control | CONFIRMED (each) | +0.00% [-0.01%, +0.06%]; +0.00% [-0.02%, +0.05%]; +0.01% [-0.01%, +0.05%] | 6 |
| H11-W1-rr | control | INCONCLUSIVE | -0.07% [-3.93%, +1.02%] | 6 |
| H11-W1-lt | control | INCONCLUSIVE | -1.16% [-5.01%, +3.46%] | 6 |
| H11-W1-lw | control | INCONCLUSIVE | -1.32% [-5.73%, +6.45%] | 6 |
| H11 | roll-up | INCONCLUSIVE | three CONFIRMED, three INCONCLUSIVE | |

No hypothesis was REFUTED and none ended INVALID. What that does and does not mean:

- **H3 (least-tokens against least-active on heavy-tail, W1) was not confirmed.** The median improvement was 0.2%, the interval runs from -4.6% to +11.2%. The data neither show a 10% gain nor rule one out (the interval is not entirely below 5%).
- **H4a (least-work against least-tokens, W2, mixed) was not confirmed.** The interval excludes zero and the Holm-adjusted p-value is below 0.05, but the median 13.14% is below the pre-registered 15%, so the tool prints INCONCLUSIVE. It is not "close enough".
- **H4b (round-robin at least 25% worse than least-work, W2, mixed) was confirmed**: least-work p95 was 51% lower in the median over 8 seeds. This is the only confirmatory claim that holds, and it is a comparison against the weakest baseline in a heterogeneous world.
- Because H4a is not confirmed, the roll-up H4 is INCONCLUSIVE, not confirmed.
- H5a is CONFIRMED by the tool's rule, but the tuning curve is nearly flat (below), so the value 0.3 is a minimiser of noise-sized differences. H5b, the held-out test of that value, is INCONCLUSIVE.
- The three W1 controls are INCONCLUSIVE (wide intervals from 6 pairs of a real-time cluster), none was entirely outside +-5%, so none is INVALID and the harness is not flagged as suspect.
- The W0 equivalences (H1, H2, H11-W0) are CONFIRMED but weak evidence: the constant-speed mock worker gives p95 of 164 to 165 ms for every strategy and seed, so differences are near zero by construction. They show the strategies add no distortion on identical workers; they say nothing about performance.

## Tuning pass (H5a)

Protocol exactly as registered: `least-work`, `input_weight` in {0.1, 0.2, 0.3, 0.5, 0.75, 1.0}, `output_fraction` 1, worlds W1 and W2, workloads `mixed` and `heavy-tail`, seeds 1 to 10, 14 clients, 60 s, `--upstream-header-timeout 120s`: 240 runs, none invalid. For each cell the median `latency_p95` over the 10 seeds at each weight, divided by that cell's value at weight 1.0; the tuned value is the weight with the lowest mean of the four ratios (ties: the larger).

| input_weight | W1 mixed | W1 heavy-tail | W2 mixed | W2 heavy-tail | mean ratio |
| --- | --- | --- | --- | --- | --- |
| 0.1 | 0.9853 | 0.9875 | 0.9845 | 1.0117 | 0.99225 |
| 0.2 | 0.9788 | 1.0203 | 0.9594 | 1.0969 | 1.01385 |
| **0.3** | 0.9894 | 0.9505 | 0.9362 | 1.0611 | **0.98430** |
| 0.5 | 1.0036 | 0.9895 | 0.9547 | 1.0575 | 1.00132 |
| 0.75 | 1.0347 | 0.9634 | 1.0214 | 1.0687 | 1.02203 |
| 1.0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 1.00000 |

Tuned `input_weight` = **0.3** (best mean ratio 0.9843, a 1.6% improvement over weight 1.0; the curve is not monotone and differences between weights are within a few percent per cell, which is the scale of the seed noise). The per-run table is `docs/benchmarks/phase-14-runs/tuning.csv`. The tuned-values file `docs/benchmarks/phase-14-tuned-values.json` and its freeze fields (`tuned_values_sha256`, `tuned_values_frozen_at`) were committed before any held-out run was started (commit "Phase 14: tuned input_weight 0.3 ..."); the timestamps satisfy `frozen_at <= tuned_at <= tuned_values_frozen_at` and the tool accepted them. Seeds 1 to 10 only were used for tuning; held-out seeds (101 to 108) were not.

## Per-arm numbers (held-out cells, median `latency_p95` over the registered seeds, ms)

All 124 runs: `worker_count` 3, `duration_seconds` 60, `upstream_header_timeout_ms` 120000, target embedded, one repeat, the world label, workload, strategy, overlay, concurrency and input weight as registered (audited from every `result.json`; no mismatch). "Error rate" is failed over sent, summed over the cell's runs; "max run" is the largest single-run error rate.

| Cell (world, workload, clients, arm) | Seeds | Median p95 (ms) | Error rate | Max run | Error codes (all runs valid) |
| --- | --- | --- | --- | --- | --- |
| W0 uniform-short 14, least-active, max | 6 | 164.9 | 0 | 0 | none |
| W0 uniform-short 14, least-active, ledger | 6 | 164.9 | 0 | 0 | none |
| W0 uniform-short 14, least-tokens | 6 | 164.9 | 0 | 0 | none |
| W0 uniform-short 14, least-work | 6 | 164.9 | 0 | 0 | none |
| W0 uniform-short 7, least-active | 6 | 164.6 | 0 | 0 | none |
| W0 uniform-short 7, round-robin | 6 | 164.7 | 0 | 0 | none |
| W0 uniform-short 7, least-tokens | 6 | 164.7 | 0 | 0 | none |
| W0 uniform-short 7, least-work | 6 | 164.7 | 0 | 0 | none |
| W1 uniform-short 7, least-active | 6 | 3401 | 0 | 0 | none |
| W1 uniform-short 7, round-robin | 6 | 3379 | 0.46% | 2.89% | status_5xx 5 |
| W1 uniform-short 7, least-tokens | 6 | 3355 | 0.18% | 1.12% | status_5xx 2 |
| W1 uniform-short 7, least-work | 6 | 3329 | 0.28% | 1.74% | status_5xx 3 |
| W1 heavy-tail 14, least-active | 8 | 22754 | 0 | 0 | none |
| W1 heavy-tail 14, least-tokens | 8 | 23212 | 0 | 0 | none |
| W2 mixed 14, round-robin | 8 | 33062 | 0.12% | 1.19% | drain_timeout 1 |
| W2 mixed 14, least-tokens | 8 | 20364 | 0.09% | 0.72% | drain_timeout 1 |
| W2 mixed 14, least-work (weight 1) | 8 | 16912 | 0 | 0 | none |
| W4 prefill-heavy 14, least-work, weight 1 | 6 | 7037 | 0 | 0 | none |
| W4 prefill-heavy 14, least-work, tuned 0.3 | 6 | 7003 | 0 | 0 | none |

Per-seed p95 (ms), seeds in ascending order (101 to 106 or 101 to 108):

| Cell | 101 | 102 | 103 | 104 | 105 | 106 | 107 | 108 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| W1 heavy-tail, least-active | 29888 | 25437 | 22079 | 22432 | 16120 | 25427 | 21016 | 23076 |
| W1 heavy-tail, least-tokens | 28374 | 25649 | 23290 | 17164 | 14320 | 26592 | 20878 | 23135 |
| W2 mixed, round-robin | 70573 | 32426 | 27255 | 20371 | 25943 | 47552 | 33699 | 47485 |
| W2 mixed, least-tokens | 21571 | 22577 | 22912 | 16852 | 20608 | 20120 | 17344 | 19793 |
| W2 mixed, least-work | 18982 | 24074 | 18184 | 16577 | 12729 | 17247 | 16456 | 15354 |
| W4 prefill-heavy, least-work, weight 1 | 7342 | 6870 | 7018 | 6543 | 7881 | 7056 | | |
| W4 prefill-heavy, least-work, tuned 0.3 | 7004 | 6940 | 6647 | 7164 | 7472 | 7002 | | |

(The W1 heavy-tail rows show the rerun values for seeds 107 and 108; the original invalid attempts are below. The remaining cells have near-identical per-seed values in W0 and are in `phase-14-runs/held/`.) Tuning-set runs had 38 failed requests of 38558 sent in total (one `drain_timeout`; 0.1%), none invalid.

## Anomalies, deviations, caveats

- **Two invalid attempts, rerun by the document's rule.** In H3's cell, `least-active` seed 107 (12 of 177 failed, 6.78%) and `least-tokens` seed 108 (11 of 166, 6.63%) were marked invalid by the harness (class `status_5xx`; the result files record the class, not the individual status codes). Both started at 18:00:48 and 18:00:59 by the machine's wall clock and ended at 21:51; the wall clock jumped forward by about 3 h 50 min while they were running (the two runs in progress at the time are exactly these two; real elapsed time was about 90 s each). The jump is the likely cause, but this was not proven. The predictions file says an invalid run makes its cell INVALID and, if rerun, the invalid attempt is moved out of the runs directory and both attempts reported. I moved both out and reran only those two arms (not their pair partners); the reruns were valid (188 sent and 0 failed; 173 sent and 0 failed). Attempt 1 numbers are kept in `phase-14-runs/invalid-attempts/` and the evaluation with them in `verdict-first-attempt.txt` (H3 INVALID, everything else identical). The reruns were run later, back to back with each other, not back to back with the pair partner. Only invalid runs were rerun; no valid run was replaced.
- **Valid runs with errors.** W1 uniform-short (7 clients) had `status_5xx` failures in `round-robin` (5), `least-tokens` (2) and `least-work` (3) runs, all below the 5% gate (maximum 2.89% of one run) and none in `least-active`; not explained. W2 mixed had one `drain_timeout` each in a round-robin and a least-tokens run. These count toward the validity gate like any error; no error code was exempted.
- **Order and parallelism.** Two benchmark processes ran at a time (two lanes, each lane ran whole units of a seed's arms back to back, arm order reversed on even seeds; shared arms such as `least-active` in W0 were run once and used in H1, H2 and as the control baseline where the selectors match). The two processes shared the machine, so every run had a concurrent neighbour; runs used `nice`.
- **Wall clock.** The Mac's clock jumped during the session (above); the harness measures latency on a monotonic clock. Apart from the two runs above no run straddled a visible jump.
- **Tuning is weak.** The tuning objective found 0.3, but the differences between weights are comparable to the noise; a tuned value that passes H5a does not show that weighting inputs below outputs helps (H5b was INCONCLUSIVE).
- **Not blind, single developer, single machine**, 6 to 8 seeds per cell; wide intervals are the honest outcome. Simulator profiles are `assumed`, not fitted to any engine (no calibration, PR H).
- **Not run (not registered):** H6 to H10, H12, H13; W3; the queue-on cell; multiple gateways; `latency-aware`.
- **Decision criteria untouched.** The defaults do not change (`round-robin`, `max`); nothing here meets D26's criteria for a default change.

## Reproducing

`make bench BENCH_ARGS="..."` per cell as in the predictions file, then `make bench-verdict PREDICTIONS=docs/benchmarks/phase-14-predictions.md FREEZE=docs/benchmarks/phase-14-freeze.json TUNED=docs/benchmarks/phase-14-tuned-values.json RUNS=docs/benchmarks/phase-14-runs/held`. The archive keeps `result.json` only (no request logs, no `report.md`); the verdict tool reads exactly those. Run directories are named `<world>_<workload>_c<clients>_s<seed>_<arm>` where the arm is `la` (least-active, max), `lal` (least-active, ledger), `rr`, `lt`, `lw` (least-work, weight 1) or `lwtuned` / `lw1` (H5b). The verdict output depends on the file contents, not the directory names.
