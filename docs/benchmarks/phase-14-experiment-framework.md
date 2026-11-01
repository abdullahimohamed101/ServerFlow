# Phase 14 experiment framework: workloads, recorded worlds and the verdict tool

Status: implemented (PR G-lite, ADR-022). This page is about the tooling. **It contains no results.** No scheduler comparison has been run for it, and nothing here says one strategy is better than another.
Every number the tools print about a simulated world is **simulator-validated, not hardware-validated**.

## What was added

| Piece | Where | What it does |
| --- | --- | --- |
| Workloads `heavy-tail`, `prefill-heavy`, `decode-heavy` | `internal/bench/workload` | Seeded request mixes with pinned digests (tests fail if a class range, weight or rule changes) |
| Recorded experiment fields | `metadata.experiment` in `result.json` | Strategy, world, parameter hash and scheduler options of the run, additive (absent in older results) |
| `compare` warnings | `internal/bench/report` | Warns when two groups ran in different worlds, with different parameter hashes or scheduler options, or when a repeat group mixes hashes |
| `internal/bench/verdict` | package | Bootstrap interval, Holm correction, TOST, and the mechanical evaluation of a predictions file |
| `benchmark verdict`, `make bench-verdict` | `cmd/benchmark` | Prints one verdict per pre-registered hypothesis |

## The workloads

| Name | Classes | Shape |
| --- | --- | --- |
| `heavy-tail` | 90% `short` (100-300 in, 50-150 out), 10% `long` (2000-8000 in, 500-1500 out) | the 10% long requests carry about two thirds of the work |
| `prefill-heavy` | `prefill`: 4000-8000 in, 20-100 out | about 40 input tokens per output token |
| `decode-heavy` | `decode`: 50-200 in, 1000-2000 out | about 0.1 input tokens per output token |

All three use one model and one tenant and stay under the gateway's `max_tokens_limit` (4096). They are decided by the implementer (the plan names only "10% long" for `heavy-tail`); see ADR-022.

## Running a simulated cell

The embedded cluster can run its workers on the simulator engine (`internal/sim/engine`) and sets the work-aware strategies' options:

```bash
make bench BENCH_ARGS="--scheduler least-work --workers 3 --sim-profile fast,medium,slow --workload heavy-tail --seed 1 --concurrency 12 --duration 30s --repeat 5"
```

`--sim-profile` takes built-in profile names, each worker taking the next in turn. `--load-overlay`, `--input-weight` and `--output-fraction` set `scheduler.load_overlay` and `scheduler.cost.*` of the embedded gateway
(`least-tokens` and `least-work` need the `ledger` overlay and get it unless you say otherwise; saying `max` is refused). They apply to the embedded cluster only; a `--target` run is refused if any is set.

Each run records, under `metadata.experiment`:

```json
"experiment": {
  "strategy": "least-work",
  "world": "sim:fast,medium,slow",
  "parameter_hash": "<sha256>",
  "options": {"load_overlay": "ledger", "output_fraction": 1, "input_weight": 1, "capacity_halflife_ms": 60000, "capacity_floor": 0.25}
}
```

and, per simulated worker, `sim_profile` and `sim_params_hash` (the hash of `engine.Params`). `parameter_hash` is the hash of the cluster's parameters (profile, heartbeat, registry refresh, and per worker model, speed, TTFT, concurrency, queue size, simulator profile and its hash). It
leaves out the strategy and the scheduler options on purpose: comparing strategies in one world is the point, and the options are compared on their own. It can be recomputed from the stored `metadata.cluster` (`report.ParameterHash`). For a `--target` run the strategy is the declared one,
the world is `remote`, and there is no hash or options (the harness cannot see them).

`benchmark compare` lists differences in `world`, `parameter hash`, `load overlay` and `scheduler options` and prints a warning for each; a difference in the overlay is expected exactly when the strategies differ (the harness picks the ledger for the work-aware ones). A result that has no experiment record is never reported as different from one that has.

## The verdict tool

```bash
make bench-verdict PREDICTIONS=docs/benchmarks/phase-14-predictions.md FREEZE=docs/benchmarks/phase-14-freeze.json RUNS=benchmark/runs [TUNED=tuned.json]
# or
go run ./cmd/benchmark verdict --predictions P --freeze F --runs DIR [--tuned T] [--allow-unfrozen]
go run ./cmd/benchmark verdict --predictions P --print-hash
```

It reads the predictions, the freeze record and every `DIR/*/result.json`, prints one verdict per hypothesis and the summary, and exits non-zero if any verdict is INVALID (it still prints everything). It never writes a file and never edits a prediction.
The output is a pure function of its inputs (no clock, no paths, no randomness beyond the seeded bootstrap); a golden test pins it.

### The predictions file

Markdown for people, plus one fenced block per hypothesis for the tool:

````text
```verdict
{"id": "H3", "kind": "superiority", "claim": "least-tokens cuts p95 latency on heavy-tail",
 "metric": "latency_p95",
 "a": {"scheduler": "least-active", "workload": "heavy-tail", "world": "sim:fast"},
 "b": {"scheduler": "least-tokens", "workload": "heavy-tail", "world": "sim:fast"},
 "seeds": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
 "direction": "lower", "confirmatory": true, "confirm_min_effect": 0.10, "refute_max_effect": 0.05}
```
````

Unknown fields, a field that does not apply to the kind, a bad number or a duplicate id are errors; nothing is skipped silently. Kinds:

| Kind | Fields | Verdict |
| --- | --- | --- |
| `superiority` | `metric`, `a`, `b`, `seeds`, `direction` (`lower` or `higher`: where B must move against A), `confirm_min_effect`, `refute_max_effect`, `confirmatory`, optional `resamples`, `bootstrap_seed` | CONFIRMED if the median effect (the improvement as a fraction of A) reaches `confirm_min_effect` and the 95% interval excludes zero (and, if confirmatory, the Holm-adjusted p-value is below 0.05). REFUTED if the whole interval lies below `refute_max_effect`. Otherwise INCONCLUSIVE |
| `equivalence` | `metric`, `a`, `b`, `seeds`, `margin`, optional `refute_margin` (default `margin`), `invalid_margin` | CONFIRMED if the 95% interval of the median relative difference lies entirely inside (-margin, +margin); REFUTED if entirely outside `refute_margin`; INVALID if entirely outside `invalid_margin` (the harness is suspect, H11); otherwise INCONCLUSIVE |
| `tuned_range` | `value_key`, `min`, `max` | CONFIRMED if the tuned value is inside, REFUTED if outside, INVALID without a tuned file (H5's first part) |
| `all_of` | `parts` (ids of other hypotheses) | CONFIRMED if all are, REFUTED if any is, INVALID if any is and none is refuted, else INCONCLUSIVE (composite hypotheses such as H4) |

Selectors (`a`, `b`) need `scheduler` and `workload`; optional `world`, `load_overlay`, `input_weight` (or `input_weight_tuned`: a key of the tuned file whose value must equal the recorded input weight), `output_fraction`, `concurrency`, `rate_per_second`.
Fractions are fractions: `0.10` is 10%.

### How a verdict is computed

Pairs are formed by `(seed, repeat index)`; only the registered `seeds` count. For each pair the relative difference is `(b - a) / a`; the summary is the median; the interval is the percentile bootstrap (default 10,000 resamples, seed 20260101, 95%); the p-value and the Holm correction are
defined in `internal/bench/verdict/doc.go`, which is the place to read the formulas. The family of Holm is every `confirmatory: true` hypothesis in the file, **whether or not it has data**: one without data counts with p = 1, so missing data can never make another claim easier to confirm.

INVALID (not "pass") is the verdict whenever the evidence cannot answer: a missing arm or pair, a registered seed with no run, fewer than 5 pairs, a run with `valid: false` (invalid runs are reported, not dropped), a run without an experiment record, runs in different worlds (different parameter hashes), two runs for one
pair, a metric that was not measured, a non-positive baseline.

### The freeze

`--freeze` is a JSON record written in the freeze PR:

```json
{"predictions_sha256": "<benchmark verdict --print-hash>", "frozen_at": "2026-11-02T10:00:00Z",
 "tuned_values_sha256": "<sha256 of the tuned file>", "tuned_values_frozen_at": "2026-11-05T10:00:00Z"}
```

The last two appear after the single tuning pass. The hash covers the predictions file **up to a line that reads exactly `## Amendments`** (the whole file when there is none). Put that heading in the file before freezing; dated amendments go under it. They are prose, are never parsed (a verdict block under the heading
is ignored) and are not part of the hash, so an amendment can explain but cannot change a verdict. If the frozen text differs from the record, every verdict is INVALID and the output says the predictions were changed after the freeze. Without `--freeze` the tool refuses, unless `--allow-unfrozen` is given for a smoke check, and then the output says the verdicts are not pre-registered evidence.

The tuned-values file is `{"tuned_at": "...", "values": {"input_weight": 0.3}}`. It is refused (no verdicts, an error) if the freeze has no tuned-values record, the file does not hash to the recorded value, `tuned_at` is later than `tuned_values_frozen_at`, or `tuned_at` is before `frozen_at`.
The tool cannot know which data the tuning used; that stays the verifier's audit.

## What this does not do

- It does not run experiments, choose thresholds, seeds or primaries, or decide whether a prediction was sensible. Those are the predictions file's (PR I).
- It does not make a result true: a CONFIRMED verdict says what the simulated cells showed under the registered seeds. It says nothing about hardware, absolute latency, multiple gateways or any world other than the one in the result files.
- The bootstrap on a handful of pairs is crude (the minimum is 5, the plan's real-time cells). Wide intervals are the honest outcome and are reported as INCONCLUSIVE.
- Pairing real-time repeats of one seed by repeat index pairs runs with the same workload but independent timing noise; that is "paired" only in the workload.
