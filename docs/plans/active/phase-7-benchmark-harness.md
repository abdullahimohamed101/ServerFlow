# Phase 7 — Baseline Benchmark Harness

Status: In review (implemented; independent review and verification findings fixed)
Owner: coding agent
Depends on: Phase 6 (multi-worker routing; PR #7). Runs in parallel with Phase 9 (PostgreSQL); the two share no code, see "Parallel work".
Spec: `docs/architecture/serverflow-spec.md` §14, §31–36, §52, §58 Phase 7, §63
Hand-offs: Phase 6 benchmark note (`docs/benchmarks/phase-6-distribution.md`: "needs the Phase 7 harness"), Phase 6 ADR-012 (the black-hole effect and the retry/rotation interaction are the first things worth measuring properly).

## Outcome

One command runs a reproducible load test against ServerFlow and writes a result that another command
can compare to a second result:

```bash
benchmark run     --scheduler round-robin --workers 4 --concurrency 12 --duration 60s --workload mixed --seed 1 --repeat 3
benchmark compare run_001 run_004
```

`run` can boot a whole simulated cluster in-process (control plane, N mock workers with agents, a gateway
using the chosen scheduler) or aim at a gateway that is already running. It drives one of the spec's six
workloads in closed-loop (fixed concurrency) or open-loop (fixed arrival rate) mode, measures throughput,
latency, TTFT, queue behaviour and worker imbalance, and persists machine-readable JSON plus a readable
report. `compare` prints the table of deltas. No scheduler is declared better by the harness itself: it
reports measurements and their spread.

Acceptance headline (spec): *load generation (concurrency, duration, prompt, rate, scheduler), persisted
results, and a scheduler comparison.*

## Non-Goals

- No new scheduler strategies and no claim about which strategy is better (spec §14, §63); Phase 14 and
  Phase 23 use this harness for that.
- No real vLLM or GPU measurements (Phase 13); GPU utilization is reported only when workers supply it.
- No Postgres-backed result storage (Phase 9 owns the `benchmark_runs` table; wiring the harness to it is a
  small follow-up after both merge). Results are files this phase.
- No Prometheus or Grafana (Phase 10), no distributed load generation, no chaos injection (Phase 15).
- No changes to the gateway, scheduler, registry, or agent code.

## Current Architecture

- `cmd/benchmark` is a Phase 0 stub (config + a log line). Nothing generates load.
- Everything needed to run a cluster in process already exists as test code: `tests/integration` has a
  control plane, mock workers behind real agents, and a registry-mode gateway helper
  (`startControlPlane`, `startModelNode`, `startRegistryGateway`). It lives in `_test.go` files, so a binary
  cannot import it.
- The mock worker is configurable (TTFT, tokens/s, output tokens, concurrency, queue, failure injection,
  seed) and serves `/stats` (`completed`, `active`, queue depth). The control plane's `GET /v1/workers` carries
  each worker's `metrics` (active requests, queue depth, queued tokens).
- The gateway streams SSE and non-stream JSON, and exposes `inference_*` Prometheus metrics (no per-worker
  labels by design).
- Spec §32 lists what a run must persist; §33 the workloads; §34 the output; §35 the commands.
- `internal/api` normalizes requests; token counts for synthetic prompts only need to be roughly right.

## Decisions (confirm before implementation)

- **D1 Layout.** Library code in `internal/bench` (workloads, generator, collector, report, compare, embedded
  cluster); the CLI in `cmd/benchmark` with subcommands `run` and `compare` (and `list` for past runs).
  Result files in `benchmark/runs/` (gitignored except a committed sample); workload parameters as Go code.
- **D2 Two targets.** `--target URL` aims at a running gateway. `--embedded` (default when no target) boots a
  simulated cluster in process on loopback: a control plane, `--workers` mock workers with agents, and a
  registry-mode gateway running `--scheduler`. This is what makes Phase 7 runnable on a laptop and
  reproducible. The embedded cluster is a small non-test copy of the integration helpers; consolidating the two
  is a later refactor, kept out of this phase to avoid touching shared files.
- **D3 Closed-loop and open-loop.** `--concurrency N` runs N clients that each send the next request when the
  last finishes. `--rate R` sends R requests per second on a schedule regardless of responses. Open-loop
  latency is measured from the **intended** send time, so a slow server cannot hide its own queueing
  (coordinated omission); both modes are documented with that distinction.
- **D4 Workloads** (spec §33), each a deterministic function of the seed: `uniform-short` (100–300 in,
  50–150 out), `uniform-long` (2k–8k in, 500–1500 out), `mixed` (60/30/10 short/medium/long), `burst`
  (normal → 10x → normal; open-loop only), `hot-model` (90% one model, 10% another), `multi-tenant` (one
  aggressive client and several normal ones, each with its own API key). Input size is realised as synthetic
  text sized to the target token count (about four characters per token); output size through `max_tokens`.
- **D5 Determinism.** The request sequence (model, prompt size, max_tokens, tenant, send time) is generated
  from `--seed` before the run starts and recorded, so two runs with the same seed offer the same load.
  Measured latencies still vary with the machine; `--repeat N` runs the same configuration N times and reports
  the spread (min, median, max of each headline number) so noise is visible.
- **D6 Warm-up and cool-down.** `--warmup` (default 5 s) sends load that is not measured; the report counts
  only the measurement window, and requests still in flight at the end are drained and counted.
- **D7 Metrics** (spec §34): requests sent, succeeded, failed (by status class); throughput in requests/s,
  input tokens/s and output tokens/s; latency p50/p95/p99; TTFT p50/p95/p99 (streaming requests: time to the
  first SSE chunk; non-stream requests report latency only); queue average and p95; Jain fairness index of
  per-worker request counts; error rate. Percentiles are exact (sorted samples), not estimated. Tokens are
  taken from the response `usage` when present, otherwise estimated from sizes, and the report says which.
- **D8 Worker-side sampling.** While the run is going, a sampler polls the control plane's `GET /v1/workers`
  (default every 250 ms) for queue depth and active requests per worker, and each mock worker's `/stats` at the
  start and end for completed counts (per-worker distribution). GPU utilization is included only if workers
  report it. A target without a reachable control plane still works; queue and imbalance are then reported as
  "not measured".
- **D9 Persisted metadata** (spec §32): git commit (and whether the tree was dirty), model(s), worker count, GPU
  type (`none` for mocks), scheduler, concurrency or rate, workload and prompt distribution, max_tokens
  settings, duration and warm-up, seed, repeat index, Go version, machine description, date, and the harness
  version. A run that cannot determine one of these records "unknown", never omits it.
- **D10 Output.** `benchmark/runs/run_NNN/result.json` (schema versioned), `report.md` (human readable), and
  `requests.jsonl` (per-request records, optional, off by default for large runs). Run IDs are sequential
  (`run_001`, `run_002`) per directory, never reused.
- **D11 Compare** (spec §35): `benchmark compare A B` prints throughput %, p95 TTFT %, p95 latency %, worker
  imbalance %, error rate delta, and flags metadata that differs (different workload, seed, worker count, or
  commit) so an unfair comparison is visible. With `--repeat` data it also shows whether the difference exceeds the
  run-to-run spread. The tool states deltas; it does not declare winners.
- **D12 Safety.** The harness refuses a non-loopback `--target` without `--yes-this-is-not-production-traffic`
  style confirmation (a simple `--allow-remote`), caps concurrency and rate (defaults 2,000 / 10,000, flags to
  raise), caps retained samples, and never logs API keys. Prompts are synthetic; nothing user-supplied is sent.
- **D13 Dependencies.** None new: standard library plus the existing config and logging packages.
- **D14 Embedded-cluster worker mix.** `--workers N` mock workers; `--worker-profile` chooses identical
  workers (default) or a heterogeneous set (for example speeds 100/60/20 tokens/s, as in `make dev-cluster`),
  because identical workers cannot show scheduling differences. Mock parameters are recorded in the metadata.

## Proposed Design

```text
benchmark run ──► embedded cluster (control plane + N mock workers/agents + gateway --scheduler)   or   --target URL
      │
      ├─ plan:   workload(seed) ─► []PlannedRequest{at, model, promptTokens, maxTokens, stream, tenantKey}
      ├─ drive:  closed-loop clients | open-loop scheduler ─► HTTP client ─► gateway
      ├─ sample: control plane /v1/workers every 250ms; worker /stats at start and end
      ├─ collect: per-request {intendedAt, startedAt, firstByteAt, doneAt, status, tokens}
      └─ report: result.json, report.md, requests.jsonl
benchmark compare run_A run_B ─► table of deltas, metadata differences
```

Packages: `internal/bench/workload` (the six generators, seeded), `internal/bench/driver` (closed and
open loop, HTTP client, SSE first-chunk timing), `internal/bench/collect` (samples, percentiles, Jain index),
`internal/bench/embedded` (cluster boot), `internal/bench/report` (JSON, markdown, compare), wired by
`cmd/benchmark`. Pure functions where possible (workload generation, percentiles, Jain index, compare) so they
unit-test without a network.

## Affected Files / Components

New: `internal/bench/...`, `docs/benchmarks/phase-7-harness.md` (method, a sample comparison, how to read it),
ADR-013 (benchmark methodology: open versus closed loop, seeds, repeats), a sample result under
`docs/benchmarks/`, tests.
Changed: `cmd/benchmark/main.go`, `Makefile` (`bench`, `bench-compare` targets), `.gitignore` (`benchmark/runs/`),
README, ARCHITECTURE. No other package changes.

## Acceptance Criteria

1. `benchmark run` completes for each of the six workloads in closed-loop mode (and `burst` in open-loop) against
   an embedded cluster, and writes `result.json` and `report.md` with every spec §34 metric present or explicitly
   "not measured".
2. Determinism: the planned request sequence for a given workload and seed is identical across runs and across
   machines (tested on the plan, not on timings); different seeds differ.
3. Workload shapes hold: token ranges, the 60/30/10 split, the 90/10 model split, the 10x burst window, and the
   multi-tenant aggressive/normal ratio are within stated tolerances over the planned sequence.
4. Open-loop latency is measured from intended send time (test with a deliberately stalled server: closed-loop
   hides the stall, open-loop shows it).
5. Percentiles, throughput, Jain index, and the compare deltas are exact against hand-computed fixtures,
   including empty, single-sample, and tie cases.
6. Warm-up requests are excluded; in-flight requests at the end are drained and counted; no request is counted
   twice or lost (sent = succeeded + failed).
7. `benchmark compare run_A run_B` prints deltas for throughput, p95 TTFT, p95 latency, and imbalance, and flags
   differing metadata; comparing a run with itself gives zero deltas.
8. Metadata (spec §32) is complete in every result; the git commit and dirty flag are correct.
9. With heterogeneous mock workers, `round-robin` and `least-active` produce measurably different per-worker
   distributions in the report, and the report states the numbers without ranking them.
10. Safety: remote targets need explicit confirmation; caps are enforced; API keys never appear in logs or
    result files.
11. `go test -race ./...`, `gofmt`, `go vet`, `golangci-lint` pass; CI green; no new dependencies.

## Verification Plan

- Unit tests with fixtures for generators, percentiles, Jain index, compare, metadata, run-ID allocation, SSE
  first-chunk timing against a scripted server, open- versus closed-loop timing with a stalled server, and the
  caps.
- An integration test that boots the embedded cluster with 3 heterogeneous workers, runs a short mixed workload
  per scheduler, and checks that results exist, are complete, are self-consistent (sent = succeeded + failed;
  per-worker completed counts sum to successes), and are comparable.
- A process test of the built `benchmark` binary: `run` then `compare`, and a bad flag giving a clear error.
- Mutation testing of the arithmetic (percentile index, Jain formula, delta sign), the warm-up boundary, and the
  intended-time latency.
- `-race -count=10` and under CPU load: timing-dependent assertions are structural (counts, ordering,
  consistency), not tight numeric bounds. Independent `verify-change` and `review-change`, `harden-change`,
  `prepare-pr`, and stop for approval.

## Risks

- **The harness shares the machine with the thing it measures.** Embedded runs put load generator, gateway and
  workers on one CPU. The report says so, and `--target` against a separate gateway is the fair setup. Embedded
  numbers are for comparison between schedulers on the same machine, not absolute capacity.
- **Coordinated omission** misleads closed-loop latency numbers; mitigated by open-loop mode and documenting
  when each applies.
- **Noise over-read as signal.** Mock workers on one laptop vary run to run. `--repeat` and the compare spread
  check exist so a small delta is not reported as a win; docs say so plainly.
- **Synthetic tokens are approximate.** Input sizes are character-based; the report notes it.
- **Overload of the host or the target.** Caps and the loopback guard limit the harm of a typo.
- **Overlap with later phases.** Phase 9 will store metadata in Postgres and Phase 10 will chart live runs; this
  phase keeps the result schema versioned so both can adopt it without a rewrite.

## Parallel work (Phases 7 and 9)

Disjoint by design: this phase adds `internal/bench`, `cmd/benchmark`, benchmark docs and Makefile targets. It
does not touch `internal/config`, `internal/gateway`, `internal/api`, or `go.mod`. The one coupling is the
`multi-tenant` workload, which sends per-tenant API keys that the gateway only understands after Phase 9; until
then the workload is generated and exercised, and its enforcement-dependent behaviour is checked after both
merge.

## Implementation Steps

1. Pure core: percentiles, Jain index, compare, metadata, result schema, run IDs. (Independent.)
2. Workload generators with seeds and their shape tests. (Independent.)
3. Driver: closed and open loop, SSE timing, collection, warm-up and drain.
4. Worker sampler and embedded cluster.
5. `cmd/benchmark` (`run`, `compare`, `list`), report writers, Makefile targets.
6. Integration and process tests; sample run and `phase-7-harness.md`, ADR-013, README, ARCHITECTURE.
7. Gates, independent verification and review, fixes, second (narrower) verification, harden, small commits,
   `prepare-pr`, and stop for approval.

Steps 1 and 2 can proceed independently; 3–5 build on them.

## Implementation Notes (deviations and additions)

- **`internal/bench/runner` was added** (not in the plan's package list) so flag validation, safety checks, and run
  orchestration are testable; `cmd/benchmark` only dispatches `run`, `compare`, `list`.
- **Integration tests are new files** `tests/integration/benchmark_test.go` and `benchmark_process_test.go`; the
  existing test files were not edited. No file outside the planned scope was changed, and go.mod/go.sum are untouched.
- **Embedded cluster timings (D2/D14):** heartbeat 1 s, gateway registry refresh 100 ms, failure thresholds
  10/20/60 s, and a settle pause of 5 refresh intervals before the first request. Found by failure: with 200 ms
  heartbeats and 2 s thresholds a busy machine made workers look suspect, and the first requests reached a gateway
  that had seen only one worker (hundreds of 503 `NO_CAPACITY` in the first 250 ms). Recorded in every result.
- **Default mock profile** is 1000 tokens/s, 20 ms first token, 8 concurrent, queue 128 (heterogeneous: speeds x1, 0.6,
  0.2; first-token x1, 1.5, 3), faster than `make dev-cluster` so runs finish quickly; `--mock-*` flags change it.
- **Repeat spread (D5/D11):** each repeat is its own run directory in one group (`repeat.group`); `compare` finds
  the group's siblings to judge spread, so no earlier result is rewritten.
- **Request balance (D7):** Jain index of per-worker completed counts among workers of one model, lowest across
  models; counts are read from `/stats` (mock workers only) at the end of warm-up and after the drain, so with a
  warm-up they include warm-up requests that finish later (stated in the report).
- **Throughput divisor** is the window plus the drain (longest counted request), not the nominal window.
- **Caps (D12):** also `--max-requests` (500,000; stops a run early and cuts the window) and a refusal of URLs with
  credentials. No secret is a flag; the control plane token comes from `SERVERFLOW_CONTROL_PLANE_TOKEN` / `--config`.
- **Closed-loop saturation:** clients as numerous as worker slots make the gateway reject instantly and the clients
  retry in a tight loop (300,000 requests, 98% failed); documented in the harness note. Not changed (gateway scope).
- **Not done / follow-ups:** Postgres storage of results (Phase 9), Poisson arrivals, `multi-tenant` enforcement
  checks (need Phase 9 API keys), remote-target runs were only tested against loopback fakes and the embedded gateway
  by URL (not against a separately started gateway process).
- Two foreground sample runs once stalled for 3 to 6 minutes at about 1% CPU while the machine was busy; they did not
  recur in about ten later runs and every wait in the harness is bounded (drain 60 s, stats 5 s, cluster close 15 s),
  so the cause is unproven (suspected host contention).

## Implementation Notes: independent review fixes

- **Invalid runs (P1.1).** `--concurrency` above the embedded slots (workers x `--mock-concurrency`) is refused unless
  `--allow-overload`; the default client count is capped at the slots; closed-loop clients back off after 429/503
  (Retry-After capped at 500 ms, else 50-150 ms); a run with an error rate above `--max-error-rate` (5%) or with nothing
  measured is `valid: false` with reasons in `result.json`, a banner in `report.md`, a warning in the headline, and exits
  non-zero unless `--allow-errors`; `compare` warns above its table. Examples in README, Makefile, usage and this plan
  now use a client count below the slots.
- **Throughput (P1.2).** Headline requests, input tokens and output tokens per second = successes that completed
  inside the window / window length; the drain-inclusive figure is separate ("including tail"). Warm-up requests that
  finish inside the window count; window requests finishing in the drain do not. ADR-013 and the harness note updated.
- **Spread (P1.3).** A verdict needs 3 runs per side; groups compare medians with min-max ranges; wording is "ranges
  overlap / do not overlap"; equal or zero values are n/a; the 10% false-positive rate of 3 v 3 is stated.
- **Cap and empty window (P1.4).** Warm-up requests do not count against `--max-requests` (they have a cap of their own
  after which clients idle until the window opens); `--duration` must be at least 100 ms; `Sent == 0` fails the run.
- **P2.** `stats_missing` persisted and the balance marked partial; non-READY remote workers recorded as missing; the
  start snapshot is abandoned if the run ends before warm-up (no delta from a start that follows an end); the number
  of warm-up requests counted in the per-worker totals is stated (the worker of a request is not known, so it cannot be
  subtracted). `LoadGroup` filters `run_NNN` directories first, skips unreadable siblings with a warning, caps file size
  and looks in the run's own directory; group names include a timestamp. `Git` prefers the binary's VCS stamp and
  every exec has a `WaitDelay` (sysctl also a timeout). Load-flaky tests rewritten to structural checks (stall tests derive
  their expectations from when the stall really ended, the interrupt test waits for the "driving" line, the Jain
  thresholds are gone); verified with `go test -race -count=3` of the benchmark tests and the benchmark integration tests
  under 20 busy loops. The sample in `docs/benchmarks/phase-7-sample/` now holds all six runs.
- **P3.** One clock reading decides the window end and the intended time; the second Ctrl-C force-quits; `/stats` read in
  parallel under one deadline; sampler bounded by entries; idle connections closed; open-loop reports note that evenly
  spaced arrivals understate burstiness; embedded runs reject unknown schedulers, `--target` schedulers are recorded
  "declared (unverified)"; preflight errors drop the URL; a plaintext control plane token to a remote host warns;
  `--verbose`; 5 s progress lines; phase timings and wall versus monotonic clock in `result.json` (the report notes when
  they disagree by more than 2 s); embedded gateway header timeout 30 s.
- **Deliberately not done:** Poisson arrivals (future); streaming usage via `stream_options` (streamed tokens remain
  estimated and the report says so); consolidating the embedded cluster with the integration helpers (follow-up); the
  boot wait is time-based (5 refresh intervals), a gateway-side readiness signal is future work; the unexplained 3 to 6
  minute stalls of two early foreground runs are still unexplained (macOS sleep or App Nap is a candidate; the new
  timings would show it).

## Implementation Notes: independent verification fixes

- **Documented commands (V1).** The README/Makefile/usage example (`--workers 4 --worker-profile heterogeneous --concurrency
  24 --duration 60s --repeat 3`) failed its own validity gate: round-robin ignores load and fills the slow worker's slots
  well below 100% of the slots. Examples now use 12 clients on 32 slots, the docs explain the 50 to 60% rule, and an
  embedded closed-loop run prints a warning above 60% of the slots (output and result notes; refusal above 100% is
  unchanged). `internal/bench/runner/docs_test.go` parses every `benchmark run` / `BENCH_ARGS` command in the README,
  Makefile, usage text, harness note and this plan through the real option parser and fails if one is refused, exceeds the
  warning threshold, or if a documented `compare` pairs two runs of one repeat group. The README examples were also run at
  their documented length (60 s, `--repeat 3`, both schedulers): see the run results in the final report.
- **Same-group compare (V2).** `compare` warns when A and B are the same run or members of one repeat group; README
  uses `A=run_001 B=run_004`.
- **Pre-run network calls (V3).** Listing the workers and the first `/stats` read now happen before the clock starts
  (`prerun_seconds` in the timings), the later start snapshot runs concurrently with the load, a first request sent more
  than max(1 s, 5% of the run) late makes the run invalid with an accurate reason, and the empty-window message says "none
  was due inside the measurement window".
- **Cut windows (V4).** A window cut by `--max-requests` is a `warnings` entry in `result.json` and a banner in the report
  (not an invalidity), says how many of the counted completions were sent during warm-up and why they are included
  (steady-state flow), and window lengths print in milliseconds below 10 s.
- **Hostile result files (V5).** All result strings printed by `list`, `compare` and the report go through `report.Clean`
  (control characters, escape sequences, bidi overrides, invalid UTF-8, length bound). `Load` refuses symlinks (file or
  directory) and non-regular files with `Lstat`, and its errors name positions and fields, never content. (`Lstat` then
  open is not atomic; `O_NOFOLLOW` is not portable to the Windows dev machine.)
- **Mutation gaps (V6).** Tests added for the input-token numerator, p99 vs p95, `--max-error-rate 0` meaning zero tolerance,
  the start-snapshot logic, the explicit `Sent == 0` failure under `--allow-errors`, the open-loop warm-up cap path, the
  settle pause (`Cluster.Settled`), and the 3-run minimum per metric. All the listed mutants are killed.
- **Clock note (V7)** tested at 100 v 10, 10 v 100, 10 v 11.9, 10 v 12.1.
- **Docs (V8).** Plan status set to "In review", the ARCHITECTURE sentence reworded, the rate-cap message names the burst
  factor only for `burst`, and the compare hint no longer says "Two single runs" for 1 v 5.
- **Left as is:** Poisson arrivals, `stream_options` usage, consolidating the embedded helpers with the integration tests,
  the time-based boot settle, the unexplained early stalls (bounded and now instrumented), and the rule that an interrupted
  run writes no result.
