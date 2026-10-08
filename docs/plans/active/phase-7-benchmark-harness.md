# Phase 7 — Baseline Benchmark Harness

Status: In progress (plan approved with all defaults; implementation under way)
Owner: coding agent
Depends on: Phase 6 (multi-worker routing; PR #7). Runs in parallel with Phase 9 (PostgreSQL); the two share no code, see "Parallel work".
Spec: `docs/architecture/serverflow-spec.md` §14, §31–36, §52, §58 Phase 7, §63
Hand-offs: Phase 6 benchmark note (`docs/benchmarks/phase-6-distribution.md`: "needs the Phase 7 harness"), Phase 6 ADR-012 (the black-hole effect and the retry/rotation interaction are the first things worth measuring properly).

## Outcome

One command runs a reproducible load test against ServerFlow and writes a result that another command
can compare to a second result:

```bash
benchmark run     --scheduler round-robin --workers 4 --concurrency 100 --duration 60s --workload mixed --seed 1
benchmark compare run_001 run_002
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
