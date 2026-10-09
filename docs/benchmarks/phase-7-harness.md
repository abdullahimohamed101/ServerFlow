# Phase 7 — Benchmark Harness

`cmd/benchmark` drives a reproducible load test against ServerFlow, writes machine-readable and readable
results, and compares two results. Method and reasoning: ADR-013. This note shows how to use it, how to
read a result, and one real sample run.

These numbers describe what happened in these runs. They are **not** evidence that one scheduler is better
than another (spec sections 14 and 63): three mock workers on a laptop that also runs the load generator,
three repeats each, one workload.

## Commands

```bash
# an embedded simulated cluster: control plane, 3 mock workers with agents, a gateway running --scheduler
go run ./cmd/benchmark run --scheduler round-robin --workers 3 --worker-profile heterogeneous \
    --concurrency 12 --duration 20s --warmup 3s --workload mixed --seed 1 --repeat 3
go run ./cmd/benchmark run --scheduler least-active ...same flags...
go run ./cmd/benchmark compare run_001 run_004
go run ./cmd/benchmark list
make bench BENCH_ARGS="..."          # the same, through make
make bench-compare A=run_001 B=run_004
```

- Load: `--concurrency N` (closed loop: N clients, each sends when its last request ends) or `--rate R`
  (open loop: R requests per second on a schedule). Exactly one; `burst` needs `--rate`.
- Workloads (spec section 33): `uniform-short`, `uniform-long`, `mixed` (60/30/10), `burst` (normal, 10x, normal),
  `hot-model` (90/10 between `--model` and `--secondary-model`), `multi-tenant` (one aggressive tenant sending
  ten times what each of four normal ones do, each with its own fake API key). `--seed` fixes the load.
- Target: without `--target` an embedded cluster is booted (`--workers`, `--worker-profile identical|heterogeneous`,
  `--mock-tps`, `--mock-ttft`, ...). `--target URL` aims at a running gateway instead; add `--control-plane URL`
  (token in `SERVERFLOW_CONTROL_PLANE_TOKEN`) so queue depth and per-worker balance can be measured. Anything
  not on loopback needs `--allow-remote`.
- Caps: `--max-concurrency` 2000, `--max-rate` 10000 (the burst peak counts), `--max-requests` 500000.
- `--repeat N` runs the configuration N times (N result directories in one group) and prints min / median / max.
- Results: `benchmark/runs/run_NNN/` (gitignored) holds `result.json` (schema version 1), `report.md`, and with
  `--save-requests` every request in `requests.jsonl`. Run IDs are never reused.

## How to read a result

- **Counts.** Sent = succeeded + failed, counting only requests due inside the window (after warm-up).
  Requests still running when the window ends are waited for and counted.
- **Throughput** divides by the window plus the drain, so a slow tail lowers it.
- **Latency** is for successful requests, from the intended send time. **TTFT** is the time to the first
  content chunk and exists only for streaming requests (`--stream-ratio`, default 0.5).
- **Queue** is the sum of worker queue depths, sampled every 250 ms from the control plane. With mock workers
  that have free slots it is usually 0, and the queue Jain index is then "not measured".
- **Request balance (Jain)**: 1.0 is perfectly even, 1/n is one worker doing everything. With workers of
  different speeds an even split is not automatically good: a faster worker should do more.
- **Not measured** lists each metric the run could not produce, with the reason (for example GPU utilization
  for mock workers). Nothing is silently zero.
- **compare** prints B relative to A for each number. Positive means higher, not better. A `SPREAD` column
  says whether the difference is larger than the range seen across the repeats of both runs
  (`outside spread`), inside it (`within spread`), or that there is no repeat data. Metadata that differs
  (seed, workload, workers, commit, dirty tree, machine, ...) is flagged with `!` and a warning; the scheduler
  is listed as an expected difference.

Do not read much into one run. A delta that is `within spread` is noise as far as these runs can tell.

## Sample run

Apple M5 Pro (15 CPUs), macOS, Go 1.27.1, commit `bb94c8c` (clean tree), 2026-10-09. Other work was running
on the machine, which is part of why the spread is wide. Three mock workers with the heterogeneous profile
(1000, 600 and 200 tokens/s; first token after 20, 30 and 60 ms; 8 concurrent requests each), `mixed` workload,
seed 1, 12 closed-loop clients, 20 s window after 3 s warm-up, 3 repeats per scheduler:

```bash
benchmark run --scheduler round-robin  --workers 3 --worker-profile heterogeneous --concurrency 12 --duration 20s --warmup 3s --workload mixed --seed 1 --repeat 3   # run_001-003
benchmark run --scheduler least-active --workers 3 --worker-profile heterogeneous --concurrency 12 --duration 20s --warmup 3s --workload mixed --seed 1 --repeat 3   # run_004-006
```

| Scheduler | Run | Requests/s | Latency p50 / p95 (ms) | TTFT p95 (ms) | Failed | Request Jain |
| --- | --- | --- | --- | --- | --- | --- |
| round-robin | 001 | 17.59 | 247 / 2211 | 61 | 0 of 433 | 0.916 |
| round-robin | 002 | 18.52 | 355 / 1870 | 61 | 0 of 428 | 0.994 |
| round-robin | 003 | 18.65 | 312 / 2144 | 61 | 0 of 429 | 0.980 |
| least-active | 004 | 19.28 | 251 / 1390 | 61 | 0 of 519 | 0.868 |
| least-active | 005 | 22.81 | 220 / 1515 | 60 | 0 of 490 | 0.813 |
| least-active | 006 | 22.20 | 236 / 1650 | 60 | 0 of 503 | 0.833 |

Completed requests per worker (fast, medium, slow): round-robin 179/181/85, 154/156/130, 161/163/117;
least-active 251/195/85, 248/196/58, 263/177/75. The full report of run_004, run_001's, and the comparison are
in `docs/benchmarks/phase-7-sample/`.

`benchmark compare run_001 run_004`:

```text
METRIC                 A          B         DELTA           CHANGE    SPREAD
throughput             17.59      19.28     +1.683 req/s    +9.57%    outside spread
latency p50            246.5      250.9     +4.418 ms       +1.79%    within spread
latency p95            2211       1390      -820.9 ms       -37.13%   outside spread
latency p99            6397       3371      -3026 ms        -47.30%   within spread
TTFT p95               60.73      60.91     +0.1752 ms      +0.29%    within spread
worker balance (Jain)  0.9164     0.8682    -0.04821 index  -5.26%    outside spread
error rate             0          0         +0 %            +0.00 pp  within spread
```

What this run does and does not say:

- In every repeat, least-active left the slowest worker with no more requests than round-robin did (58 to 85
  against 85 to 130) and the fastest with more. That is the distribution difference acceptance criterion 9
  asks for, and it is what one would expect from a policy that follows load.
- Its throughput and p95 latency were better in these three repeats than round-robin's, with non-overlapping
  ranges. That is a measurement of this setup: mock workers with no contention model, long requests (up to
  1500 tokens) that make a slow worker's share matter, 12 clients on 3 workers, and a shared machine. It does
  not show that least-active is a better scheduler, and the p99 and p50 differences are within the spread.
- TTFT does not differ: the mock worker's first token delay is configured, and the gateway adds little.
- Both used the same plan (digest `d065b561cbd74cf2`), so the offered request sequence was identical.

## Things the harness taught us while being built

- **Do not size closed-loop clients to the worker slots.** With 24 clients and 3 workers of 8 slots, the
  gateway answered 503 `NO_CAPACITY` instantly whenever all slots were taken, the clients retried in a tight
  loop, and runs recorded 300,000 requests of which 98% failed. Failures are fast, so a closed loop at
  saturation produces a flood of them; the error rate and failure classes in the report show it. Use fewer
  clients than slots, or `--rate`.
- **A cluster needs a moment before it takes load.** Right after the control plane listed all workers, the
  gateway had seen only one, so the first 250 ms of runs got hundreds of 503s. The embedded cluster now waits
  for several gateway snapshots, and uses 10 s failure thresholds because starvation of a loaded machine made
  healthy workers look suspect.
- **Per-worker counts are approximate with a warm-up.** They are read from each worker's `/stats` at the end
  of warm-up and after the drain, so requests sent during warm-up that finish later are included.

## Limits

Mock workers have no batching or contention. The embedded cluster shares the CPU with the load generator.
Arrivals in open loop are evenly spaced. GPU utilization needs workers that report it (Phase 13). Results are
files; storing them in PostgreSQL is a follow-up after Phase 9.
