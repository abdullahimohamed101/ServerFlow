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
go run ./cmd/benchmark compare run_001 run_004      # runs 001-003 and 004-006 are two repeat groups
go run ./cmd/benchmark list
make bench BENCH_ARGS="..."          # the same, through make
make bench-compare A=run_001 B=run_004
```

- Load: `--concurrency N` (closed loop: N clients, each sends when its last request ends) or `--rate R`
  (open loop: R requests per second on a schedule). Exactly one; `burst` needs `--rate`.
- **Keep clients below the slots.** The embedded workers serve `--mock-concurrency` (default 8) requests at once
  each, so 3 workers have 24 slots. Beyond that the gateway answers 503 instantly and the run measures
  rejections: the harness refuses `--concurrency` above `workers x mock-concurrency` unless `--allow-overload`,
  and the default is capped at the slots. (The spec's `--workers 4 --concurrency 100` needs
  `--mock-concurrency 32` or more.) Closed-loop clients also wait 50 to 150 ms (or the capped `Retry-After`)
  after a 429 or 503, so even an allowed overload cannot spin the machine.
- Workloads (spec section 33): `uniform-short`, `uniform-long`, `mixed` (60/30/10), `burst` (normal, 10x, normal),
  `hot-model` (90/10 between `--model` and `--secondary-model`), `multi-tenant` (one aggressive tenant sending
  ten times what each of four normal ones do, each with its own fake API key). `--seed` fixes the load.
- Target: without `--target` an embedded cluster is booted (`--workers`, `--worker-profile identical|heterogeneous`,
  `--mock-tps`, `--mock-ttft`, ...; `--verbose` shows its logs). `--target URL` aims at a running gateway instead; add
  `--control-plane URL` (token in `SERVERFLOW_CONTROL_PLANE_TOKEN`) so queue depth and per-worker balance can be
  measured. Anything not on loopback needs `--allow-remote`. The scheduler of a `--target` is recorded as declared
  and "unverified".
- Caps: `--max-concurrency` 2000, `--max-rate` 10000 (the burst peak counts), `--max-requests` 500000 (window
  requests; warm-up does not count against it).
- **Validity.** If more than `--max-error-rate` (5%) of the measured requests fail, or nothing was measured, the
  run is marked invalid (`"valid": false` and reasons in `result.json`, a banner at the top of `report.md`, a
  warning in the headline) and the command exits non-zero. `--allow-errors` accepts the exit status; the result
  stays invalid. An empty measurement always fails.
- `--repeat N` runs the configuration N times (N result directories in one group) and prints min / median / max.
  The harness prints a progress line every 5 s, and `result.json` records the phase timings (boot, warm-up,
  window, drain, stats, close) and the wall-clock and monotonic elapsed time of the run.
- Results: `benchmark/runs/run_NNN/` (gitignored) holds `result.json` (schema version 1), `report.md`, and with
  `--save-requests` every request in `requests.jsonl`. Run IDs are never reused.

## How to read a result

- **Counts.** Sent = succeeded + failed, counting only requests due inside the window (after warm-up).
  Requests still running when the window ends are waited for and counted.
- **Throughput (headline)** is the successful requests that *completed inside the window* divided by the window
  length, in requests, input tokens and output tokens per second. Warm-up requests that finish inside the window
  count; requests sent inside the window that finish in the drain do not. **Throughput including tail** divides
  the requests sent inside the window by the time until the last of them finished, so it follows the slowest
  request; it is shown separately and is not what `compare` uses. The two can differ a lot when the window ends
  while long requests are running (run_002 below: 21.75 against 16.52).
- **Latency** is for successful requests, from the intended send time. **TTFT** is the time to the first
  content chunk and exists only for streaming requests (`--stream-ratio`, default 0.5). Streamed token counts are
  estimated from chunks; non-streamed ones come from the response `usage`.
- **Queue** is the sum of worker queue depths, polled every 250 ms from the control plane. The control plane's
  numbers only change when a worker's heartbeat arrives (every 1 s in the embedded cluster), so the effective
  resolution is about a second. With mock workers that have free slots it is usually 0, and the queue Jain index
  is then "not measured".
- **Request balance (Jain)**: 1.0 is perfectly even, 1/n is one worker doing everything. With workers of
  different speeds an even split is not automatically good: a faster worker should do more. If any worker's count
  could not be read (or a worker on a remote target is not READY) the result lists it under `stats_missing` and the
  index is marked partial. Counts come from each worker's `/stats` at the end of warm-up and after the drain, so
  warm-up requests that finish later are included (the report says how many).
- **Not measured** lists each metric the run could not produce, with the reason (for example GPU utilization
  for mock workers). Nothing is silently zero.
- **compare** prints B relative to A for each number; positive means higher, not better. A `SPREAD` verdict needs
  at least 3 runs (`--repeat 3`) on **both** sides; then the table shows the median of each group with its min-max
  range in brackets, the delta is between the medians, and the verdict is "ranges overlap" or "ranges do not
  overlap". With fewer runs it says "insufficient repeats", and equal or zero values print "n/a". This is a range
  check, not a significance test: with 3 runs per side and no real difference the ranges are disjoint 10% of the
  time (2 of the 20 ways to split 6 values into two groups of 3), per metric, and a table has about eleven
  metrics. Metadata that differs (seed, workload, workers, commit, dirty tree, machine, ...) is flagged with `!` and a
  warning; the scheduler is listed as an expected difference. If either run is invalid, or has an error rate above
  5%, a warning is printed above the table.

Do not read much into one run, or into one metric.

## Sample run

Apple M5 Pro (15 CPUs), macOS, Go 1.27.1, commit `4ee0774` (clean tree), 2026-10-09. Other processes on the machine were not controlled. Three mock workers with the heterogeneous profile (1000, 600 and 200 tokens/s; first token after
20, 30 and 60 ms; 8 concurrent requests each, so 24 slots), `mixed` workload, seed 1, 12 closed-loop clients,
20 s window after 3 s warm-up, 3 repeats per scheduler. All six runs are in `docs/benchmarks/phase-7-sample/`
(`result.json` and `report.md` each), so the comparison below can be reproduced with
`go run ./cmd/benchmark compare --dir docs/benchmarks/phase-7-sample run_001 run_004`.

```bash
benchmark run --scheduler round-robin  --workers 3 --worker-profile heterogeneous --concurrency 12 --duration 20s --warmup 3s --workload mixed --seed 1 --repeat 3   # run_001-003
benchmark run --scheduler least-active --workers 3 --worker-profile heterogeneous --concurrency 12 --duration 20s --warmup 3s --workload mixed --seed 1 --repeat 3   # run_004-006
```

| Scheduler | Run | Requests/s | Including tail | Latency p50 / p95 (ms) | TTFT p95 (ms) | Failed | Request Jain | Completed (fast/medium/slow) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| round-robin | 001 | 22.00 | 19.46 | 260 / 1971 | 60 | 0 of 440 | 0.954 | 179 / 169 / 104 |
| round-robin | 002 | 21.75 | 16.52 | 280 / 2141 | 60 | 0 of 435 | 0.976 | 164 / 167 / 116 |
| round-robin | 003 | 21.75 | 16.59 | 267 / 2246 | 61 | 0 of 435 | 0.954 | 173 / 171 / 103 |
| least-active | 004 | 25.10 | 21.79 | 249 / 1508 | 60 | 0 of 502 | 0.847 | 251 / 188 / 75 |
| least-active | 005 | 25.25 | 22.59 | 235 / 1698 | 60 | 0 of 505 | 0.856 | 256 / 178 / 83 |
| least-active | 006 | 25.30 | 23.58 | 222 / 1520 | 60 | 0 of 506 | 0.821 | 268 / 179 / 71 |

`benchmark compare run_001 run_004` (medians of the two groups of three; the full output is in
`docs/benchmarks/phase-7-sample/compare_run_001_run_004.txt`):

```text
METRIC                 A                     B                     DELTA          CHANGE    SPREAD
throughput             21.75 [21.75-22]      25.25 [25.1-25.3]     +3.5 req/s     +16.09%   ranges do not overlap
latency p50            267.2 [260.4-280.4]   235.3 [222-248.7]     -31.82 ms      -11.91%   ranges do not overlap
latency p95            2141 [1971-2246]      1520 [1508-1698]      -620.4 ms      -28.98%   ranges do not overlap
latency p99            3466 [2842-3856]      3281 [2524-3850]      -184.6 ms      -5.33%    ranges overlap
TTFT p95               60.41 [60.38-60.52]   60.39 [60.38-60.42]   -0.01767 ms    -0.03%    ranges overlap
worker balance (Jain)  0.9545 [0.9536-0.976] 0.847 [0.8213-0.8558] -0.1075 index  -11.26%   ranges do not overlap
error rate             0 [0-0]               0 [0-0]               +0 %           +0.00 pp  n/a
```

What this run does and does not say:

- In all three repeats of each scheduler, least-active gave the slow worker fewer requests (71 to 83 against 103
  to 116 for round-robin) and the fast worker more (251 to 268 against 164 to 179). That is the distribution
  difference acceptance criterion 9 asks for, and the direction a policy that follows load would produce.
- Throughput, p50 and p95 latency, and balance are higher or lower in these three repeats with ranges that do not
  overlap; p99 and TTFT do. That describes this setup: mock workers with no contention model, long requests (up to
  1500 tokens) that make a slow worker's share matter, 12 clients on 3 workers, one machine. It does not rank the
  schedulers, and the 10% chance of disjoint ranges per metric without any real difference applies to every row.
- TTFT does not differ: the mock worker's first-token delay is configured and the gateway adds little.
- Both groups used the same workload generator and seed, so request `i` is the same in every run. A closed loop
  does not consume the same number of requests in each run, so the *consumed prefix* differs (435 to 506
  requests here); the digest in `result.json` covers the first 1000 planned requests, not the ones consumed.

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
  of warm-up and after the drain, so requests sent during warm-up that finish later are included (the report counts them).

## Limits

Mock workers have no batching or contention. The embedded cluster shares the CPU with the load generator.
Arrivals in open loop are evenly spaced. GPU utilization needs workers that report it (Phase 13). Results are
files; storing them in PostgreSQL is a follow-up after Phase 9.
