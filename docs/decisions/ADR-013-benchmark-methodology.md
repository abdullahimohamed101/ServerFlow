# ADR-013: Benchmark Methodology

Status: Accepted
Date: 2026-10-08

## Context

Spec sections 14 and 63 forbid calling a scheduler better without benchmark evidence, and section 32
lists what a run must persist. Phase 7 builds the harness (`cmd/benchmark`, `internal/bench`). Load
tests are easy to run and easy to misread, so this ADR records the choices that decide what the numbers
mean.

## Decisions

- **Latency is measured from the intended send time.** A closed-loop client sends when its last request
  ends, so its intended time is its send time. An open-loop request is due on a schedule; if the harness
  or its in-flight limit delays the send, the delay is part of the latency. Otherwise a stalled server
  would slow the load generator, which would send fewer requests into the stall, and the stall would
  barely show (coordinated omission). The driver test with a stalled server shows one slow request in
  closed loop and many in open loop.
- **Closed loop for "how fast with N users", open loop for "what happens at R requests per second".**
  Closed loop lowers its offered load when the server slows; open loop does not. `burst` (normal, then
  10x, then normal) is open loop only because it is a statement about the arrival rate. Open-loop arrivals
  are evenly spaced, not Poisson, so the load is identical run to run.
- **The load is a function of the seed.** Request `i` depends only on the workload, the seed and `i`
  (`math/rand/v2` PCG, one stream per index), so concurrency and timing do not change what is offered.
  Each result records a digest of the first 1000 planned requests; a test pins it.
- **Warm-up, window and drain.** Requests due before the window are sent but not counted in the request
  totals or latency. Requests due inside it are counted even if they finish later; the harness waits for them
  (bounded by `--drain-timeout`, after which they count as failures). Warm-up requests do not count against
  `--max-requests`.
- **Headline throughput is windowed.** Requests per second, input tokens per second and output tokens per
  second are the successful requests that *completed inside the window* divided by the nominal window length.
  A warm-up request that finishes inside the window counts (it is steady-state work); a request sent inside the
  window that finishes in the drain does not. The first version divided by the window plus the drain, so one slow
  tail request moved the headline by 35% between identical configurations. The drain-inclusive figure is kept as
  a separate, labelled "throughput including tail" number.
- **Percentiles are exact** nearest-rank values over all successful requests. Failed requests have no
  latency sample; they appear in the error rate and the failure classes.
- **TTFT** is the time to the first non-empty content chunk of a streaming response. Non-streaming requests
  report latency only.
- **Imbalance is the Jain index** of per-worker completed requests among workers serving the same model
  (the lowest across models), and of per-worker mean queue depth. 1 is perfectly even. It measures
  evenness, not goodness: with workers of different speeds an uneven split can be right.
- **Noise is shown, not hidden, and not over-sold.** `--repeat N` writes N runs in one group. `compare` gives a
  spread verdict only when *both* sides have at least 3 runs; it then compares the medians (the delta is
  between medians, with each side's min-max range shown) and says "ranges overlap" or "ranges do not overlap".
  With fewer runs it says "insufficient repeats". This is a range check, not a significance test: with 3 runs on
  each side and no real difference the ranges are disjoint 2/C(6,3) = 10% of the time, per metric, so one
  "do not overlap" among eleven metrics is weak evidence. Equal or zero values print "n/a".
- **Validity gate.** Failed requests are instant, so a run that overloads its target records thousands of
  failures and computes latency, TTFT and balance over the few survivors. A run whose error rate exceeds
  `--max-error-rate` (5%), or that measured nothing, is marked `valid: false` with reasons in `result.json`, gets
  a warning at the top of `report.md` and in the headline, and exits non-zero unless `--allow-errors` (an empty
  measurement always fails). `compare` prints a warning above its table when either run is invalid or has a high
  error rate. An embedded closed-loop run also refuses more clients than the workers have slots unless
  `--allow-overload`.
- **Closed-loop clients back off.** After a 429 or 503 a client waits the response's `Retry-After` (capped at
  500 ms) or 50 to 150 ms, so a rejected run cannot spin at thousands of requests per second. Open-loop runs
  do not back off: the schedule is the load.
- **The embedded cluster shares the machine** with the load generator. It uses long failure thresholds
  (suspect after 10s) so CPU starvation cannot make healthy workers look dead, and the harness waits for
  the gateway to see every worker before the first request. Both were found by failures: with 2s
  thresholds and no settle, a loaded machine produced hundreds of 503 `NO_CAPACITY` in the first 250ms.
  Embedded numbers compare schedulers on one machine; they are not capacity figures.
- **Safety.** Non-loopback targets need `--allow-remote`; concurrency and rate have caps (and the burst
  peak counts); the number of requests per run is capped; target URLs with credentials are refused;
  secrets are never flags, are not written to logs or results, and prompts are synthetic. Per-tenant API
  keys are fixed fake strings.
- **Results are files** (`benchmark/runs/run_NNN`, schema version 1, IDs never reused). Phase 9 can store
  the same schema in PostgreSQL.

## Consequences

- Numbers from different machines, commits or seeds are comparable only as the comparison table flags.
- Mock workers have no batching or contention model, so even a clean embedded result says little about
  vLLM; Phase 13 repeats this on real workers.
- Evenly spaced arrivals understate burstiness; a Poisson option is a later addition.
