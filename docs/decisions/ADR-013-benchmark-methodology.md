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
- **Warm-up and drain.** Requests due before the window are sent but not counted. Requests due inside it
  are counted even if they finish later; the harness waits for them (bounded by `--drain-timeout`, after
  which they count as failures). Throughput divides by the longer of the window and the time until the
  last counted request ended, so the drain is not free.
- **Percentiles are exact** nearest-rank values over all successful requests. Failed requests have no
  latency sample; they appear in the error rate and the failure classes.
- **TTFT** is the time to the first non-empty content chunk of a streaming response. Non-streaming requests
  report latency only.
- **Imbalance is the Jain index** of per-worker completed requests among workers serving the same model
  (the lowest across models), and of per-worker mean queue depth. 1 is perfectly even. It measures
  evenness, not goodness: with workers of different speeds an uneven split can be right.
- **Noise is shown, not hidden.** `--repeat N` writes N runs in one group; `compare` reports whether two
  numbers differ by more than the spread of the repeats. A single pair of runs says "no repeat data".
- **`compare` states deltas and never declares a winner**, and flags every metadata difference except the
  scheduler (commit, seed, workload, worker count, machine, ...) as making the comparison unfair.
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
