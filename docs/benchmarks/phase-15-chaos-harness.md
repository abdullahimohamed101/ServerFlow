# Phase 15 — Chaos harness, mock-worker faults and the master baseline

PR B of the seven-PR delivery of Phase 15 (fault tolerance; plan `docs/plans/active/phase-15-fault-tolerance.md`
on the PR A branch, decisions D17 to D21). It adds **no product code**: it adds the means to measure the
resilience feature before it exists, and the baseline taken on the gateway as it is on master. Every
number below describes this machine on this day; none of it is a claim about the feature (there is no feature
yet) and a range that overlaps another is not a difference.

## What was added

| Piece | Where | What it does |
| --- | --- | --- |
| `benchmark chaos` | `cmd/benchmark`, `internal/bench/chaos` | Runs a scenario in three windows (baseline, fault, recovery) against an embedded cluster, with a validity gate, two pre-registered primary measures and a few descriptive ones. |
| `mockworker.Faults` | `internal/mockworker/faults.go` | In-process handle on a running mock worker: error rate and mode, extra delay, blackhole, crash and restart. No HTTP endpoint. Counters of requests each fault touched are the evidence. |
| `--drain-announce` | `internal/mockworker`, `cmd/mock-worker` | On SIGTERM the worker reports `draining` but keeps serving for the announce period, then refuses, then finishes in-flight requests up to `--drain-timeout`. Default 0 is today's behaviour. |
| `embedded.Cluster.DrainWorker`, `ReplaceWorker` | `internal/bench/embedded` | Drain one worker the way a SIGTERM does (mock drain **and** `agent.Shutdown`), and start a fresh worker under its ID (a rolling replacement). |
| `embedded.Config.GatewayMutate`, `Metrics()`, `Faults(id)`, `Specs()` | `internal/bench/embedded` | Edit the gateway configuration before it starts, read its Prometheus registry in process, reach the faults of a worker. |
| `--resilience on\|off` | `benchmark run` and `benchmark chaos` | A **no-op on this build**, which has no resilience configuration; the result records `resilience: absent`. See below. |
| `compare` warning | `internal/bench/report` | Warns, and counts as an unfair difference, when two groups differ in `resilience`, or a group mixes values. The two primary measures of a chaos run are compared like the Phase 7 headlines. |

The embedded cluster never called `agent.Shutdown` before: cancelling an agent simulates a crash (`agent.go`,
`Run`: "Run does not deregister: cancelling it simulates a crash"), and `embedded.go` cancelled one context for
everything. `DrainWorker` gives each mock and each agent its own context, stops the agent's loop (the agent is
not safe for concurrent use), calls `Shutdown` (reports DRAINING, deregisters) and waits for the mock's drain.

## How to run

```bash
go run ./cmd/benchmark chaos -list                          # the scenarios, what each shows and cannot show
go run ./cmd/benchmark chaos --scenario worker-freeze --repeat 3          # one scenario, 3 repeats, scale 1 (about 1 minute each)
go run ./cmd/benchmark chaos --scenario all --repeat 3 --out benchmark/runs   # about 35 minutes
go run ./cmd/benchmark chaos --scenario worker-freeze --scale 0.2 --allow-dirty   # a quick look while developing
go run ./cmd/benchmark compare run_001 run_004              # two groups of 3: ranges, SPREAD, and the resilience warning
```

- Build with `go build` and run the binary for a result you mean to keep: the commit and tree state are read from
  the binary's build stamp, so a `go run` from another directory cannot record the wrong commit.
- `--scale` multiplies every duration (a window bucket is one second times the scale; the windows are always
  3 + 10 + 20 + 20 buckets of warm-up, baseline, fault and recovery). Scale 1 is the record. Below about 0.3 the
  fixed heartbeat (1 s) and registry refresh (100 ms) of the embedded cluster stop being small next to a bucket.
  `drain-roll` keeps a fault window of at least 4 s per worker whatever the scale.
- Load is closed-loop: by default 60% of the slots left serving once the fault takes a worker (14 clients on
  4 workers x 8 slots), `uniform-short` requests, half streaming, round-robin. `--concurrency` overrides it and the
  gate decides whether the override was sane.
- Each run writes `run_NNN/result.json` (schema version 1, additive: `metadata.resilience` and a `chaos` object)
  and `report.md`. After the repeats the command prints min / median / max of every measure and lists invalid runs
  with their reasons; the exit status is non-zero when any run was invalid.

## Windows and measures

A run is a warm-up (not measured), then **baseline**, **fault** and **recovery**. The windows follow what the
conductor really did (the instant the fault was applied and the instant it was cleared, which includes a restart
that took time), not the plan. Requests belong to the window they were *sent* in; the gateway's own counters are
read at the tick that opens and closes a window, so an attempt of a request that was in flight at a boundary falls
in the next window.

**Two primary measures per scenario are pre-registered** (the same two for every scenario, written down before any
number was seen): the **success rate in the fault window**, and the **attempts per request in it**
(`Δ inference_attempts_total / Δ inference_requests_total`; requests the gateway refuses without any attempt pull it
down). Only these support a claim, and only under the 3-repeat spread rule: with three runs per side and no real
difference, the min-max ranges are disjoint about 10% of the time, per measure, so ten descriptive measures would
produce a "finding" by chance. Everything else is descriptive:

- **time to isolate**: seconds from the fault's start to the first bucket of a final stretch of at least 3 buckets, reaching the end of the fault window, in which the faulted worker gets no
  non-probe selections while the gateway was making attempts (`scheduler_selections_total` deltas for the worker, minus
  `circuit_breaker_probes_total`, which does not exist on master and reads 0). *Not reached* when it kept being selected;
- **time to recover**: seconds from the end of the fault until five consecutive buckets with at least five requests each
  have a success ratio at least the baseline window's;
- tail latency of successes in the fault window (p95, p99) and the latency of failures;
- hard bounds, recorded: `scheduler_ineligible_selections_total` must be 0 (a non-zero value is a warning on the
  result, not an invalid run: it is a routing bug), goroutines and heap before and after (informational on master).

## The validity gate

A run is valid only when every clause holds. An invalid run is written with `valid=false` and its reasons, listed by
the command, and left out of the spread summary: invalid runs are never compared.

| Clause | Fails when |
| --- | --- |
| load driver | the load generator returned an error or no output |
| fault applied and cleared | applying or clearing the fault failed, or the fault was still on at the end of the run |
| baseline error rate | more than `--max-error-rate` (5%) of the baseline window failed, or it is empty |
| fault evidenced | the mock workers' own counters show no request touched / no crash / no drain (not the harness's intention); for overload, no failed request and no gateway refusal in the fault window |
| clients within the surviving slots | closed-loop clients exceed 60% of the **surviving slots**: the registry slots of eligible workers minus the faulted worker's, **so a frozen but still READY worker is not counted**. `overload` inverts it: the fault window must offer more clients than slots |
| minimum requests per window | any window sent fewer than `--min-requests` (default 100 x scale, at least 20) |
| cluster settled | `Start` returned before five registry refreshes passed after the control plane listed every worker |
| clean working tree | the commit is unrecorded or the tree is dirty (`--allow-dirty` waives it and says so in the result) |
| no sleep or clock step | the wall clock moved against the monotonic clock by more than 250 ms during the run (a laptop that slept: the Phase 8 lesson) |

"The harness fell behind (open loop)" from the plan does not apply: chaos runs are closed-loop only, and the burst
workload (which needs open loop) is refused.

## Scenarios

Each scenario faults the first worker (`worker-01`) unless it says otherwise. `-list` prints this table with the
same text.

| Scenario | What it does and shows | What it cannot show on a mock cluster |
| --- | --- | --- |
| `worker-crash` | The first worker's listener closes and its connections are reset; it listens again when the fault ends. The agent reports FAILED within one heartbeat and the registry drops the worker within a refresh, so this is a registry-plus-retry baseline, not a breaker test. | Whether the circuit breaker detects a crash: the registry gets there first. It shows that a crash costs the clients nothing the retry cannot absorb, and that nothing regresses. A real process kill (SIGKILL) differs in that the OS resets connections the same way, but also frees the port for a replacement. |
| `worker-freeze` | The first worker reads requests and never answers (a hung process); its /stats keeps answering, so the agent says READY and only the gateway's own header timeout (lowered to 2 s) can detect it. | A frozen process stops its heartbeat too, which the registry notices after the suspect threshold (10 s here); the mock keeps answering /stats so that the breaker is the only detector. SIGSTOP of a real worker is the process-level version (not run). |
| `slow-worker-below-timeout` | The first worker takes an extra 5 s before it answers and the gateway's header timeout (2 s) is below that: its requests time out, which is a fault. | The delay holds the mock's slot but not a CPU: a real slow worker (a long prefill, a GPU stall) also degrades its neighbours on the same device, which a mock cannot. |
| `slow-worker-above-timeout` | The first worker takes an extra 5 s before it answers and the header timeout (8 s) is above that: its requests are slow but succeed, so a breaker must not open (the false-positive check). | The delay holds the mock's slot but not a CPU: a real slow worker (a long prefill, a GPU stall) also degrades its neighbours on the same device, which a mock cannot. |
| `error-burst-unavailable` | Every request to the first worker is answered 503 mock_failure (a fault, retried on another worker). | The breaker's detection of a persistent fault cannot be told from the registry's here, because the embedded agent reports a dead worker within one heartbeat. |
| `error-burst-unavailable-partial` | Half of the requests to the first worker are answered 503 mock_failure. | The breaker's detection of a persistent fault cannot be told from the registry's here, because the embedded agent reports a dead worker within one heartbeat. |
| `error-burst-500` | Every request to the first worker is answered 500 mock_failure. A 500 is not retried today; the clients see it. | Whether a 500 would be ejected by a ratio rule (Phase 15 D4): on master nothing reacts to it, which is the baseline. |
| `midstream` | Half of the requests to the first worker start their response and then break the connection after two tokens. | Midstream failures cannot be retried by anyone once output has begun; this shows how many clients see one and that the worker keeps being chosen. |
| `conn-reset` | Every request to the first worker has its connection closed without a response; the gateway sees a reset before any byte and may retry. | The breaker's detection of a persistent fault cannot be told from the registry's here, because the embedded agent reports a dead worker within one heartbeat. |
| `drain-roll` | Every worker is replaced in turn, one at a time: the mock announces draining and keeps serving for a period above the heartbeat plus the registry refresh, then refuses and finishes what is in flight; the agent deregisters; a fresh worker takes its place. | Process signals: the harness calls the same drain path a SIGTERM takes (mock drain and agent.Shutdown) in process. It tells the registry at once instead of through the next probe, so the announce period only has to cover the gateway's refresh. A real preStop is Phase 17. |
| `overload` | More closed-loop clients than the cluster has request slots for the length of the fault window: the workers' slots and queues fill and the gateway answers 503. | What a real queue in the gateway does (Phase 15 D11): on master there is none, so this shows the rejections the clients get. Closed-loop clients also back off on a 503, which softens the overload an open-loop crowd would cause. |

## The resilience flag: `absent`, not `off`

The harness has to run before and after the feature exists, so it never names the feature's configuration type.
`embedded.ApplyResilience` looks by reflection for a `Resilience` struct with a string `Mode` in `config.Config`:

- **not found (master, this PR)**: `--resilience on|off` changes nothing, the run prints a note, and every result records
  `metadata.resilience = "absent"`;
- **found**: `on`/`off` set `Mode`, no flag keeps the default, and the effective value is recorded;
- **found with another shape**: an error that says to update the harness, instead of silently recording something wrong.

`absent` is never `off`: "off" means the feature exists and is switched off, which is the behavioural check of the
later PRs ("a third group with `mode: off` must agree with this baseline within spread"). `benchmark compare` warns when
the two sides differ in the value (and treats results from before the field as `not recorded`).

## Time and clock seams

`chaos.Clock` (Now, SleepUntil, WallOffset) drives the timeline: one conductor goroutine samples the gateway's
counters every bucket, applies the fault at the end of the baseline, clears it at the end of the fault window, and
samples the wall-versus-monotonic offset each tick. `FakeClock` jumps instead of sleeping, so the harness's own tests
(`internal/bench/chaos`, fake cluster, fake loader, fake clock) run the whole pipeline in milliseconds and pass
`-race -count=10` in about ten seconds. The load driver and the embedded cluster use real time; the one test that runs every
scenario for real (`TestRealScenarios`, scale 0.15, about two minutes) is skipped under `-short`.

## Master baseline (resilience absent)

Taken with the binary built from commit `92c5488` (clean tree), `darwin/arm64, 15 CPUs, Apple M5 Pro, go1.27.1`, scale 1,
4 workers x 8 slots, 14 closed-loop clients, round-robin, `uniform-short`, seed 1, **resilience `absent`**, 3 repeats per scenario
(33 runs, run_001 to run_033, all valid). The raw results are in `docs/benchmarks/phase-15-baseline/` (`result.json` and `report.md` per run, no
`requests.jsonl`; `run-log.txt` is the command output with the load average before each scenario). Each cell is min / median / max over the 3 runs.

| Scenario | Runs (valid) | Success in fault window, % (primary) | Attempts per request (primary) | Time to isolate, s | Time to recover, s | Success p95 / p99 ms |
| --- | --- | --- | --- | --- | --- | --- |
| worker-crash | 001,002,003 (3) | 100.00 / 100.00 / 100.00 | 1.009 / 1.009 / 1.009 | 1.0 / 1.0 / 1.0 | 1.0 / 1.0 / 1.0 | 164 / 165 / 165 ; 168 / 168 / 169 |
| worker-freeze | 004,005,006 (3) | 93.45 / 93.45 / 93.45 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 ; 169 / 169 / 169 |
| slow-worker/header-timeout-below-delay | 007,008,009 (3) | 96.15 / 96.15 / 96.15 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 164 / 164 / 165 ; 168 / 168 / 168 |
| slow-worker/header-timeout-above-delay | 010,011,012 (3) | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 167 / 167 / 167 ; 5140 / 5140 / 5140 |
| error-burst/unavailable-100 | 013,014,015 (3) | 100.00 / 100.00 / 100.00 | 1.333 / 1.333 / 1.333 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 ; 169 / 169 / 169 |
| error-burst/unavailable-50 | 016,017,018 (3) | 100.00 / 100.00 / 100.00 | 1.143 / 1.143 / 1.143 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 ; 169 / 169 / 169 |
| error-burst/error-100 | 019,020,021 (3) | 74.99 / 74.99 / 74.99 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 ; 169 / 169 / 169 |
| midstream | 022,023,024 (3) | 87.57 / 87.58 / 87.60 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 ; 169 / 169 / 169 |
| conn-reset | 025,026,027 (3) | 100.00 / 100.00 / 100.00 | 1.332 / 1.333 / 1.334 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 ; 169 / 169 / 169 |
| drain-roll | 028,029,030 (3) | 100.00 / 100.00 / 100.00 | 1.000 / 1.000 / 1.000 | not reached | 1.0 / 1.0 / 1.0 | 165 / 165 / 165 ; 168 / 168 / 168 |
| overload | 031,032,033 (3) | 67.30 / 67.98 / 68.34 | 0.672 / 0.679 / 0.683 | not reached | 1.0 / 1.0 / 1.0 | 163 / 164 / 164 ; 168 / 168 / 169 |

**Conditions, honestly.** The Mac was running on and off AC power (charging, 22% at the start) with `caffeinate`, and was shared with other
agent sessions: the one-minute load average at the start of the runs was between 2.7 and 6.2 (15 CPUs), recorded in every run's notes. No full test suite
held `/tmp/sf-fullsuite.lock` before any scenario (the script waited on it; it never had to). Load shows in tail latency, not in the validity gate;
the p95/p99 columns are nevertheless within 1 ms across repeats, so it did not move these groups visibly. The harness was restarted twice before
this group (once to stop writing `requests.jsonl`, once because the first definition of "time to isolate" called a frozen worker isolated
for being quiet in one bucket); the baseline is entirely from the final harness.

**The spread is tiny because the embedded cluster is deterministic**, not because the measurement is precise: same seed, round-robin, identical workers and
fault counts that repeat (80 held requests in each freeze run). Where all three runs agree to the displayed digit, `compare` reports `n/a` for the SPREAD (equal medians); a
later group has to move by more than this to be distinguishable, but a tiny range also means a real small difference could show as "ranges do not overlap". Read the sizes of the differences, not only the verdict.

What the baseline says about master (descriptive; no feature is being claimed):

- **worker-crash**: the clients lose nothing (100%), 0.8% of the fault window's requests are retried (attempts 1.009). The control plane saw `FAILED` at the end of the fault. The
  registry drops the worker in about one heartbeat (isolate 1.0 s). This is the registry-plus-retry baseline the plan wanted.
- **worker-freeze**: 93.45% success, 72 of 1,100 requests fail with a 504 after the 2 s header timeout, and **no retry happens on a timeout** (attempts 1.000). The failures
  come in pulses of 8 every 2 s: master's gateway stops selecting the frozen worker while 8 requests are hung on it (its reported concurrency), then picks it again when they time out. That is
  capacity accounting, not detection, and the harness correctly reports the worker as *not isolated* (it was selected in the last buckets).
- **slow-worker**: with the timeout below the 5 s delay, 96.15% success (the slow worker's requests time out at 2 s, again without a retry); with it above, 100% success and a p99 of 5.1 s: nothing to open a breaker on, which is the false-positive check the feature must keep passing.
- **error-burst**: 503 `mock_failure` is retried on another worker (100% success; attempts 1.333 at 100% and 1.143 at 50%, 33% and 14% of requests retried), 500 is not (74.99%, attempts 1.000). The worker is never isolated: master keeps sending it a share of traffic.
- **midstream**: 87.6% success; nothing can retry after output began. **conn-reset**: retried (100%, attempts 1.333).
- **drain-roll**: 100% success and 1.000 attempts while 4 workers are replaced one at a time with a 1.5 s announce. With announce 0 the same roll would show `draining` retries (acceptance 18, PR F).
- **overload**: 48 clients on 32 slots: 67-68% success, attempts 0.68 (a third of the requests are refused before any attempt: no queue on master).

Not measured on master: breaker state, probes, queue wait, `retry.denied`; their series do not exist and read as 0.

## Honest limits

- **One machine.** The load generator, the gateway, the control plane and the mock workers share one process and one
  CPU budget. Numbers compare configurations run back to back on this machine; they say nothing about capacity.
- **Closed loop.** Clients wait for each response, so a failing worker also lowers the offered load, and a 503 makes a client back
  off 50-150 ms (`driver.Backoff`). An open-loop crowd would see more of every fault. Chaos runs cannot use open loop yet.
- **Mock workers.** Faults are injected at the mock's HTTP layer. A mock has no GPU, no shared device and no memory: a slow mock does not
  slow its neighbours, a crash frees nothing, and `Crash` resets connections the way a killed process does but reuses the port at once.
- **A blackhole holds no slot**, so a load-aware scheduler is not steered away from a frozen worker by its reported load (that is the
  point: the breaker must be the only detector), whereas a real hung worker usually holds its slots. The extra delay of a slow worker
  does hold the slot.
- **The registry is fast here.** The agent reports FAILED within one heartbeat (1 s) and the gateway refreshes every 100 ms, so a crash is
  absorbed by the registry long before any breaker could act. That is why `worker-crash` is a registry-plus-retry baseline, not a
  breaker scenario.
- **Windows are small.** Twenty seconds of fault and at most a few thousand requests per window. The success rate of a window
  has a resolution of about 0.1 percentage point and the time measures a resolution of one bucket (1 s).
- **The mock's `unavailable` mode is `mock_failure` (503)**, the case master retries. A real engine's 503 may differ.
- **Process-level chaos** (SIGKILL, SIGSTOP/SIGCONT, a real SIGTERM through `--drain-announce`) is not run here: the flag exists and is tested, and the
  harness calls the same code paths in process. Real signals belong to the Docker deployment phase.
- **Load on the machine.** Other work was running on this Mac while the baseline was taken (see the load averages below). A
  full test suite from another session would have shown up as a lower success rate or higher tail latency in a group, not as an
  invalid run: the gate catches a machine that slept, not one that was busy.

## Deviations from the plan

1. **No `SetUnready`, no flapping scenario** (D17), as the brief says; the "unready via a safe path" of the brief is `DrainWorker`, which makes a worker report
   `draining` through the real shutdown path.
2. **`DrainWorker` tells the registry at once.** In production the agent learns of a drain from its next probe (up to one heartbeat later);
   here the agent is stopped and `Shutdown` reports DRAINING and deregisters immediately. The announce period therefore only has to cover the gateway's
   registry refresh. The mock's own `draining` status and its announce behaviour are the real ones. `ReplaceWorker`, new in this PR, is not in the plan: the
   drain-roll needs a worker to roll *to*.
3. **`drain.readiness_grace: 0` and the `cool_down` floor are not set.** They are keys of the resilience configuration, which does not exist on master;
   `Config.GatewayMutate` is where PRs D to F set them for their runs. Nothing in this PR reads them.
4. **The rolling drain keeps the fault window at least 4 s per worker** regardless of `--scale`, because one step (announce 1.5 s, drain, a fresh worker
   registering) takes about 3 s of real time.
5. **Attempts per request is a ratio of gateway counters**, not of the clients' `X-ServerFlow-Attempts` header, which the gateway sends only when
   a request needed more than one attempt (`attempt.go`, `noteAttempts`). The share of requests that header reports as retried is printed next to it.
6. **The validity gate fails the whole run, the hard bounds only warn.** `scheduler_ineligible_selections_total != 0` means the router has a bug; an invalid run
   would hide it from a comparison.
7. **`--resilience` is also accepted by `benchmark run`** and recorded there (so a plain Phase 7 run on a later build says whether the feature was on).
   A remote `--target` records no value (`resilience` is empty, shown as `not recorded`): the harness cannot know.
8. **Queue depth** is sampled from the control plane as in Phase 7 (`result.json` `queue`), over the whole run, not per window.
9. **No `tests/chaos` yet.** Acceptance 19 puts the chaos tests in the harness self-tests of this PR (done: fake cluster, fake clock, fake loader, and one real run
   of each scenario) and in the product behaviour of PR D (not done: there is nothing to test).
