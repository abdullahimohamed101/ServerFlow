# Benchmark run_032

2026-10-11T05:56:37Z, workload `uniform-short`, scheduler `round-robin`, closed-loop with 19 clients. Seed 1, repeat 2 of 3.

## Setup

| | |
| --- | --- |
| Target | embedded simulated cluster (mock workers) |
| Commit | 92c5488eb643fc436822f906aeec757bfab55f46 (clean) |
| Resilience | absent |
| Models | qwen-7b |
| Workers | 4, profile identical |
| GPU | none (mock workers) |
| Duration / warm-up | 50s / 3s |
| Streaming share | 0.5 |
| Plan digest | `5c17a8e856433f54` |
| Go / machine | go1.27.1, darwin/arm64, 15 CPUs, Apple M5 Pro |

Prompt distribution (input tokens are synthetic text, about four characters each):

| Class | Weight | Input tokens | max_tokens |
| --- | --- | --- | --- |
| short | 1 | 100-300 | 50-150 |

## Requests

Sent 8712, succeeded 7414, failed 1298 (error rate 14.90%). 484 warm-up requests were not counted; 0 needed a second attempt.

Failures by class: status_5xx 1298. Responses by status class: 2xx 7414, 5xx 1298.

## Throughput

Headline throughput counts the 7414 requests that completed successfully inside the 50.0 s measurement window (whenever they were sent; 19 of them were sent during warm-up) and divides by the window, so one slow tail request cannot move it.

| Requests/s | Input tokens/s | Output tokens/s |
| --- | --- | --- |
| 148.28 | 29860.7 | 14823.7 |

Throughput including tail: the 7414 requests sent inside the window divided by the 50.11s until the last of them finished (the drain counts against it, so it follows the slowest request): 147.97 requests/s, 29793.2 input and 14799.2 output tokens/s.

Tokens: 3762 requests reported usage, 3652 were estimated (request size for input, stream chunks for streamed output).

## Latency and time to first token (ms)

Over successful requests, measured from the intended send time (in closed-loop mode that is when the client sent, so a slow server also slows the clients: see ADR-013).

| | n | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Latency | 7414 | 119.3 | 164.2 | 168.4 | 169.8 |
| TTFT (streaming) | 3652 | 20.2 | 20.3 | 20.6 | 22.7 |

## Queues and worker balance

200 samples. Total queue depth across workers: average 0.00, p95 0.0, max 0. Average requests being served: 17.52.

Request balance (Jain index, 1 is perfectly even): not measured (the workers behind the gateway could not be listed (pass --control-plane with --target)).
Queue balance (Jain index of mean queue depth): not measured (every worker's queue stayed empty, so the index is undefined).
GPU utilization: not measured (workers did not report GPU utilization (mock workers have no GPU)).

## Chaos: overload

More closed-loop clients than the cluster has request slots for the length of the fault window: the workers' slots and queues fill and the gateway answers 503.

Time scale 1, fault applied at 13.0s and cleared at 33.0s after the start of the run, on -. Parameters: bucket=1s, clients=19, extra_clients_in_fault=29, mock_concurrency=8, scheduler=round-robin, workers=4.

| Window | Seconds | Sent | Succeeded | Failed | Success | Attempts/request | Retried | p95 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 3.0-13.0 | 1593 | 1593 | 0 | 100.00% | 1.000 | 0 | 164 | 169 |
| fault | 13.0-33.0 | 3969 | 2671 | 1298 | 67.30% | 0.672 | 0 | 164 | 168 |
| recovery | 33.0-53.0 | 3150 | 3150 | 0 | 100.00% | 1.000 | 0 | 165 | 169 |

Pre-registered primary measures: success rate in the fault window 67.30%, attempts per request in it 0.672.
Descriptive: time to isolate not reached (not defined: no single faulted worker); time to recover 1.0s.

Fault evidence (the mock worker's own counters): applied true, 0 requests touched (0 errored, 0 delayed, 0 held), 0 crashes, 0 drains (0 requests served while announcing); control plane saw the worker as -; 48 clients on 32 surviving slots.
Hard bounds: ineligible selections 0 (must be 0); goroutines 46 to 142; heap 2.0 to 5.0 MiB.

Validity gate:

- load driver: ok (the load generator ran to the end of the run)
- fault applied and cleared: ok (applied true (err <nil>), cleared true (err <nil>))
- baseline error rate: ok (0.00% of 1593 baseline requests failed, limit 5.00%)
- fault evidenced: ok (1298 failed requests, 1298 gateway no-capacity refusals in the fault window (the mock workers' counters, not the harness's intention))
- clients exceed slots (overload): ok (48 clients in the fault window against 32 slots: an overload needs more clients than slots)
- minimum requests per window: ok (every window needs at least 100 requests; short: none)
- cluster settled: ok (Start waited 500ms after the control plane listed every worker (five registry refreshes = 500ms))
- clean working tree: ok (tree clean, commit 92c5488eb6: the commit must be recorded and the tree clean for a result to be reproducible)
- no sleep or clock step: ok (the wall clock moved 0s against the monotonic clock during the run (limit 250ms): a larger move means the machine slept or its clock was stepped)

Only the two primary measures support a claim, and only when the ranges of repeat groups do not overlap; with ten or so descriptive measures, one range in ten is disjoint by chance.

## Not measured

- gpu_utilization: workers did not report GPU utilization (mock workers have no GPU)
- queue_imbalance: every worker's queue stayed empty, so the index is undefined
- request_imbalance: the workers behind the gateway could not be listed (pass --control-plane with --target)

## Timings

Before the clock started 0.0s (worker list and first counts), boot 0.6s, warm-up 3.0s, window 50.0s, drain 0.0s, worker stats 0.0s, close 0.0s; 53.7s wall clock, 53.7s monotonic.

## How to read this

- These are measurements of one run, not a ranking. A different seed, machine, or load can change them; use `--repeat` to see run-to-run spread.
- The load generator, gateway, control plane and mock workers share one machine, so absolute numbers say little about capacity; compare runs made on the same machine.
- A balanced distribution (Jain near 1) is not automatically good when workers differ in speed.
- Chaos run: More closed-loop clients than the cluster has request slots for the length of the fault window: the workers' slots and queues fill and the gateway answers 503.
- What this embedded mock cluster cannot show here: What a real queue in the gateway does (Phase 15 D11): on master there is none, so this shows the rejections the clients get. Closed-loop clients also ...
- Windows follow when the fault was really applied and cleared. Gateway counters (attempts per request, retries) are read at the tick that opens and closes a window (every 1s), so an attempt of a reques...
- Closed-loop clients: 19 for the whole run, plus 29 in the fault window on 32 request slots (32 left serving). Closed-loop clients wait for each response, so a failing worker also slows the offered loa...
- Load average (1, 5 and 15 minutes) at the start of the run: 4.25 4.55 4.47; at the end: 4.54 4.61 4.50.
