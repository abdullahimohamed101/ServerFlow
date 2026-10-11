# Benchmark run_016

2026-10-11T05:42:11Z, workload `uniform-short`, scheduler `round-robin`, closed-loop with 14 clients. Seed 1, repeat 1 of 3.

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

Sent 5848, succeeded 5848, failed 0 (error rate 0.00%). 358 warm-up requests were not counted; 334 needed a second attempt.

## Throughput

Headline throughput counts the 5848 requests that completed successfully inside the 50.0 s measurement window (whenever they were sent; 14 of them were sent during warm-up) and divides by the window, so one slow tail request cannot move it.

| Requests/s | Input tokens/s | Output tokens/s |
| --- | --- | --- |
| 116.96 | 23535.2 | 11745.8 |

Throughput including tail: the 5848 requests sent inside the window divided by the 50.13s until the last of them finished (the drain counts against it, so it follows the slowest request): 116.66 requests/s, 23484.5 input and 11712.5 output tokens/s.

Tokens: 2975 requests reported usage, 2873 were estimated (request size for input, stream chunks for streamed output).

## Latency and time to first token (ms)

Over successful requests, measured from the intended send time (in closed-loop mode that is when the client sent, so a slow server also slows the clients: see ADR-013).

| | n | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Latency | 5848 | 120.3 | 165.2 | 169.2 | 176.3 |
| TTFT (streaming) | 2873 | 20.3 | 20.5 | 21.0 | 29.4 |

## Queues and worker balance

200 samples. Total queue depth across workers: average 0.00, p95 0.0, max 0. Average requests being served: 13.94.

Request balance (Jain index, 1 is perfectly even): not measured (the workers behind the gateway could not be listed (pass --control-plane with --target)).
Queue balance (Jain index of mean queue depth): not measured (every worker's queue stayed empty, so the index is undefined).
GPU utilization: not measured (workers did not report GPU utilization (mock workers have no GPU)).

## Chaos: error-burst (unavailable-50)

Half of the requests to the first worker are answered 503 mock_failure.

Time scale 1, fault applied at 13.0s and cleared at 33.0s after the start of the run, on worker-01. Parameters: bucket=1s, clients=14, error_rate=0.5, failure_mode=unavailable, mock_concurrency=8, scheduler=round-robin, workers=4.

| Window | Seconds | Sent | Succeeded | Failed | Success | Attempts/request | Retried | p95 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 3.0-13.0 | 1177 | 1177 | 0 | 100.00% | 1.000 | 0 | 164 | 169 |
| fault | 13.0-33.0 | 2340 | 2340 | 0 | 100.00% | 1.143 | 334 | 165 | 169 |
| recovery | 33.0-53.0 | 2331 | 2331 | 0 | 100.00% | 1.000 | 0 | 165 | 169 |

Pre-registered primary measures: success rate in the fault window 100.00%, attempts per request in it 1.143.
Descriptive: time to isolate not reached (the worker was still selected within the last 3 buckets of the fault window); time to recover 1.0s.

Fault evidence (the mock worker's own counters): applied true, 334 requests touched (334 errored, 0 delayed, 0 held), 0 crashes, 0 drains (0 requests served while announcing); control plane saw the worker as READY; 14 clients on 24 surviving slots.
Hard bounds: ineligible selections 0 (must be 0); goroutines 46 to 115; heap 1.5 to 3.3 MiB.

Validity gate:

- load driver: ok (the load generator ran to the end of the run)
- fault applied and cleared: ok (applied true (err <nil>), cleared true (err <nil>))
- baseline error rate: ok (0.00% of 1177 baseline requests failed, limit 5.00%)
- fault evidenced: ok (334 requests failed by the worker (the mock workers' counters, not the harness's intention))
- clients within the surviving slots: ok (14 clients against 24 surviving slots (32 slots less the faulted worker's): at most 60% of them = 14, or the run measures rejections)
- minimum requests per window: ok (every window needs at least 100 requests; short: none)
- cluster settled: ok (Start waited 501ms after the control plane listed every worker (five registry refreshes = 500ms))
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
- Chaos run: Half of the requests to the first worker are answered 503 mock_failure.
- What this embedded mock cluster cannot show here: The breaker's detection of a persistent fault cannot be told from the registry's here, because the embedded agent reports a dead worker within one hea...
- Windows follow when the fault was really applied and cleared. Gateway counters (attempts per request, retries) are read at the tick that opens and closes a window (every 1s), so an attempt of a reques...
- Closed-loop clients: 14 for the whole run on 32 request slots (24 left serving). Closed-loop clients wait for each response, so a failing worker also slows the offered load.
- Load average (1, 5 and 15 minutes) at the start of the run: 4.83 3.98 3.94; at the end: 5.80 4.31 4.06.
