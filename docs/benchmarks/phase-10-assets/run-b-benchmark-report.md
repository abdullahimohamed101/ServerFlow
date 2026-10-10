# Benchmark run_001

2026-10-10T01:46:21Z, workload `mixed`, scheduler `least-active`, closed-loop with 12 clients. Seed 1, repeat 1 of 1.

## Setup

| | |
| --- | --- |
| Target | remote 127.0.0.1:58080 |
| Commit | eab1e767a93b1558c57081a79a0e352741113ab7 (clean) |
| Models | mock-model |
| Workers | 3 |
| GPU | unknown |
| Duration / warm-up | 120s / 5s |
| Streaming share | 0.5 |
| Plan digest | `a67e32db638c7143` |
| Go / machine | go1.27.1, darwin/arm64, 15 CPUs, Apple M5 Pro |

Prompt distribution (input tokens are synthetic text, about four characters each):

| Class | Weight | Input tokens | max_tokens |
| --- | --- | --- | --- |
| short | 0.6 | 100-300 | 50-150 |
| medium | 0.3 | 500-1500 | 200-500 |
| long | 0.1 | 2000-8000 | 500-1500 |

## Requests

Sent 1198, succeeded 1198, failed 0 (error rate 0.00%). 54 warm-up requests were not counted; 0 needed a second attempt.

## Throughput

Headline throughput counts the 1198 requests that completed successfully inside the 120.0 s measurement window (whenever they were sent; 12 of them were sent during warm-up) and divides by the window, so one slow tail request cannot move it.

| Requests/s | Input tokens/s | Output tokens/s |
| --- | --- | --- |
| 9.98 | 8315.8 | 632.2 |

Throughput including tail: the 1198 requests sent inside the window divided by the 121.03s until the last of them finished (the drain counts against it, so it follows the slowest request): 9.90 requests/s, 8239.6 input and 626.9 output tokens/s.

Tokens: 605 requests reported usage, 593 were estimated (request size for input, stream chunks for streamed output).

## Latency and time to first token (ms)

Over successful requests, measured from the intended send time (in closed-loop mode that is when the client sent, so a slow server also slows the clients: see ADR-013).

| | n | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Latency | 1198 | 733.0 | 3452.6 | 3453.6 | 3565.2 |
| TTFT (streaming) | 593 | 103.0 | 302.1 | 303.1 | 304.5 |

## Queues and worker balance

480 samples. Total queue depth across workers: average 0.00, p95 0.0, max 0. Average requests being served: 11.98.

| Worker | Model | Completed | Share | Mean queue | Mean active |
| --- | --- | --- | --- | --- | --- |
| mock-1 | mock-model | 648 | 53.55% | 0.00 | 3.63 |
| mock-2 | mock-model | 419 | 34.63% | 0.00 | 3.70 |
| mock-3 | mock-model | 143 | 11.82% | 0.00 | 4.66 |

Request balance (Jain index, 1 is perfectly even): 0.7924.
Queue balance (Jain index of mean queue depth): not measured (every worker's queue stayed empty, so the index is undefined).
GPU utilization: not measured (workers did not report GPU utilization (mock workers have no GPU)).

## Not measured

- gpu_utilization: workers did not report GPU utilization (mock workers have no GPU)
- queue_imbalance: every worker's queue stayed empty, so the index is undefined

## Timings

Before the clock started 0.0s (worker list and first counts), boot 0.0s, warm-up 5.0s, window 120.0s, drain 1.0s, worker stats 0.0s, close 0.0s; 152.9s wall clock, 126.0s monotonic.

## How to read this

- These are measurements of one run, not a ranking. A different seed, machine, or load can change them; use `--repeat` to see run-to-run spread.
- The scheduler of a remote gateway is as declared by the user and was not verified.
- A balanced distribution (Jain near 1) is not automatically good when workers differ in speed.
- Per-worker completed counts start when warm-up ends, so the 12 warm-up requests that finished after that are included in them.
- Closed-loop clients wait for each response before sending the next, so a slow server also slows the offered load; use --rate (open loop) to offer a fixed load whatever the server does.
- Streamed token counts are estimated from chunks (the gateway does not request usage for streams); non-streamed counts come from the response usage.
- The wall clock advanced 152.9s but the monotonic clock 126.0s: the machine probably slept or its clock was changed during the run, which can explain unexpected stalls.
