# Benchmark run_002

2026-10-09T04:56:58Z, workload `mixed`, scheduler `round-robin`, closed-loop with 12 clients. Seed 1, repeat 2 of 3.

## Setup

| | |
| --- | --- |
| Target | embedded simulated cluster (mock workers) |
| Commit | 4ee077414512863d76a7ce4934d34965e66d7fee (clean) |
| Models | qwen-7b |
| Workers | 3, profile heterogeneous |
| GPU | none (mock workers) |
| Duration / warm-up | 20s / 3s |
| Streaming share | 0.5 |
| Plan digest | `d065b561cbd74cf2` |
| Go / machine | go1.27.1, darwin/arm64, 15 CPUs, Apple M5 Pro |

Prompt distribution (input tokens are synthetic text, about four characters each):

| Class | Weight | Input tokens | max_tokens |
| --- | --- | --- | --- |
| short | 0.6 | 100-300 | 50-150 |
| medium | 0.3 | 500-1500 | 200-500 |
| long | 0.1 | 2000-8000 | 500-1500 |

## Requests

Sent 435, succeeded 435, failed 0 (error rate 0.00%). 70 warm-up requests were not counted; 0 needed a second attempt.

## Throughput

Headline throughput counts the 435 requests that completed successfully inside the 20s measurement window (whenever they were sent) and divides by the window, so one slow tail request cannot move it.

| Requests/s | Input tokens/s | Output tokens/s |
| --- | --- | --- |
| 21.75 | 16565.8 | 5266.2 |

Throughput including tail: the 435 requests sent inside the window divided by the 26.33s until the last of them finished (the drain counts against it, so it follows the slowest request): 16.52 requests/s, 12405.6 input and 4021.9 output tokens/s.

Tokens: 228 requests reported usage, 207 were estimated (request size for input, stream chunks for streamed output).

## Latency and time to first token (ms)

Over successful requests, measured from the intended send time (in closed-loop mode that is when the client sent, so a slow server also slows the clients: see ADR-013).

| | n | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Latency | 435 | 280.4 | 2140.8 | 3465.8 | 7500.4 |
| TTFT (streaming) | 207 | 30.3 | 60.4 | 60.7 | 60.8 |

## Queues and worker balance

80 samples. Total queue depth across workers: average 0.00, p95 0.0, max 0. Average requests being served: 11.93.

| Worker | Model | Completed | Share | Mean queue | Mean active |
| --- | --- | --- | --- | --- | --- |
| worker-01 | qwen-7b | 164 | 36.69% | 0.00 | 2.12 |
| worker-02 | qwen-7b | 167 | 37.36% | 0.00 | 3.62 |
| worker-03 | qwen-7b | 116 | 25.95% | 0.00 | 6.17 |

Request balance (Jain index, 1 is perfectly even): 0.9760.
Queue balance (Jain index of mean queue depth): not measured (every worker's queue stayed empty, so the index is undefined).
GPU utilization: not measured (workers did not report GPU utilization (mock workers have no GPU)).

## Not measured

- gpu_utilization: workers did not report GPU utilization (mock workers have no GPU)
- queue_imbalance: every worker's queue stayed empty, so the index is undefined

## Timings

Boot 0.6s, warm-up 3.0s, window 20.0s, drain 6.3s, worker stats 0.0s, close 0.0s; 29.9s wall clock, 29.9s monotonic.

## How to read this

- These are measurements of one run, not a ranking. A different seed, machine, or load can change them; use `--repeat` to see run-to-run spread.
- The load generator, gateway, control plane and mock workers share one machine, so absolute numbers say little about capacity; compare runs made on the same machine.
- A balanced distribution (Jain near 1) is not automatically good when workers differ in speed.
- Per-worker completed counts start when warm-up ends, so the 12 warm-up requests that finished after that are included in them.
- Closed-loop clients wait for each response before sending the next, so a slow server also slows the offered load; use --rate (open loop) to offer a fixed load whatever the server does.
- Streamed token counts are estimated from chunks (the gateway does not request usage for streams); non-streamed counts come from the response usage.
