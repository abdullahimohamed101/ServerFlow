# Benchmark run_004

2026-10-09T03:28:18Z, workload `mixed`, scheduler `least-active`, closed-loop with 12 clients. Seed 1, repeat 1 of 3.

## Setup

| | |
| --- | --- |
| Target | embedded simulated cluster (mock workers) |
| Commit | bb94c8c625ad7f3c5ce4eed284d26285aabfecb1 (clean) |
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

Sent 519, succeeded 519, failed 0 (error rate 0.00%). 83 warm-up requests were not counted; 0 needed a second attempt.

## Throughput

Measured over 26.92s (the 20s window plus the drain of requests still in flight at its end).

| Requests/s | Input tokens/s | Output tokens/s |
| --- | --- | --- |
| 19.28 | 15495.2 | 4837.8 |

Tokens: 271 requests reported usage, 248 were estimated (request size for input, stream chunks for streamed output).

## Latency and time to first token (ms)

Over successful requests, measured from the intended send time (in closed-loop mode that is when the client sent, so a slow server also slows the clients: see ADR-013).

| | n | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Latency | 519 | 250.9 | 1389.8 | 3371.0 | 7532.5 |
| TTFT (streaming) | 248 | 30.4 | 60.9 | 86.0 | 116.8 |

## Queues and worker balance

80 samples. Total queue depth across workers: average 0.00, p95 0.0, max 0. Average requests being served: 11.95.

| Worker | Model | Completed | Share | Mean queue | Mean active |
| --- | --- | --- | --- | --- | --- |
| worker-01 | qwen-7b | 251 | 47.27% | 0.00 | 3.60 |
| worker-02 | qwen-7b | 195 | 36.72% | 0.00 | 4.30 |
| worker-03 | qwen-7b | 85 | 16.01% | 0.00 | 4.05 |

Request balance (Jain index, 1 is perfectly even): 0.8682.
Queue balance (Jain index of mean queue depth): not measured (every worker's queue stayed empty, so the index is undefined).
GPU utilization: not measured (workers did not report GPU utilization (mock workers have no GPU)).

## Not measured

- gpu_utilization: workers did not report GPU utilization (mock workers have no GPU)
- queue_imbalance: every worker's queue stayed empty, so the index is undefined

## How to read this

- These are measurements of one run, not a ranking. A different seed, machine, or load can change them; use `--repeat` to see run-to-run spread.
- The load generator, gateway, control plane and mock workers share one machine, so absolute numbers say little about capacity; compare runs made on the same machine.
- A balanced distribution (Jain near 1) is not automatically good when workers differ in speed.
- Per-worker completed counts start at the end of warm-up, so requests sent in warm-up that finish later are counted in them.
- Closed-loop clients wait for each response before sending the next, so a slow server also slows the offered load; use --rate (open loop) to offer a fixed load whatever the server does.
