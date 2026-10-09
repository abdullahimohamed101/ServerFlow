# Benchmark run_001

2026-10-09T03:26:56Z, workload `mixed`, scheduler `round-robin`, closed-loop with 12 clients. Seed 1, repeat 1 of 3.

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

Sent 433, succeeded 433, failed 0 (error rate 0.00%). 71 warm-up requests were not counted; 0 needed a second attempt.

## Throughput

Measured over 24.61s (the 20s window plus the drain of requests still in flight at its end).

| Requests/s | Input tokens/s | Output tokens/s |
| --- | --- | --- |
| 17.59 | 12828.3 | 4228.4 |

Tokens: 227 requests reported usage, 206 were estimated (request size for input, stream chunks for streamed output).

## Latency and time to first token (ms)

Over successful requests, measured from the intended send time (in closed-loop mode that is when the client sent, so a slow server also slows the clients: see ADR-013).

| | n | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Latency | 433 | 246.5 | 2210.7 | 6396.8 | 7500.6 |
| TTFT (streaming) | 206 | 30.4 | 60.7 | 61.2 | 62.5 |

## Queues and worker balance

80 samples. Total queue depth across workers: average 0.00, p95 0.0, max 0. Average requests being served: 12.00.

| Worker | Model | Completed | Share | Mean queue | Mean active |
| --- | --- | --- | --- | --- | --- |
| worker-01 | qwen-7b | 179 | 40.22% | 0.00 | 2.38 |
| worker-02 | qwen-7b | 181 | 40.67% | 0.00 | 3.15 |
| worker-03 | qwen-7b | 85 | 19.10% | 0.00 | 6.47 |

Request balance (Jain index, 1 is perfectly even): 0.9164.
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
