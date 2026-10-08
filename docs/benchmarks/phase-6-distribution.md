# Phase 6 — Distribution and Retries (1000 synthetic requests)

Spec acceptance (section 58, Phase 6): 1000 synthetic requests across 3 or more workers.

These numbers describe what happened in these runs. They are **not** evidence that one strategy is better
than another (spec sections 14 and 63): the workers are identical mock workers on one machine, and the load
is a synthetic mix. That comparison needs the Phase 7 benchmark harness and, later, real workers.

## Method

`tests/integration/distribution_test.go`. A real control plane, real worker agents, mock workers, and a real
gateway in registry mode, all in one process on loopback. 16 concurrent clients send 1000 requests; every
fourth request streams; three of four ask for `qwen-7b` (four workers) and one of four for `llama-8b` (two
workers). Mock workers answer in a few milliseconds. Each worker counts the chat requests it receives.

- Run: `go test -count=1 -v -run 'TestThousand|TestAFlaky|TestAWorkerKilled' ./tests/integration`
- Hardware: Apple M5 Pro, macOS 26.6; Go 1.27.1 darwin/arm64; 2026-10-07; no race detector.
- Each scenario asserts what is stated below, so a regression fails the test; the tables are one run.

## 1. Healthy cluster: distribution by strategy

750 `qwen-7b` requests over 4 workers (ideal 187.5 each); 250 `llama-8b` over 2 (ideal 125).

| Strategy | qwen-7b per worker | llama-8b per worker | Failures | Retried |
| --- | --- | --- | --- | --- |
| round-robin | 188, 188, 187, 187 | 125, 125 | 0 | 0 |
| random (unseeded) | 189, 197, 198, 166 | 122, 128 | 0 | 0 |
| least-active | 197, 195, 205, 153 | 125, 125 | 0 | 0 |
| least-queue | 188, 188, 187, 187 | 125, 125 | 0 | 0 |

- Every `qwen-7b` request reached a `qwen-7b` worker and every `llama-8b` request a `llama-8b` worker
  (the workers' counts sum to exactly 750 and 250).
- Round-robin splits within one request of even. `least-queue` matches it because the mock workers' queues
  stay empty, so every worker ties and ties take turns. `least-active` follows reported load that is up to
  one heartbeat old, so it varies run to run (this run: 153 to 205).
- `random` and `least-active` are asserted only within loose bounds (nobody starved or swamped).

## 2. One always-failing worker among four

One `qwen-7b` worker answers every request with 503 `unavailable` while looking healthy to the registry.

| Strategy | Flaky worker's first attempts (of 750) | Healthy workers' served | Client failures | Retried requests |
| --- | --- | --- | --- | --- |
| round-robin | 246 | 250, 250, 250 | 0 | 246 |
| random | 195 | 255, 259, 236 | 0 | 195 |
| least-active | 732 | 216, 279, 255 | 0 | 732 |
| least-queue | 246 | 250, 250, 250 | 0 | 246 |

- **Zero client failures**; every request that reached the flaky worker was retried exactly once, on a
  different worker, and the total attempts equal requests plus retries.
- Round-robin gives the flaky worker a third of the first attempts rather than a quarter: a retry takes a
  turn in the rotation, so the worker after the failing one gets its own turn and the retry.
- **`least-active` sent most first attempts to the flaky worker** (613 and 732 in two runs). A worker that
  fails instantly always reports no active requests, so it always looks least loaded. Retries hid it from
  clients, but every one of those requests paid a second attempt. This is the black-hole effect a circuit
  breaker removes (Phase 15). It is a property of "pick the least loaded" meeting a fast-failing worker,
  not a ranking of strategies.

## 3. A worker killed mid-run

Three `qwen-7b` workers (round-robin) and one `llama-8b` worker. After 300 requests the backend of one
`qwen-7b` worker is shut down (its agent keeps running and reports FAILED at the next heartbeat).

| Phase | Requests | Failures | Retried | Notes |
| --- | --- | --- | --- | --- |
| before the kill | 300 | 0 | 0 | |
| the registry has not yet noticed | 200 | 0 | 63 | each request that hit the dead worker was retried after a connection refused |
| after the registry marks it not eligible (51 ms later, plus a refresh) | 500 | 0 | 0 | `inference_retries_total{reason="connect"}` stayed at 63 |

Nothing reached the dead worker once the registry and the gateway's cache had caught up.

## Real binaries

`tests/integration/gateway_process_test.go` repeats the idea with the built `gateway`, `control-plane`,
`worker-agent` and `mock-worker` binaries: 90 requests with one always-503 worker among three all succeed
(25 to 55 of them retried), and 90 requests after `kill -9` of one backend all succeed with the connection
failures on record in the gateway log.

## What this does not show

- Real model latency, GPU contention, or a network. Mock workers answer in milliseconds.
- Behaviour with two bad workers out of three: two attempts can both land on bad ones, and then the client
  gets the last worker's error. That is the spec's attempt budget.
- Which strategy is better. See Phase 7 and Phase 14.
