# Phase 2 — Gateway Overhead

Spec target (section 31): gateway overhead excluding inference < 25 ms p95.

## Method

`TestGatewayOverhead` in `internal/gateway/overhead_test.go`. An in-process
fake upstream answers instantly (non-stream: a small JSON body; stream: one SSE
chunk then `[DONE]`). The same load is run twice with the same client settings:
directly against the fake upstream, then through the gateway. Overhead is the
difference in p95 latency.

- 3000 measured requests per scenario after a 200-request warm-up, concurrency 16
- Non-stream: total request latency. Stream: time to the first chunk
- Run with `go test -run TestGatewayOverhead -count=1 -v ./internal/gateway`
  (skipped under `-short`)

## Environment

| Item | Value |
| --- | --- |
| Hardware | Apple M5 Pro, macOS 26.6 |
| Go | 1.27.1 darwin/arm64 |
| Network | loopback; client, gateway, and fake upstream share one machine and CPU |
| Date | 2026-10-06 (re-measured after the independent review fixes) |

## Results (no race detector)

| Scenario | Direct p50 / p95 / p99 | Gateway p50 / p95 / p99 | p95 overhead |
| --- | --- | --- | --- |
| Non-stream (total latency) | 167 µs / 567 µs / 785 µs | 551 µs / 1.05 ms / 1.32 ms | **0.48 ms** |
| Stream (time to first chunk) | 167 µs / 499 µs / 792 µs | 372 µs / 999 µs / 1.17 ms | **0.50 ms** |

With `-race` (what CI runs) overhead is roughly 1.4 ms (non-stream) and
0.8 ms (stream) p95. Run-to-run variation is a few hundred microseconds. Both are well inside the 25 ms budget, which the test
asserts.

## Caveats

- This is a lower bound: a zero-latency local upstream, loopback networking, and
  no TLS. It isolates what the gateway itself adds (request parsing and
  validation, ID generation, logging, metrics, one proxy hop, flushing). It says
  nothing about vLLM.
- Client, gateway, and upstream compete for the same CPU, so absolute numbers are
  noisy; compare against the direct baseline, not against fixed values.
- "Time to first chunk" is the first streamed SSE chunk, which may be a role-only
  delta rather than the first generated token. True TTFT needs the real vLLM
  worker (Phase 13).
- Numbers are from one machine and should be re-measured on the target hardware
  before any claim is made outside this repo.
