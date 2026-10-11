# Phase 12 — Cost of lifecycle events and of token capture

Budget (plan D20, acceptance 14): with events on and the broker up, down or slow, gateway p95 overhead is within run-to-run spread of
events off and never more than +1 ms; memory and goroutines are bounded. The Phase 2 budget (25 ms p95) is untouched.

## Method

- **Events (`TestEventsOverhead`, `tests/integration/events_overhead_test.go`)**: runs only with `SERVERFLOW_EVENTS_BENCH=1`, needs the
  Kafka test environment and Docker. An in-process upstream answers instantly. A static-mode gateway is measured in four scenarios,
  interleaved in five rounds (off, up, down, frozen, then again): 3,000 requests at concurrency 16 after a 200-request warm-up, for
  non-streaming (total latency) and streaming (time to the first chunk), the same method as `TestGatewayOverhead`. No race detector.
  - `off`: no observer.
  - `on, broker up`: franz-go producer to Redpanda v25.1.1 with SASL/SCRAM (`scripts/dev-kafka.sh`), `acks=all`, idempotent.
  - `on, broker down`: the producer points at a closed port.
  - `on, broker frozen`: a private Redpanda container is `docker pause`d for the run: connections stay open and nothing answers, the
    extreme case of a slow broker. (A broker that is slow but alive sits between "up" and "frozen".)
- **Token capture (D8)**: `TestGatewayOverhead` run five times alternately on the base commit (master, `675e43f`, extracted with
  `git archive`) and on this branch, events off, so the only difference on the request path is the token scanner and the observer
  value fields.
- **Go benchmarks**: `go test -run '^$' -bench ObserverLifecycle ./internal/events` and `-bench TokenScanner ./internal/gateway`.
- Machine: Apple M5 Pro, macOS 26.6, Go 1.27.1, loopback, `caffeinate -i`. Docker runs under Colima. **Two other agents were running
  builds and containers on the same machine**, so absolute numbers are noisy (round 1 and 2 of `off` are slow outliers). Compare within
  a round, and read the spread, not the single numbers.

## Results: events

p95 latency in microseconds; per scenario, min / median / max over 5 rounds.

| Scenario | Non-stream p95 | Stream TTFT p95 |
| --- | --- | --- |
| off | 804 / 871 / 1610 | 936 / 972 / 1773 |
| on, broker up | 814 / 899 / 1109 | 1046 / 1117 / 1210 |
| on, broker down | 786 / 826 / 931 | 771 / 882 / 913 |
| on, broker frozen | 734 / 791 / 796 | 810 / 847 / 964 |

Reading it:

- The quiet rounds (3 to 5) are the fair comparison. Non-stream, up versus off: 891 vs 862, 899 vs 871, 814 vs 804 µs, so about **+10 to
  +30 µs**. Streaming TTFT, up versus off: 1117 vs 949, 1138 vs 972, 1046 vs 936 µs, so about **+110 to +170 µs** (a streamed request
  emits four events instead of three, and the first token event is built on the streaming path). Both are far below the +1 ms budget.
- Down and frozen are not slower than off: nothing on the request path waits for Kafka. They look faster than off only because `off`
  always runs first in a round, while the machine is still settling; the claim is "no worse", not "faster".
- Goroutines: +95 to +101 in every scenario including off (the HTTP clients and servers of the test), so the producer adds none that
  grow. Heap after a run with the broker down or frozen: +13 to +22 MiB, the bounded buffers (10,000 events in the publisher, 10,000
  records in the client) full of undelivered events; with the broker up +1 to +6 MiB. Nothing grows without bound.
- Drops with the broker down or frozen: 12,400 events per 6,200 requests were refused by the client's full buffer and counted
  (`producer_full`); none were dropped by the publisher buffer, and no request noticed. With the broker up nothing was dropped
  (about 22,000 events delivered per round).
- The per-lifecycle cost of the observer itself (a full non-streaming request with a first token: seven Observer calls and six
  `Enqueue`s): **about 2.0 µs and 35 allocations (2.7 KB)** with a discarding sink (`BenchmarkObserverLifecycle`, three runs, 2.03 to
  2.07 µs; an earlier run before the benchmark's work was included measured 1.3 µs). Encoding happens on the drain goroutine, not on the
  request.

## Results: token capture (D8)

Events off, `TestGatewayOverhead`, p95 overhead, five alternating runs:

| | Non-stream min / median / max | Stream TTFT min / median / max |
| --- | --- | --- |
| base (master) | 593 / 744 / 783 µs | 582 / 603 / 705 µs |
| this branch | 623 / 683 / 752 µs | 554 / 618 / 658 µs |

Indistinguishable within the spread. The scanner itself (`BenchmarkTokenScanner*`): **130 ns and 0 allocations per streamed chunk**,
and **1.4 µs and 0 allocations for a 64 KiB body**. It keeps a 2 KiB sliding window that is overwritten as the response passes.

## Not measured

- Real vLLM response shapes (Phase 13): the scanner is tested against the OpenAI format and the mock worker only.
- A slow-but-alive broker with real network latency (only the frozen extreme was run); a multi-broker cluster; TLS cost.
- The race detector's effect (CI runs with `-race`; the gate does not assert on these numbers).
- Throughput limits of the producer: the test sends about 3,000 requests per second per gateway and the broker-up run delivered every
  event; the ceiling was not sought.
