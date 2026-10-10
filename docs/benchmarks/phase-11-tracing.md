# Phase 11: tracing demonstration, binary size and overhead

Measured on one machine (Apple M5 Pro, macOS, Go 1.27.1 darwin/arm64, other work running on it at the time), 2026-10-10. Nothing here
is a claim about another CPU or OS. Reproduce with the commands under each heading.

## A real retried request in Jaeger

```bash
docker compose -f observability/tracing/docker-compose.yml up -d       # jaegertracing/jaeger:2.22.0 (here with TRACING_OTLP_PORT=58110 TRACING_UI_PORT=58111)
TRACE_DEMO_OTLP_PORT=58110 TRACE_DEMO_UI_PORT=58111 TRACE_DEMO_PORT_BASE=58120 scripts/trace-demo.sh
```

The script runs a control plane, three mock workers behind real agents (`mock-ok`, `mock-ok2`, and `mock-flaky`, which fails every request
with 503) and a registry-mode gateway with `SERVERFLOW_TRACING_ENABLED=true`, sends streaming requests until one is retried, waits for
the 5 s span batch, and reads the trace back from Jaeger's query API (`GET /api/v3/traces`, filtered by `serverflow.request_id`). The
response carries spans from both services. Excerpt for request `req_b43dbba87acf3b9c`, trace `75e6df4e3ec949c019073f55b01a8a3b`
(span IDs shortened to 8 characters; times relative to the request start; kinds: 1 internal, 2 server, 3 client):

| Span | Service | Span ID | Parent | Kind | Start | Duration | Status | Notable attributes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `gateway.receive` | gateway | d3baee88 | none | 2 | 0.0 ms | 397.0 ms | unset | `serverflow.attempts=2`, `http.response.status_code=200`, `serverflow.model=mock-model`, `serverflow.ttft_ms=83` |
| `scheduler.select` | gateway | 5d4e06fe | d3baee88 | 1 | 0.1 ms | 0.1 ms | unset | `attempt=1`, `worker_id=mock-flaky`, `strategy=round-robin` |
| `worker.forward` #1 | gateway | 71ebaf8e | d3baee88 | 3 | 0.2 ms | 0.8 ms | error | `attempt=1`, `worker_id=mock-flaky`, `attempt.outcome=retried`, `attempt.class=status_503`, `attempt_id=att_8063cf760c2cbee8`; event `retry` (`class=status_503`, `next_attempt=2`) |
| `inference` | worker | f654a6a7 | 71ebaf8e | 2 | 0.5 ms | 0.2 ms | error | `worker_id=mock-flaky`, `attempt_id=att_8063cf760c2cbee8`, `injected_failure=unavailable` |
| `scheduler.select` | gateway | 3aae2d7a | d3baee88 | 1 | 0.9 ms | 0.0 ms | unset | `attempt=2`, `worker_id=mock-ok` |
| `worker.forward` #2 | gateway | 06469a6f | d3baee88 | 3 | 0.9 ms | 396.0 ms | unset | `attempt=2`, `worker_id=mock-ok`, `attempt.outcome=ok`, `attempt_id=att_a9d8c1e57048d28c`; **link** to 71ebaf8e (`serverflow.link=retry_of`) |
| `first_token` | gateway | 3caac4b7 | 06469a6f | 1 | 0.9 ms | 82.5 ms | unset | `ttft_ms=83` |
| `inference` | worker | 74bf2e3e | 06469a6f | 2 | 1.2 ms | 395.5 ms | unset | `worker_id=mock-ok`, `attempt_id=att_a9d8c1e57048d28c`, `queue_ms=0`, `prompt_tokens=2`, `output_tokens=64`; event `first_token` (`ttft_ms=81`) |
| `queue_wait` | worker | d564f14a | 74bf2e3e | 1 | 1.3 ms | 0.0 ms | unset | none |
| `completion` | gateway | 08db25db | 06469a6f | 1 | 83.4 ms | 313.5 ms | unset | `attempt=2` |

Both attempts and both worker spans share one trace ID, the two `worker.forward` spans are siblings under the root, the second links to the
first, and each worker `inference` span is parented to the attempt that called it and carries that attempt's ID. The same run's
request without a retry (`req_c2094bda58136f19`) shows `gateway.receive` → {`scheduler.select`, `worker.forward` → {`first_token`,
`inference` → `queue_wait`, `completion`}}. The tree printed by the script for the retried request:

```text
gateway.receive [serverflow-gateway] 397.0ms attempts=2 http.response.status_code=200
  scheduler.select [serverflow-gateway] attempt=1 worker_id=mock-flaky scheduler.strategy=round-robin
  worker.forward [serverflow-gateway] 0.8ms attempt=1 worker_id=mock-flaky attempt.outcome=retried attempt.class=status_503 event:retry
    inference [serverflow-mock-worker] 0.2ms worker_id=mock-flaky attempt.outcome=failed
  scheduler.select [serverflow-gateway] attempt=2 worker_id=mock-ok scheduler.strategy=round-robin
  worker.forward [serverflow-gateway] 396.0ms attempt=2 worker_id=mock-ok attempt.outcome=ok link->71ebaf8e
    first_token [serverflow-gateway] 82.5ms attempt=2
    inference [serverflow-mock-worker] 395.5ms worker_id=mock-ok attempt.outcome=ok event:first_token
      queue_wait [serverflow-mock-worker] 0.0ms
    completion [serverflow-gateway] 313.5ms attempt=2
```

What this does not show: the Jaeger web UI was not looked at in a browser; the check is through its query API. The Colima Docker daemon on
this Mac ran the container.

## Binary size

`go build` of `./cmd/gateway` and `./cmd/mock-worker` for darwin/arm64, master (675e43f) against this branch.

| Binary | master | Phase 11 | Change | Stripped (`-s -w`) master | Stripped Phase 11 |
| --- | --- | --- | --- | --- | --- |
| gateway | 33.0 MB | 38.9 MB | +5.9 MB (+18%) | 21.8 MB | 25.7 MB |
| mock-worker | 10.0 MB | 22.0 MB | +12.1 MB | 6.7 MB | 15.0 MB |

The plan expected under 9 MB for the gateway, and that held. The mock worker had little else linked, so the OpenTelemetry SDK, the OTLP
proto and gRPC stubs, and protobuf (the gateway already carried protobuf through Prometheus) more than doubled it. Packages in the gateway's
dependency graph went from 321 to 470. Only the worker's tracing flag needs this; a real worker (Phase 13) is not this binary.

## Overhead

**In process** (one whole request through `Handler()`, the upstream stubbed, real export queue and batcher, an exporter that discards):

```bash
go test -run '^$' -bench BenchmarkTracingObserver -benchmem -count=6 -benchtime=20000x ./internal/gateway
```

| Mode | ns/op (median, min..max of 6) | B/op | allocs/op |
| --- | --- | --- | --- |
| off, non-stream | 9 762 (9 695..9 817) | 28 157 | 101 |
| off, stream | 10 578 (10 557..10 917) | 28 337 | 108 |
| on, unsampled, non-stream | 11 168 (11 115..11 300) | 29 358 | 119 |
| on, unsampled, stream | 11 818 (11 688..11 922) | 29 562 | 126 |
| on, 100% sampled, non-stream | 18 608 (18 498..18 894) | 35 218 | 154 |
| on, 100% sampled, stream | 22 196 (21 657..22 564) | 39 040 | 188 |

Off is the same code path as before this phase (no observer registered): 101 and 108 allocations, as recorded in the prep notes.
Unsampled adds about 1.4 microseconds and 18 allocations (the root span is created to obtain valid IDs for the logs and the
`traceparent`; nothing else is built). 100% sampling adds about 8.9 microseconds and 53 allocations (non-stream) or 11.6 microseconds and
80 (stream), for 2 spans (non-stream) or 4 spans (stream) per request in this static-mode benchmark; registry mode adds one `scheduler.select` per attempt, and the worker adds its own two spans in its own process.

**Over HTTP** (opt-in `TestTracingOverhead`: a real gateway on a socket, a zero-latency upstream, 16 clients, 3 000 streaming requests per
round after 300 warm-up, 5 rounds per mode with the modes interleaved, time to first chunk; the exporter sends to a local fake OTLP receiver):

```bash
SERVERFLOW_TRACING_OVERHEAD=1 go test -run TestTracingOverhead -v -count=1 -timeout 300s ./tests/integration
```

Three runs of the test (each 5 rounds; the figures are medians of the rounds; "spread" is the lowest to highest per-round p95 of tracing off):

| Run | off p50 / p95 | unsampled added p50 / p95 | 100% sampled added p50 / p95 | spread of off p95 |
| --- | --- | --- | --- | --- |
| 1 | 418 µs / 1.04 ms | +5 µs / +0.5 µs | +21 µs / +187 µs | 0.97..1.25 ms |
| 2 | 467 µs / 1.30 ms | -29 µs / -40 µs | +9 µs / +37 µs | 1.16..2.31 ms |
| 3 | 439 µs / 1.19 ms | -10 µs / -129 µs | +6 µs / +89 µs | 1.02..1.69 ms |

At 100% sampling the added p95 was 0.04 to 0.19 ms, below the 1 ms target and inside the run-to-run spread of the baseline; unsampled is
indistinguishable from off (the negative values are noise of the same size). Phase 2's `TestGatewayOverhead` (tracing off) passed at the
same time: p95 overhead 0.66 ms non-stream and 0.66 ms stream against the 25 ms budget. These are loopback numbers with a zero-latency
upstream, which is the worst case for relative overhead; with real inference times the added microseconds vanish.

**Collector failure** (`TestADeadCollectorNeverTouchesRequests`): 400 sequential requests against a gateway whose collector answers 500,
hangs forever, answers after 800 ms, or is a closed port; every request returned 200, p95 stayed within 4x the tracing-off p95 plus 3 ms
(the test's bound, deliberately loose; it passed), the queue never exceeded its bound, drop and failure counters moved, shutdown
returned within its 2 s budget, and at most two export log lines were written.
