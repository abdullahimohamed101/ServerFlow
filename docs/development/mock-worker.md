# Mock Worker

`mock-worker` is a fake inference worker: an OpenAI-compatible HTTP server whose
latency, throughput, queueing, and failures you control. It stands in for vLLM so
the gateway, scheduler, and benchmarks can be built and tested without a GPU
(spec §53). It is a development tool, not the worker agent.

## Quick start

```bash
go run ./cmd/mock-worker --model=qwen-7b --addr=127.0.0.1:9001 --ttft=200ms --tokens-per-second=50
```

Point the gateway at it:

```bash
go run ./cmd/mock-worker --model=qwen-7b --addr=127.0.0.1:9001 &
SERVERFLOW_GATEWAY_UPSTREAM_URL=http://localhost:9001 SERVERFLOW_GATEWAY_MODELS=qwen-7b \
  go run ./cmd/gateway
curl -N localhost:8080/v1/chat/completions \
  -d '{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

Three workers with different speeds (simulation mode, §54), on `127.0.0.1:9001`,
`:9002`, `:9003` at 100, 60, and 20 tokens/s:

```bash
make mock-workers
```

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--model` | `mock-model` | Model name served; any other model is a 404 |
| `--addr` | `127.0.0.1:9000` | Listen address. Loopback only by default; use `:9000` to listen on every interface |
| `--worker-id` | `mock-<port>` | Worker ID reported by `/stats` (the real port, so `--addr=:0` stays unique) |
| `--ttft` | `200ms` | Time to first token once a request starts running |
| `--tokens-per-second` | `50` | Generation speed per request (0.001 to 1e6) |
| `--output-tokens` | `64` | Tokens generated per request, capped by `max_tokens` |
| `--max-concurrency` | `4` | Requests generating at once |
| `--queue-size` | `32` | Requests allowed to wait; more get 503 `queue_full` |
| `--failure-rate` | `0` | Fraction of requests that fail (0 to 1) |
| `--failure-mode` | `error` | `error`, `unavailable`, `drop`, or `midstream` |
| `--seed` | random | Failure-injection seed; the chosen seed is logged at startup |
| `--startup-delay` | `0` | Stay not-ready for this long after starting |
| `--drain-timeout` | `30s` | How long SIGTERM waits for in-flight requests |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |

Invalid values exit 1 with a message and start nothing.

## Endpoints

| Endpoint | Behavior |
| --- | --- |
| `POST /v1/chat/completions` | Stream (SSE) and non-stream. Parsed with the same strict validation as the gateway |
| `GET /v1/models` | The one model; 503 while starting or draining (so the gateway's default readiness probe works) |
| `GET /health` | Liveness: always 200 while the process is up |
| `GET /readyz` | 200 `ready`; 503 `starting` or `draining` |
| `GET /stats` | Worker metadata (provisional JSON, below) |

## How a request behaves

1. Validate the request (400, 404, 413 as the gateway would).
2. Roll the failure injector. `error` and `unavailable` answer 500 or 503 at once;
   `drop` closes the connection with no response.
3. Take a slot. If all `--max-concurrency` slots are busy the request waits in a
   bounded FIFO queue (strict arrival order). If the queue is full it is rejected
   immediately with 503 `queue_full`. The queue never grows past `--queue-size`.
4. Once running, wait `--ttft`, then emit one token every `1/--tokens-per-second`
   seconds. Pacing uses absolute deadlines, so timer jitter does not accumulate
   into drift.
5. Streaming responses send a role chunk and the first token at the TTFT, then one
   chunk per token, a final chunk with `finish_reason` (`stop`, or `length` when
   the client's `max_tokens` limit ended the output, including when it equals the
   natural length), and `data: [DONE]`. Non-stream responses
   arrive after the last token with a `usage` block.
6. If the client disconnects, a queued request leaves the queue and a running one
   stops generating and frees its slot.

`midstream` failures let the role chunk and up to two tokens through, then abort the
connection (with a one-token output nothing is sent before the abort); a
non-stream `midstream` failure sends half the body and aborts.

Failures are decided by a seeded generator, so the same `--seed` and the same
request order reproduce the same failure sequence. With concurrent requests the
order is not fixed, so reproducibility holds for sequential traffic.

## Lifecycle

`--startup-delay` makes the worker report `starting` (not ready) for a while, like a
real worker loading a model. On SIGINT or SIGTERM it drains: `/readyz` and
`/v1/models` go to 503, new requests get 503 `draining`, and every request already
accepted finishes, including ones still sending their body and ones waiting in the
queue. Then it exits 0 within about half a second, even if an idle or never-used
connection is still open. `--drain-timeout` bounds the whole shutdown: if requests
do not finish within it the worker exits 1 and cuts them off. A second SIGINT or
SIGTERM during a drain force-quits immediately.

## `/stats`

```json
{"worker_id":"mock-9001","model":"qwen-7b","status":"ready","active_requests":2,
 "queue_depth":1,"queued_input_tokens":14,"recent_tokens_per_second":96.4,
 "completed":120,"failed":3,"rejected":0,"cancelled":2,"tokens_generated":7680,
 "configured_ttft_ms":200,"configured_tokens_per_second":50}
```

The fields are the spec §10 worker metadata that makes sense without a GPU.
`queued_input_tokens` estimates input as whitespace-separated words.
`recent_tokens_per_second` is the average over the last 5 seconds of complete
100ms buckets, divided by the time actually covered, so a worker that has just
started (or resumed after idling) reads its true rate; it includes idle time inside
the window, so a worker that stopped working reads lower and falls to 0.
`configured_ttft_ms` is the configured value, not an observed TTFT. The shape is
provisional: Phase 4 defines the real heartbeat contract.

## Logs

One `chat` line per request with `component`, `worker_id`, `outcome` (`ok`, `failed`,
`cancelled`, `rejected`), token counts, `queue_ms`, `duration_ms`, `ttft_ms` for
streams, and `request_id` when the caller (the gateway) sent `X-Request-ID`, so a
request can be followed from gateway to worker. The startup line records the seed.

## What it is not

- It has no batching or contention: each generation runs at its own speed whatever
  the load, so it will not show the slowdown a busy real GPU does.
- Tokens are `tok0 tok1 ...`, not real text, and input tokens are word counts.
- Streaming response headers go out when a request gets a slot, whereas vLLM sends
  them at once even if the request is still queued internally.
- `stream_options.include_usage` is ignored: streams never carry a final usage chunk.
- Unknown paths and wrong methods return Go's plain-text 404/405, not an OpenAI error.
- Results measured against it say nothing about vLLM. Use it to test behavior
  (routing, failures, cancellation, queues), not to claim performance.
