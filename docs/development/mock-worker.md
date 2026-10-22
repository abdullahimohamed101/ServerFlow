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
| `--otlp-endpoint` | empty (tracing off) | OTLP/HTTP base URL, e.g. `http://127.0.0.1:4318`. Records `inference` and `queue_wait` spans for requests whose `traceparent` the gateway sampled (`docs/operations/tracing.md`) |
| `--trace-insecure-ok` | `false` | Allow plaintext `http://` to a non-loopback `--otlp-endpoint` |
| `--model-engine` | `simple` | `simple` is the constant-speed model below; `sim` is the continuous-batching simulator ([Simulator engine](#simulator-engine---model-engine-sim)). Opt-in: nothing changes unless it is set |
| `--profile` | empty | With `sim`: a built-in profile (`default`, `fast`, `medium`, `slow`, `kv-tight`, `noisy`) |
| `--sim-params` | empty | With `sim`: a JSON parameter file instead of a profile; its SHA-256 is logged at startup |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |

Invalid values exit 1 with a message and start nothing. With `--model-engine=sim`, `--ttft` and
`--tokens-per-second` are rejected (the profile decides timing); `--profile` and `--sim-params` are
rejected without `sim` and are mutually exclusive.

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
 "max_concurrency":4,"queue_size":32,"configured_ttft_ms":200,"configured_tokens_per_second":50}
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

## Simulator engine (`--model-engine sim`)

The default model above has no contention: every generation runs at its own speed whatever the load, so
a token-aware scheduler has nothing to be aware of (Phase 14, `docs/plans/active/phase-14-workload-aware-scheduler.md`,
D21). `--model-engine sim` swaps the timing for a small model of an LLM inference engine. **Every number it
produces is a simulator result, not a measurement of any engine or GPU.** Nothing about the default
worker, its flags, `/stats` or logs changes unless the flag is given (`cmd/mock-worker/differential_test.go`
compares both against the base commit).

```bash
go run ./cmd/mock-worker --model=qwen-7b --model-engine=sim --profile=slow --output-tokens=200 --seed=7
```

### One engine, two drivers

The model is `internal/sim/engine`: a pure state machine over virtual time. Every call is given the time
as a `time.Duration` since an arbitrary epoch; the package never reads a clock, sleeps, takes a lock or
starts a goroutine (a test parses its sources to enforce that), and all randomness comes from the seed, so
the same configuration and call sequence give the same events bit for bit. The mock worker
(`internal/mockworker/sim.go`) is the real-time driver: virtual time is the wall-clock offset since the
server was built, and each waiting request handler advances the engine to "now" and sleeps until its next
event. A future offline simulator (Phase 14 PR F) calls the same engine with event times instead, so there is
one worker model, not two.

### What it models

Work proceeds in iterations, one at a time:

1. **Admission.** Waiting requests join in strict arrival order while a sequence slot is free
   (`--max-concurrency`) and the KV cache has room for the prompt (one token more, for the token its
   prefill produces). If the head does not fit, nothing behind it is tried.
2. **Prefill.** An admitted request first processes its prompt: `prefill_fixed_ms + prefill_per_token_ms * tokens`.
   By default the whole prompt is one blocking step during which the running batch decodes nothing;
   `prefill_chunk_tokens > 0` splits it into chunks that share a step with the batch's decode. Finishing
   the prefill emits the first token, so **TTFT = queue wait + prefill (+ noise)**.
3. **Decode.** A step for a batch of `b` sequences takes `step_base_ms + step_per_seq_ms * b` and gives
   every sequence one token. The per-stream rate therefore falls as the batch grows while the aggregate
   rate `b / (t0 + t1*b)` saturates at `1/t1`.
4. **KV cache.** `kv_budget_tokens` is shared by all sequences; a sequence holds its prompt plus the
   tokens generated so far. When a decode step cannot get its tokens, the most recently started sequence is
   preempted: its KV is freed, it returns to the front of the queue and re-runs its prefill (prompt plus what
   it had generated) later, like recompute preemption. A sequence that is alone and still cannot grow ends
   with `finish_reason: "length"`. A prompt that can never fit is refused with 400. Because the KV budget
   binds before `--max-concurrency` for long prompts, `queue_depth` and `queued_input_tokens` become non-zero
   while slots are still free: the regime the Phase 5 `least-queue` strategy was built for.
5. **Noise.** Decode steps and prefills are multiplied by seeded lognormal factors with mean 1 and the
   configured coefficient of variation (`step_noise_cv`, `prefill_noise_cv`), clamped to a factor of 20.
6. **Output length.** Drawn per request from a private seeded stream, independent of `max_tokens`:
   lognormal around `--output-tokens` with `output_cv`, then capped by `max_tokens` (`finish_reason`
   `length` when the cap ends it). With `output_fraction = f > 0` and a `max_tokens` given, the length is
   `f * max_tokens` (times the noise factor), which makes the gateway's `input + max_tokens` estimate wrong by
   a known amount.

Everything else stays as in the default worker: validation, failure injection and the run-time faults,
queue bound (`--queue-size`, 503 `queue_full`), drain, tracing, metrics, response shapes and header timing.

### Profiles and parameter files

A profile is a `Params` value (`internal/sim/engine/params.go`); a parameter file is the same as JSON with
`"version": 1`. Files are parsed strictly (unknown fields, a wrong version, trailing data and out-of-range
values are errors). `Params.Hash()` is the SHA-256 of the canonical JSON, logged at startup and served in
`/stats`, so a result can say which world it came from; changing any field changes it (a test checks every
field). The built-in profiles are all **assumed**, not fitted: round numbers of the order of a 7-8 B model on
one datacenter GPU, chosen to give the shapes below, with `fast`, `medium` and `slow` as three hardware
generations for a heterogeneous fleet. `provenance` says so in every file and in `/stats`; only a calibration
against a real engine (Phase 14 PR H) may claim more, and nothing here has been calibrated.

The embedded benchmark cluster takes the profiles per worker (`embedded.Config.SimProfiles`, cycled over
the workers).

### `/stats` in sim mode

The default fields keep their meaning, filled from the engine: `active_requests` counts sequences holding a
slot, `queue_depth` and `queued_input_tokens` the waiting requests (input is counted in words, as in the
default worker; the benchmark prompts are one word per token), `recent_tokens_per_second` is the same 5-second
window over virtual time. `configured_ttft_ms` and `configured_tokens_per_second` are 0 (no constant exists).
Added, only in this mode:

| Field | Meaning |
| --- | --- |
| `gpu_utilization` | Batch fill, `active / max_concurrency` in percent. A modelled quantity: it says nothing about how real GPU utilisation counters behave (they typically saturate well before the batch is full) |
| `gpu_memory_used_mb` | `weights_mb` plus KV tokens in use times `kv_bytes_per_token` |
| `sim` | `engine`, `profile`, `provenance`, `params_hash`, `prefilling`, `running`, `kv_used_tokens`, `kv_budget_tokens`, `preemptions`, `kv_exhausted` |

The worker agent forwards the two GPU fields in its heartbeat (`internal/worker/backend.go`), so the
registry and the scheduler see them; a worker that does not send them is unchanged.

### The model's output curves

This is what the engine produces, not a fit to data (regenerated and checked by
`go test ./internal/sim/engine -run TestDocsCurves`; the closed forms are pinned by `TestCurvesMatchClosedForms`).
Use it to see the shape the scheduler experiments will run against, and compare it with a real engine's
measurements once they exist (the calibration table in the plan, Option B).

<!-- sim-curves:begin -->
Profile `default` (hash `e7d211cd6bbb`), noise off, one request on an idle worker.

Prefill: time to the first token against prompt length.

| Prompt tokens | `default` TTFT (ms) | `fast` (ms) | `slow` (ms) |
| ---: | ---: | ---: | ---: |
| 1 | 5.1 | 3.0 | 10.2 |
| 128 | 17.8 | 9.4 | 35.6 |
| 512 | 56.2 | 28.6 | 112.4 |
| 1024 | 107.4 | 54.2 | 214.8 |
| 2048 | 209.8 | 105.4 | 419.6 |
| 4096 | 414.6 | 207.8 | 829.2 |
| 8192 | 824.2 | 412.6 | 1648.4 |

Decode: the gap between tokens of one stream, and the throughput, against the number of concurrent streams (`default`).

| Concurrent streams | Step time (ms) | Per-stream tokens/s | Aggregate tokens/s |
| ---: | ---: | ---: | ---: |
| 1 | 21.0 | 47.6 | 48 |
| 2 | 22.0 | 45.5 | 91 |
| 4 | 24.0 | 41.7 | 167 |
| 8 | 28.0 | 35.7 | 286 |
| 16 | 36.0 | 27.8 | 444 |
| 32 | 52.0 | 19.2 | 615 |
| 64 | 84.0 | 11.9 | 762 |

KV budget: the most requests holding a slot at once (the rest wait in the queue) when 64 identical requests arrive together at a worker with 64 slots, each with `max_tokens` 500. A request is admitted when its prompt fits, so the peak is reached in the first admission wave; later KV growth can then force preemptions. 0 means the prompt can never fit and is refused.

| Prompt tokens | `default` (32768 KV tokens) | `kv-tight` (6144) | `slow` (16384) |
| ---: | ---: | ---: | ---: |
| 128 | 64 | 47 | 64 |
| 512 | 63 | 11 | 31 |
| 2048 | 15 | 2 | 7 |
| 4096 | 7 | 1 | 3 |
| 8192 | 3 | 0 | 1 |
<!-- sim-curves:end -->

### What it cannot show

- Real batch scaling: the linear step time `t0 + t1*b` is a stand-in for a family of shapes (memory-bound
  flat region, compute-bound knee, attention cost growing with context). Real engines are not linear.
- Prefix caching, speculative decoding, quantisation effects, paged-attention fragmentation and swap-based
  preemption; the preemption modelled is recompute only, and prefill of different requests never overlaps.
- Real noise: lognormal timing noise is independent per iteration; real latency has heavy tails, thermal and
  garbage-collection effects, and correlation in time.
- What real GPU utilisation or memory counters report (see `gpu_utilization` above).
- Token counts: input is counted as words, the gateway estimates three bytes per token, and the model's
  token is whatever `--output-tokens` and `max_tokens` say.
- The network, the tokenizer and the HTTP server: those are the real ones, running on the same laptop as
  the load generator and the gateway, so wall-clock results carry that machine's scheduling noise
  (ADR-013). Only the engine itself is deterministic.
- Anything about CUDA, vLLM, Metal or llama.cpp. Results measured against it say nothing about them.

## What it is not

- In the default engine there is no batching or contention: each generation runs at its own speed whatever
  the load, so it will not show the slowdown a busy real GPU does. `--model-engine sim` adds a model of that
  slowdown, which is still a model.
- Tokens are `tok0 tok1 ...`, not real text, and input tokens are word counts.
- Streaming response headers go out when a request gets a slot, whereas vLLM sends
  them at once even if the request is still queued internally.
- `stream_options.include_usage` is ignored: streams never carry a final usage chunk.
- Unknown paths and wrong methods return Go's plain-text 404/405, not an OpenAI error.
- Results measured against it say nothing about vLLM. Use it to test behavior
  (routing, failures, cancellation, queues), not to claim performance. Results from the simulator engine are
  "simulator-validated, not hardware-validated" at best (Phase 14 plan, D26).
