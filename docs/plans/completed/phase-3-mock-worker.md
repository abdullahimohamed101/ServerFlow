# Phase 3 — Mock Worker Framework

Status: Completed (implemented, independently verified twice and reviewed once; merged with its PR)
Owner: coding agent
Depends on: Phase 2 (complete). Phase 1 is deferred (needs a GPU); this phase is what makes GPU-free development possible.
Spec: `docs/architecture/serverflow-spec.md` §10–12, §24, §32, §53–54, §58 Phase 3, §63

## Outcome

A standalone CLI, `mock-worker`, that behaves like a vLLM inference worker without a
GPU: an OpenAI-compatible HTTP server whose latency, throughput, queueing, and
failures are configurable and reproducible. Three instances with different
characteristics (simulation mode, §54: 100 / 60 / 20 tokens/s) run side by side, and
the Phase 2 gateway can stream through any of them.

```bash
mock-worker --model=qwen-7b --ttft=200ms --tokens-per-second=50 --failure-rate=0.01 \
            --addr=:9001 --max-concurrency=4 --queue-size=32 --seed=42
```

Endpoints on the worker:

```text
POST /v1/chat/completions   stream and non-stream
GET  /v1/models
GET  /health                liveness (always 200 while the process is up)
GET  /readyz                readiness (503 while starting or draining)
GET  /stats                 worker metadata, spec §10 fields (provisional JSON)
```

## Non-Goals

- No registration, heartbeats, or registry (Phase 4). The mock does not call anything.
- No scheduler, and no multi-upstream routing in the gateway (Phases 5–6).
- No real tokenizer, no GPU metrics, no batching/contention model (each generation
  runs at its own configured tokens/s regardless of load).
- No Prometheus `/metrics` (Phase 10), no Docker image, no Kubernetes.
- Not the worker agent. The mock is a fake vLLM; the agent that wraps vLLM comes later.
- No runtime reconfiguration endpoint (chaos tooling, Phase 15).

## Current Architecture

- `cmd/worker-agent` is a Phase 0 stub (config + log line). Untouched here.
- `internal/api` has OpenAI request validation and the error model (strict-key
  parsing, ADR-003) but no response types.
- `internal/gateway` proxies to one static upstream behind the `Upstream` interface;
  the readiness probe defaults to `GET /v1/models`; upstream non-2xx is passed through;
  mid-stream upstream failure ends with an SSE error event.
- `pkg/protocol` holds `InferenceRequest` and IDs. No `tests/` directory yet.
- Dependency direction (ARCHITECTURE.md): `cmd/*` → `internal/*` → `pkg/protocol`.

## Decisions (confirm before implementation)

- **D1 Name and location.** New binary `cmd/mock-worker` with logic in
  `internal/mockworker`. The spec's example calls it `mock`; `mock-worker` is clearer
  next to `worker-agent`. Alternative: `worker/mock` (spec layout reserves `worker/` for
  runtime and vLLM integration).
- **D2 Flags only.** The CLI is configured by flags, validated at startup (fail fast,
  exit 1), separate from the shared YAML/env `internal/config`. It is a dev tool, not a
  platform component.
- **D3 Reuse `internal/api`.** Request parsing goes through `api.ParseChatRequest`
  (so the mock is a strict upstream and catches parser-differential bugs), and the
  OpenAI response/chunk wire types are added to `internal/api`, where they belong.
- **D4 Lifecycle in scope.** `--startup-delay` (not ready until it elapses) and graceful
  drain on SIGTERM (readiness 503, new requests 503, in-flight finish, exit 0). Small,
  reuses the gateway's shutdown pattern, and Phases 4 and 15 need it (spec §11, §39,
  §40). Cut if you want a smaller phase.
- **D5 `/stats` is JSON, not Prometheus**, and its shape is provisional. Phase 4 defines
  the real heartbeat contract in `pkg/protocol`.
- **D6 Queue-full is 503**, with an OpenAI-shaped error (`code: queue_full`), not 429
  (which implies a rate limit). The queue is bounded; it never grows without limit (§63).
- **D7 No contention model.** Each generation runs at its own tokens/s. Load-dependent
  slowdown is deferred to Phase 14 if scheduler experiments need it.
- **D8 Reproducible failures.** Failure decisions use a seeded RNG. Default seed is
  random and logged at startup; `--seed` fixes it (spec §32 requires recording the seed).
- **D9 Integration tests live in `tests/integration`**, matching the spec layout.
- **D10 No new dependencies.** Standard library only.

## Proposed Design

### Packages

```text
cmd/mock-worker/        flags → mockworker.Config → Validate → Serve (SIGTERM = drain)
internal/mockworker/    config.go    Config + Validate
                        engine.go    slots, bounded FIFO queue, cancellation, stats
                        failure.go   seeded failure injector and modes
                        server.go    HTTP handlers, SSE streaming, lifecycle (ready/drain)
internal/api/           + ChatCompletion, ChatChunk, Usage wire types
tests/integration/      gateway ↔ mock-worker end to end; three workers at once
docs/development/mock-worker.md   usage, flags, simulation recipe
```

### Request timeline

1. Parse and validate (`api.ParseChatRequest`, model allowlist = the one model).
2. Failure roll (seeded). Modes below.
3. Admission: if all `max-concurrency` slots are busy, join the bounded FIFO queue; if
   the queue is full, reject immediately with 503 `queue_full`.
4. When a slot frees (strict arrival order), wait `ttft`, then emit tokens at
   `tokens-per-second` (inter-token delay = 1/tps). Output length is
   `min(max_tokens, output-tokens)`; `finish_reason` is `stop` or `length`.
5. Stream: SSE chunks in the vLLM shape (role chunk at TTFT, one content chunk per
   token, a finish chunk, then `data: [DONE]`). Non-stream: one JSON body with `usage`
   after TTFT plus generation time.
6. Client disconnect cancels the request: a queued request leaves the queue, an active
   one stops generating and frees its slot promptly (spec §24).

### Failure modes (`--failure-rate` in [0,1], `--failure-mode`)

| Mode | Behavior |
| --- | --- |
| `error` (default) | HTTP 500 with an OpenAI-shaped body, before generation |
| `unavailable` | HTTP 503 before generation |
| `drop` | close the connection with no response |
| `midstream` | stream a few tokens, then abort the connection (non-stream: abort the body) |

### Worker metadata (`/stats`, spec §10 subset)

`worker_id`, `model`, `status` (`starting`, `ready`, `draining`), `active_requests`,
`queue_depth`, `queued_input_tokens` (word-count estimate), `recent_tokens_per_second`
(rolling window), plus counters (`completed`, `failed`, `rejected`) and the configured
`ttft` and `tokens_per_second`. GPU fields are omitted (no GPU).

### Flags

`--model` (default `mock-model`), `--addr` (`:9000`), `--worker-id` (derived from the
address), `--ttft`, `--tokens-per-second`, `--output-tokens` (default 64),
`--max-concurrency` (4), `--queue-size` (32), `--failure-rate`, `--failure-mode`,
`--seed`, `--startup-delay`, `--drain-timeout`, `--log-level`. Validation: ttft ≥ 0,
tokens/s > 0, 0 ≤ failure-rate ≤ 1, concurrency ≥ 1, queue-size ≥ 0, output-tokens in
[1, 100000], known failure mode.

## Affected Files / Components

New: `cmd/mock-worker/`, `internal/mockworker/`, `tests/integration/`,
`docs/development/mock-worker.md`.
Changed: `internal/api` (response types), `Makefile` (`mock-workers` target starting
three workers at 100/60/20 tokens/s), `README.md` and `ARCHITECTURE.md` (phase status,
mock worker), `docs/plans` (this file moves to `completed/` on finish).

## Acceptance Criteria

1. The flags above parse; invalid values exit 1 with a clear message and start nothing.
2. Streaming timing: the first chunk is not sent before `ttft`; chunk spacing matches
   1/tps within tolerance; the stream ends with a finish chunk and `[DONE]`; token count
   is `min(max_tokens, output-tokens)` with the right `finish_reason`.
3. Non-stream returns one JSON completion with correct `usage` after about
   `ttft + n/tps`.
4. With `max-concurrency=1` and `queue-size=2`: requests run and complete in arrival
   order, a fourth is rejected at once with 503 `queue_full`, and `/stats` shows
   `active_requests`, `queue_depth`, and `queued_input_tokens` accurately while they wait.
5. A client that disconnects while queued leaves the queue; one that disconnects
   mid-generation stops token emission and frees its slot within a short bound.
6. Failure injection: rate 0 never fails and rate 1 always fails, each mode behaves as
   tabled, and the same `--seed` plus the same request order reproduces the same
   failure sequence.
7. `/readyz` is 503 until `startup-delay` elapses and 200 after; `/health` is always
   200; on SIGTERM readiness goes 503, new requests get 503, an in-flight stream
   finishes, and the process exits 0 (or non-zero after `drain-timeout` is exceeded).
8. `/v1/models` lists the model (503 while starting or draining, so the gateway's
   default readiness probe tracks worker readiness); an unknown model is a 404
   OpenAI-shaped error.
9. `/stats` returns the fields above; `recent_tokens_per_second` tracks real output
   within tolerance.
10. Through the Phase 2 gateway: streaming and non-stream succeed; a `midstream` failure
    surfaces as the gateway's SSE error event; a 503 `queue_full` passes through.
11. Three workers (100 / 60 / 20 tokens/s) run concurrently; measured throughput is
    ordered correctly and within about 15% of configured for each.
12. `gofmt`, `go vet`, `go build`, `go test -race ./...`, and `golangci-lint run ./...`
    pass; CI green; no new dependencies.

## Verification Plan

- Unit: config validation, the timing schedule, the FIFO queue and slot accounting, the
  seeded failure injector, the throughput window, response wire types.
- Server tests with `httptest` and short real durations: criteria 2–9.
- Integration (`tests/integration`): criteria 10–11 with the real gateway in process.
- Race detector on everything; timing tests assert hard lower bounds ("not before
  ttft") and loose upper bounds, and prefer ordering over absolute values, to stay stable
  on loaded CI machines. Run the package under `-race -count=10` and under CPU load.
- Manual: run `make mock-workers`, call each worker with `curl -N` and the `openai`
  client, then through the gateway; SIGTERM mid-stream against the built binary.
- Independent `verify-change` and `review-change` passes by fresh read-only subagents,
  then `harden-change`, then `prepare-pr` and stop for approval.

## Implementation Notes (deviations and additions)

- `/v1/models` is 503 while the worker is starting or draining (added so the
  gateway's default readiness probe follows the worker; the plan had it always 200).
- Response types live in `internal/api` as one `ChatCompletion` struct used for
  both full responses and chunks (the plan said `ChatChunk`).
- Streaming headers go out when a request gets a slot, not at once as vLLM does
  (documented limitation).
- **Independent verify and review passes** (fresh read-only subagents; 105 mutants
  in the first verification and 73 in the second) found, and these are fixed with tests that failed first:
  - `recent_tokens_per_second` under-read by up to 20-50% (it divided partial
    wall-clock seconds); it now averages complete 100ms buckets over the real
    covered time, and recovers correctly after idle gaps.
  - A request admitted before a drain but still sending its body was invisible to
    the drain and got cut off, and a stray idle connection made a clean drain exit
    1 after 5s (and made a drain test flaky). The drain now counts in-flight
    handlers from entry and ignores idle connections.
  - Missing coverage that mutants exposed: non-stream cancellation during TTFT and
    mid-generation, absolute-deadline pacing, the cancel-versus-grant race (now
    deterministic), midstream failure with a single token, `/stats` status while
    starting and draining, and several counters.
  - The **second independent verification** confirmed those fixes on the real binary
    (throughput within about 1% of the measured truth at 20-400 tokens/s and
    concurrency 1-4; a model test against a brute-force oracle matched exactly) and
    found more, also fixed with tests that failed first: a never-used connection still
    cost a 5s drain (exit 0 but slow) and `--drain-timeout` did not bound total
    shutdown (5.7s with a 1s timeout), so the post-drain grace is now 500ms and comes
    out of the drain budget; the throughput window's constants were barely pinned by
    tests (a 1s window passed everything), so exact-value tests pin window length,
    ring size, bucket width, and the reset boundary; the window now measures time from
    a monotonic epoch so a wall-clock step cannot zero it; a second SIGINT/SIGTERM
    during a drain was swallowed and now force-quits; `cmd/mock-worker` had no tests,
    so process-level tests cover `--help`, bad flags, a busy port, worker IDs from the
    real port, SIGTERM exit codes, and the forced second signal. A race in one of those
    new test helpers (found by repeating the suite under `-race`) was fixed.
  - Smaller: the mock logs the gateway's `X-Request-ID`; `--help` prints once;
    `--addr=:0` workers get distinct IDs; the default listen address is loopback;
    `max_tokens` equal to the natural length reports `length`; the stats field is
    `configured_ttft_ms` (it is the configured value); flaky timing windows widened
    and the three-worker tolerance tightened to the plan's 15%.

## Known Limitations / Follow-ups

- No batching or contention model; tokens are `tokN`, not text.
- Stream headers are sent when a slot is granted, not immediately.
- `stream_options.include_usage` is ignored.
- Unknown paths and methods return plain-text 404/405.
- A `--queue-size` near its cap of 100000 would hold roughly 2.8 GB of connection
  state (measured about 28 KB per queued connection); fine at the defaults.
- `/stats` status values (`starting`, `ready`, `draining`) do not yet map to the
  spec's state machine (`LOADING_MODEL`, `WARMING`, `READY`, ...); Phase 4 defines
  the contract.
- Mock error codes are lowercase (`queue_full`, `draining`, `mock_failure`) while
  gateway codes follow spec section 51; they mimic an upstream, not the gateway.
- `recent_tokens_per_second` is a window average including idle time: a single
  short burst decays hyperbolically (the divisor is the busy time covered, capped at
  5s), and a lone one-token request never registers because the first bucket of a
  busy period is skipped.
- `--help`/process tests build the binary with `go build` (they skip under `-short`).
- Timing-based tests have generous windows but remain load-sensitive in principle;
  they passed 32 parallel race-instrumented processes under 120 CPU burners on 15
  cores in verification.
- `cmd/gateway` still swallows a second signal during shutdown (same pattern the
  mock worker had); out of scope here.

## Risks

- **Flaky timing tests** under CI load. Mitigated as above; the Phase 2 overhead test
  showed load sensitivity is real.
- **Fidelity.** The mock has no batching slowdown and token-granular emission; results
  from it say nothing about vLLM. Docs and the benchmark notes must not over-claim.
- **Scope creep into Phase 4** (registration, heartbeats). The mock stays passive.
- **Goroutine or slot leaks** on cancel and failure paths; covered by criterion 5 and a
  leak assertion across all failure modes.
- **Unbounded resources:** the queue, `output-tokens`, and request size are all bounded.
- **`/stats` becoming a de facto contract.** Marked provisional (D5).
- **Seeded RNG under concurrency** is only reproducible for a fixed request order;
  stated in the docs.

## Implementation Steps

1. `internal/api`: response and chunk wire types, with tests.
2. `internal/mockworker/config.go`: config and validation, with tests.
3. Engine: slots, bounded FIFO queue, cancellation, stats and throughput window, with
   tests.
4. Seeded failure injector and modes, with tests.
5. Server: models, health, readyz, stats, and chat (stream and non-stream) with
   cancellation and failure modes, with tests.
6. Lifecycle: startup delay, drain, serve, with tests.
7. `cmd/mock-worker`: flags, validation, SIGTERM; manual smoke test.
8. `tests/integration`: gateway ↔ mock, and three workers at once.
9. `Makefile` target, `docs/development/mock-worker.md`, README and ARCHITECTURE.
10. Full gate, independent verify and review, harden, then `prepare-pr` and stop for
    approval. Move this plan to `docs/plans/completed/` on completion.
