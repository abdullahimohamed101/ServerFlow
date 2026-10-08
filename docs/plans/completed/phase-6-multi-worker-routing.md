# Phase 6 — Multi-Worker Routing: Retries and Request Attempts

Status: Completed (implemented, reviewed once and verified; the second verification pass was cut off by a usage limit; merged with its PR)
Owner: coding agent
Depends on: Phase 5 (scheduler framework; PR #6). Phase 1 remains deferred (needs a GPU).
Spec: `docs/architecture/serverflow-spec.md` §7 (attempts), §15, §25 (retry policy), §28–29 (logs, metrics), §52, §58 Phase 6, §63
Hand-offs: ADR-003 ("error classification for retries arrives with Phase 6"), Phase 2 plan D5 and D7
(propagate `request_id` and `attempt_id` to workers; classify upstream errors for retry), Phase 5 plan
("retries and request attempts belong to Phase 6").

## Outcome

With three or more workers behind the gateway, a request that fails *before any output reaches the
client* is retried once on a different worker, every try is recorded as its own attempt, and a request
whose first worker was dead or refusing still succeeds. Once the first byte has been sent to the client
the gateway never retries: the stream ends with an error (spec §25). The registry still keeps unhealthy
and wrong-model workers out of every attempt, and the acceptance run proves it with 1000 synthetic requests.

```text
request req_1 ──► attempt att_1 ──► worker-02   FAILED  (connection refused / 503 / dropped before output)
              └─► attempt att_2 ──► worker-04   SUCCESS (different worker; same request_id)
```

Acceptance headline (spec): *1000 synthetic requests across 3+ workers: distribution by strategy is as
expected, a flaky or dead worker costs no client-visible failures while a healthy alternative exists,
nothing reaches an unhealthy or wrong-model worker, and every attempt is on record.*

## Non-Goals

- No circuit breaker, no passive ejection cooldown, no backpressure queues (spec §26–27; Phase 15). A
  dead worker can still cost one failed attempt per affected request until the registry notices
  (about `suspect_timeout`); the retry absorbs it.
- No retry after the first byte has been sent, ever; no transparent stream restart (spec §25).
- No more than `max_attempts` tries per request (default 2, the spec's number); no backoff sleeps,
  no hedged or parallel requests.
- No persisted attempt history (Postgres Phase 9, Kafka events Phase 12). Attempts live in logs,
  metrics and the request's in-memory record this phase.
- No change to the control plane, the agent, the scheduler strategies, or static mode.
- No benchmark harness (Phase 7); the 1000-request run is a test plus a short document.

## Current Architecture

- `handleChatCompletions` (`internal/gateway/handlers.go`) makes exactly one try: parse, `Route`, send
  the body with `up.Do`, and relay. `info.attemptID` is a single ID per request. In registry mode the
  response headers are written the moment the worker's headers arrive (`WriteHeader`, then `Flush` for SSE).
- The request body is already fully buffered (`body []byte`), so it can be replayed.
- `router.Route(ctx, req)` reserves a slot on the chosen worker; the handler defers `release`. The
  scheduler sees only the workers it is given; nothing excludes a worker that has just failed.
- `mapUpstreamError` already separates unreachable (503 `WORKER_UNAVAILABLE`), timeout (504) and other
  failures (502). Worker error responses before streaming are passed through (ADR-003).
- The mock worker can inject failures: `error` (500), `unavailable` (503), `drop` (connection closed with
  no response) and `midstream` (aborts after a few tokens), plus a full queue (503 `queue_full`). Phase 7's
  load generator is not needed to exercise them.
- Spec §7: a request can have several attempts, "do not overwrite attempt history"; §28: logs always
  carry `request_id` and `attempt_id`; §25: max 2 attempts, retry only on connection-before-generation
  failure, worker unavailable, or an explicitly retryable transport failure.

## Decisions (confirm before implementation)

- **D1 Scope: retries and attempts in registry mode only.** Static mode (one upstream) keeps its single
  try, byte for byte. A retry needs a second worker, which only registry mode has.
- **D2 What is retried** (all of it strictly before any byte reaches the client):
  (a) connection failures: refused, reset or closed before the response headers, and the dial guard's
  refusal of a worker address; (b) a worker response with status **502 or 503** (`unavailable`,
  `queue_full`, draining or starting; 504 was dropped in review because a worker-reported 504 means its
  backend already timed out); (c) a 200, stream or not, whose body ends or errors before its **first byte**. Everything else is final: client errors (4xx), a 500 from the worker (it can be a
  deterministic failure that every worker would repeat), a successful status, and any failure after the
  first byte. The status list is configurable (`gateway.retry_statuses`).
- **D3 Timeouts are not retried by default.** A worker that did not answer within
  `upstream_header_timeout` may still be generating; sending the same prompt elsewhere doubles GPU work
  and piles load onto an already slow worker. Left for Phase 15 with breakers. The client gets 504 as today.
- **D4 Streams: hold the response headers until the first body byte.** To keep "retry is allowed until
  output starts" true, the gateway reads the first chunk of an SSE body before it writes headers to the
  client. If the read fails or hits EOF with nothing, the attempt is retryable; if it yields bytes, headers
  and that chunk go out together and the attempt is final. This delays headers by at most the worker's time
  to first byte, which is when the client sees anything anyway, bounded by the idle timeout.
  Non-stream responses already carry their status in the headers, so they need no holding.
- **D5 `max_attempts`** (default 2, valid 1–5; 1 disables retries) at `gateway.max_attempts`. An attempt
  budget, not a time budget: each attempt keeps its own header and idle timeouts, so a request is bounded by
  `max_attempts` times those.
- **D6 A retry goes to a different worker.** The router gets an `exclude` set (workers already tried for
  this request) applied before the scheduler runs. If no other worker is selectable, the request does not
  wait: it finishes with the last attempt's outcome. The slot of the abandoned attempt is released as soon
  as the next attempt holds its own (or the request ends).
- **D7 When a retryable response cannot be retried, relay it.** On a retryable status the gateway first
  tries to route attempt 2; only if that succeeds is attempt 1's response discarded (its connection is cancelled and closed,
  not drained). If there is no second worker or attempts are used up, the worker's own response is relayed
  as today (ADR-003), so a request that fails everywhere shows the worker's real error.
- **D8 Attempt records.** A request owns an ordered list of attempts: `attempt_id`, `worker_id`,
  start, duration, outcome (`ok`, `retried`, `failed`, `client_closed`), and an error class (`connect`,
  `status_502`/`503`/`504`, `empty_stream`, `reset`, `other`). Each attempt logs one line when it ends
  (`attempt finished`) and the request line carries `attempts` and the final `worker_id`. History is never
  overwritten: the second attempt gets a new `attempt_id`; the request keeps one `request_id`.
- **D9 Propagate identity to workers.** Every attempt sends `X-Request-ID` (unchanged) and a new
  `X-Attempt-ID`, on the existing header allow-list; nothing else crosses. Workers may log them.
- **D10 Client-visible signals.** The response keeps `X-Request-ID`. Add `X-ServerFlow-Attempts`
  (the number of attempts used) only when more than one was needed, so a retry is visible to a client's
  support trail without changing the common case. No worker IDs are exposed to clients.
- **D11 Metrics.** `inference_attempts_total{model,outcome}` and `inference_retries_total{model,reason}`
  on the existing private Prometheus registry. Labels are bounded: `outcome` and `reason` are fixed enums,
  and `model` is only a registry-confirmed model (the Phase 5 rule).
- **D12 Slot accounting per attempt.** Each attempt reserves and releases its own slot through the router,
  so the in-flight overlay stays exact. The failing worker is excluded from this request only; no
  gateway-wide memory of failures (that is Phase 15).
- **D13 Client disconnects stop everything.** If the client's context ends, no further attempt starts, the
  current one is cancelled, and the request is logged as client-closed. Retries never run for a gone client.
- **D14 Acceptance harness.** A test drives 1000 synthetic requests through a real gateway in registry
  mode against mock workers (some with injected failures), for each strategy, asserting distribution,
  zero client failures while a healthy worker exists, zero requests to unhealthy or wrong-model workers, and
  a complete attempt record. A short `docs/benchmarks/phase-6-distribution.md` records the measured numbers.

## Proposed Design

**Attempt loop** (`internal/gateway/attempt.go`, new). The handler's single forward becomes
`s.forward(ctx, ireq, body, info)`, which loops up to `max_attempts`:

```text
tried := {}
for n := 1; n <= max; n++ {
    target, apiErr := router.Route(ctx, ireq, exclude=tried)     // n=1: nothing excluded
    ... first attempt failing to route is the existing 404/503 behaviour; later ones end the loop
    outcome := s.tryOnce(ctx, target, ...)                       // sends the body, reads headers (+ first chunk for SSE)
    switch outcome.kind {
      case relay:      write headers/body to the client and finish      // success, final error, or no retry possible
      case retryable:  tried[target.worker] = {}; record attempt; continue (after routing the next, see D7)
      case clientGone: record; return
    }
}
```

`tryOnce` returns either a response ready to relay (with the already-read first chunk glued in front of the
body for SSE) or a retryable failure with its class. The relay code (`pump`, `relayStream`, idle timer)
stays as it is and receives the glued reader. Everything that can fail after the first byte keeps today's
behaviour: abort or SSE error event, no retry.

**Router.** `Route(ctx, req, exclude map[string]struct{})` removes excluded workers from the view before
the overlay and the scheduler; the "no selectable worker" errors are unchanged. `release` is per attempt.

**Classification** (`internal/gateway/retry.go`, new): `classify(resp, err) (retryable bool, class string)`
with the D2 table, unit-tested exhaustively. Configurable list `retry_statuses` (default 502, 503).

**Config**: `gateway.max_attempts` (2), `gateway.retry_statuses` ([502,503,504]); env overrides in the existing
style; validated only for registry mode (static ignores them).

**Docs**: ADR-012 (retry policy and attempt records, documents §25 explicitly as the spec asks), a retries
section in `docs/architecture/scheduling.md`, README and ARCHITECTURE updates, the benchmark note.

## Affected Files / Components

New: `internal/gateway/attempt.go`, `internal/gateway/retry.go` (+tests), `tests/integration/distribution_test.go`,
ADR-012, `docs/benchmarks/phase-6-distribution.md`.
Changed: `internal/gateway/handlers.go` (single forward becomes the loop; static path untouched),
`router.go` (exclude set), `upstream.go` (`X-Attempt-ID`), `middleware.go` (attempt list in `reqInfo` and
the request log), `metrics.go` (two counters), `internal/config` (two settings), docs, README, ARCHITECTURE.

## Acceptance Criteria

1. Classification: connection refused, reset before headers, closed before headers, dial-guard refusal,
   502/503, and a 200 body that is empty or errors before its first byte are retryable; 2xx, 3xx,
   4xx, 500, a timeout before headers, and any failure after the first byte are not. Table-tested, including
   a configured status list.
2. A retry never uses the worker that just failed for the same request, and never exceeds `max_attempts`;
   with `max_attempts: 1` behaviour equals Phase 5.
3. Before-output failure recovers: with one of three workers refusing connections, answering 503, dropping
   the connection, or ending a stream before its first byte, the client still gets a 200 and the logs show two
   attempts with distinct `attempt_id`s and the same `request_id`.
4. After-output failure is not retried: a worker that dies after streaming its first chunk gives the client a
   stream ended with the error event, exactly one attempt, and no second worker contacted.
5. When no retry is possible (single worker, or all tried), the client gets the last worker's response
   unchanged (a 503 with its body) rather than a gateway-invented error, and slots are released.
6. Attempts are recorded: a request log line carries the attempt count and final worker; each attempt logs
   its own line with worker, duration, outcome and class; metrics counters move as expected with bounded labels.
7. Slot accounting: after any mix of retries, cancellations and failures the in-flight counters return to zero
   and no worker exceeds its `MaxConcurrency` as seen by this gateway.
8. Client disconnects at any point (before routing, during an attempt, between attempts) start no further
   attempts and are recorded as client-closed; no goroutine or slot leaks.
9. Streams: headers reach the client only with the first chunk; the first chunk is delivered intact and in
   order; TTFT is still measured; a stalled worker is cut by the idle timeout as before.
10. 1000-request distribution (D14), per strategy over 3+ workers and two models: round-robin splits evenly
    (within one request of equal), random and least-active within stated tolerances; all requests to a model
    reach only workers of that model; with one flaky worker, client failures are zero; with a worker killed
    mid-run, nothing reaches it after the registry/cache bounds; every request has its attempts on record.
11. Static mode is unchanged: existing gateway tests pass untouched, and the overhead test stays within budget.
12. Headers to workers: `X-Request-ID` and `X-Attempt-ID` only (plus the existing allow-list); the client's
    `Authorization` and any other client header never reach a worker. Worker IDs and addresses never appear
    in client responses.
13. `gofmt`, `go vet`, `go build`, `go test -race ./...`, `golangci-lint run ./...` pass; CI green; no new
    dependencies.

## Verification Plan

- Unit: the classifier table; router exclusion (including exclusion that leaves no worker); the attempt
  loop with a scripted fake upstream covering every outcome combination, budget exhaustion, and cancellation
  at each boundary; first-chunk holding with readers that fail, EOF, stall, or deliver one byte at a time.
- Gateway handler tests in registry mode with fake workers for criteria 3–9, 12, asserting on bodies, headers,
  logs, metrics and counters.
- Integration with real mock workers and real failure injection (`error`, `unavailable`, `drop`, `midstream`,
  queue full), in process and with the real binaries (kill -9 a backend mid-run).
- The 1000-request run for each strategy, repeated and under CPU load; assertions are exact where the
  strategy is deterministic (round-robin) and statistical with generous bounds otherwise.
- `-race -count=10`, mutation testing of the new tests (flip the retry table, the budget comparison, the
  exclusion, the "after first byte" boundary, the release paths), the overhead test, then independent
  `verify-change` (twice) and `review-change`, `harden-change`, `prepare-pr`, and stop for approval. The
  header allow-list and body replay get an explicit security look (AGENTS.md).

## Implementation Notes (deviations and additions)

- **The attempt ID reaches the worker through the request context**, not a new parameter, so the
  `Upstream` interface and its existing test doubles are unchanged; static mode sends no `X-Attempt-ID`.
- **`Route(ctx, req, exclude ...string)`** takes the exclusion as variadic strings, so existing callers
  are untouched. Excluding every worker answers `NO_CAPACITY`, never `MODEL_NOT_FOUND`.
- **The handler's response tail became `relay`**, shared by the static path (behaviour unchanged) and the
  attempt loop, which gives `relay` a body with the already-read first chunk put back in front.
- **An empty or failing stream that cannot be retried is a gateway error**, not an empty 200: nothing has
  been sent yet, so the client gets 502 (or 504 when the idle clock cut a stalled worker) instead of
  headers followed by nothing.
- **Retries take a turn in stateful strategies.** The retry's pick advances the round-robin position, so
  the worker after the failing one is picked twice as often and the failing worker is first choice for a
  third to a half of requests. Found by the process test; recorded in ADR-012 and the benchmark note rather
  than "fixed", since changing it needs a peek API on schedulers.
- **A fast-failing worker attracts `least-active` traffic** (613 and 732 of 750 first attempts in two
  runs). Recorded as the motivating case for Phase 15's breaker; no mitigation here (non-goal).
- With two bad workers out of three, two attempts can both fail; the process tests therefore use one bad
  worker per scenario.
- New process-test helper: `startMockProc` accepts extra flags; the integration load helpers live in
  `distribution_test.go`.

## Review round 1 and what changed

The review found no blockers; the attempt loop released every slot on every path. Changed in response:
the first-byte hold now covers every 200, not only SSE (a JSON 200 that died before any body byte used to be
a dropped connection); the default retry list is 502 and 503 (a worker-reported 504 means a backend
timeout, which ADR-012 refuses to repeat; still configurable); a worker's 1xx status is a gateway error;
tests for the JSON-200 case, large bodies, cancelling during the hold, a hard `MaxConcurrency` bound with
retries, a goroutine-leak check, and a killed-worker run that cannot pass without exercising retries; the
ADR now lists poison-request replay, retry amplification and the outcome vocabulary; small cleanups
(an unused parameter, an index pointer, test leftovers); the mock worker logs `attempt_id`. Not done: a
retry budget and a peek API for the rotation (Phase 15 and later), per the plan's non-goals.

## Independent verification and what changed

An independent verifier ran against the final code but was cut off by a usage limit before its closing
report; its saved notes cover most of the plan and are summarised here. A second full pass was not completed
and is listed as unverified in the PR.

Verified on real binaries: every retryable and non-retryable case in the retry table (503, 502, drop, reset,
stream or JSON dying before the first byte, empty bodies are retried; 400/404/429/500/504, mid-stream and
mid-body failures, a redirect to the metadata address, a 101, and a header bomb are not), timeouts not
retried, `max_attempts` 1 and 3, exhaustion relaying the real worker error, client disconnects at every
point starting no further attempt, hostile workers (endless 503 bodies, endless first chunks) neither
hanging the gateway nor leaking, headers and credentials never forwarded, 78 bodies from 1 byte to over 5 MiB
byte-exact around the 16 KiB boundary, 1800 concurrent mixed requests with no race, leak or capacity
overrun, bounded metric labels, and a differential run showing **static mode identical to master** across 17
upstream behaviours.

Findings and fixes: the 1000-request acceptance tests failed under heavy CPU load (6 of 10 runs) because
the shared test registry used 150 ms thresholds against 50 ms heartbeats, so a starved machine made healthy
workers look suspect; those tests now use relaxed registry thresholds (a dead worker is still noticed at
once, since its agent reports FAILED) and pass 10 of 10 under the same load. A stale comment still named
504 as a default; a worker status outside 200-599 now becomes a gateway error (it had made the status
metric label worker-chosen); `X-ServerFlow-Attempts` on gateway-generated errors, a double `release`, the
"never the same worker twice across three or more attempts" property, an empty 4xx body, and the 599
boundary now have tests that fail without them.

Known and unchanged: a single worker answering 503 with an endless body is relayed until the client leaves
(same as before Phase 5, a relay-path property); a legitimately empty 200 body is treated as a failed
attempt; `inference_requests_total` carries the final status, now bounded to 200-599.

## Risks

- **Duplicate work.** A retry repeats a prompt. Bounded by `max_attempts` and by D3 (no timeout retries);
  the cost is documented. A worker that returns 503 to everything makes every request that lands on it pay
  one extra attempt; Phase 15's breaker removes that.
- **Header delay for streams (D4).** Clients see response headers at first byte rather than at worker
  headers. For a real model this is the same moment the first token appears. Documented; tested.
- **Streaming correctness.** Gluing a pre-read chunk to the body is a place for subtle bugs (ordering, a
  chunk lost on retry, idle timer state). Covered by dedicated reader tests and mutation testing.
- **Retry amplification under overload.** If all workers return 503 because they are full, every request
  makes two attempts. The `queue_full` and capacity checks keep the second attempt from being a blind pile-on
  (it only runs if a different worker is selectable), but a global backpressure story is Phase 15.
- **Replaying bodies.** Bodies are replayed only to workers the registry vouches for, over the guarded
  transport, with the same header allow-list; no new credential exposure.
- **Statistical tests can flake.** Use exact assertions where possible and wide bounds elsewhere; run under
  load before declaring done.
- **Strategy claims.** The distribution document reports what was measured; it makes no claim that one
  strategy is better than another (spec §14, §63).

## Implementation Steps

1. Config: `max_attempts`, `retry_statuses`, validation, env overrides, tests. (Independent.)
2. `retry.go`: classification and tests. (Independent.)
3. Router: exclusion and tests.
4. Upstream: `X-Attempt-ID`; `reqInfo` attempt records; log lines; metrics counters and tests.
5. Attempt loop and first-chunk holding in the handler; keep the static path untouched; handler tests for
   criteria 2–9, 12.
6. Integration and process tests; the 1000-request distribution run and its document.
7. ADR-012, `scheduling.md` retries section, README, ARCHITECTURE; run the gates; independent verification
   and review; fix findings; second verification; harden; commit in small logical commits; prepare the PR
   and stop for approval.

Steps 1 and 2 can proceed independently; 3–5 build on them.
