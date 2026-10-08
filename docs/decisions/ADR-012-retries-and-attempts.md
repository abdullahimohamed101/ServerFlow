# ADR-012: Retry Policy and Request Attempts

Status: Accepted
Date: 2026-10-07

## Context

With several workers behind the gateway, one worker's failure should not fail a request that another
worker could serve. Spec section 25 sets the rules (at most 2 attempts; retry only when nothing has
been produced yet; never restart a stream once output began) and asks that the policy be documented
explicitly. Spec section 7 asks that every try is kept as its own attempt and history is never
overwritten. This ADR records both.

## The retry policy (spec section 25)

A request is retried on a **different worker** only while **nothing has been sent to the client**, and
only for failures that mean the worker produced no output:

| Retried | Why |
| --- | --- |
| connection refused, unreachable, dial timeout, or the dial guard refused the address | nothing was sent |
| connection reset or closed before response headers | the worker died or dropped the request before answering |
| worker response 502 or 503 (configurable: `gateway.retry_statuses`) | worker unavailable, queue full, draining, starting, or its sidecar could not reach its backend |
| a 200 (stream or not) whose body ends or fails before its **first byte** | no token was produced |

| Not retried | Why |
| --- | --- |
| 2xx, 3xx, 4xx | success, or the client's own error; another worker would repeat it |
| 504 (unless listed) | a worker answering 504 means its own backend already spent a full timeout, so a retry doubles exactly the slow work the next row refuses to repeat |
| 500 (unless listed) | can be a deterministic failure every worker would repeat, and a poisoned request would be doubled |
| a timeout waiting for response headers, or a stream stalled before its first byte | the worker may still be generating; a second copy of the prompt doubles GPU work and loads an already slow worker. Circuit breakers (Phase 15) are the better answer |
| anything after the first byte reached the client | the stream ends with an error event, exactly as before (never a transparent restart) |

`gateway.max_attempts` (default 2, range 1 to 5; 1 turns retries off) is an attempt budget, not a time
budget: each attempt keeps its own header and idle timeouts. Retries apply in registry mode only; static
mode (one upstream) keeps its single try.

## Decisions

- **Different worker every time.** The router takes an exclusion set (workers already tried for this
  request) before the scheduler runs. If nothing else is selectable the request does not wait.
- **A retryable response is held until the next worker is secured.** The gateway routes attempt 2 first
  and only then drops attempt 1's response. If there is no second worker or the budget is spent, the
  worker's own response is relayed (ADR-003), so a request that fails everywhere shows the real error.
  Only when the failure was a transport error or an empty stream does the client get a gateway error.
- **Responses: headers wait for the first body byte.** To keep "retry until output starts" true, the
  gateway reads the first chunk of any 200 body (stream or not) before writing anything to the client,
  bounded by the idle timeout, and streams the rest through unbuffered. A response that fails before then is
  retried; one that yields bytes is final. Clients see headers at the first byte instead of at worker
  headers, which is when they see anything useful anyway. A reverse proxy or browser in front of the gateway
  therefore sees nothing during a long prefill, as it would with most inference servers. A worker answering
  with an informational status (1xx, such as 101) or one outside 200-599 is a gateway error and is never
  relayed (which also keeps the status metric label bounded). A 200 with a legitimately empty body is
  treated as a failed attempt: a chat completion always has a body.
- **Attempt records.** Each request owns an append-only list of attempts (`attempt_id`, worker, start,
  duration, outcome `ok`/`failed`/`retried`/`client_closed`, and an error class `connect`, `reset`,
  `empty_stream`, `status_NNN`). Each attempt logs one line when it ends; the request line carries the
  attempt count and the final worker. A retry has a new `attempt_id` and the same `request_id`.
- **Identity to workers.** Every attempt sends `X-Request-ID` and a gateway-minted `X-Attempt-ID`. A client
  cannot choose either; no other client header crosses.
- **Client signal.** `X-ServerFlow-Attempts: N` is sent only when more than one attempt was used. Worker
  IDs and addresses are never exposed to clients.
- **Metrics.** `inference_attempts_total{model,outcome}` and `inference_retries_total{model,reason}`, with
  fixed value sets and a registry-confirmed model label only.
- **A slot is held while waiting for the first byte.** An attempt reserves its worker slot before sending, so
  requests stalled before their first byte occupy slots until the idle timeout cuts them (120 s by default).
  Enough slow requests can saturate a worker, exactly as with any slow stream; they are not retried (see
  timeouts above). Worst case, one attempt can take the header timeout plus the idle timeout, and the budget is
  attempts, not time.
- **Slots per attempt.** Each attempt reserves and releases its own slot through the router, so the
  in-flight overlay stays exact. A failure is remembered for that request only, never gateway-wide.
- **A client that leaves stops everything:** no further attempt starts and the request is client-closed.

## Alternatives

- **Retry timeouts too**: more requests recover, but doubles work on slow workers and hammers them.
- **Retry any 5xx**: simpler, but a deterministic 500 would be repeated and doubled.
- **Hedged (parallel) requests**: lower tail latency, but doubles load always; not in scope.
- **Send headers at once and retry only before the first byte of body**: impossible; once headers are
  sent the status is committed.
- **A gateway-wide cooldown for failing workers**: better than retrying the same dead worker per request,
  but it is a circuit breaker (spec section 26, Phase 15).

## Consequences

- **Positive**: a dead, draining, full or flaky worker costs no client failures while a healthy
  alternative exists; every try is on record; the acceptance run shows zero failures in 1000 requests
  with a flaky worker and with a worker killed mid-run (`docs/benchmarks/phase-6-distribution.md`).
- **Negative, measured**:
  - *Two bad workers out of three can still fail a request* (both attempts land on them). The budget is
    the spec's two.
  - *Retries consume a turn in stateful strategies.* A retry advances the round-robin position, so the
    worker after the failing one gets its own turn and the retry, and the failing worker is first choice
    for more than its share (a third to a half of requests with one flaky worker of three or four).
  - *A fast-failing worker looks idle.* It reports no active requests, so `least-active` and
    `least-queue` can prefer it: in the acceptance runs one always-503 worker received 613 and 732 of the 750
    `qwen-7b` first attempts under `least-active`, against about 220 to 250 under the other strategies. Retries kept every
    request successful, but each paid an extra attempt. This is the case Phase 15's circuit breaker
    exists for. No strategy is claimed better than another from this.
  - *A prompt that kills workers is replayed.* A request that crashes a backend produces a reset or an
    empty body, which is retryable, so it can take a second worker down. The blast radius is bounded by
    `max_attempts` per request, and there is no gateway-wide memory of poison requests (Phase 15).
  - *Retries multiply load in an outage.* If every worker answers 503 because all are full, each request makes
    up to `max_attempts` attempts whenever a different worker is selectable. The default of 2 doubles it;
    the configurable ceiling of 5 is a fivefold multiplier and should be used with care. A retry budget
    (for example, retries limited to a share of recent requests) belongs with backpressure in Phase 15.
  - *Attempt outcomes describe the gateway's work, not the client's success:* a relayed worker 4xx
    (including 429) counts as `ok` in `inference_attempts_total`.
  - *A retry repeats the prompt* (GPU and token cost), bounded by `max_attempts`.
- **Follow-ups**: circuit breakers, ejection cooldowns and backpressure (Phase 15); persisted attempt
  history (Phase 9 and Kafka events in Phase 12); tracing spans per attempt (Phase 11).
