# ADR-016: A Request-Lifecycle Observer Seam, and a Config Package Split per Feature

Status: Accepted (prep for Phases 10-12)
Date: 2026-10-09

## Context

Phases 10 (Prometheus), 11 (OpenTelemetry) and 12 (Kafka) all need to know what happens to a request: when it was accepted,
admitted or refused, which worker each attempt used, when the first token left, and how it ended. Before this change the gateway
reached into a concrete `metrics` value from five files (`middleware.go`, `handlers.go`, `attempt.go`, `auth.go`, `ratelimit.go`).
Adding tracing and events the same way would have meant three kinds of instrumentation interleaved through the request path, and
three phases editing the same lines. `internal/config/config.go` had the same problem: 766 lines holding every feature's types and
validation, which three phases adding `metrics`, `tracing` and `events` sections would all have edited.

## Decision

- **One narrow interface, `gateway.Observer`, with one method per lifecycle moment.** `RequestStarted`, `RequestAdmitted`,
  `RequestRejected`, `AttemptStarted`, `FirstToken`, `AttemptEnded`, `RequestCompleted`. Each takes the request `context.Context` and a
  small immutable value struct (`observer_events.go`). The structs hold plain values only (strings, numbers, durations, times), never a
  pointer into `reqInfo`, a body, a prompt or an API key, so an observer can neither change how a request is served nor leak a secret.
- **The call sequence is part of the contract** (`docs/development/observers.md`):

  | Situation | Sequence |
  | --- | --- |
  | success | `RequestStarted`, `RequestAdmitted`, `AttemptStarted`, [`FirstToken` if streamed], `AttemptEnded`, `RequestCompleted` |
  | refused by authentication | `RequestStarted`, `RequestRejected(auth)`, `RequestCompleted` |
  | refused by validation, an unknown model (static mode), or a rate limit | `RequestStarted`, `RequestRejected(validation, model or rate_limit)`, `RequestCompleted` |
  | unknown model, or no capacity (registry mode) | `RequestStarted`, `RequestAdmitted`, `RequestRejected(model or capacity)`, `RequestCompleted` |
  | retry on a second worker | `... AttemptStarted(1), AttemptStarted(2), AttemptEnded(1, WillRetry), AttemptEnded(2) ...` |
  | client disconnect | as a success, with `AttemptEnded(client_closed)` and `RequestCompleted(499)` |
  | handler panic | `AttemptEnded(failed)` (static mode), `RequestCompleted(500)` |

  `RequestStarted` fires for inference requests only (`POST /v1/chat/completions`), before authentication, so an authentication
  refusal is observable. `RequestCompleted` fires exactly once, last. In registry mode the second attempt starts before the first is
  closed: ADR-012 finds the next worker before letting go of the failed response, and the events report that order truthfully.
- **Context threading.** `RequestStarted` and `AttemptStarted` return a `context.Context`. The gateway uses the first for the rest of
  the request and the second for that attempt, including the upstream call; `FirstToken` and `AttemptEnded` receive the attempt
  context. A tracing observer can therefore put its span in the context and propagate trace headers (Phase 11). `multiObserver` threads
  the context through the observers in order; an observer with nothing to add returns the context it was given.
- **Attempt events fire in static mode too**, with one attempt per request and an empty worker ID, so traces and events look the same
  in both modes. The Prometheus observer skips them: `inference_attempts_total` stays a registry-mode series.
- **Fan-out is a `multiObserver` built at server construction**: a slice and a loop of direct method calls, no reflection, no channels,
  no allocation beyond the value structs. Metrics is always registered first; `gateway.WithObserver(o)` appends more (a nil observer is
  ignored). Like every `Option` it is applied before the server serves.
- **Observers must be fast and must not panic.** Methods run synchronously on the request goroutine, so anything slow (Kafka, span
  export) buffers internally; the gateway does not hide a slow observer. `multiObserver` recovers a panicking observer, logs it at
  most once per observer per minute, and still runs the others. A nil context returned by an observer is ignored.
- **Metrics is the first observer**, behaviour-preserving. The `metrics` type implements `Observer`; the Prometheus instruments stayed
  where they were. `inference_requests_active` is driven by `RequestStarted`/`RequestCompleted`. A golden test
  (`metrics_golden_test.go`, generated from the pre-change code) pins every series name, type, help string, label names and histogram
  buckets for both modes with authentication and rate limiting configured.
- **Logging is not an observer.** Structured request logs are the contract of every earlier phase and tests assert on them; they may
  become one later.
- **The translation from `reqInfo` to events lives in one file** (`observer_translate.go`), so changing `reqInfo` touches one place.
- **`internal/config` is split into one file per feature** (`gateway.go`, `scheduler.go`, `worker.go`, `controlplane.go`,
  `admission.go`, `redis.go`, `ratelimit.go`, `postgres.go`, `auth.go`, `log.go`), each holding its struct, constants and validator;
  `config.go` keeps `Config`, `Default()` and `Validate()`. Moves only: no identifier, redaction method or error text changed. Phases 10-12
  add `metrics.go`, `tracing.go` and `events.go`.

## Consequences

- Phases 10-12 add an observer (and a config file) without editing the request path again. A change to the lifecycle itself, such as
  token counts (Phase 12) or a circuit breaker (Phase 20), adds a field or a moment here, once.
- Cost: allocations per in-process request are unchanged (101 non-streaming, 108 streaming; about 100 bytes more per request for the
  event copies), and the wall-clock difference is below the run-to-run spread (`docs/plans/completed/prep-lifecycle-observer-and-config-split.md`).
- `inference_requests_active` now also counts, for the instant it is being refused, a request that fails authentication; before, it
  started counting after authentication. It is the only observable difference.
- A panic inside an upstream call in registry mode still skips `AttemptEnded` (the attempt's cleanup is registered after the send
  loop; this predates the seam and also skips the worker slot release). The upstream is the standard HTTP client, which does not panic.
- The event set is deliberately small. Add a field only when a phase needs it; token counts arrive with Phase 12's proxy change.
- Alternatives rejected: a channel or goroutine per observer (allocation and ordering cost on the hot path, and it hides back
  pressure), passing `*reqInfo` (observers could mutate request state and every `reqInfo` change would become an API change), and
  making logging an observer now (it would have moved every log assertion in the test suite for no benefit).
