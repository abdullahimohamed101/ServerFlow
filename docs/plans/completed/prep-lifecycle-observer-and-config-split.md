# Prep: request-lifecycle observer seam and per-feature config files

Status: Completed 2026-10-09 (decisions D1-D8 and amendments A1-A4 implemented; see Implementation Notes for deviations)
Owner: coding agent
Depends on: Phases 8 and 9 merged (they are)
Purpose: let Phases 10 (Prometheus), 11 (OpenTelemetry) and 12 (Kafka) be built in parallel without editing the same lines, and without sprinkling three kinds of instrumentation through the request path.
Spec: `docs/architecture/serverflow-spec.md` §6 (request lifecycle), §7 (IDs), §20 and §22 (events), §28-29 (observability)

## Outcome

1. The gateway reports what happens to a request through one narrow interface, `Observer`. Prometheus metrics become the first implementation. Tracing (Phase 11) and event publishing (Phase 12) are additional implementations, added without touching the request path again.
2. `internal/config/config.go` (766 lines, every feature's types and validation) is split into one file per feature, so three phases adding `metrics`, `tracing` and `events` config sections do not collide.

No behaviour change. Every existing test passes unmodified (tests that reach into `s.metrics` internals may change their access path, never their assertions), and `/metrics` output is byte-for-byte the same set of series.

## Non-Goals

- No new metrics, spans or events (those are Phases 10-12).
- No change to config keys, defaults, validation messages or environment variable names.
- No change to the request path's timing or ordering (the Phase 2 overhead budget and Phase 8 rate-limit measurements must still hold).
- No exported plugin system: the interface is internal to `serverflow/internal/gateway`.

## Current Architecture

- `internal/gateway/middleware.go` `withRequest` creates `reqInfo`, runs the handler, then calls `s.metrics.observe(...)` and `logRequest`.
- Other call sites reach into `s.metrics` directly: `observeAttempt` and `observeRetry` (`attempt.go`), `observeAuthReject` (`auth.go`), `observeRateReject`, `observeRateDecision` and bypass counting (`ratelimit.go`). Gauges (`active`) are touched in `handlers.go`.
- `reqInfo` already carries what an observer needs: request ID, attempt history (`attemptRecord`), model, stream, tenant, TTFT, error code, rate-limit outcome.
- `internal/config/config.go` holds `Config`, all section structs, `Default()`, `Validate()` and the per-feature validators; `load.go` holds file/env loading.

## Decisions (confirm before implementation)

- **D1. One interface, event-shaped, synchronous and cheap.** `Observer` has one method per lifecycle moment, each taking a small immutable value struct (not `*reqInfo`, so observers cannot mutate request state and `reqInfo` can change freely):

  | Method | Moment | Carries |
  | --- | --- | --- |
  | `RequestStarted(ctx, RequestStart)` | request accepted by the middleware (inference paths only) | request ID, method, path, time |
  | `RequestAdmitted(ctx, Admission)` | auth and rate limit passed, model validated | model, stream, tenant ID, estimated cost |
  | `RequestRejected(ctx, Rejection)` | refused before reaching a worker | kind (`auth`, `rate_limit`, `validation`, `model`, `capacity`), reason/limit, HTTP status, decision duration |
  | `AttemptStarted(ctx, AttemptStart)` | a worker was chosen | attempt ID and number, worker ID, model, scheduler strategy |
  | `FirstToken(ctx, FirstToken)` | first streamed chunk reached the client | attempt ID, TTFT |
  | `AttemptEnded(ctx, AttemptEnd)` | an attempt finished | attempt ID, worker ID, outcome, duration, whether a retry follows, failure class |
  | `RequestCompleted(ctx, Completion)` | final status decided (success, error, client closed) | status, error code, duration, attempts, TTFT, bytes if known |

  **Amendments after planning Phases 10-12 (A1-A4, approved):**
  - **A1.** `RequestStarted` and `AttemptStarted` return a `context.Context`; the gateway uses the returned context for the rest of that request (or attempt) and, for an attempt, for the upstream call. This lets a tracing observer put its span in the context (Phase 11) so trace headers can be propagated. Observers that do not need it return the context they were given. `multiObserver` threads the context through the observers in order.
  - **A2.** Attempt events (`AttemptStarted`, `FirstToken`, `AttemptEnded`) also fire in static (non-registry) mode, with one attempt per request and an empty worker ID. `FirstToken` and `AttemptEnded` receive the attempt context.
  - **A3.** Event fields the metrics, tracing and events observers need, added now so later phases do not reopen the interface: `RequestStart{ID, Method, Path, Time, TraceHeaders}` where `TraceHeaders` carries only `traceparent` and `tracestate`, each length-capped at 512 bytes and otherwise unvalidated; `Admission{..., RateLimitDuration}`; `Rejection{Kind, Reason, Status, DecisionDuration}` where kind `capacity` carries the reason; `AttemptStart{AttemptID, Number, WorkerID, Model, Strategy, SelectDuration, SinceRequestStart, WorkerState}`; `AttemptEnd{..., Class, WillRetry, NextWorkerUnavailable}`; `Completion{Status, ErrorCode, Duration, Attempts, TTFT}`. Token counts are NOT added here (Phase 12 adds them with the proxy-path change that produces them).
  - **A4.** ADR numbers: this prep takes ADR-016; Phase 10 ADR-017; Phase 11 ADR-018; Phase 12 ADR-019.

  Rationale: these seven are exactly the points the spec's lifecycle (§6) and Kafka event list (§20: received, routed, first_token, completed, failed) and trace span list (§28: gateway.receive, rate_limit, scheduler.select, worker.forward, inference, first_token, completion) need. `ctx` is the request context so a tracing observer can find its span.
- **D2. Fan-out through a `multiObserver` built once at server construction.** Observers are held in a slice; each call is a loop of direct method calls. No reflection, no channels, no allocation on the hot path beyond the value structs (passed by value; contexts are the only reference that crosses the seam). A nil/absent observer list is a no-op, not a nil check at every call site.
- **D3. Observers must not block and must not panic the request.** The contract is documented on the interface: methods return quickly (anything slow, such as Kafka, buffers internally), and `multiObserver` recovers a panicking observer, logs once per observer per minute, and continues with the others. A dedicated test pins this.
- **D4. Metrics is the first observer, behaviour-preserving.** The existing `metrics` type implements `Observer`; the existing `observe*` call sites are replaced by `Observer` calls, and the Prometheus instruments stay where they are. The gauge `inference_requests_active` is driven by `RequestStarted`/`RequestCompleted`. Series names, labels and buckets do not change; a test compares the `/metrics` series set before and after.
- **D5. Logging stays as it is.** `logRequest` is not an observer in this phase (structured logs are the contract of every earlier phase and tests assert on them). It may become one later.
- **D6. Observers are registered with a functional option** (`gateway.WithObserver(o)`) on `New`/`NewFromConfig`/`NewRegistry`, the same style as the existing `Option`s. Metrics is always registered first. With A1 the option list is applied in order and each observer sees the context returned by the ones before it.
- **D7. Config split by feature, mechanically.** `config.go` keeps `Config`, `Default()`, and `Validate()` (which calls the per-feature validators); each feature moves with its struct, defaults helper and validator into `gateway.go`, `scheduler.go`, `worker.go`, `controlplane.go`, `admission.go`, `redis.go`, `ratelimit.go`, `postgres.go`, `auth.go`, `log.go` (the exact set follows the existing structs). Exported identifiers, `String()`/`LogValue()` redaction methods and error text are untouched. `config_test.go` (694 lines) is split the same way only where a test clearly belongs to one feature; no assertion changes.
- **D8. New sections are reserved, not added.** The split leaves an obvious place (`metrics.go`, `tracing.go`, `events.go`) but adds none; Phases 10-12 each add theirs.

## Proposed Design

```go
// internal/gateway/observer.go
type Observer interface {
    RequestStarted(ctx context.Context, e RequestStart) context.Context
    RequestAdmitted(ctx context.Context, e Admission)
    RequestRejected(ctx context.Context, e Rejection)
    AttemptStarted(ctx context.Context, e AttemptStart) context.Context
    FirstToken(ctx context.Context, e FirstToken)
    AttemptEnded(ctx context.Context, e AttemptEnd)
    RequestCompleted(ctx context.Context, e Completion)
}
```

A `NopObserver` struct with empty methods lets implementations embed it and override only what they need (Phase 11 and 12 observers will not care about every moment). `multiObserver` lives in the same file. Event structs live in `observer_events.go` and contain only plain values (strings, ints, durations, times).

The middleware and handlers build the event structs at the places they already update `reqInfo`; the translation from `reqInfo` is in one small file so a future change to `reqInfo` touches one place.

## Affected Files / Components

- New: `internal/gateway/observer.go`, `observer_events.go`, `observer_test.go`; per-feature files under `internal/config/`.
- Changed: `middleware.go`, `handlers.go`, `attempt.go`, `auth.go`, `ratelimit.go`, `metrics.go`, `server.go` (option and wiring), `internal/config/config.go` (shrinks).
- Docs: `ARCHITECTURE.md` (observer seam paragraph), `docs/development/` note on adding an observer, ADR-016 (the seam), README phase table untouched.

## Acceptance Criteria

1. All existing tests pass; no assertion in an existing test is changed or removed.
2. The set of Prometheus series (name, labels, help, type) on `/metrics` is identical before and after, verified by a golden test generated from master.
3. A recording observer test proves the exact call sequence for: a success (streaming and non-streaming), an auth refusal, a rate-limit refusal, an unknown model, a retry on a second worker (two `AttemptStarted`/`AttemptEnded`, one `RequestCompleted`), a client disconnect (499), and a handler panic.
4. A panicking observer does not fail the request and does not stop other observers; a slow observer is documented as the observer's problem, not hidden.
5. Gateway overhead is not measurably worse: the Phase 2 overhead test still passes and the Phase 8 `overhead` benchmark moves by less than its run-to-run spread (document both numbers).
6. `config` package: file list matches D7, `go doc` output for exported identifiers is unchanged (compare before and after), and `git diff --stat` shows the split is moves plus the new files, with no logic edits (reviewed by diff with `--color-moved`).
7. `scripts/quality.sh full` passes; the CI-parity rules in AGENTS.md hold.

## Verification Plan

- Focused: `go test -race ./internal/gateway/... ./internal/config/...` repeatedly (count 5) for the new observer tests.
- Golden series test (acceptance 2) generated on master first, committed, then run on the branch.
- `go doc -all ./internal/config` diff before/after.
- Full gate: `scripts/quality.sh full` with both test servers (Postgres, Redis) running.
- Independent read-only verifier: re-derives the call-sequence table from the spec lifecycle and checks the code against it, then mutates three call sites (remove `FirstToken`, double-fire `RequestCompleted`, skip `RequestRejected` on a rate refusal) and confirms a test fails each time.

## Risks

- **Double counting or a missed event** while moving `observe*` calls: mitigated by the golden series test and the sequence tests.
- **Hot-path cost** of the loop and struct copies: expected nanoseconds; verified by the overhead tests, not assumed.
- **Event structs growing into `reqInfo` copies:** keep them small and add fields only when a phase needs one.
- **Merge conflicts with other work:** none open now; this lands before Phases 10-12 start.

## Implementation Steps

1. Generate the golden `/metrics` series test on master and commit it first.
2. Split `internal/config` mechanically; run the tests; commit.
3. Add `observer.go`, events, `multiObserver`, `NopObserver`, tests for the contract (D3).
4. Make `metrics` an `Observer`; replace direct calls one site at a time, running tests after each; commit per site group (middleware, auth, rate limit, attempts).
5. Add `WithObserver`; recording-observer sequence tests (acceptance 3).
6. Docs and ADR-016; overhead numbers; full gate; independent verification; fix round; PR.

## Implementation Notes

Implemented on branch `chore/lifecycle-observer-and-config-split`. Order followed the steps above.

**Evidence**

- Golden `/metrics` series test (`internal/gateway/metrics_golden_test.go`, `testdata/metrics_series_{static,registry}.golden`) was generated from the unchanged
  master code and committed first; it passes unchanged after the refactor. It compares name, type, help, label names and histogram buckets (Go runtime and
  process collectors excluded: they vary by host), driving success, streaming, unknown model, auth refusal, rate-limit refusal, bypass, unavailable, and in
  registry mode a retry on a second worker.
- Allocations per in-process request (`BenchmarkObserverRequest`, `-benchmem`, count 5, Apple M5 Pro): non-streaming 101 allocs/op before and after
  (27,958 B to 28,057 B), streaming 108 before and after (28,144 B to 28,243 B); ns/op 6.1-6.5 us before, 6.1-6.4 us after (inside run-to-run spread);
  two extra no-op observers: still 101 allocs/op. `TestGatewayOverhead` p95 overhead: non-stream 536 us before / 461 us after, stream TTFT 500 us before / 459 us after
  (budget 25 ms).
- `go doc -all ./internal/config` before and after (`/tmp/config-doc-before.txt`, `-after.txt`): the same lines; the only difference is that the `AuthModeOff/AuthModeRequired`
  constant block is listed first instead of last, because go doc orders blocks by file name and `auth.go` sorts before `gateway.go`. The code lines of the split files are, as a sorted
  multiset, identical to the old `config.go` apart from package/import lines.

- Phase 8 rate-limit overhead benchmark (`SERVERFLOW_BENCH_RATELIMIT=1 go test -run TestBenchmarkRateLimits ./tests/integration`, one run each, local Redis in Colima), master vs branch:
  no tenant p50/p95 331/918 us vs 327/888 us; tenant with no quotas 312/696 us vs 287/668 us; tenant with three quotas 815/1422 us vs 745/1128 us; Redis round trip added
  504/726 us vs 458/460 us. The branch is not slower; the differences are within the spread of single runs.

**Deviations and additions**

- Commit grouping: the interface, the metrics observer and the call-site changes are one commit (they cannot build separately); tests and docs are separate commits.
- Event fields beyond A3: `RequestID` on every event, `TenantID` on `Admission`/`Rejection`/`Completion`, `Admission.RateLimitChecked`/`RateLimitBypassed` (so metrics keeps
  `rate_limit_decision_seconds` and `rate_limit_bypassed_total` exactly), and `Completion.Handled` (so an authentication refusal, which `RequestCompleted` also reports, is not counted
  in `inference_requests_total` as before). Rejection kind `internal` was added for the fail-closed wiring errors (limiter or authenticator required but not wired).
- `RequestStarted` fires before authentication (so an auth refusal is observable); consequently `inference_requests_active` counts a request that is about to fail authentication for
  the instant it takes to refuse it. Nothing else observable changed.
- `RequestAdmitted` fires after the rate limit in both modes. In registry mode the model is only confirmed by routing, so an unknown model there produces started, admitted, rejected(model);
  `Admission.Model` is then the client's unconfirmed string, and `Completion.Model` stays empty (it is the metrics label).
- A retry's `AttemptStarted` precedes the previous attempt's `AttemptEnded` (ADR-012 finds the next worker before letting go of the failed response). The tests pin that order.
- Static-mode `AttemptEnded` is emitted from a deferred function that re-panics a handler panic unchanged after recording the attempt as failed. The metrics observer ignores attempt events
  with an empty worker ID, keeping `inference_attempts_total` a registry-mode series.
- Known gap (pre-existing, not changed): a panic raised inside the upstream call in registry mode skips `AttemptEnded` (and the worker slot release), because the attempt's cleanup is registered after the send loop.
- Config test split: `gateway_test.go` now holds the gateway, registry-source and retry tests; the rest of `config_test.go` stays (they exercise several features); the auth, Redis and rate-limit tests already lived in their own files. No assertion changed (34 tests before, 34 after).
- The independent read-only verifier of the Verification Plan was not run by the implementing agent. Instead the three mutations it names were applied by hand and each made the sequence tests fail:
  removing the `FirstToken` call, firing `RequestCompleted` twice, and skipping `RequestRejected` on a rate-limit refusal (all reverted).
