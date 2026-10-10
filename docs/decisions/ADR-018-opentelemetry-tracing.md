# ADR-018: OpenTelemetry Tracing

Status: Accepted (Phase 11)
Date: 2026-10-10

## Context

A request crosses two processes (the gateway, then a worker) and, in registry mode, may be tried on more than one worker. Logs show
each process's side; nothing shows one request's whole life in one place, and nothing shows a retry as a retry. Spec section 28 says
logs always carry a `trace_id`, and none did. Phase 11 adds distributed tracing without touching how requests are served.

## Decision

- **Tracing is a second `gateway.Observer`** (`internal/tracing/gwtrace`), registered after metrics by `cmd/gateway` only when
  `tracing.enabled`. It is **off by default** and costs nothing when off: no observer is registered, no OpenTelemetry code runs on the
  request path, no header is added. OpenTelemetry is imported **only under `internal/tracing/...`**; a test fails if `internal/gateway`
  depends on `go.opentelemetry.io`, directly or transitively. `internal/tracing` (the core) imports no other internal package;
  `gwtrace` imports `internal/gateway` for its types and `internal/config` for the policy names; `internal/mockworker` and the commands import the core.
- **The seam needed only additive changes.** The prep work (ADR-016) already had context-returning `RequestStarted` and `AttemptStarted`,
  attempt events in static mode, `TraceHeaders`, `SelectDuration` and `NextWorkerUnavailable`. Phase 11 added `Admission.RateLimitStart` and
  `Rejection.DecisionStart` (so a `rate_limit` or failed `scheduler.select` span has its true start; `Rejection.DecisionDuration` now
  carries the selection time for capacity and model refusals instead of a stale limiter time), and drops a repeated `traceparent` or
  `tracestate` header instead of guessing. `gateway.TraceRef` is a context value the tracing observer sets and the gateway reads: only
  `traceparent` and `tracestate` from it are sent to the worker (an allow-list, like the attempt ID), and `trace_id` goes into the
  request, attempt-finished and worker-failure log lines. Without tracing no `TraceRef` exists and nothing changes.
- **Span model.** `gateway.receive` (SERVER) is the root. Its children are `rate_limit` (only when limiting is on), one `scheduler.select`
  per attempt (registry mode) and one `worker.forward` (CLIENT) per attempt. For streams `worker.forward` has `first_token` (attempt
  start to first chunk) and `completion` (first chunk to end). The worker adds `inference` (SERVER) and `queue_wait` under the
  `worker.forward` that called it. A retry is a **sibling** `worker.forward` under the root with a **link** to the failed attempt
  and a `retry` event on the failed one: the failed attempt does not "contain" the retry, the request does. A client leaving
  (499) is not an error status; a 5xx, or a stream that failed after its 200, is.
- **Incoming `traceparent` from clients: `link` by default.** The gateway is an internet-facing trust boundary. Continuing a client's
  trace would let any client force full recording for every request (sampling amplification), choose trace IDs and graft spans into
  someone else's trace, and send oversized or malformed `tracestate`. So by default the gateway starts a **new trace** and adds the
  client's IDs as a **link** (not its flags or tracestate). `ignore` adds no link. `trust` continues the client's trace and forwards its
  `tracestate`; it exists for a gateway behind a trusted proxy or mesh and is documented as unsafe elsewhere. Malformed headers are ignored. Baggage
  is never read or sent; the only propagation format is W3C Trace Context.
- **Attributes are an allow-list in code** (`internal/tracing/attrs.go`), enforced by tests that export real spans from both processes
  and fail on any key not in the list, and by a canary test that plants a prompt, API key, `Authorization`, `User-Agent`,
  `X-Forwarded-For`, cookie, baggage, query string, client `tracestate` and a worker error body and searches the full dump. Never
  recorded: prompt or response text, any header value, the API key or its prefix or ID, the tenant name, client IP, raw URL or query,
  worker address, error strings (`err.Error()` can carry addresses and upstream bodies; only the bounded class or code is used).
  Every string is from a closed set, a validated token or ID, or cut to 128 bytes. The opaque tenant ID is recorded unless
  `tracing.include_tenant_id` is false. The resource carries only `service.name`, `service.version` and `service.instance.id`: the
  `OTEL_*` environment variables are not honoured (a test proves `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_RESOURCE_ATTRIBUTES`,
  `OTEL_TRACES_SAMPLER` and the span-limit variables have no effect), and no global tracer provider or propagator is set.
- **Dependencies: OpenTelemetry Go v1.46.0, pinned.** The SDK, the OTLP/HTTP exporter and W3C parsing are not worth hand-rolling.
  **v1.47.0 sets `go 1.26.0` and would raise the module floor from 1.25.0, so it must not be used**; Dependabot is told to ignore
  `go.opentelemetry.io/otel*` versions from 1.47.0 (`.github/dependabot.yml`), and the floor is raised only by a deliberate decision. The
  change adds about 13 indirect modules (grpc, genproto, protobuf bump, grpc-gateway, backoff, logr, uuid, proto/otlp, auto/sdk) and
  grows the binaries (measured in `docs/benchmarks/phase-11-tracing.md`).
- **Exporter: OTLP/HTTP (protobuf).** In a scratch build the gRPC exporter added almost nothing over HTTP (the HTTP exporter's proto
  package already links gRPC stubs), so weight does not decide it; HTTP is plain `net/http`, has no long-lived channel or reconnect logic, is
  trivial to fake in a test, and both a collector and Jaeger accept it on 4318. The export client ignores proxy variables and never follows redirects.
- **Head sampling, ratio 1.0 when enabled.** `TraceIDRatioBased`; with `incoming: trust` it is wrapped in `ParentBased`. Head sampling
  cannot keep "all error traces" because the decision precedes the outcome; for that use tail sampling in a collector (documented, not
  built here). Unsampled requests still get valid IDs, so `trace_id` is in every log line and the worker gets `traceparent` with flags
  `00`. The mock worker's own sampler is `ParentBased(never)`: it never starts a recorded trace of its own and records only under a sampled
  remote parent. That does **not** stop a caller who can reach the worker directly: a request with a valid sampled `traceparent` makes
  the mock worker record `inference` and `queue_wait` spans (an independent verifier did exactly this). The harm is bounded by the export
  queue (2048 spans, then drops) and it affects the mock worker only, but the worker must not be exposed to untrusted callers. A guard (for
  example recording only when a shared secret header is present) was considered and not added: the mock worker is a development tool
  that already trusts its callers, and a real worker (Phase 13) must make its own decision.
- **A dead collector never slows or fails a request.** A bounded queue (default 2048 spans), a single export goroutine, batches of up to
  512 every 5 s, a 5 s export timeout that also caps retries. `OnEnd` never blocks: a full queue drops the span and counts it. The SDK's
  `BatchSpanProcessor` was not used because it does not report drops; the replacement is about 150 lines (`internal/tracing/processor.go`)
  with exact accounting (`ended = exported + failed + dropped + queued`, tested). Starting with the collector down is not an error.
  Spans in a batch the collector rejects are counted in neither the exported nor the dropped counter: they appear as one increment of
  `tracing_export_failures_total` (batches), and the identity `ended = exported + failed + dropped + queued` holds only when the
  pipeline is at rest (spans are in flight in between). Export errors are logged as a short class (`timeout`, `connection_refused`, `export_error`, never the error text, which can name
  the collector) once, then at most once a minute, plus one recovery line. Counters `tracing_spans_exported_total`,
  `tracing_spans_dropped_total` and `tracing_export_failures_total` (failed batches) exist on `/metrics` only when tracing is enabled
  (a new gateway file, `tracing_metrics.go`; Phase 10's metrics code is untouched). Shutdown flushes for at most 5 s (the context given to
  `Shutdown`), so a dead collector delays exit by up to 5 s; orchestration drain timeouts must allow for it.
- **Configuration** is the `tracing` section (`internal/config/tracing.go`; env `SERVERFLOW_TRACING_ENABLED|ENDPOINT|SAMPLE_RATIO|INCOMING|INCLUDE_TENANT_ID`).
  The endpoint must be an http(s) URL without credentials, query or fragment; plaintext to a non-loopback host is refused unless
  `allow_insecure_transport`; validation errors never echo the endpoint, and the config prints it redacted. With `enabled: false` nothing
  else is validated. The mock worker has `--otlp-endpoint` (and `--trace-insecure-ok`) instead of a config file.

## Known vulnerability exception (golang.org/x/net)

`govulncheck` (v1.8.0) on this branch reports GO-2026-6617, 6612, 6611, 6610 and 6603 in `golang.org/x/net@v0.58.0`, fixed in v0.60.0. v0.59.0
and v0.60.0 declare `go 1.26.0`, so taking them would raise the Go floor (the same reason OpenTelemetry v1.47 is refused), and no older
release carries the fixes. `google.golang.org/grpc` was bumped to v1.83.2 (GO-2026-6443), which builds on Go 1.25. The facts, checked:
`golang.org/x/net/http2` is linked only through `google.golang.org/grpc/internal/transport`, which the OTLP exporter package pulls in for
its shared (gRPC-oriented) option types; nothing in this repository or in the HTTP exporter creates a gRPC client or server, and the
exporter sends with `net/http`. In source mode govulncheck still lists these as reachable, through over-approximated call traces
(for example `sync.Once.Do` or `fmt.Fprintln` "eventually" calling `http2.Framer` methods, and `http.Client.Do` calling the
transport); I have not proved from the code that none of those paths runs, only that no gRPC or x/net HTTP/2 server or client is
constructed. The same five advisories also apply to the standard library's own `net/http` HTTP/2 (fixed in the Go 1.27.2 toolchain), which the
gateway links on master too; the gateway serves plain HTTP and Go serves HTTP/2 only over TLS. Decision (the owner chose this): the five advisories are accepted through an **enforced exception list**, not ignored.
`scripts/quality.sh vuln` runs `govulncheck -format json` and pipes it to `scripts/vulnfilter`, which fails on every called
vulnerability except those listed in the marked `VULN_EXCEPTIONS` block of `quality.sh`. An exception names the module **and** the OSV id
(`golang.org/x/net:GO-2026-6617`, and so on), so the same id in another module, or another advisory in `x/net`, still fails; standard
library findings are not silenced either (CI runs the stable toolchain, which carries the stdlib fixes; on this machine's Go 1.27.1 the
stdlib findings fail the check, and with Go 1.27.2 it passes). Every run prints the exception list as a NOTICE, prints each excepted
finding, and flags an exception that matched nothing. The block must be **deleted when the Go floor is raised to 1.26 and
`golang.org/x/net` v0.60.0 or later is taken** (a separate, approved change). `scripts/vulnfilter/main_test.go` proves, with canned
govulncheck JSON: only excepted ids pass with the notice; an excepted id in another module fails; an unrelated new id fails; stdlib
findings fail; empty findings pass; invalid JSON fails. The plan's criterion 14 is therefore **met with a documented, enforced
exception**, not "clean".

## Consequences

- One request can be inspected as one trace across two processes, with request, attempt and worker IDs on the spans; a retried request
  shows two worker attempts under one request, the second linked to the first.
- Cost when on: measured in `docs/benchmarks/phase-11-tracing.md` (about 9 to 12 microseconds and 50 to 80 allocations per request in
  process; about 0.1 ms at p95 over HTTP in the local run). Off: nothing.
- The agent and control plane have no spans (neither is on a request path), nor do Postgres, Redis or auth lookups; hand-written spans
  only, so every attribute is chosen.
- A worker that is not the mock worker must propagate `traceparent` and record its own spans to appear in the trace (Phase 13).
- Clocks: each process stamps its own wall-clock time, so a worker clock ahead of the gateway can show a child starting before its
  parent. Not corrected.
- Phase 16 moves the Jaeger file into the root compose behind the existing collector; the only gateway-side change is
  `tracing.endpoint`.
- Alternatives rejected: an optional side interface for propagation (it splits one lifecycle moment across two calls and hides the
  dependency); continuing client traces by default (sampling amplification and trace grafting); a local "record all, export errors"
  processor (the worker would already have exported its half, so traces would be inconsistent across processes); auto-instrumentation
  libraries (`otelhttp`, `otelpgx`, `redisotel`) because they choose attributes for us; the SDK batch processor (no drop count).
