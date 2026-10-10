# Tracing (OpenTelemetry)

With tracing on, one request is one trace across the gateway and the worker: where it waited, which worker each attempt used, whether it
was retried, when the first token left and when it finished. Tracing is **off by default**. The reasoning is in ADR-018.

## Turning it on

```yaml
tracing:
  enabled: true
  endpoint: http://127.0.0.1:4318   # OTLP/HTTP URL; /v1/traces is added when the URL has no path
  sample_ratio: 1.0                 # 0-1, chosen at the gateway
  incoming: link                    # link | ignore | trust (below)
  include_tenant_id: true           # put the opaque tenant ID on spans
  queue_size: 2048                  # spans waiting to be exported; more are dropped
  max_export_batch: 512
  batch_timeout: 5s                 # how often a partial batch is sent
  export_timeout: 5s                # one export, retries included
  allow_insecure_transport: false   # permit plaintext http:// to a non-loopback endpoint
```

Environment: `SERVERFLOW_TRACING_ENABLED`, `SERVERFLOW_TRACING_ENDPOINT`, `SERVERFLOW_TRACING_SAMPLE_RATIO`,
`SERVERFLOW_TRACING_INCOMING`, `SERVERFLOW_TRACING_INCLUDE_TENANT_ID`. The endpoint must be an `http://` or `https://` URL with no
credentials, query or fragment, a valid port, and plaintext is refused for any host that is not loopback unless `allow_insecure_transport` is set.
Put a collector in front if the backend needs authentication: the gateway sends no auth headers. The standard `OTEL_*` variables are
**not** read.

The mock worker has no config file: `mock-worker --otlp-endpoint=http://127.0.0.1:4318` (and `--trace-insecure-ok` for plaintext to a
non-loopback host). A worker records spans only for requests whose `traceparent` is valid and marked sampled; it never starts a recorded trace itself. That is not a security control: anyone who can reach the mock worker directly can send a sampled `traceparent` and make it record (bounded by its export queue). Do not expose the mock worker to untrusted callers.

## What a trace looks like

```text
gateway.receive                      (SERVER)  one per request; status, error code, attempts, model
  rate_limit                                   only with rate_limit.mode: required
  scheduler.select  #1                         registry mode; strategy, chosen worker
  worker.forward    #1                (CLIENT) attempt 1: worker_id, attempt_id, outcome, class      [event: retry]
    inference                         (worker) joined through traceparent
      queue_wait
    first_token                                streams: attempt start to first chunk
    completion                                 streams: first chunk to end of relay
  scheduler.select  #2
  worker.forward    #2                (CLIENT) link -> #1 (serverflow.link=retry_of)
    inference ...
```

A retry is a sibling attempt under the request, not a child of the failed one. A client that disconnects ends the request with status
499 and no error; a 5xx, or a stream that failed after output began, is an error. The request ID (`X-Request-ID`) is on the root and
every gateway span, so Jaeger can search `serverflow.request_id=req_...`. Logs carry the same `trace_id` for every request, sampled or
not, once tracing is on.

`serverflow.ttft_ms` on the root is the request-level time to first token (from request accepted); `first_token` spans from the start of
the attempt. Both are shown so the difference (earlier attempts, selection) is visible.

## Incoming `traceparent`

| `incoming` | A client sends `traceparent` | Use |
| --- | --- | --- |
| `link` (default) | a new trace starts; the client's trace and span ID become a **link** on `gateway.receive` | any gateway reachable by clients |
| `ignore` | ignored | same, with no link |
| `trust` | the client's trace is **continued** (parent = client span, its sampling flag decides, `tracestate` is forwarded) | only behind a proxy or mesh that you trust to set it |

Do not use `trust` on an internet-facing gateway. Any client could then force every request to be recorded and exported (cost), pick trace
IDs and attach your spans to someone else's trace, and send oversized `tracestate`. Malformed, repeated or oversized trace headers are
ignored in every mode. `baggage` is never read or forwarded, and a client's `traceparent` is never passed to the worker as sent.

## What is recorded, and what never is

Recorded: HTTP method, the fixed route, status code, request / attempt / worker IDs, attempt number and outcome, a bounded failure class
(`connect`, `reset`, `empty_stream`, `status_NNN`), the confirmed model name, stream flag, scheduler strategy, rate limit outcome and limit
name, estimated token cost (a number), TTFT, and (unless `include_tenant_id: false`) the opaque tenant ID. On the worker: token counts,
queue wait, the injected failure mode. The complete list is the constants in `internal/tracing/attrs.go`; a test fails if a span uses any
other key.

Never recorded: prompts or responses, any header value (`Authorization`, `User-Agent`, `Cookie`, `X-Forwarded-For`), the API key or any
part of it, the key ID, the tenant name, client addresses, URLs or query strings, worker addresses, error text, the client's `tracestate`.

## When the collector is down

Requests are not affected: spans go to a bounded queue; when it is full, spans are dropped. Exporting is retried within `export_timeout`,
then the batch is counted as failed. The gateway logs the first failure, then at most one line per minute while it continues, then one
line when exporting works again. The log line names a reason (`timeout`, `connection_refused`, `export_error`), never the address.

| Series (only when tracing is enabled) | Meaning |
| --- | --- |
| `tracing_spans_exported_total` | spans the collector accepted |
| `tracing_spans_dropped_total` | spans lost before export: queue full, or arrived during shutdown |
| `tracing_export_failures_total` | batches the collector did not accept. The spans in such a batch are counted in neither of the two series above (they are lost), and the exact identity ended = exported + failed + dropped + queued holds only at rest |

Growing `dropped` or `failures` means traces are being lost. At exit the gateway flushes the queue for up to **5 seconds**; with a dead
collector it therefore needs that long to stop, so orchestration drain timeouts must allow for it.

## Sampling

Head sampling at the gateway: `sample_ratio` of requests are recorded, in both processes (the worker follows the gateway's flag). It
cannot keep "only the failed requests", because the decision is made before the outcome is known. To keep all errors and a sample of the
rest, record everything (`1.0`) and let a collector's `tail_sampling` processor decide; that is a collector configuration, not done here.
An unsampled request is still given trace and span IDs, so its `trace_id` is in the logs.

## Trying it locally

```bash
make dev-tracing          # Jaeger v2 in Docker: OTLP/HTTP on 127.0.0.1:14318, UI on http://127.0.0.1:16686
scripts/trace-demo.sh     # a small cluster with one failing worker; prints the trace tree of a retried request
make dev-tracing-stop
```

`TRACING_OTLP_PORT`, `TRACING_UI_PORT` and `TRACING_CONTAINER` change the ports and container name (and `TRACE_DEMO_OTLP_PORT`,
`TRACE_DEMO_UI_PORT` must match for the script). Jaeger keeps traces in memory only. The file is `observability/tracing/docker-compose.yml`;
Phase 16 moves it into the full compose behind the collector, and the gateway then only needs a new `tracing.endpoint`. Search the UI
for service `serverflow-gateway`, tag `serverflow.request_id`.

## Dependency note: OpenTelemetry v1.46 and the follow-up bump

OpenTelemetry Go is pinned at **v1.46.0**. Versions 1.47.0 and later declare `go 1.26.0`. That used to be above this module's Go floor; the floor is
now 1.26, so the bump is possible, but it is a separate follow-up pull request (it also takes `golang.org/x/net` v0.60.0 or later, deletes the
vulnerability exception below, and removes the `go.opentelemetry.io/otel*` ignore from `.github/dependabot.yml`). Until then Dependabot is told not to
propose v1.47+. Do not bump OpenTelemetry piecemeal: review the whole set of `go.opentelemetry.io/*` modules together.

## Known vulnerability exception

`govulncheck` reports five `golang.org/x/net@v0.58.0` advisories (GO-2026-6617, 6612, 6611, 6610, 6603) fixed in v0.60.0. They are accepted until the
follow-up pull request above takes v0.60.0. The affected HTTP/2 code is linked
through gRPC types the OTLP/HTTP exporter imports but does not use; govulncheck's source mode still marks them reachable through
over-approximated call traces, so "not reachable at runtime" is the expectation from the code structure, not something the tool confirms.
`scripts/quality.sh vuln` (and so the nightly job) accepts exactly these five ids for `golang.org/x/net` through an enforced, visible exception
list (the `VULN_EXCEPTIONS` block in `scripts/quality.sh`, filtered by `scripts/vulnfilter`) and fails on every other called vulnerability,
including the same id in a different module. The follow-up pull request deletes the block (ADR-018).

## Known limits

- Clocks are not corrected: a worker whose clock is ahead of the gateway's can show a child starting before its parent.
- The agent, control plane, Postgres, Redis and authentication have no spans; they are not on a request path or are better seen in metrics.
- A real inference worker must propagate `traceparent` itself to appear in the trace (Phase 13).
