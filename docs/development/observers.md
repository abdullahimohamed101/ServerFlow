# Adding a gateway observer

An observer watches what happens to an inference request without being part of the request path. Metrics is the first one
(`internal/gateway/metrics.go`); tracing (Phase 11) and event publishing (Phase 12) are written the same way. The reasoning is in
ADR-016.

## The interface

`gateway.Observer` (`internal/gateway/observer.go`) has one method per moment, each with a small value struct from
`observer_events.go`:

| Method | Fires when |
| --- | --- |
| `RequestStarted(ctx, RequestStart) ctx` | the middleware accepted `POST /v1/chat/completions` (before authentication) |
| `RequestAdmitted(ctx, Admission)` | authentication, validation and the rate limit passed |
| `RequestRejected(ctx, Rejection)` | refused before a worker was used (auth, rate_limit, validation, model, capacity, internal) |
| `AttemptStarted(ctx, AttemptStart) ctx` | a worker was chosen (static mode: the upstream is about to be called; worker ID empty) |
| `FirstToken(ctx, FirstToken)` | the first streamed chunk was written to the client |
| `AttemptEnded(ctx, AttemptEnd)` | an attempt finished (outcome ok, failed, retried or client_closed) |
| `RequestCompleted(ctx, Completion)` | the final status is decided; exactly once, last |

Typical sequences: success is started, admitted, attempt started, (first token), attempt ended, completed. A refusal is started,
rejected, completed (with admitted before the rejection for the registry-mode model and capacity refusals). A retry starts the second
attempt before ending the first. The tests in `internal/gateway/observer_test.go` pin every sequence.

## Writing one

1. Embed `gateway.NopObserver` and override only the moments you need.
2. Be fast. Methods run on the request goroutine. Anything slow (the network, a full queue) must be buffered and dropped inside the
   observer, never awaited. The gateway does not protect requests from a slow observer.
3. Be concurrency safe; many requests call you at once.
4. Do not panic. If you do, the gateway recovers, logs it at most once per minute per observer, and runs the other observers.
5. In registry mode `Admission.Model` is client-controlled text (at most 128 characters) that routing has not yet confirmed. Bound or validate it before using it as a metric label or span attribute; `Completion.Model` is the confirmed value (empty if unconfirmed).
6. Use only the event fields. They hold plain values; there are no bodies, prompts or API keys, and none must be added.
7. `RequestStarted` and `AttemptStarted` may return a context derived from the one given (for example carrying a span). The gateway
   uses the first for the rest of the request and the second for that attempt, including the upstream call. Return the given context
   if you have nothing to add; never return a context that is not derived from it.

```go
type counter struct{ gateway.NopObserver; n atomic.Int64 }

func (c *counter) RequestCompleted(context.Context, gateway.Completion) { c.n.Add(1) }

srv := gateway.New(cfg.Gateway, log, gateway.WithObserver(&counter{}))
```

`WithObserver` is an `Option`, applied before the server serves; observers run in registration order after metrics, and each sees
the context returned by those before it.

## Testing one

Copy the recording observer in `observer_test.go` (`recObserver`) to assert the events your observer should see, and run
`go test -race ./internal/gateway`. To check the cost of the seam: `go test -run '^$' -bench BenchmarkObserverRequest -benchmem ./internal/gateway`.
If your observer exports Prometheus series, extend `testdata/metrics_series_*.golden` deliberately
(`go test ./internal/gateway -run MetricsSeriesGolden -update-golden`) and say so in the change.
