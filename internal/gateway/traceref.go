package gateway

import "context"

// TraceRef is the trace context of the span an observer started for a request or an attempt. The gateway
// never interprets it: an observer that traces sets it with WithTraceRef from RequestStarted and
// AttemptStarted, the gateway copies traceparent and tracestate (and only those) onto the call to the worker,
// and puts the trace ID in its log lines (spec section 28). Without tracing no TraceRef exists and nothing
// changes.
type TraceRef struct {
	TraceID     string // 32 hex characters
	Traceparent string // the W3C traceparent to send to the worker
	Tracestate  string // sent only when the observer decided to continue a trusted trace
}

type traceRefKey struct{}

// WithTraceRef returns ctx carrying r.
func WithTraceRef(ctx context.Context, r TraceRef) context.Context {
	return context.WithValue(ctx, traceRefKey{}, r)
}

// TraceRefFrom returns the TraceRef in ctx, or the zero value.
func TraceRefFrom(ctx context.Context) TraceRef {
	r, _ := ctx.Value(traceRefKey{}).(TraceRef)
	return r
}

// Header length bounds for what leaves the gateway: a traceparent is exactly 55 characters.
const (
	maxTraceparentBytes = 55
	maxTracestateBytes  = 512
)

// traceAttrs is the log attribute list for ctx: trace_id when the request is traced.
func traceAttrs(ctx context.Context) []any {
	if id := TraceRefFrom(ctx).TraceID; id != "" {
		return []any{"trace_id", id}
	}
	return nil
}
