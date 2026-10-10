package mockworker

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"serverflow/internal/tracing"
)

// traceState is the tracing of one chat request. A nil *traceState is "tracing off": every method is then a
// no-op, so the handler calls them unconditionally.
type traceState struct {
	tracer trace.Tracer
	span   trace.Span
	ctx    context.Context
	queue  trace.Span
}

// beginTrace starts the inference span when tracing is on and the caller's traceparent is valid and sampled.
// The worker's sampler records only under a sampled remote parent, so a caller that reaches the worker
// directly, or an unsampled request, costs almost nothing and cannot make the worker record. The trace ID is
// still logged for any valid traceparent. Only traceparent is read; the request and attempt IDs are checked
// before they become attributes because a direct caller could send anything.
func (s *Server) beginTrace(r *http.Request, rl *chatLog, start time.Time) *traceState {
	if s.tracing == nil {
		return nil
	}
	sc := tracing.RemoteSpanContext(r.Header, false)
	if !sc.IsValid() {
		return nil
	}
	rl.traceID = sc.TraceID().String()
	ctx, span := s.tracing.Tracer().Start(trace.ContextWithRemoteSpanContext(r.Context(), sc), tracing.SpanInference,
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(start))
	if !span.IsRecording() {
		return nil
	}
	if id := validID(rl.requestID, "req_"); id != "" {
		span.SetAttributes(tracing.KeyRequestID.String(id))
	}
	if id := validID(rl.attemptID, "att_"); id != "" {
		span.SetAttributes(tracing.KeyAttemptID.String(id))
	}
	span.SetAttributes(tracing.KeyWorkerID.String(s.cfg.EffectiveWorkerID()))
	return &traceState{tracer: s.tracing.Tracer(), span: span, ctx: ctx}
}

// validID returns id if it is prefix followed by 16 lower-case hex digits, else "".
func validID(id, prefix string) string {
	if len(id) != len(prefix)+16 || id[:len(prefix)] != prefix {
		return ""
	}
	for i := len(prefix); i < len(id); i++ {
		c := id[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ""
		}
	}
	return id
}

func (t *traceState) queueStart(at time.Time) {
	if t == nil {
		return
	}
	_, t.queue = t.tracer.Start(t.ctx, tracing.SpanQueueWait, trace.WithTimestamp(at))
}

func (t *traceState) queueEnd() {
	if t == nil || t.queue == nil {
		return
	}
	t.queue.End()
	t.queue = nil
}

// finish ends the inference span with what the request did.
func (t *traceState) finish(rl *chatLog) {
	if t == nil {
		return
	}
	t.queueEnd() // a panic between queueStart and queueEnd must not leave the span open
	sp := t.span
	outcome := "ok"
	switch rl.outcome {
	case "failed", "rejected":
		outcome = "failed"
		sp.SetStatus(codes.Error, rl.outcome)
	case "cancelled":
		outcome = "client_closed"
	}
	sp.SetAttributes(tracing.KeyAttemptOutcome.String(outcome), tracing.KeyQueueMillis.Int64(rl.queueMillis))
	if rl.model != "" {
		sp.SetAttributes(tracing.StringAttr(tracing.KeyModel, rl.model), tracing.KeyStream.Bool(rl.stream),
			tracing.KeyPromptTokens.Int(rl.promptTokens), tracing.KeyOutputTokens.Int(rl.outputTokens))
	}
	if rl.injected != "" {
		sp.SetAttributes(tracing.StringAttr(tracing.KeyInjectedFailure, rl.injected))
	}
	if !rl.firstTokenAt.IsZero() {
		sp.AddEvent(tracing.EventFirstToken, trace.WithTimestamp(rl.firstTokenAt), trace.WithAttributes(tracing.KeyTTFTMillis.Int64(rl.ttftMillis)))
	}
	sp.End()
}
