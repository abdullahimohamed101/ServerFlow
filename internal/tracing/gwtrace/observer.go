// Package gwtrace is the gateway's tracing observer (ADR-018): it turns the request lifecycle events of
// gateway.Observer into OpenTelemetry spans and gives each worker call a W3C traceparent. It is the only
// bridge between the gateway and OpenTelemetry; internal/gateway imports neither.
//
// Per-request state lives in the context the observer returns from RequestStarted, never in a map, so a
// missed event cannot leak it. Every method starts with a check that returns at once for a request that is
// not being recorded.
package gwtrace

import (
	"context"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/tracing"
)

// Options configure the observer.
type Options struct {
	// Incoming is config.TraceIncomingLink (default), TraceIncomingIgnore or TraceIncomingTrust.
	Incoming string
	// IncludeTenantID adds the opaque tenant ID to spans.
	IncludeTenantID bool
}

// Observer implements gateway.Observer.
type Observer struct {
	gateway.NopObserver
	tracer trace.Tracer
	opts   Options
}

// New returns the tracing observer for p. Register it with gateway.WithObserver.
func New(p *tracing.Provider, opts Options) *Observer {
	if opts.Incoming == "" {
		opts.Incoming = config.TraceIncomingLink
	}
	return &Observer{tracer: p.Tracer(), opts: opts}
}

const route = "/v1/chat/completions"

type (
	stateKey struct{}
	attKey   struct{}
)

// reqState is one traced request. The lock matters only if events ever arrive from two goroutines; today
// they do not, but the cost is nil and a missed ordering assumption must not become a data race.
type reqState struct {
	mu      sync.Mutex
	root    trace.Span
	rootCtx context.Context
	prev    trace.SpanContext // the previous attempt, for the retry link
	open    []*attState
	ended   bool
	state   string // tracestate to forward (trust mode only)
}

// attState is one attempt: the worker.forward span and, for streams, the completion span.
type attState struct {
	span       trace.Span
	ctx        context.Context
	number     int
	start      time.Time
	completion trace.Span
	done       bool
}

func stateFrom(ctx context.Context) *reqState {
	st, _ := ctx.Value(stateKey{}).(*reqState)
	return st
}

func attFrom(ctx context.Context) *attState {
	a, _ := ctx.Value(attKey{}).(*attState)
	return a
}

func traceparent(sc trace.SpanContext) string {
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-" + sc.TraceFlags().String()
}

func ref(sc trace.SpanContext, state string) gateway.TraceRef {
	return gateway.TraceRef{TraceID: sc.TraceID().String(), Traceparent: traceparent(sc), Tracestate: state}
}

// RequestStarted starts the root span.
func (o *Observer) RequestStarted(ctx context.Context, e gateway.RequestStart) context.Context {
	start := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithTimestamp(e.Time),
		trace.WithAttributes(
			tracing.KeyHTTPMethod.String(e.Method), tracing.KeyHTTPRoute.String(route), tracing.KeyRequestID.String(e.ID)),
	}
	state := ""
	sctx := ctx
	switch o.opts.Incoming {
	case config.TraceIncomingTrust:
		if sc := tracing.ParseTraceparent(e.TraceHeaders.Traceparent, e.TraceHeaders.Tracestate, true); sc.IsValid() {
			sctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	case config.TraceIncomingIgnore:
		start = append(start, trace.WithNewRoot())
	default: // link
		start = append(start, trace.WithNewRoot())
		if sc := tracing.ParseTraceparent(e.TraceHeaders.Traceparent, "", false); sc.IsValid() {
			// Only the IDs: the client's flags and tracestate are not copied.
			ids := trace.NewSpanContext(trace.SpanContextConfig{TraceID: sc.TraceID(), SpanID: sc.SpanID(), Remote: true})
			start = append(start, trace.WithLinks(trace.Link{
				SpanContext: ids, Attributes: []attribute.KeyValue{tracing.KeyLink.String(tracing.LinkClientTrace)},
			}))
		}
	}
	rctx, root := o.tracer.Start(sctx, tracing.SpanReceive, start...)
	sc := root.SpanContext()
	if o.opts.Incoming == config.TraceIncomingTrust {
		state = sc.TraceState().String()
	}
	// Derive from the caller's ctx, not from rctx: only the span and the references below are added.
	out := trace.ContextWithSpan(ctx, root)
	if !root.IsRecording() {
		// Unsampled: no state, no further work. The worker still gets a valid traceparent with flags 00.
		return gateway.WithTraceRef(out, ref(sc, state))
	}
	st := &reqState{root: root, rootCtx: rctx, state: state}
	out = context.WithValue(out, stateKey{}, st)
	return gateway.WithTraceRef(out, ref(sc, state))
}

// RequestAdmitted adds what became known once the request passed authentication and the rate limit.
func (o *Observer) RequestAdmitted(ctx context.Context, e gateway.Admission) {
	st := stateFrom(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.ended {
		return
	}
	st.root.SetAttributes(tracing.KeyStream.Bool(e.Stream))
	o.tenant(st.root, e.TenantID)
	if e.RateLimitChecked {
		outcome := "allowed"
		if e.RateLimitBypassed {
			outcome = "bypassed"
		}
		o.rateSpan(st, e.RateLimitStart, e.RateLimitDuration, outcome, "", e.EstimatedCost)
	}
}

// RequestRejected records a refusal. A rate limit refusal gets its rate_limit span, a capacity or model
// refusal that came from selection gets a failed scheduler.select span.
func (o *Observer) RequestRejected(ctx context.Context, e gateway.Rejection) {
	st := stateFrom(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.ended {
		return
	}
	st.root.SetAttributes(tracing.KeyRejectKind.String(pick(e.Kind, rejectKinds)))
	o.tenant(st.root, e.TenantID)
	switch e.Kind {
	case gateway.RejectRateLimit:
		outcome := "rejected"
		if e.Reason == "unavailable" {
			outcome = "unavailable"
		}
		o.rateSpan(st, e.DecisionStart, e.DecisionDuration, outcome, pick(e.Reason, rateLimits), 0)
	case gateway.RejectCapacity, gateway.RejectModel:
		if !e.DecisionStart.IsZero() {
			_, sp := o.tracer.Start(st.rootCtx, tracing.SpanSelect, trace.WithTimestamp(e.DecisionStart), trace.WithAttributes(
				tracing.KeyRequestID.String(e.RequestID), tracing.KeySelectOutcome.String(pick(e.Reason, selectOutcomes))))
			sp.SetStatus(codes.Error, pick(e.Reason, selectOutcomes))
			sp.End(trace.WithTimestamp(e.DecisionStart.Add(e.DecisionDuration)))
		}
	}
}

func (o *Observer) rateSpan(st *reqState, start time.Time, d time.Duration, outcome, limit string, cost int) {
	if start.IsZero() {
		start = time.Now().Add(-d)
	}
	attrs := []attribute.KeyValue{tracing.KeyRateOutcome.String(outcome)}
	if limit != "" {
		attrs = append(attrs, tracing.KeyRateLimit.String(limit))
	}
	if cost > 0 {
		attrs = append(attrs, tracing.KeyEstCost.Int(cost))
	}
	_, sp := o.tracer.Start(st.rootCtx, tracing.SpanRateLimit, trace.WithTimestamp(start), trace.WithAttributes(attrs...))
	if outcome == "unavailable" {
		sp.SetStatus(codes.Error, "unavailable")
	}
	sp.End(trace.WithTimestamp(start.Add(d)))
}

func (o *Observer) tenant(sp trace.Span, id string) {
	if o.opts.IncludeTenantID && id != "" {
		sp.SetAttributes(tracing.KeyTenantID.String(safeID(id)))
	}
}

// AttemptStarted starts the worker.forward span (and the scheduler.select span that chose its worker) and
// returns a context whose TraceRef carries the traceparent for the worker call.
func (o *Observer) AttemptStarted(ctx context.Context, e gateway.AttemptStart) context.Context {
	st := stateFrom(ctx)
	if st == nil {
		return ctx // unsampled or untraced: the request's own TraceRef (if any) is used for the call
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.ended {
		return ctx
	}
	now := time.Now()
	if e.Strategy != "" { // registry mode: a worker was selected
		_, sel := o.tracer.Start(st.rootCtx, tracing.SpanSelect, trace.WithTimestamp(now.Add(-e.SelectDuration)), trace.WithAttributes(
			tracing.KeyRequestID.String(e.RequestID), tracing.KeyAttempt.Int(e.Number), tracing.KeyWorkerID.String(safeID(e.WorkerID)),
			tracing.KeyStrategy.String(pick(e.Strategy, strategies)), tracing.KeySelectOutcome.String("selected")))
		sel.End(trace.WithTimestamp(now))
	}
	attrs := []attribute.KeyValue{
		tracing.KeyRequestID.String(e.RequestID), tracing.KeyAttemptID.String(safeID(e.AttemptID)), tracing.KeyAttempt.Int(e.Number),
	}
	if e.WorkerID != "" {
		attrs = append(attrs, tracing.KeyWorkerID.String(safeID(e.WorkerID)))
	}
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindClient), trace.WithTimestamp(now), trace.WithAttributes(attrs...)}
	if st.prev.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: st.prev, Attributes: []attribute.KeyValue{tracing.KeyLink.String(tracing.LinkRetryOf)}}))
	}
	actx, span := o.tracer.Start(st.rootCtx, tracing.SpanForward, opts...)
	st.prev = span.SpanContext()
	att := &attState{span: span, ctx: actx, number: e.Number, start: now}
	st.open = append(st.open, att)
	out := context.WithValue(trace.ContextWithSpan(ctx, span), attKey{}, att)
	return gateway.WithTraceRef(out, ref(span.SpanContext(), st.state))
}

// FirstToken ends the first_token span (attempt start to first chunk) and starts the completion span.
func (o *Observer) FirstToken(ctx context.Context, e gateway.FirstToken) {
	st, att := stateFrom(ctx), attFrom(ctx)
	if st == nil || att == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if att.done || att.completion != nil {
		return
	}
	now := time.Now()
	_, ft := o.tracer.Start(att.ctx, tracing.SpanFirstToken, trace.WithTimestamp(att.start), trace.WithAttributes(
		tracing.KeyRequestID.String(e.RequestID), tracing.KeyAttempt.Int(att.number), tracing.KeyTTFTMillis.Int64(e.TTFT.Milliseconds())))
	ft.End(trace.WithTimestamp(now))
	_, att.completion = o.tracer.Start(att.ctx, tracing.SpanCompletion, trace.WithTimestamp(now), trace.WithAttributes(
		tracing.KeyRequestID.String(e.RequestID), tracing.KeyAttempt.Int(att.number)))
}

// AttemptEnded ends the attempt's spans.
func (o *Observer) AttemptEnded(ctx context.Context, e gateway.AttemptEnd) {
	st, att := stateFrom(ctx), attFrom(ctx)
	if st == nil || att == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	o.endAttempt(st, att, &e)
}

// endAttempt ends one attempt; e is nil when the request finished with the attempt still open.
func (o *Observer) endAttempt(st *reqState, att *attState, e *gateway.AttemptEnd) {
	if att.done {
		return
	}
	att.done = true
	now := time.Now()
	for i, a := range st.open {
		if a == att {
			st.open = append(st.open[:i], st.open[i+1:]...)
			break
		}
	}
	if att.completion != nil {
		att.completion.End(trace.WithTimestamp(now))
	}
	span := att.span
	if e != nil {
		class := token(e.Class, 24)
		span.SetAttributes(tracing.KeyAttemptOutcome.String(pick(e.Outcome, attemptOutcomes)))
		if class != "" {
			span.SetAttributes(tracing.KeyAttemptClass.String(class))
		}
		if e.NextWorkerUnavailable {
			span.SetAttributes(tracing.KeyNextWorkerAbsent.Bool(true))
		}
		switch e.Outcome {
		case gateway.AttemptFailed, gateway.AttemptRetried:
			desc := class
			if desc == "" {
				desc = e.Outcome
			}
			span.SetStatus(codes.Error, desc)
		case gateway.AttemptClientClosed:
			span.SetAttributes(tracing.KeyClientClosed.Bool(true))
		}
		if e.WillRetry {
			span.AddEvent(tracing.EventRetry, trace.WithTimestamp(now), trace.WithAttributes(
				tracing.KeyAttemptClass.String(class), tracing.KeyNextAttempt.Int(e.Number+1)))
		}
	} else {
		span.SetAttributes(tracing.KeyAttemptOutcome.String("unfinished"))
	}
	span.End(trace.WithTimestamp(now))
}

// RequestCompleted ends the root span, and any attempt span still open.
func (o *Observer) RequestCompleted(ctx context.Context, e gateway.Completion) {
	st := stateFrom(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.ended {
		return
	}
	st.ended = true
	for len(st.open) > 0 {
		o.endAttempt(st, st.open[0], nil)
	}
	root := st.root
	attrs := []attribute.KeyValue{
		tracing.KeyHTTPStatusCode.Int(e.Status), tracing.KeyAttempts.Int(e.Attempts), tracing.KeyStream.Bool(e.Stream),
	}
	if m := validModel(e.Model); m != "" {
		attrs = append(attrs, tracing.KeyModel.String(m))
	}
	if code := errorCode(e.ErrorCode); code != "" {
		attrs = append(attrs, tracing.KeyErrorCode.String(code))
	}
	if e.TTFT > 0 {
		attrs = append(attrs, tracing.KeyTTFTMillis.Int64(e.TTFT.Milliseconds()))
	}
	switch {
	case e.Status == 499:
		// A client leaving is not a server error.
		attrs = append(attrs, tracing.KeyClientClosed.Bool(true))
	case e.Status >= 500, e.Status < 400 && e.ErrorCode != "":
		// A 5xx, or a stream that failed after its 200 was sent.
		desc := errorCode(e.ErrorCode)
		if desc == "" {
			desc = strconv.Itoa(e.Status)
		}
		root.SetStatus(codes.Error, desc)
	}
	o.tenant(root, e.TenantID)
	root.SetAttributes(attrs...)
	root.End(trace.WithTimestamp(time.Now()))
}
