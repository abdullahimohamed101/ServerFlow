package events

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/api"
	"serverflow/internal/gateway"
	"serverflow/pkg/protocol"
)

// Observer turns the gateway's request lifecycle into events (D3). It is the third gateway.Observer (after
// metrics and tracing). Every method builds a small struct from the plain values it is given and makes one
// non-blocking Enqueue; nothing here waits on the network, a lock another request can hold for long, or a
// full queue. It copies fields one by one and so cannot forward content: the gateway's event values hold none.
type Observer struct {
	gateway.NopObserver
	pub    *Publisher
	source string
	now    func() time.Time
}

var _ gateway.Observer = (*Observer)(nil)

// NewObserver builds the observer and the publisher behind it. Close the observer on shutdown.
func NewObserver(sink Sink, cfg Config, log *slog.Logger) *Observer {
	o := &Observer{pub: NewPublisher(sink, cfg, log), source: cfg.Source, now: cfg.Now}
	if o.source == "" {
		o.source = "gateway"
	}
	if o.now == nil {
		o.now = time.Now
	}
	return o
}

// Collectors returns the Prometheus collectors to register on the gateway (gateway.WithExtraCollectors).
func (o *Observer) Collectors() []prometheus.Collector { return o.pub.Collectors() }

// Publisher exposes the publisher, for tests and metrics.
func (o *Observer) Publisher() *Publisher { return o.pub }

// Close flushes and stops publishing, bounded by ctx. See Publisher.Close.
func (o *Observer) Close(ctx context.Context) error { return o.pub.Close(ctx) }

// reqState is what the observer remembers about one request. It lives on the request's context, so it needs no
// map and cannot leak: it is garbage with the request.
type reqState struct {
	mu         sync.Mutex
	admitted   bool
	model      string // as the client asked; unconfirmed
	stream     bool
	tenantID   string
	apiKeyID   string
	estimated  int64
	attempts   []protocol.AttemptData
	firstToken bool
}

type stateKey struct{}

func stateFrom(ctx context.Context) *reqState {
	st, _ := ctx.Value(stateKey{}).(*reqState)
	return st
}

// RequestStarted attaches the per-request state to the context.
func (o *Observer) RequestStarted(ctx context.Context, _ gateway.RequestStart) context.Context {
	return context.WithValue(ctx, stateKey{}, &reqState{})
}

func (o *Observer) envelope(st *reqState, typ, requestID, attemptID, worker string) protocol.Event {
	return protocol.Event{
		EventID: protocol.NewEventID(requestID, typ, attemptID), EventType: typ, SchemaVersion: protocol.EventSchemaVersion,
		Timestamp: o.now().UTC(), Source: o.source, RequestID: requestID, AttemptID: attemptID,
		TenantID: st.tenantID, APIKeyID: st.apiKeyID, Model: st.model, WorkerID: worker,
	}
}

// RequestAdmitted emits received. Requests refused before this point produce no event (D3).
func (o *Observer) RequestAdmitted(ctx context.Context, e gateway.Admission) {
	st := stateFrom(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.admitted, st.model, st.stream, st.tenantID, st.apiKeyID = true, e.Model, e.Stream, e.TenantID, e.APIKeyID
	st.estimated = int64(e.EstimatedCost)
	ev := o.envelope(st, protocol.EventReceived, e.RequestID, "", "")
	st.mu.Unlock()
	ev.Received = &protocol.ReceivedData{Stream: e.Stream, EstimatedCostTokens: int64(e.EstimatedCost)}
	o.pub.Enqueue(ev)
}

// AttemptStarted emits routed and opens the attempt's history entry.
func (o *Observer) AttemptStarted(ctx context.Context, e gateway.AttemptStart) context.Context {
	st := stateFrom(ctx)
	if st == nil {
		return ctx
	}
	st.mu.Lock()
	if !st.admitted {
		st.mu.Unlock()
		return ctx
	}
	if len(st.attempts) < protocol.MaxEventAttempts {
		st.attempts = append(st.attempts, protocol.AttemptData{AttemptID: e.AttemptID, Number: e.Number, WorkerID: e.WorkerID})
	}
	ev := o.envelope(st, protocol.EventRouted, e.RequestID, e.AttemptID, e.WorkerID)
	st.mu.Unlock()
	ev.Routed = &protocol.RoutedData{AttemptNumber: e.Number, Strategy: e.Strategy}
	o.pub.Enqueue(ev)
	return ctx
}

// FirstToken emits first_token once per request.
func (o *Observer) FirstToken(ctx context.Context, e gateway.FirstToken) {
	st := stateFrom(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	if !st.admitted || st.firstToken {
		st.mu.Unlock()
		return
	}
	st.firstToken = true
	ev := o.envelope(st, protocol.EventFirstToken, e.RequestID, e.AttemptID, st.workerOf(e.AttemptID))
	st.mu.Unlock()
	ev.FirstToken = &protocol.FirstTokenData{TTFTMS: e.TTFT.Milliseconds()}
	o.pub.Enqueue(ev)
}

func (st *reqState) workerOf(attemptID string) string {
	for i := range st.attempts {
		if st.attempts[i].AttemptID == attemptID {
			return st.attempts[i].WorkerID
		}
	}
	return ""
}

// AttemptEnded records the attempt's outcome for the terminal event. It emits nothing itself.
func (o *Observer) AttemptEnded(ctx context.Context, e gateway.AttemptEnd) {
	st := stateFrom(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	for i := range st.attempts {
		if st.attempts[i].AttemptID == e.AttemptID {
			a := &st.attempts[i]
			a.Outcome, a.FailureClass, a.DurationMS = e.Outcome, e.Class, e.Duration.Milliseconds()
			if a.WorkerID == "" {
				a.WorkerID = e.WorkerID
			}
			return
		}
	}
}

// RequestCompleted emits exactly one terminal event for an admitted request.
func (o *Observer) RequestCompleted(ctx context.Context, e gateway.Completion) {
	st := stateFrom(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	if !st.admitted {
		st.mu.Unlock()
		return
	}
	var attemptID, worker string
	if n := len(st.attempts); n > 0 {
		attemptID, worker = st.attempts[n-1].AttemptID, st.attempts[n-1].WorkerID
	}
	class := classify(e)
	typ := protocol.EventCompleted
	if class != "" {
		typ = protocol.EventFailed
	}
	ev := o.envelope(st, typ, e.RequestID, attemptID, worker)
	ev.Model = e.Model // the confirmed model, or empty: client text never becomes usage data
	attempts := make([]protocol.AttemptData, len(st.attempts))
	copy(attempts, st.attempts)
	t := &protocol.TerminalData{
		Stream: e.Stream, HTTPStatus: e.Status, DurationMS: e.Duration.Milliseconds(), TTFTMS: e.TTFT.Milliseconds(),
		EstimatedCostTokens: st.estimated, FailureClass: class, Attempts: attempts, TokensSource: protocol.TokensFromEstimate,
	}
	st.mu.Unlock()
	for i := range attempts {
		if attempts[i].Outcome == "" {
			attempts[i].Outcome = "unknown"
		}
	}
	switch e.TokensSource {
	case gateway.TokensUsage:
		in, out := e.InputTokens, e.OutputTokens
		t.InputTokens, t.OutputTokens, t.TokensSource = &in, &out, protocol.TokensFromUsage
	case gateway.TokensChunks:
		out := e.OutputTokens
		t.OutputTokens, t.TokensSource = &out, protocol.TokensFromChunks
	}
	ev.Terminal = t
	o.pub.Enqueue(ev)
}

// classify returns "" for a successful request, else the failure class.
func classify(e gateway.Completion) string {
	switch e.ErrorCode {
	case "":
	case api.CodeNoCapacity:
		return protocol.FailureNoCapacity
	case api.CodeWorkerUnavailable:
		return protocol.FailureWorkerUnavailable
	case api.CodeUpstreamTimeout:
		return protocol.FailureTimeout
	case api.CodeInferenceFailed:
		return protocol.FailureWorkerError
	default:
		return protocol.FailureInternal
	}
	switch {
	case e.Status == 499:
		return protocol.FailureClientClosed
	case e.Status >= 200 && e.Status < 300:
		return ""
	case e.Status >= 500:
		return protocol.FailureWorkerError
	case e.Status >= 400:
		return protocol.FailureWorkerError // a worker's own 4xx, relayed to the client
	}
	return protocol.FailureInternal
}
