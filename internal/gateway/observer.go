package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// Observer is told what happens to an inference request. It is the one seam through which metrics, tracing
// and event publishing watch the request path (ADR-016); the path itself does not know what they do.
//
// Contract for implementations:
//   - Methods run synchronously on the request goroutine, so they must return quickly and never block on
//     the network or a full queue. Anything slow (publishing to Kafka, exporting spans) must buffer and drop
//     internally. A slow observer slows every request; the gateway does not hide that.
//   - Methods are called concurrently from many requests and must be safe for that.
//   - An observer must not panic. If it does, the gateway recovers, logs it (at most once a minute per
//     observer), and carries on with the remaining observers; the request is unaffected.
//   - Events hold plain values only. They never contain a request body, a prompt or an API key.
//
// RequestStarted and AttemptStarted may return a context derived from the one they were given, for example
// carrying a trace span. The gateway uses the returned context for the rest of that request, or for that
// attempt including the upstream call. Return the given context if you have nothing to add.
//
// Call sequence for one inference request (see docs/development/observers.md):
//
//	RequestStarted
//	  RequestRejected                          refused by authentication
//	  RequestAdmitted
//	    RequestRejected                        refused by validation, model, rate limit, or capacity
//	    (AttemptStarted [FirstToken] AttemptEnded)+   one pair per attempt; a retry adds a pair
//	RequestCompleted                           always last, exactly once
type Observer interface {
	RequestStarted(ctx context.Context, e RequestStart) context.Context
	RequestAdmitted(ctx context.Context, e Admission)
	RequestRejected(ctx context.Context, e Rejection)
	AttemptStarted(ctx context.Context, e AttemptStart) context.Context
	FirstToken(ctx context.Context, e FirstToken)
	AttemptEnded(ctx context.Context, e AttemptEnd)
	RequestCompleted(ctx context.Context, e Completion)
}

// NopObserver implements Observer with empty methods. Embed it to implement only the moments you care about.
type NopObserver struct{}

func (NopObserver) RequestStarted(ctx context.Context, _ RequestStart) context.Context { return ctx }
func (NopObserver) RequestAdmitted(context.Context, Admission)                         {}
func (NopObserver) RequestRejected(context.Context, Rejection)                         {}
func (NopObserver) AttemptStarted(ctx context.Context, _ AttemptStart) context.Context { return ctx }
func (NopObserver) FirstToken(context.Context, FirstToken)                             {}
func (NopObserver) AttemptEnded(context.Context, AttemptEnd)                           {}
func (NopObserver) RequestCompleted(context.Context, Completion)                       {}

// WithObserver registers an additional observer. Metrics is always registered first; observers run in
// registration order and each sees the context returned by those before it. Like every Option it must be
// applied before the server starts serving. A nil observer is ignored.
func WithObserver(o Observer) Option {
	return func(s *Server) {
		if o != nil {
			s.obs.add(o)
		}
	}
}

// panicLogInterval is how often a panicking observer may be logged.
const panicLogInterval = time.Minute

// guarded is one registered observer plus what is needed to contain it.
type guarded struct {
	o       Observer
	name    string
	lastLog atomic.Int64 // unix nanoseconds of the last panic log
}

// recoverPanic is deferred around every observer call. It swallows the panic and logs it at most once per
// panicLogInterval per observer, so a broken observer cannot flood the log at request rate.
func (g *guarded) recoverPanic(m *multiObserver, method string) {
	p := recover()
	if p == nil {
		return
	}
	now := m.now().UnixNano()
	last := g.lastLog.Load()
	if last != 0 && now-last < int64(panicLogInterval) {
		return
	}
	if !g.lastLog.CompareAndSwap(last, now) {
		return
	}
	m.log.Error("observer panicked; the request was not affected", "component", "gateway", "observer", g.name, "method", method, "panic", fmt.Sprint(p))
}

// multiObserver fans each event out to its observers in order, containing panics.
type multiObserver struct {
	obs []*guarded
	log *slog.Logger
	now func() time.Time
}

func newMultiObserver(log *slog.Logger) *multiObserver {
	return &multiObserver{log: log, now: time.Now}
}

func (m *multiObserver) add(o Observer) {
	m.obs = append(m.obs, &guarded{o: o, name: fmt.Sprintf("%T", o)})
}

func (m *multiObserver) RequestStarted(ctx context.Context, e RequestStart) context.Context {
	for _, g := range m.obs {
		ctx = m.requestStarted(g, ctx, e)
	}
	return ctx
}

func (m *multiObserver) requestStarted(g *guarded, ctx context.Context, e RequestStart) (out context.Context) {
	out = ctx
	defer g.recoverPanic(m, "RequestStarted")
	if c := g.o.RequestStarted(ctx, e); c != nil {
		out = c
	}
	return out
}

func (m *multiObserver) RequestAdmitted(ctx context.Context, e Admission) {
	for _, g := range m.obs {
		m.requestAdmitted(g, ctx, e)
	}
}

func (m *multiObserver) requestAdmitted(g *guarded, ctx context.Context, e Admission) {
	defer g.recoverPanic(m, "RequestAdmitted")
	g.o.RequestAdmitted(ctx, e)
}

func (m *multiObserver) RequestRejected(ctx context.Context, e Rejection) {
	for _, g := range m.obs {
		m.requestRejected(g, ctx, e)
	}
}

func (m *multiObserver) requestRejected(g *guarded, ctx context.Context, e Rejection) {
	defer g.recoverPanic(m, "RequestRejected")
	g.o.RequestRejected(ctx, e)
}

func (m *multiObserver) AttemptStarted(ctx context.Context, e AttemptStart) context.Context {
	for _, g := range m.obs {
		ctx = m.attemptStarted(g, ctx, e)
	}
	return ctx
}

func (m *multiObserver) attemptStarted(g *guarded, ctx context.Context, e AttemptStart) (out context.Context) {
	out = ctx
	defer g.recoverPanic(m, "AttemptStarted")
	if c := g.o.AttemptStarted(ctx, e); c != nil {
		out = c
	}
	return out
}

func (m *multiObserver) FirstToken(ctx context.Context, e FirstToken) {
	for _, g := range m.obs {
		m.firstToken(g, ctx, e)
	}
}

func (m *multiObserver) firstToken(g *guarded, ctx context.Context, e FirstToken) {
	defer g.recoverPanic(m, "FirstToken")
	g.o.FirstToken(ctx, e)
}

func (m *multiObserver) AttemptEnded(ctx context.Context, e AttemptEnd) {
	for _, g := range m.obs {
		m.attemptEnded(g, ctx, e)
	}
}

func (m *multiObserver) attemptEnded(g *guarded, ctx context.Context, e AttemptEnd) {
	defer g.recoverPanic(m, "AttemptEnded")
	g.o.AttemptEnded(ctx, e)
}

func (m *multiObserver) RequestCompleted(ctx context.Context, e Completion) {
	for _, g := range m.obs {
		m.requestCompleted(g, ctx, e)
	}
}

func (m *multiObserver) requestCompleted(g *guarded, ctx context.Context, e Completion) {
	defer g.recoverPanic(m, "RequestCompleted")
	g.o.RequestCompleted(ctx, e)
}
