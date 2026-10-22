package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// Phase 15, PR A: the observer seam additions. These tests pin the contract: the new fields and methods exist,
// NopObserver supplies defaults, the fan-out contains panics in the new methods, and with resilience off every one
// of them stays at its zero value (PR D and PR E fire them with it on).

type seamObserver struct {
	NopObserver
	mu     sync.Mutex
	queued []QueueWait
	trans  []BreakerChange
}

func (o *seamObserver) QueueWaited(_ context.Context, e QueueWait) {
	o.mu.Lock()
	o.queued = append(o.queued, e)
	o.mu.Unlock()
}

func (o *seamObserver) BreakerTransition(_ context.Context, e BreakerChange) {
	o.mu.Lock()
	o.trans = append(o.trans, e)
	o.mu.Unlock()
}

type panicSeamObserver struct{ NopObserver }

func (panicSeamObserver) QueueWaited(context.Context, QueueWait)           { panic("queue") }
func (panicSeamObserver) BreakerTransition(context.Context, BreakerChange) { panic("breaker") }

func TestTheNewObserverMethodsFanOutAndContainPanics(t *testing.T) {
	m := newMultiObserver(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	seen := &seamObserver{}
	m.add(panicSeamObserver{})
	m.add(seen)
	m.add(NopObserver{})
	m.QueueWaited(context.Background(), QueueWait{RequestID: "r", Model: "m", Waited: time.Second, Result: QueueServed})
	m.BreakerTransition(context.Background(), BreakerChange{WorkerID: "w", Model: "m", To: "open", Reason: "consecutive_failures", Failures: 5, CoolDown: 10 * time.Second})
	if len(seen.queued) != 1 || seen.queued[0].Result != QueueServed || seen.queued[0].Waited != time.Second {
		t.Fatalf("QueueWaited: %+v", seen.queued)
	}
	if len(seen.trans) != 1 || seen.trans[0].To != "open" || seen.trans[0].Failures != 5 || seen.trans[0].CoolDown != 10*time.Second {
		t.Fatalf("BreakerTransition: %+v", seen.trans)
	}
}

func TestEveryRegistrySequenceLeavesTheNewSeamFieldsAtTheirZeroValues(t *testing.T) {
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	bad := newScriptWorker(t, "c-bad", status(503, `{"error":{"code":"x"}}`))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "a-bad"), good.snapshot("qwen-7b")})
	solo := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b")})
	seam := &seamObserver{}
	rec, rec2 := &recObserver{}, &recObserver{}
	for _, env := range []struct {
		e *regEnv
		r *recObserver
	}{{e, rec}, {solo, rec2}} {
		WithObserver(env.r)(env.e.gw)
		WithObserver(seam)(env.e.gw)
		env.e.post(t, false)
		env.e.chat(t, "no-such-model")
		env.r.await(t)
		eventually(t, 5*time.Second, func() bool { env.r.mu.Lock(); defer env.r.mu.Unlock(); return len(env.r.done) == 2 }, "both requests")
	}
	for _, r := range []*recObserver{rec, rec2} {
		for _, a := range r.att {
			if a.Probe || a.QueueWait != 0 {
				t.Fatalf("with resilience off nothing probes or queues: %+v", a)
			}
		}
		for _, a := range r.ends {
			if a.RetryDenied != "" {
				t.Fatalf("with resilience off nothing denies a retry: %+v", a)
			}
		}
		for _, j := range r.rej {
			if j.Kind == RejectOverload {
				t.Fatalf("with resilience off nothing is shed: %+v", j)
			}
		}
	}
	if len(seam.queued) != 0 || len(seam.trans) != 0 {
		t.Fatalf("with resilience off QueueWaited and BreakerTransition are never fired: %+v %+v", seam.queued, seam.trans)
	}
}

func TestAttemptEndNamesWhyARetryFoundNoWorker(t *testing.T) {
	cases := []struct {
		name     string
		setup    func(t *testing.T) *regEnv
		wantNext string
	}{
		{"every other worker is excluded", func(t *testing.T) *regEnv {
			bad := newScriptWorker(t, "a-bad", status(503, `{"error":{"code":"x"}}`))
			return newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b")})
		}, NextRouteNoCapacity},
		{"the registry view went stale", func(t *testing.T) *regEnv {
			var e *regEnv
			bad := newScriptWorker(t, "a-bad", func(w http.ResponseWriter, r *http.Request) {
				e.clock.Advance(time.Hour)
				status(503, `{"error":{"code":"x"}}`)(w, r)
			})
			good := newScriptWorker(t, "b-good", okJSON("b-good"))
			e = newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
			return e
		}, NextRouteWorkerUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.setup(t)
			rec := &recObserver{}
			WithObserver(rec)(e.gw)
			if resp, _ := e.post(t, false); resp.StatusCode != 503 {
				t.Fatalf("%d", resp.StatusCode)
			}
			rec.await(t)
			end := rec.ends[0]
			if end.NextRouteResult != tc.wantNext || !end.NextWorkerUnavailable || end.WillRetry || end.RetryDenied != "" {
				t.Fatalf("attempt end: %+v", end)
			}
		})
	}
}

func TestARoutedRetryHasNoNextRouteResult(t *testing.T) {
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{refusing(t, "a-bad"), good.snapshot("qwen-7b")})
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	e.post(t, false)
	rec.await(t)
	for _, end := range rec.ends {
		if end.NextRouteResult != "" || end.NextWorkerUnavailable {
			t.Fatalf("the retry was routed: %+v", end)
		}
	}
}

// A shed request is admission, not scheduling: it must never reach scheduler_decisions_total or
// scheduler_no_capacity_total, whatever its reason (Phase 15 D13).
func TestAnOverloadRejectionIsNotASchedulingDecision(t *testing.T) {
	m := registryMetrics("round-robin")
	for _, reason := range []string{OverloadGlobalLimit, OverloadQueueFull, OverloadQueueTimeout, OverloadDraining} {
		m.RequestRejected(context.Background(), Rejection{
			RequestID: "r", Kind: RejectOverload, Reason: reason, Status: http.StatusServiceUnavailable, Model: "qwen-7b",
			DecisionStart: time.Now(), DecisionDuration: time.Millisecond,
		})
	}
	for _, name := range []string{"scheduler_decisions_total", "scheduler_no_capacity_total", "scheduler_decision_duration_seconds"} {
		if n := sumSeries(t, m, name); n != 0 {
			t.Fatalf("%s = %v after four shed requests", name, n)
		}
	}
}
