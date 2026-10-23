package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Phase 15 PR D: gateway tests with resilience on. The breakers, the budget and the first-byte deadline run on the
// same fake clock as the snapshot cache (advance moves it and refreshes the view), and the cool-down jitter is
// fixed at its midpoint, so nothing here sleeps to move a breaker.

// resRec is a recording observer that also keeps breaker transitions.
type resRec struct {
	recObserver
	tmu    sync.Mutex
	trans  []BreakerChange
	queued []QueueWait
}

// QueueWaited records the wait and puts "queue:<result>" in the call sequence.
func (r *resRec) QueueWaited(_ context.Context, e QueueWait) {
	r.tmu.Lock()
	r.queued = append(r.queued, e)
	r.tmu.Unlock()
	r.add("queue:" + e.Result)
}

func (r *resRec) queueWaits() []QueueWait {
	r.tmu.Lock()
	defer r.tmu.Unlock()
	return append([]QueueWait(nil), r.queued...)
}

func (r *resRec) BreakerTransition(_ context.Context, e BreakerChange) {
	r.tmu.Lock()
	r.trans = append(r.trans, e)
	r.tmu.Unlock()
}

func (r *resRec) transitions() []BreakerChange {
	r.tmu.Lock()
	defer r.tmu.Unlock()
	return append([]BreakerChange(nil), r.trans...)
}

// states lists the transitions as "worker:state:reason".
func (r *resRec) states() []string {
	var out []string
	for _, c := range r.transitions() {
		out = append(out, fmt.Sprintf("%s:%s:%s", c.WorkerID, c.To, c.Reason))
	}
	return out
}

func (r *resRec) attemptStarts() []AttemptStart {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]AttemptStart(nil), r.att...)
}

func (r *resRec) attemptEnds() []AttemptEnd {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]AttemptEnd(nil), r.ends...)
}

type resEnv struct {
	*regEnv
	rec *resRec
	rs  *resilienceState
}

// jitterMid makes the cool-down jitter factor exactly 1.
func jitterMid() float64 { return 0.5 }

// newResEnv starts a registry gateway with resilience on and a recording observer. The defaults of the plan apply
// except consecutive_failures, which is 3 so a test needs few requests; mutate may change anything.
func newResEnv(t *testing.T, ws []protocol.WorkerSnapshot, mutate ...func(*config.Config)) *resEnv {
	t.Helper()
	cfg := config.Default()
	cfg.Gateway = testConfig("http://127.0.0.1:1")
	cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	cfg.Resilience.Mode = config.ResilienceOn
	cfg.Resilience.Breaker.ConsecutiveFailures = 3
	for _, m := range mutate {
		m(&cfg)
	}
	logs := &lockedBuffer{}
	lister := &fakeLister{workers: ws}
	gw, err := newRegistryServer(cfg.Gateway, "round-robin", 5*time.Second, lister, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	gw.router.cache.now = clock.Now
	gw.enableResilience(cfg, resilienceDeps{now: clock.Now, rand: jitterMid, peekWait: 40 * time.Millisecond})
	rec := &resRec{}
	WithObserver(rec)(gw)
	srv := httptest.NewServer(gw.Handler())
	tr := &http.Transport{}
	t.Cleanup(func() { tr.CloseIdleConnections(); srv.Close() })
	e := &regEnv{gw: gw, url: srv.URL, lister: lister, clock: clock, logs: logs, client: &http.Client{Transport: tr, Timeout: 30 * time.Second}}
	if len(ws) > 0 {
		e.refresh(t)
	}
	return &resEnv{regEnv: e, rec: rec, rs: gw.res}
}

// advance moves the fake clock and refreshes the registry view (the view would otherwise be stale).
func (e *resEnv) advance(t *testing.T, d time.Duration) {
	t.Helper()
	e.clock.Advance(d)
	e.refresh(t)
}

// do sends one chat request and returns the status.
func (e *resEnv) do(t *testing.T) int {
	t.Helper()
	resp, _ := e.chat(t, "qwen-7b")
	return resp.StatusCode
}

// key is the breaker key of a snapshot worker as the test lister lists it.
func (e *resEnv) key(id string) resilience.Key { return resilience.KeyOf(id, time.Time{}) }

func (e *resEnv) state(id string) resilience.State { return e.rs.breakers.State(e.key(id)) }

// settle waits until every slot is free, every ticket is back and every started attempt has been ended. The slot
// and the ticket are given back before AttemptEnded is delivered, so a test that reads the last AttemptEnd must
// wait for it: the last one recorded may otherwise belong to an earlier attempt.
func (e *resEnv) settle(t *testing.T, ids ...string) {
	t.Helper()
	allIdle(t, e.regEnv, ids...)
	eventually(t, 5*time.Second, func() bool {
		return len(e.rec.attemptEnds()) == len(e.rec.attemptStarts())
	}, "every started attempt must be ended")
	for _, in := range e.rs.breakers.Snapshot() {
		if in.Probes != 0 {
			eventually(t, 3*time.Second, func() bool {
				for _, x := range e.rs.breakers.Snapshot() {
					if x.Probes != 0 {
						return false
					}
				}
				return true
			}, "probe tickets must come back")
			break
		}
	}
}

// workerFlag is a switchable script worker handler.
type workerFlag struct {
	mu sync.Mutex
	h  func(http.ResponseWriter, *http.Request)
}

func (f *workerFlag) set(h func(http.ResponseWriter, *http.Request)) {
	f.mu.Lock()
	f.h = h
	f.mu.Unlock()
}

func (f *workerFlag) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	h := f.h
	f.mu.Unlock()
	h(w, r)
}

// openAPI503 is an OpenAI-shaped worker 503 with the given error code.
func openAPI503(code string) func(http.ResponseWriter, *http.Request) {
	return status(503, `{"error":{"message":"x","type":"server_error","code":"`+code+`"}}`)
}

func bodyWith(code string, pad int) string {
	return `{"error":{"message":"` + strings.Repeat("p", pad) + `","type":"server_error","code":"` + code + `"}}`
}
