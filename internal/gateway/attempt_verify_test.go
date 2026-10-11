package gateway

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/scheduler"
	"serverflow/pkg/protocol"
)

// cancelOnSecondSelect cancels the client's request when the scheduler is asked a second time, then fails the
// selection: the retry's routing fails because the client has gone.
type cancelOnSecondSelect struct {
	inner  scheduler.Scheduler
	mu     sync.Mutex
	calls  int
	cancel context.CancelFunc
}

func (c *cancelOnSecondSelect) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	c.mu.Lock()
	c.calls++
	n := c.calls
	c.mu.Unlock()
	if n == 2 {
		c.cancel()
		time.Sleep(20 * time.Millisecond) // let the server notice the closed connection
		return nil, ctx.Err()
	}
	return c.inner.SelectWorker(ctx, req, ws)
}

func TestAClientThatLeavesDuringTheRetrysRoutingIsNotASchedulingOutcome(t *testing.T) {
	bad := newScriptWorker(t, "a-bad", func(w http.ResponseWriter, r *http.Request) {
		status(503, `{"error":{"code":"x"}}`)(w, r)
	})
	good := newScriptWorker(t, "b-good", okJSON("b-good"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), good.snapshot("qwen-7b")})
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	inner := e.gw.router.sched
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		e.gw.router.sched = &cancelOnSecondSelect{inner: inner, cancel: cancel}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+chatCompletionsPath,
			strings.NewReader(`{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`))
		if resp, err := e.client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
		cancel()
		n := i + 1
		eventually(t, 5*time.Second, func() bool { rec.mu.Lock(); defer rec.mu.Unlock(); return len(rec.done) == n }, "completion")
	}
	allIdle(t, e, "a-bad", "b-good")
	var firsts int
	for _, end := range rec.ends {
		if end.NextRouteResult != "" {
			t.Fatalf("a client that left is not a scheduling outcome: %+v", end)
		}
		if end.NextWorkerUnavailable {
			firsts++
			if end.Outcome != AttemptClientClosed {
				t.Fatalf("the first attempt ends client_closed: %+v", end)
			}
		}
	}
	if firsts == 0 {
		t.Fatal("the cancellation never landed in the second routing")
	}
	// Selections: the first attempt of each request is counted; nothing is counted for the abandoned retry.
	if v, _ := sample(t, e.gw.metrics, "scheduler_decisions_total", "result=error"); v != 0 {
		t.Fatalf("an extra scheduling decision was counted: %v", v)
	}
	if v, _ := sample(t, e.gw.metrics, "scheduler_decisions_total", "result=no_capacity"); v != 0 {
		t.Fatalf("an extra scheduling decision was counted: %v", v)
	}
}

func TestRouteErrorCarriesTheSchedulersCounts(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sa, sb := a.snapshot("qwen-7b", 1), b.snapshot("qwen-7b", 1)
	sa.Metrics.ActiveRequests, sb.Metrics.ActiveRequests = 1, 1
	drain := a.snapshot("qwen-7b", 1)
	drain.WorkerID, drain.State, drain.Eligible = "c", protocol.StateDraining, false
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{sa, sb, drain})
	_, rerr := e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"})
	if rerr == nil || rerr.Serving != 3 || rerr.Eligible != 2 || rerr.AtCapacity != 2 || rerr.Code != "NO_CAPACITY" {
		t.Fatalf("%+v", rerr)
	}
	_, rerr = e.gw.router.Route(context.Background(), &protocol.InferenceRequest{Model: "qwen-7b"}, "a", "b", "c")
	if rerr == nil || rerr.Serving != 3 || rerr.Eligible != 0 || rerr.Code != "NO_CAPACITY" {
		t.Fatalf("excluding every worker: %+v", rerr)
	}
}

// ADR-016: the slot is given back before AttemptEnded is delivered, so an observer that reads the load sees it
// already lowered.
func TestTheSlotIsReleasedBeforeAttemptEndedFires(t *testing.T) {
	good := newScriptWorker(t, "a", okJSON("a"))
	e := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{good.snapshot("qwen-7b")})
	var mu sync.Mutex
	var seen []int
	WithObserver(&inflightObserver{f: func(w string) {
		mu.Lock()
		seen = append(seen, e.gw.router.InFlight(w))
		mu.Unlock()
	}})(e.gw)
	e.post(t, false)
	eventually(t, 5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 1 }, "AttemptEnded")
	if seen[0] != 0 {
		t.Fatalf("in flight at AttemptEnded: %d", seen[0])
	}
}

type inflightObserver struct {
	NopObserver
	f func(worker string)
}

func (o *inflightObserver) AttemptEnded(_ context.Context, e AttemptEnd) { o.f(e.WorkerID) }
