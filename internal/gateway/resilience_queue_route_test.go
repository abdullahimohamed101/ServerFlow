package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/pkg/protocol"
)

// Phase 15 PR E: how the queue meets the router. A slot is handed to the head waiter only when its worker is still
// a valid choice; a request waits only when the model is served and every eligible worker is full; the queue key is
// always a registry-confirmed model; and a randomized run checks the whole thing against a simple model.

// twoGateWorkers returns an environment with two single-slot gate workers, w-1 and w-2, each with one request in
// flight, and one waiter queued. The caller decides what happens to the workers before releasing them.
func twoGateWorkers(t *testing.T, mutate ...func(*config.Config)) (e *resEnv, w1, w2 *gateWorker, waiter <-chan chatResult, held1, held2 <-chan chatResult) {
	t.Helper()
	w1, w2 = newGateWorker(t, "w-1"), newGateWorker(t, "w-2")
	e = newResEnv(t, []protocol.WorkerSnapshot{w1.snap(1), w2.snap(1)}, append([]func(*config.Config){noBreaker}, mutate...)...)
	held1 = e.chatAsync(context.Background(), "h1")
	w1.awaitArrivals(t, 1) // round-robin starts at w-1
	held2 = e.chatAsync(context.Background(), "h2")
	w2.awaitArrivals(t, 1)
	waiter = e.chatAsync(context.Background(), "waiter")
	e.awaitDepth(t, 1)
	return
}

// A slot freed on a worker that is no longer eligible in the registry's view is not handed over: the waiter keeps
// its place, and is served by the first slot that frees on a worker that is.
func TestASlotOfAWorkerThatIsNoLongerEligibleIsNotHandedOver(t *testing.T) {
	e, w1, w2, waiter, held1, held2 := twoGateWorkers(t)
	drained := w1.snap(1)
	drained.State = protocol.StateDraining
	drained.Eligible = false
	e.lister.set([]protocol.WorkerSnapshot{drained, w2.snap(1)}, nil)
	e.refresh(t)

	w1.release(t, 1)
	awaitResult(t, held1)
	time.Sleep(150 * time.Millisecond)
	if n := len(w1.arrivals()); n != 1 {
		t.Fatalf("the draining worker was handed the waiter: %v", w1.arrivals())
	}
	if e.rs.waitq.Depth("qwen-7b") != 1 {
		t.Fatal("the waiter must keep waiting")
	}
	w2.release(t, 1)
	awaitResult(t, held2)
	w2.awaitArrivals(t, 2)
	w2.release(t, 1)
	if r := awaitResult(t, waiter); r.resp.StatusCode != 200 || whoAnswered(t, r.body) != "w-2" {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	e.settle(t, "w-1", "w-2")
}

// The same for a worker whose breaker is open: the hand-off consults the breaker, so ejection cannot be bypassed.
func TestASlotOfAWorkerWithAnOpenBreakerIsNotHandedOver(t *testing.T) {
	w1, w2 := newGateWorker(t, "w-1"), newGateWorker(t, "w-2")
	e := newResEnv(t, []protocol.WorkerSnapshot{w1.snap(1), w2.snap(1)}) // real breakers: 3 faults open one
	held1 := e.chatAsync(context.Background(), "h1")
	w1.awaitArrivals(t, 1)
	held2 := e.chatAsync(context.Background(), "h2")
	w2.awaitArrivals(t, 1)
	waiter := e.chatAsync(context.Background(), "waiter")
	e.awaitDepth(t, 1)
	k := e.key("w-1")
	for i := 0; i < 3; i++ {
		tk, ok := e.rs.breakers.Acquire(k)
		if !ok {
			t.Fatalf("acquire %d", i)
		}
		e.rs.breakers.Record(tk, resilience.Fault)
	}
	if e.state("w-1") != resilience.Open {
		t.Fatalf("the breaker is %v", e.state("w-1"))
	}

	w1.release(t, 1)
	awaitResult(t, held1)
	time.Sleep(150 * time.Millisecond)
	if n := len(w1.arrivals()); n != 1 {
		t.Fatalf("the ejected worker was handed the waiter: %v", w1.arrivals())
	}
	w2.release(t, 1)
	awaitResult(t, held2)
	w2.awaitArrivals(t, 2)
	w2.release(t, 1)
	if r := awaitResult(t, waiter); r.resp.StatusCode != 200 || whoAnswered(t, r.body) != "w-2" {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	e.settle(t, "w-1", "w-2")
}

// A request waits only when the model is served and every eligible worker is full. With no eligible worker (all
// draining) or an unusable view it is refused at once, as it always was.
func TestARequestDoesNotWaitWhenThereIsNothingToWaitFor(t *testing.T) {
	g := newGateWorker(t, "w1")
	drained := g.snap(1)
	drained.State = protocol.StateDraining
	drained.Eligible = false
	e := newResEnv(t, []protocol.WorkerSnapshot{drained}, noBreaker)
	began := time.Now()
	r := awaitResult(t, e.chatAsync(context.Background(), "x"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || time.Since(began) > time.Second {
		t.Fatalf("no eligible worker is an immediate 503: %d %s after %v", r.resp.StatusCode, r.body, time.Since(began))
	}
	want(t, e.gw.metrics, 1, "scheduler_decisions_total", "result=no_capacity")
	if len(e.rec.queueWaits()) != 0 {
		t.Fatal("it did not wait")
	}

	e.gw.router.cache.cur.Store(nil) // no snapshot at all
	r = awaitResult(t, e.chatAsync(context.Background(), "y"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "WORKER_UNAVAILABLE" {
		t.Fatalf("an unusable view is an immediate WORKER_UNAVAILABLE: %d %s", r.resp.StatusCode, r.body)
	}
	if e.rs.waitq.Depth("qwen-7b") != 0 {
		t.Fatal("nothing queued")
	}
}

// A model beyond the model cap (metrics.max_models) has no queue: it is refused at once as before, while the model
// that already has one is unaffected.
func TestAModelBeyondTheModelCapHasNoQueue(t *testing.T) {
	g1, g2 := newGateWorker(t, "w1"), newGateWorker(t, "w2")
	s1 := g1.snap(1)
	s2 := g2.snapshot("other-model")
	s2.MaxConcurrency = 1
	e := newResEnv(t, []protocol.WorkerSnapshot{s1, s2}, noBreaker, func(c *config.Config) { c.Metrics.MaxModels = 1 })

	held := e.chatAsync(context.Background(), "r0")
	g1.awaitArrivals(t, 1)
	queued := e.chatAsync(context.Background(), "q1")
	e.awaitDepth(t, 1) // qwen-7b took the only queue

	// other-model is served, its worker is full, and no queue is left for it.
	heldOther := postModel(e, "other-model", "o0")
	g2.awaitArrivals(t, 1)
	began := time.Now()
	r := awaitResult(t, postModel(e, "other-model", "o1"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || time.Since(began) > time.Second {
		t.Fatalf("%d %s after %v", r.resp.StatusCode, r.body, time.Since(began))
	}
	if got := e.rs.waitq.Models(); len(got) != 1 || got[0] != "qwen-7b" {
		t.Fatalf("queues: %v", got)
	}
	g1.release(t, 2)
	g2.release(t, 1)
	awaitResult(t, held)
	awaitResult(t, queued)
	awaitResult(t, heldOther)
}

func postModel(e *resEnv, model, content string) <-chan chatResult {
	out := make(chan chatResult, 1)
	go func() {
		body := `{"model":"` + model + `","messages":[{"role":"user","content":"` + content + `"}]}`
		resp, err := e.client.Post(e.url+chatCompletionsPath, "application/json", strings.NewReader(body))
		if err != nil {
			out <- chatResult{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		out <- chatResult{resp: resp, body: string(b)}
	}()
	return out
}

// The tenant, model and worker ID are all registry or authentication values; a hostile model name stays a 404.
func TestAHostileModelNameIsRefusedWithoutQueueingOrSeries(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g)
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	q := e.chatAsync(context.Background(), "q1")
	e.awaitDepth(t, 1)
	for i := 0; i < 20; i++ {
		resp, _ := e.chat(t, fmt.Sprintf("no-such-model-%d", i))
		if resp.StatusCode != 404 {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	if got := e.rs.waitq.Models(); len(got) != 1 || got[0] != "qwen-7b" {
		t.Fatalf("queues exist for %v", got)
	}
	if _, ok := sample(t, e.gw.metrics, "admission_rejections_total"); ok {
		t.Fatal("an unknown model is not an admission shed")
	}
	g.release(t, 2)
	awaitResult(t, held)
	awaitResult(t, q)
}

// perReqWorker answers each request when the test releases that request by name, so a model-based test knows which
// request leaves when.
type perReqWorker struct {
	*scriptWorker
	mu    sync.Mutex
	gates map[string]chan struct{}
	order []string
	done  chan struct{}
}

func newPerReqWorker(t *testing.T, id string) *perReqWorker {
	t.Helper()
	p := &perReqWorker{gates: map[string]chan struct{}{}, done: make(chan struct{})}
	p.scriptWorker = newScriptWorker(t, id, func(w http.ResponseWriter, r *http.Request) {
		c := contentOf(r)
		p.mu.Lock()
		p.order = append(p.order, c)
		ch := p.gateLocked(c)
		p.mu.Unlock()
		select {
		case <-ch:
		case <-p.done:
			return
		case <-r.Context().Done():
			return
		}
		okJSON(id)(w, r)
	})
	t.Cleanup(func() { close(p.done) })
	return p
}

func (p *perReqWorker) gateLocked(c string) chan struct{} {
	ch, ok := p.gates[c]
	if !ok {
		ch = make(chan struct{})
		p.gates[c] = ch
	}
	return ch
}

func (p *perReqWorker) release(c string) {
	p.mu.Lock()
	ch := p.gateLocked(c)
	p.mu.Unlock()
	close(ch)
}

func (p *perReqWorker) served() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.order...)
}

// The randomized check: arrivals, completions, cancellations and refreshes in random order against a model of what
// must happen (capacity never exceeded, strict FIFO, depth bound, nothing lost), for several seeds.
func TestTheQueueMatchesAModelUnderRandomArrivalsCompletionsAndCancellations(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) { runQueueModel(t, seed) })
	}
}

func runQueueModel(t *testing.T, seed int64) {
	const (
		conc  = 3
		depth = 5
		steps = 70
	)
	rng := rand.New(rand.NewSource(seed))
	w := newPerReqWorker(t, "w1")
	snap := w.snapshot("qwen-7b")
	snap.MaxConcurrency = conc
	e := newResEnv(t, []protocol.WorkerSnapshot{snap}, noBreaker, queueCfg(depth, 30*time.Second),
		func(c *config.Config) { c.Resilience.Queue.MaxTenantShare = 0 })

	type req struct {
		id     string
		cancel context.CancelFunc
		res    <-chan chatResult
	}
	var inflight, queue []*req
	results := map[string]<-chan chatResult{}
	next := 0
	var wantServed []string

	checkState := func(label string) {
		t.Helper()
		eventually(t, 5*time.Second, func() bool { return e.rs.waitq.Depth("qwen-7b") == len(queue) }, fmt.Sprintf("%s: depth %d", label, len(queue)))
		eventually(t, 5*time.Second, func() bool { return len(w.served()) == len(wantServed) }, fmt.Sprintf("%s: %d served", label, len(wantServed)))
		got := w.served()
		for i := range wantServed {
			if got[i] != wantServed[i] {
				t.Fatalf("%s: service order %v, model %v", label, got, wantServed)
			}
		}
		if n := e.gw.router.InFlight("w1"); n != len(inflight) || n > conc {
			t.Fatalf("%s: %d slots in flight, model %d, capacity %d", label, n, len(inflight), conc)
		}
	}

	for step := 0; step < steps; step++ {
		switch a := rng.Intn(10); {
		case a < 5: // arrival
			id := fmt.Sprintf("s%d-r%d", seed, next)
			next++
			ctx, cancel := context.WithCancel(context.Background())
			r := &req{id: id, cancel: cancel, res: e.chatAsync(ctx, id)}
			results[id] = r.res
			switch {
			case len(inflight) < conc && len(queue) == 0:
				inflight = append(inflight, r)
				wantServed = append(wantServed, id)
			case len(queue) < depth:
				queue = append(queue, r)
			default:
				res := awaitResult(t, r.res) // shed by the depth bound: it answers at once; nothing joins the model
				cancel()
				if res.resp.StatusCode != 503 {
					t.Fatalf("seed %d step %d: over the depth bound, status %d", seed, step, res.resp.StatusCode)
				}
				delete(results, id)
			}
		case a < 8: // a running request completes
			if len(inflight) == 0 {
				continue
			}
			i := rng.Intn(len(inflight))
			done := inflight[i]
			inflight = append(inflight[:i], inflight[i+1:]...)
			w.release(done.id)
			if r := awaitResult(t, done.res); r.resp.StatusCode != 200 {
				t.Fatalf("seed %d step %d: %s answered %d", seed, step, done.id, r.resp.StatusCode)
			}
			delete(results, done.id)
			if len(queue) > 0 { // the freed slot goes to the head
				head := queue[0]
				queue = queue[1:]
				inflight = append(inflight, head)
				wantServed = append(wantServed, head.id)
			}
		case a < 9: // a waiter's client leaves
			if len(queue) == 0 {
				continue
			}
			i := rng.Intn(len(queue))
			gone := queue[i]
			queue = append(queue[:i], queue[i+1:]...)
			gone.cancel()
			<-gone.res
			delete(results, gone.id)
		default: // a registry refresh with unchanged numbers
			e.refresh(t)
		}
		checkState(fmt.Sprintf("seed %d step %d", seed, step))
	}

	// Drain: everything still running completes, and the queue empties in order.
	for len(inflight) > 0 || len(queue) > 0 {
		done := inflight[0]
		inflight = inflight[1:]
		w.release(done.id)
		awaitResult(t, done.res)
		delete(results, done.id)
		if len(queue) > 0 {
			head := queue[0]
			queue = queue[1:]
			inflight = append(inflight, head)
			wantServed = append(wantServed, head.id)
		}
		checkState(fmt.Sprintf("seed %d drain", seed))
	}
	e.settle(t, "w1")
	if d := e.rs.waitq.Depth("qwen-7b"); d != 0 || e.rs.admission.Active() != 0 {
		t.Fatalf("seed %d: depth %d, global %d at the end", seed, d, e.rs.admission.Active())
	}
	if len(results) != 0 {
		t.Fatalf("seed %d: %d requests never answered", seed, len(results))
	}
}

// contentOf returns the first message's content of a chat request body at a worker.
func contentOf(r *http.Request) string {
	b, _ := io.ReadAll(r.Body)
	var v struct {
		Messages []struct{ Content string } `json:"messages"`
	}
	_ = json.Unmarshal(b, &v)
	if len(v.Messages) == 0 {
		return ""
	}
	return v.Messages[0].Content
}

// Joining a busy queue needs a worker that could serve: with every worker ineligible the arrival is refused at once
// instead of waiting behind waiters that cannot be served either, and its body does not claim a worker count.
func TestAnArrivalAtABusyQueueWithNoEligibleWorkerIsRefusedAtOnce(t *testing.T) {
	g := newGateWorker(t, "w1")
	drained := g.snap(4)
	drained.State = protocol.StateDraining
	drained.Eligible = false
	e := newResEnv(t, []protocol.WorkerSnapshot{drained}, noBreaker)
	stand, err := e.rs.waitq.Enqueue("qwen-7b", "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.rs.waitq.Leave(stand)
	began := time.Now()
	r := awaitResult(t, e.chatAsync(context.Background(), "x"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || time.Since(began) > time.Second {
		t.Fatalf("%d %s after %v", r.resp.StatusCode, r.body, time.Since(began))
	}
	if e.rs.waitq.Depth("qwen-7b") != 1 {
		t.Fatal("it must not have queued")
	}
}

// A queue that is full reports no worker count it does not know: eligible_workers is left out.
func TestAShedBodyLeavesOutAnUnknownWorkerCount(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(1, 3*time.Second))
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	q1 := e.chatAsync(context.Background(), "q1")
	e.awaitDepth(t, 1)
	r := awaitResult(t, e.chatAsync(context.Background(), "over"))
	if r.resp.StatusCode != 503 || strings.Contains(r.body, "eligible_workers") {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	g.release(t, 2)
	awaitResult(t, held)
	awaitResult(t, q1)
}

// panicSched wraps the scheduler and panics when armed.
type panicSched struct {
	inner interface {
		SelectWorker(context.Context, *protocol.InferenceRequest, []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error)
	}
	armed bool
	mu    sync.Mutex
}

func (p *panicSched) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, ws []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	p.mu.Lock()
	armed := p.armed
	p.mu.Unlock()
	if armed {
		panic("scheduler blew up")
	}
	return p.inner.SelectWorker(ctx, req, ws)
}

// A waiter whose re-route panics (here the scheduler, when a refresh wakes it) leaves the queue: nothing is left
// behind to be handed a slot nobody will take.
func TestAWaiterWhoseRerouteIsUnwoundLeavesTheQueue(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g)
	ps := &panicSched{inner: e.gw.router.sched}
	e.gw.router.sched = ps
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	doomed := e.chatAsync(context.Background(), "doomed")
	e.awaitDepth(t, 1)

	ps.mu.Lock()
	ps.armed = true
	ps.mu.Unlock()
	spare := g.snap(2) // a refresh sees a free slot and wakes the waiter, whose routing then panics
	e.lister.set([]protocol.WorkerSnapshot{spare}, nil)
	e.refresh(t)
	if r := awaitResult(t, doomed); r.resp.StatusCode != 500 {
		t.Fatalf("a panic is a 500: %d", r.resp.StatusCode)
	}
	if d := e.rs.waitq.Depth("qwen-7b"); d != 0 {
		t.Fatalf("the unwound waiter is still queued: depth %d", d)
	}
	ps.mu.Lock()
	ps.armed = false
	ps.mu.Unlock()
	g.release(t, 1)
	awaitResult(t, held)
	e.settle(t, "w1")
	if e.rs.admission.Active() != 0 {
		t.Fatal("a global place was kept")
	}
}

// A slot is handed over only to the registration it was taken on: if the worker has registered again, the slot is
// released normally and a waiter is asked to route.
func TestAHandOffChecksTheRegistrationOfTheWorker(t *testing.T) {
	g := newGateWorker(t, "w1")
	old := g.snap(1)
	old.RegisteredAt = time.Unix(1_700_000_000, 0)
	e := newResEnv(t, []protocol.WorkerSnapshot{old}, noBreaker)
	stand, err := e.rs.waitq.Enqueue("qwen-7b", "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.rs.waitq.Leave(stand)

	renewed := old
	renewed.RegisteredAt = old.RegisteredAt.Add(time.Minute)
	e.lister.set([]protocol.WorkerSnapshot{renewed}, nil)
	e.refresh(t)
	e.rs.waitq.Park(stand) // the refresh saw a free slot and woke the stand-in; put it back to waiting
	e.gw.router.mu.Lock()
	handed, wake := e.gw.router.handoffLocked(&old)
	e.gw.router.mu.Unlock()
	if handed || !wake {
		t.Fatalf("handed %v wake %v: a slot of the old registration must not be handed over", handed, wake)
	}
	e.gw.router.mu.Lock()
	handed, wake = e.gw.router.handoffLocked(&renewed)
	e.gw.router.mu.Unlock()
	if !handed || wake {
		t.Fatalf("handed %v wake %v: the current registration takes it", handed, wake)
	}
	if g := e.rs.waitq.Depth("qwen-7b"); g != 0 {
		t.Fatalf("the stand-in waiter received the slot and left: depth %d", g)
	}
}

// When a freed slot cannot be handed over (its worker registered again meanwhile), a waiter is asked to route at
// once: it is served on the new registration without waiting for the next refresh.
func TestADeclinedHandOffWakesAWaiterToRouteItself(t *testing.T) {
	g := newGateWorker(t, "w1")
	old := g.snap(1)
	old.RegisteredAt = time.Unix(1_700_000_000, 0)
	e := newResEnv(t, []protocol.WorkerSnapshot{old}, noBreaker)
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	waiter := e.chatAsync(context.Background(), "r1")
	e.awaitDepth(t, 1)
	renewed := old
	renewed.RegisteredAt = old.RegisteredAt.Add(time.Minute)
	e.lister.set([]protocol.WorkerSnapshot{renewed}, nil)
	e.refresh(t) // no free slot yet: the old request still counts for the worker ID
	g.release(t, 1)
	awaitResult(t, held)
	g.awaitArrivals(t, 2) // no further refresh: only the wake-up can have routed it
	g.release(t, 1)
	if r := awaitResult(t, waiter); r.resp.StatusCode != 200 {
		t.Fatalf("status %d", r.resp.StatusCode)
	}
	e.settle(t, "w1")
}

// A refresh wakes as many head waiters as the new view has free slots, not one: two free slots, two waiters, both
// are served without a second refresh.
func TestARefreshWakesAsManyWaitersAsThereAreFreeSlots(t *testing.T) {
	g := newGateWorker(t, "w1")
	full := g.snap(2)
	full.Metrics.ActiveRequests = 2 // stale: another gateway's requests occupy both slots
	e := newResEnv(t, []protocol.WorkerSnapshot{full}, noBreaker)
	r1 := e.chatAsync(context.Background(), "r1")
	e.awaitDepth(t, 1)
	r2 := e.chatAsync(context.Background(), "r2")
	e.awaitDepth(t, 2)
	e.lister.set([]protocol.WorkerSnapshot{g.snap(2)}, nil)
	e.refresh(t)
	g.awaitArrivals(t, 2) // both reach the worker on this one refresh
	e.awaitDepth(t, 0)
	g.release(t, 2)
	awaitResult(t, r1)
	awaitResult(t, r2)
	e.settle(t, "w1")
}

// freeSlots counts what a routing could take: slots of workers the registry offers and the breakers admit, with the
// larger of the reported and local load. A worker whose local count fills it, or whose breaker is open, adds nothing.
func TestFreeSlotsHonourLocalLoadAndBreakers(t *testing.T) {
	a, b := newGateWorker(t, "w-1"), newGateWorker(t, "w-2")
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snap(2), b.snap(2)})
	if n := e.gw.router.freeSlots("qwen-7b"); n != 4 {
		t.Fatalf("free slots %d, want 4", n)
	}
	e.gw.router.mu.Lock()
	e.gw.router.inflight["w-1"] = 2 // this gateway has two requests on w-1: reported load 0 does not matter
	e.gw.router.mu.Unlock()
	if n := e.gw.router.freeSlots("qwen-7b"); n != 2 {
		t.Fatalf("free slots %d, want 2 (w-1 is full by the local count)", n)
	}
	e.gw.router.mu.Lock()
	delete(e.gw.router.inflight, "w-1")
	e.gw.router.mu.Unlock()
	for i := 0; i < 3; i++ {
		tk, ok := e.rs.breakers.Acquire(e.key("w-1"))
		if !ok {
			t.Fatal("acquire")
		}
		e.rs.breakers.Record(tk, resilience.Fault)
	}
	if e.state("w-1") != resilience.Open {
		t.Fatalf("breaker %v", e.state("w-1"))
	}
	if n := e.gw.router.freeSlots("qwen-7b"); n != 2 {
		t.Fatalf("free slots %d, want 2 (w-1 is ejected)", n)
	}
}

// A hand-off declined because the head waiter is already routing gives the breaker ticket back: a half-open worker's
// only probe ticket must not leak, or the worker would stay ejected.
func TestADeclinedHandOffReturnsTheProbeTicket(t *testing.T) {
	a, b := newGateWorker(t, "w-1"), newGateWorker(t, "w-2")
	sa := a.snap(2)
	e := newResEnv(t, []protocol.WorkerSnapshot{sa, b.snap(2)})
	for i := 0; i < 3; i++ {
		tk, ok := e.rs.breakers.Acquire(e.key("w-1"))
		if !ok {
			t.Fatal("acquire")
		}
		e.rs.breakers.Record(tk, resilience.Fault)
	}
	e.advance(t, 11*time.Second) // past the cool-down: the next Acquire is the half-open probe
	stand, err := e.rs.waitq.Enqueue("qwen-7b", "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.rs.waitq.Leave(stand)
	e.rs.waitq.Wake("qwen-7b", 1) // the head is routing: it cannot receive a hand-off
	e.gw.router.mu.Lock()
	handed, wake := e.gw.router.handoffLocked(&sa)
	e.gw.router.mu.Unlock()
	if handed || wake {
		t.Fatalf("handed %v wake %v: a routing head declines the hand-off", handed, wake)
	}
	for _, in := range e.rs.breakers.Snapshot() {
		if in.Probes != 0 {
			t.Fatalf("%d probe ticket(s) of %s were not returned", in.Probes, in.Key.ID)
		}
	}
}
