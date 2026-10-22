package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/pkg/protocol"
)

// Phase 15 PR E (D11, D12; acceptance 12, 13, 14, 15): the admission queue, the hand-off, the global cap, the tenant
// share and the shed responses at the gateway. The library tests the queue's own invariants; these tests pin where
// the gateway enqueues, hands over, sheds and reports. Waits are real (a timer decides them), so the tests use the
// 100 ms floor of queue.max_wait where they need a timeout and 3 s where they must not time out.

// gateWorker is a worker with a concurrency of 1 whose answers a test releases one at a time, and which records the
// content of the requests in the order they reach it.
type gateWorker struct {
	*scriptWorker
	gate  chan struct{}
	done  chan struct{}
	mu    sync.Mutex
	order []string
}

func newGateWorker(t *testing.T, id string) *gateWorker {
	t.Helper()
	g := &gateWorker{gate: make(chan struct{}), done: make(chan struct{})}
	g.scriptWorker = newScriptWorker(t, id, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var v struct {
			Messages []struct{ Content string } `json:"messages"`
		}
		_ = json.Unmarshal(b, &v)
		c := ""
		if len(v.Messages) > 0 {
			c = v.Messages[0].Content
		}
		g.mu.Lock()
		g.order = append(g.order, c)
		g.mu.Unlock()
		select {
		case <-g.gate:
		case <-g.done:
			return
		case <-r.Context().Done():
			return
		}
		okJSON(id)(w, r)
	})
	t.Cleanup(func() { close(g.done) }) // runs before the server's own Close: no handler outlives the test
	return g
}

func (g *gateWorker) snap(conc int) protocol.WorkerSnapshot {
	s := g.snapshot("qwen-7b")
	s.MaxConcurrency = conc
	return s
}

// release lets n waiting requests answer.
func (g *gateWorker) release(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case g.gate <- struct{}{}:
		case <-time.After(5 * time.Second):
			t.Fatal("no request is waiting at the worker")
		}
	}
}

func (g *gateWorker) arrivals() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.order...)
}

func (g *gateWorker) awaitArrivals(t *testing.T, n int) {
	t.Helper()
	eventually(t, 5*time.Second, func() bool { return len(g.arrivals()) >= n }, fmt.Sprintf("%d requests at the worker", n))
}

type chatResult struct {
	resp *http.Response
	body string
	err  error
}

// chatAsync posts one chat request (content distinguishes requests at the worker) and delivers the answer.
func (e *resEnv) chatAsync(ctx context.Context, content string, hdr ...string) <-chan chatResult {
	out := make(chan chatResult, 1)
	go func() {
		body := `{"model":"qwen-7b","messages":[{"role":"user","content":"` + content + `"}]}`
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+chatCompletionsPath, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		resp, err := e.client.Do(req)
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

func awaitResult(t *testing.T, c <-chan chatResult) chatResult {
	t.Helper()
	select {
	case r := <-c:
		if r.err != nil {
			t.Fatalf("request failed: %v", r.err)
		}
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("no answer")
		return chatResult{}
	}
}

func (e *resEnv) awaitDepth(t *testing.T, n int) {
	t.Helper()
	eventually(t, 5*time.Second, func() bool { return e.rs.waitq.Depth("qwen-7b") == n }, fmt.Sprintf("queue depth %d", n))
}

func queueEnv(t *testing.T, g *gateWorker, mutate ...func(*config.Config)) *resEnv {
	t.Helper()
	return newResEnv(t, []protocol.WorkerSnapshot{g.snap(1)}, append([]func(*config.Config){noBreaker}, mutate...)...)
}

func queueCfg(depth int, wait time.Duration) func(*config.Config) {
	return func(c *config.Config) { c.Resilience.Queue.MaxModelDepth, c.Resilience.Queue.MaxWait = depth, wait }
}

// reqSeq records the observer calls of each request separately, by request ID, so a test can pin the sequence of one
// request while others run.
type reqSeq struct {
	NopObserver
	mu sync.Mutex
	m  map[string][]string
}

func (o *reqSeq) add(id, tok string) {
	o.mu.Lock()
	if o.m == nil {
		o.m = map[string][]string{}
	}
	o.m[id] = append(o.m[id], tok)
	o.mu.Unlock()
}

func (o *reqSeq) RequestStarted(ctx context.Context, e RequestStart) context.Context {
	o.add(e.ID, "start")
	return ctx
}
func (o *reqSeq) RequestAdmitted(_ context.Context, e Admission) { o.add(e.RequestID, "admit") }
func (o *reqSeq) RequestRejected(_ context.Context, e Rejection) {
	o.add(e.RequestID, fmt.Sprintf("reject:%s:%s:%d", e.Kind, e.Reason, e.Status))
}
func (o *reqSeq) AttemptStarted(ctx context.Context, e AttemptStart) context.Context {
	o.add(e.RequestID, fmt.Sprintf("attempt+%d:%s", e.Number, e.WorkerID))
	return ctx
}
func (o *reqSeq) AttemptEnded(_ context.Context, e AttemptEnd) {
	o.add(e.RequestID, fmt.Sprintf("attempt-%d:%s:%s", e.Number, e.WorkerID, e.Outcome))
}
func (o *reqSeq) QueueWaited(_ context.Context, e QueueWait) { o.add(e.RequestID, "queue:"+e.Result) }
func (o *reqSeq) RequestCompleted(_ context.Context, e Completion) {
	o.add(e.RequestID, fmt.Sprintf("done:%d", e.Status))
}

// sequenceWith returns the sequence of the request whose recorded tokens include marker (and waits for its
// completion).
func (o *reqSeq) sequenceWith(t *testing.T, marker string) []string {
	t.Helper()
	var out []string
	eventually(t, 5*time.Second, func() bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		for _, seq := range o.m {
			for _, tok := range seq {
				if tok == marker && strings.HasPrefix(seq[len(seq)-1], "done:") {
					out = append([]string(nil), seq...)
					return true
				}
			}
		}
		return false
	}, "the sequence containing "+marker)
	return out
}

func retryAfter(t *testing.T, r chatResult) string {
	t.Helper()
	return r.resp.Header.Get("Retry-After")
}

// Every eligible worker is full: the request waits, and the slot the first request frees is handed to it directly,
// even though the count the worker reported is stale and still says full (no refresh is needed or has happened).
func TestAWaiterIsHandedTheFreedSlotEvenWhenTheReportedCountIsStale(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g)
	seq := &reqSeq{}
	WithObserver(seq)(e.gw)

	r1 := e.chatAsync(context.Background(), "r1")
	g.awaitArrivals(t, 1)
	r2 := e.chatAsync(context.Background(), "r2")
	e.awaitDepth(t, 1)

	// The worker's heartbeat now reports the request our gateway sent: reported 1 of 1, local 1 of 1.
	stale := g.snap(1)
	stale.Metrics.ActiveRequests = 1
	e.lister.set([]protocol.WorkerSnapshot{stale}, nil)
	e.refresh(t)
	if got := g.arrivals(); len(got) != 1 {
		t.Fatalf("a refresh with no free slot must not release the waiter: %v", got)
	}

	g.release(t, 1)
	if r := awaitResult(t, r1); r.resp.StatusCode != 200 {
		t.Fatalf("r1: %d", r.resp.StatusCode)
	}
	// r2 reaches the worker although reported (1) and local (0) arithmetic would still call the worker full if it
	// had to route again: the slot went to it under the router's lock.
	g.awaitArrivals(t, 2)
	g.release(t, 1)
	r := awaitResult(t, r2)
	if r.resp.StatusCode != 200 || whoAnswered(t, r.body) != "w1" {
		t.Fatalf("r2: %d %s", r.resp.StatusCode, r.body)
	}
	if got := g.arrivals(); fmt.Sprint(got) != "[r1 r2]" {
		t.Fatalf("arrival order %v", got)
	}
	wantSeq(t, seq.sequenceWith(t, "queue:served"), "start", "admit", "queue:served", "attempt+1:w1", "attempt-1:w1:ok", "done:200")
	starts := e.rec.attemptStarts()
	if len(starts) != 2 || starts[0].QueueWait != 0 || starts[1].QueueWait < 5*time.Millisecond {
		t.Fatalf("only the request that waited carries a queue wait: %+v", starts)
	}
	e.settle(t, "w1")
	if d := e.rs.waitq.Depth("qwen-7b"); d != 0 {
		t.Fatalf("depth %d", d)
	}
	if a := e.rs.admission.Active(); a != 0 {
		t.Fatalf("global slots still held: %d", a)
	}
}

// Strict FIFO: the requests are served in the order they arrived.
func TestWaitersAreServedInArrivalOrder(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g)
	var chans []<-chan chatResult
	chans = append(chans, e.chatAsync(context.Background(), "r0"))
	g.awaitArrivals(t, 1)
	for i := 1; i <= 4; i++ {
		chans = append(chans, e.chatAsync(context.Background(), fmt.Sprintf("r%d", i)))
		e.awaitDepth(t, i)
	}
	for i := 1; i <= 5; i++ {
		g.release(t, 1)
		if i < 5 {
			g.awaitArrivals(t, i+1)
		}
	}
	for _, c := range chans {
		if r := awaitResult(t, c); r.resp.StatusCode != 200 {
			t.Fatalf("status %d", r.resp.StatusCode)
		}
	}
	if got := fmt.Sprint(g.arrivals()); got != "[r0 r1 r2 r3 r4]" {
		t.Fatalf("service order %s", got)
	}
}

// An arrival that finds waiters must queue behind them even when a worker has room: nobody jumps the queue. The
// waiter ahead is a stand-in that is never served; the arrival is released by the next refresh.
func TestAnArrivalQueuesBehindWaitersEvenWithFreeCapacity(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := newResEnv(t, []protocol.WorkerSnapshot{g.snap(4)}, noBreaker)
	stand, err := e.rs.waitq.Enqueue("qwen-7b", "")
	if err != nil {
		t.Fatal(err)
	}
	r1 := e.chatAsync(context.Background(), "r1")
	e.awaitDepth(t, 2)
	time.Sleep(100 * time.Millisecond)
	if n := len(g.arrivals()); n != 0 {
		t.Fatalf("the arrival bypassed the queue (%d requests at the worker)", n)
	}
	e.rs.waitq.Leave(stand)
	e.refresh(t) // the refresh sees free slots and wakes the head
	g.awaitArrivals(t, 1)
	g.release(t, 1)
	if r := awaitResult(t, r1); r.resp.StatusCode != 200 {
		t.Fatalf("status %d", r.resp.StatusCode)
	}
}

// The depth bound: requests beyond it are refused at once with 503 NO_CAPACITY and Retry-After, never 429, before the
// rate limiter, so they spend no quota. The queue and its waiters are unharmed.
func TestARequestOverTheQueueDepthIsShedBeforeTheLimiter(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(2, 3*time.Second))
	lim := &fakeLimiter{}
	e.gw.SetLimiter(lim)
	seq := &reqSeq{}
	WithObserver(seq)(e.gw)

	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	q1 := e.chatAsync(context.Background(), "q1")
	e.awaitDepth(t, 1)
	q2 := e.chatAsync(context.Background(), "q2")
	e.awaitDepth(t, 2)
	if n := len(lim.calls()); n != 3 {
		t.Fatalf("three requests were charged: %d", n)
	}

	for i := 0; i < 3; i++ {
		r := awaitResult(t, e.chatAsync(context.Background(), fmt.Sprintf("shed%d", i)))
		if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || retryAfter(t, r) != "1" {
			t.Fatalf("shed: %d %s Retry-After=%q", r.resp.StatusCode, r.body, retryAfter(t, r))
		}
		var b struct {
			Error struct{ Message, Type, Code, Model string } `json:"error"`
		}
		_ = json.Unmarshal([]byte(r.body), &b)
		if b.Error.Type != "api_error" || b.Error.Model != "qwen-7b" || !strings.Contains(b.Error.Message, "queue") {
			t.Fatalf("the body keeps the NO_CAPACITY shape and names the reason: %s", r.body)
		}
	}
	if n := len(lim.calls()); n != 3 {
		t.Fatalf("a shed request spent quota: %d limiter calls", n)
	}
	wantSeq(t, seq.sequenceWith(t, "reject:overload:queue_full:503"), "start", "admit", "reject:overload:queue_full:503", "done:503")
	want(t, e.gw.metrics, 3, "admission_rejections_total", "model=qwen-7b", "reason=queue_full")
	if _, ok := sample(t, e.gw.metrics, "scheduler_decisions_total", "result=no_capacity"); ok {
		t.Fatal("a shed is admission, never a scheduling decision")
	}
	if _, ok := sample(t, e.gw.metrics, "inference_requests_total", "status=429"); ok {
		t.Fatal("overload is never a 429")
	}
	g.release(t, 3)
	for _, c := range []<-chan chatResult{held, q1, q2} {
		if r := awaitResult(t, c); r.resp.StatusCode != 200 {
			t.Fatalf("status %d", r.resp.StatusCode)
		}
	}
	e.settle(t, "w1")
	if got := fmt.Sprint(g.arrivals()); got != "[r0 q1 q2]" {
		t.Fatalf("service order %s", got)
	}
}

// A wait that runs out is shed after the quota was charged (not refunded): QueueWaited(timeout) then the overload
// rejection, 503 NO_CAPACITY with Retry-After from the recent waits, and the queue is left empty.
func TestAWaiterPastMaxWaitIsShedWithTheRetryAfterOfRecentWaits(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(8, 100*time.Millisecond))
	lim := &fakeLimiter{}
	e.gw.SetLimiter(lim)
	seq := &reqSeq{}
	WithObserver(seq)(e.gw)
	e.rs.waitq.ObserveWait(4200 * time.Millisecond) // recent waits were long: Retry-After rounds the average up

	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	began := time.Now()
	r := awaitResult(t, e.chatAsync(context.Background(), "late"))
	took := time.Since(began)
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	if took < 90*time.Millisecond || took > 2*time.Second {
		t.Fatalf("max_wait 100ms: the answer took %v", took)
	}
	// 4.2 s then a 0.1 s wait: 0.2*0.1 + 0.8*4.2 = 3.38 s, rounded up.
	if ra := retryAfter(t, r); ra != "4" {
		t.Fatalf("Retry-After %q", ra)
	}
	wantSeq(t, seq.sequenceWith(t, "queue:timeout"), "start", "admit", "queue:timeout", "reject:overload:queue_timeout:503", "done:503")
	if n := len(lim.calls()); n != 2 {
		t.Fatalf("the shed request was charged and is not refunded: %d limiter calls", n)
	}
	if d := e.rs.waitq.Depth("qwen-7b"); d != 0 {
		t.Fatalf("the waiter must have left the queue: %d", d)
	}
	waits := e.rec.queueWaits()
	if len(waits) != 1 || waits[0].Result != QueueTimeout || waits[0].Waited < 90*time.Millisecond {
		t.Fatalf("%+v", waits)
	}
	want(t, e.gw.metrics, 1, "admission_rejections_total", "model=qwen-7b", "reason=queue_timeout")
	want(t, e.gw.metrics, 1, "inference_queue_duration_seconds", "model=qwen-7b")
	if got, _ := sample(t, e.gw.metrics, "scheduler_decisions_total", "result=selected"); got != 1 {
		t.Fatalf("only the first request was scheduled: %v", got)
	}
	g.release(t, 1)
	awaitResult(t, held)
	e.settle(t, "w1")
	if lim.LocalLeases() != 0 {
		t.Fatalf("a queued request holds its concurrency lease and must give it back: %d", lim.LocalLeases())
	}
}

// A client that leaves while queued leaves the queue at once and is recorded as gone, not shed.
func TestAClientThatLeavesWhileQueuedLeavesTheQueue(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g)
	lim := &fakeLimiter{}
	e.gw.SetLimiter(lim)
	seq := &reqSeq{}
	WithObserver(seq)(e.gw)

	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	gone := e.chatAsync(ctx, "gone")
	e.awaitDepth(t, 1)
	cancel()
	<-gone
	e.awaitDepth(t, 0)
	wantSeq(t, seq.sequenceWith(t, "queue:client_gone"), "start", "admit", "queue:client_gone", "done:499")
	eventually(t, 3*time.Second, func() bool { return lim.LocalLeases() == 1 }, "the abandoned request's lease is released")
	if _, ok := sample(t, e.gw.metrics, "admission_rejections_total"); ok {
		t.Fatal("a client that left was not shed")
	}
	g.release(t, 1)
	awaitResult(t, held)
	e.settle(t, "w1")
	if n := len(g.arrivals()); n != 1 {
		t.Fatalf("the departed request must never reach the worker: %v", g.arrivals())
	}
}

// A registry refresh that corrects the counts wakes the waiters it can now serve, in order.
func TestARefreshThatFreesCapacityWakesTheWaiters(t *testing.T) {
	g := newGateWorker(t, "w1")
	full := g.snap(1)
	full.Metrics.ActiveRequests = 1 // the first view is stale: another gateway's request occupies the slot
	e := newResEnv(t, []protocol.WorkerSnapshot{full}, noBreaker)
	r1 := e.chatAsync(context.Background(), "r1")
	e.awaitDepth(t, 1)
	time.Sleep(50 * time.Millisecond)
	if len(g.arrivals()) != 0 {
		t.Fatal("the worker looked full")
	}
	e.lister.set([]protocol.WorkerSnapshot{g.snap(1)}, nil)
	began := time.Now()
	e.refresh(t)
	g.awaitArrivals(t, 1)
	if time.Since(began) > time.Second {
		t.Fatalf("the woken waiter took %v", time.Since(began))
	}
	g.release(t, 1)
	if r := awaitResult(t, r1); r.resp.StatusCode != 200 {
		t.Fatalf("status %d", r.resp.StatusCode)
	}
}

// The global cap counts queued and running requests from before the body is read; over it a request is refused at
// once with 503 NO_CAPACITY (global_limit), Retry-After 1 whatever the queue's recent waits were, and no quota.
func TestTheGlobalCapRefusesNewArrivalsAtOnce(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, func(c *config.Config) { c.Admission.MaxGlobalRequests = 2 })
	lim := &fakeLimiter{}
	e.gw.SetLimiter(lim)
	seq := &reqSeq{}
	WithObserver(seq)(e.gw)
	e.rs.waitq.ObserveWait(9 * time.Second)

	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	queued := e.chatAsync(context.Background(), "q1")
	e.awaitDepth(t, 1)
	if a := e.rs.admission.Active(); a != 2 {
		t.Fatalf("one running and one queued request count: %d", a)
	}
	r := awaitResult(t, e.chatAsync(context.Background(), "over"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || retryAfter(t, r) != "1" {
		t.Fatalf("%d %s Retry-After=%q", r.resp.StatusCode, r.body, retryAfter(t, r))
	}
	if !r.resp.Close && r.resp.Header.Get("Connection") != "close" {
		t.Fatal("the body was not read, so the connection must close")
	}
	if strings.Contains(r.body, "eligible_workers") || strings.Contains(r.body, `"model"`) {
		t.Fatalf("nothing is known about the model before the body is read: %s", r.body)
	}
	if n := len(lim.calls()); n != 2 {
		t.Fatalf("the refused request must spend no quota: %d limiter calls", n)
	}
	wantSeq(t, seq.sequenceWith(t, "reject:overload:global_limit:503"), "start", "reject:overload:global_limit:503", "done:503")
	want(t, e.gw.metrics, 1, "admission_rejections_total", "model=unknown", "reason=global_limit")

	g.release(t, 2)
	awaitResult(t, held)
	awaitResult(t, queued)
	eventually(t, 3*time.Second, func() bool { return e.rs.admission.Active() == 0 }, "the global slots come back")
	g.release(t, 0)
	again := e.chatAsync(context.Background(), "again")
	g.awaitArrivals(t, 3)
	g.release(t, 1)
	if r := awaitResult(t, again); r.resp.StatusCode != 200 {
		t.Fatalf("after the load went away: %d", r.resp.StatusCode)
	}
}

// The cap counts from the start of the handler, body read included: a client that is still sending its body holds a
// place.
func TestTheGlobalCapCountsARequestStillSendingItsBody(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, func(c *config.Config) { c.Admission.MaxGlobalRequests = 1 })
	conn, err := net.Dial("tcp", strings.TrimPrefix(e.url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{\"model\":", chatCompletionsPath)
	eventually(t, 3*time.Second, func() bool { return e.rs.admission.Active() == 1 }, "the slow request holds a place")
	r := awaitResult(t, e.chatAsync(context.Background(), "second"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	_ = conn.Close()
	eventually(t, 5*time.Second, func() bool { return e.rs.admission.Active() == 0 }, "the place comes back when the client goes")
}

// The tenant guard: one tenant cannot fill a model's queue (it gets its share, then queue_full) while another
// tenant can still queue. Overload is a 503; the tenant's own quota stays the 429.
func TestOneTenantCannotFillTheQueue(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(4, 3*time.Second)) // share 0.5 of 4 = 2
	store := newAuthStore()
	keyA, keyB := store.add("a", nil), store.add("b", nil)
	e.gw.SetAuthenticator(newAuthenticator(store, e.logs, &testClock{t: time.Now()}))
	ha, hb := []string{"Authorization", "Bearer " + keyA}, []string{"Authorization", "Bearer " + keyB}

	held := e.chatAsync(context.Background(), "a0", ha...)
	g.awaitArrivals(t, 1)
	a1 := e.chatAsync(context.Background(), "a1", ha...)
	e.awaitDepth(t, 1)
	a2 := e.chatAsync(context.Background(), "a2", ha...)
	e.awaitDepth(t, 2)
	r := awaitResult(t, e.chatAsync(context.Background(), "a3", ha...))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" {
		t.Fatalf("the third queued request of one tenant is shed: %d %s", r.resp.StatusCode, r.body)
	}
	b1 := e.chatAsync(context.Background(), "b1", hb...)
	e.awaitDepth(t, 3)
	want(t, e.gw.metrics, 1, "admission_rejections_total", "model=qwen-7b", "reason=queue_full")
	g.release(t, 4)
	for _, c := range []<-chan chatResult{held, a1, a2, b1} {
		if r := awaitResult(t, c); r.resp.StatusCode != 200 {
			t.Fatalf("status %d", r.resp.StatusCode)
		}
	}
	if got := fmt.Sprint(g.arrivals()); got != "[a0 a1 a2 b1]" {
		t.Fatalf("service order %s", got)
	}
}

// The gateway overhead histogram excludes the queue wait: a request that waited 300 ms adds the wait to
// inference_queue_duration_seconds and nothing like it to the overhead.
func TestTheOverheadHistogramExcludesTheQueueWait(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g)
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	waiter := e.chatAsync(context.Background(), "r1")
	e.awaitDepth(t, 1)
	time.Sleep(300 * time.Millisecond)
	g.release(t, 1)
	g.awaitArrivals(t, 2)
	g.release(t, 1)
	awaitResult(t, held)
	awaitResult(t, waiter)
	e.settle(t, "w1")
	want(t, e.gw.metrics, 2, "inference_gateway_overhead_seconds", "model=qwen-7b")
	if s, _ := histSum(t, e.gw.metrics, "inference_gateway_overhead_seconds", "model=qwen-7b"); s > 0.1 {
		t.Fatalf("the overhead of two requests sums to %.3fs: the wait leaked into it", s)
	}
	want(t, e.gw.metrics, 1, "inference_queue_duration_seconds", "model=qwen-7b")
	if s, _ := histSum(t, e.gw.metrics, "inference_queue_duration_seconds", "model=qwen-7b"); s < 0.3 {
		t.Fatalf("the queue histogram sums to %.3fs, want at least the 0.3s waited", s)
	}
}

// A retry never waits: when the only other worker is full the failed first attempt's response is relayed at once,
// and the retry is not queued.
func TestARetryNeverWaitsInTheQueue(t *testing.T) {
	bad := newScriptWorker(t, "a-1", openAPI503("mock_failure"))
	other := newScriptWorker(t, "b-2", okJSON("b-2"))
	sb := other.snapshot("qwen-7b")
	sb.MaxConcurrency = 1
	e := newResEnv(t, []protocol.WorkerSnapshot{bad.snapshot("qwen-7b"), sb}, noBreaker)
	e.gw.router.inflight["b-2"] = 1 // a request of this gateway occupies the second worker's only slot

	began := time.Now()
	r := awaitResult(t, e.chatAsync(context.Background(), "first"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "mock_failure" {
		t.Fatalf("with the other worker full the failed response is relayed: %d %s", r.resp.StatusCode, r.body)
	}
	if time.Since(began) > time.Second {
		t.Fatalf("a retry waited: %v", time.Since(began))
	}
	if n := other.hits.Load(); n != 0 {
		t.Fatalf("the retry reached the full worker: %d", n)
	}
	if got := e.rec.attemptEnds(); len(got) != 1 || got[0].NextRouteResult != NextRouteNoCapacity {
		t.Fatalf("%+v", got)
	}
	if len(e.rec.queueWaits()) != 0 {
		t.Fatal("a retry must not join the queue")
	}
}

// With queue.max_model_depth 0 there is no queue: the refusal is the classic NO_CAPACITY, counted as a scheduling
// decision, not an overload shed.
func TestWithoutAQueueAFullModelIsRefusedAsBefore(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(0, 3*time.Second))
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	r := awaitResult(t, e.chatAsync(context.Background(), "r1"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || retryAfter(t, r) != "1" {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	want(t, e.gw.metrics, 1, "scheduler_decisions_total", "result=no_capacity")
	if _, ok := sample(t, e.gw.metrics, "admission_rejections_total"); ok {
		t.Fatal("no queue, no overload shed")
	}
	g.release(t, 1)
	awaitResult(t, held)
}

// The queue stops taking waiters when the gateway drains, and whoever is still waiting when the shutdown budget is
// nearly spent is answered 503 NO_CAPACITY (draining) with QueueWaited(drained).
func TestDrainingStopsQueueingAndFlushesTheWaiters(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(8, 10*time.Second))
	seq := &reqSeq{}
	WithObserver(seq)(e.gw)
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	waiter := e.chatAsync(context.Background(), "w1")
	e.awaitDepth(t, 1)

	stop := e.rs.beginDrain(drainFlushMargin + 200*time.Millisecond)
	defer stop()
	r := awaitResult(t, e.chatAsync(context.Background(), "late"))
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || retryAfter(t, r) != "1" {
		t.Fatalf("an arrival that would have to wait is shed as draining: %d %s", r.resp.StatusCode, r.body)
	}
	began := time.Now()
	w := awaitResult(t, waiter)
	if w.resp.StatusCode != 503 || time.Since(began) > 3*time.Second {
		t.Fatalf("the waiter is flushed when the budget is nearly spent: %d after %v", w.resp.StatusCode, time.Since(began))
	}
	wantSeq(t, seq.sequenceWith(t, "queue:drained"), "start", "admit", "queue:drained", "reject:overload:draining:503", "done:503")
	want(t, e.gw.metrics, 2, "admission_rejections_total", "model=qwen-7b", "reason=draining")
	g.release(t, 1)
	awaitResult(t, held)
	e.settle(t, "w1")
}

// A request that panics right after the queue handed it a slot (the recorder can; observers are contained) must not
// leave a waiter, a slot or a global place behind.
func TestAPanicAfterTheHandOffLeavesNoWaiterAndNoSlot(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g)
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	e.gw.recorder = panicRecorder{}
	boom := e.chatAsync(context.Background(), "boom")
	e.awaitDepth(t, 1)
	g.release(t, 1)
	awaitResult(t, held)
	if r := awaitResult(t, boom); r.resp.StatusCode != 500 {
		t.Fatalf("a panic is a 500: %d", r.resp.StatusCode)
	}
	e.gw.recorder = nil
	e.settle(t, "w1")
	eventually(t, 3*time.Second, func() bool { return e.rs.waitq.Depth("qwen-7b") == 0 && e.rs.admission.Active() == 0 }, "no waiter, no global place")
	again := e.chatAsync(context.Background(), "again")
	g.awaitArrivals(t, 2)
	g.release(t, 1)
	if r := awaitResult(t, again); r.resp.StatusCode != 200 {
		t.Fatalf("the slot must be usable again: %d", r.resp.StatusCode)
	}
}

type panicRecorder struct{}

func (panicRecorder) Record(string, string, string, string, int) { panic("recorder blew up") }

// Shed log lines are rate limited to one per second per reason, with the count of those held back.
func TestShedLogLinesAreRateLimitedPerReason(t *testing.T) {
	var l shedLimiter
	t0 := time.Unix(1_700_000_000, 0)
	if ok, n := l.allow("queue_full", t0); !ok || n != 0 {
		t.Fatal("the first shed is logged")
	}
	for i := 0; i < 5; i++ {
		if ok, _ := l.allow("queue_full", t0.Add(time.Duration(i)*100*time.Millisecond)); ok {
			t.Fatal("within a second only one line per reason")
		}
	}
	if ok, n := l.allow("global_limit", t0.Add(200*time.Millisecond)); !ok || n != 0 {
		t.Fatal("reasons are limited separately")
	}
	if ok, n := l.allow("queue_full", t0.Add(1100*time.Millisecond)); !ok || n != 5 {
		t.Fatalf("after a second the line reports the %d held back", n)
	}
}

// The queue's events cost the metrics observer no allocation in the steady state, like every request-path method.
func TestTheQueueEventsAllocateNothingInTheSteadyState(t *testing.T) {
	m := newMetrics()
	m.setStrategy("least-active")
	m.configure(config.Default().Metrics)
	cfg := config.Default()
	cfg.Resilience.Mode = config.ResilienceOn
	m.enableResilience(newResilienceState(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), resilienceDeps{}))
	step := func() {
		m.QueueWaited(bg, QueueWait{RequestID: "r", Model: "qwen-7b", Waited: 200 * time.Millisecond, Result: QueueServed})
		m.AttemptStarted(bg, AttemptStart{RequestID: "r", Number: 1, WorkerID: "w1", Model: "qwen-7b", Strategy: "least-active", SinceRequestStart: 300 * time.Millisecond, QueueWait: 200 * time.Millisecond, WorkerState: "READY"})
		m.RequestRejected(bg, Rejection{RequestID: "r", Kind: RejectOverload, Reason: OverloadQueueFull, Status: 503, Model: "qwen-7b"})
	}
	step() // resolve the children
	if n := testing.AllocsPerRun(200, step); n != 0 {
		t.Fatalf("%v allocations per queue event sequence", n)
	}
}

// The first-byte deadline covers the wait (D6): a request that waited most of its deadline in the queue has what is
// left for its attempt, and is cut with 504 UPSTREAM_TIMEOUT when the worker is slow.
func TestTheFirstByteDeadlineIncludesTheQueueWait(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, func(c *config.Config) { c.Resilience.Retry.FirstByteDeadline = 2 * time.Second })
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	waiter := e.chatAsync(context.Background(), "r1")
	e.awaitDepth(t, 1)
	e.clock.Advance(1700 * time.Millisecond) // the deadline's clock: 300 ms are left once the wait is over
	g.release(t, 1)
	awaitResult(t, held)
	g.awaitArrivals(t, 2)
	began := time.Now()
	r := awaitResult(t, waiter) // the worker never answers r1: only the deadline can end it
	if r.resp.StatusCode != 504 || errorCode(t, r.body) != "UPSTREAM_TIMEOUT" {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	if took := time.Since(began); took > 1500*time.Millisecond {
		t.Fatalf("the attempt ran %v: the 300 ms that remained after the wait was not applied", took)
	}
	e.settle(t, "w1")
}

// Serve begins the queue's drain when its context ends: new arrivals stop queueing, and a waiter that cannot finish
// before the shutdown budget is spent is answered 503 NO_CAPACITY (draining) instead of being cut.
func TestServeFlushesTheQueueNearTheEndOfTheShutdownBudget(t *testing.T) {
	g := newGateWorker(t, "w1")
	e := queueEnv(t, g, queueCfg(8, 20*time.Second), func(c *config.Config) { c.Gateway.ShutdownTimeout = 900 * time.Millisecond })
	e.gw.cfg.ShutdownTimeout = 900 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- e.gw.Serve(ctx, ln) }()
	url := "http://" + ln.Addr().String()
	post := func(content string) <-chan chatResult {
		out := make(chan chatResult, 1)
		go func() {
			body := `{"model":"qwen-7b","messages":[{"role":"user","content":"` + content + `"}]}`
			resp, err := (&http.Client{Timeout: 20 * time.Second}).Post(url+chatCompletionsPath, "application/json", strings.NewReader(body))
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
	held := post("r0")
	g.awaitArrivals(t, 1)
	waiter := post("w")
	e.awaitDepth(t, 1)
	began := time.Now()
	cancel()
	r := awaitResult(t, waiter)
	if r.resp.StatusCode != 503 || errorCode(t, r.body) != "NO_CAPACITY" || !strings.Contains(r.body, "shutting down") {
		t.Fatalf("%d %s", r.resp.StatusCode, r.body)
	}
	if took := time.Since(began); took < 400*time.Millisecond || took > 2*time.Second {
		t.Fatalf("the waiter was flushed after %v, want near 900ms minus the flush margin", took)
	}
	if r.resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("Retry-After %q", r.resp.Header.Get("Retry-After"))
	}
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
	<-held
}

// Many waiters that all give up leave nothing behind: no waiter, no slot, no global place, and the goroutines of the
// requests are gone.
func TestManyCancelledWaitersLeaveNothingBehind(t *testing.T) {
	g := newGateWorker(t, "w1")
	const n = 120
	e := queueEnv(t, g, queueCfg(n+10, 30*time.Second), func(c *config.Config) { c.Resilience.Queue.MaxTenantShare = 0 })
	held := e.chatAsync(context.Background(), "r0")
	g.awaitArrivals(t, 1)
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	var all []<-chan chatResult
	for i := 0; i < n; i++ {
		all = append(all, e.chatAsync(ctx, fmt.Sprintf("c%d", i)))
	}
	e.awaitDepth(t, n)
	cancel()
	for _, c := range all {
		<-c
	}
	e.awaitDepth(t, 0)
	g.release(t, 1)
	awaitResult(t, held)
	e.settle(t, "w1")
	eventually(t, 5*time.Second, func() bool { return e.rs.admission.Active() == 0 }, "global places")
	e.client.CloseIdleConnections()
	eventually(t, 10*time.Second, func() bool { return runtime.NumGoroutine() <= base+30 }, fmt.Sprintf("goroutines back near %d", base))
	if got := g.arrivals(); len(got) != 1 {
		t.Fatalf("a cancelled waiter reached the worker: %v", got)
	}
}
