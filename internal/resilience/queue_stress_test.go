package resilience

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/testwait"
)

// simRouter is a stand-in for the gateway router: one mutex, a slot count, and the rules the queue exists for.
// Arrivals take a free slot only when the model is not busy; otherwise they enqueue. A release hands the slot to
// the head waiter under the same mutex.
type handed struct{ token, seq int64 }

type simRouter struct {
	mu       sync.Mutex
	free     int
	capacity int
	q        *Queue

	inflight atomic.Int64
	peak     atomic.Int64
	serial   atomic.Int64

	handed   []handed // each hand-off: its token (assigned under mu, so in hand-off order) and the receiver's enqueue sequence
	seqOf    map[*Waiter]int64
	nextSeq  int64
	tokens   sync.Map // hand-off token -> struct{}: a token seen twice is a double hand-off
	dupes    atomic.Int64
	served   atomic.Int64
	timedOut atomic.Int64
	gone     atomic.Int64
	flushed  atomic.Int64
}

func newSimRouter(capacity int, q *Queue) *simRouter {
	return &simRouter{free: capacity, capacity: capacity, q: q, seqOf: map[*Waiter]int64{}}
}

func (s *simRouter) use() {
	n := s.inflight.Add(1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	runtime.Gosched()
	s.inflight.Add(-1)
}

// release frees a slot: handed to the head waiter if there is one, else back to the pool.
func (s *simRouter) release(model string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := s.serial.Add(1)
	if s.q.Handoff(model, token) {
		return // the slot stays taken; its new owner is the waiter
	}
	s.free++
}

// request runs one request through the sim and reports how it ended.
func (s *simRouter) request(ctx context.Context, model, tenant string, wait time.Duration, rerouteOK bool) {
	s.mu.Lock()
	if !s.q.Busy(model) && s.free > 0 {
		s.free--
		s.mu.Unlock()
		s.served.Add(1)
		s.use()
		s.release(model)
		return
	}
	w, err := s.q.Enqueue(model, tenant)
	if err != nil {
		s.mu.Unlock()
		return // shed at once
	}
	s.nextSeq++
	s.seqOf[w] = s.nextSeq
	s.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		g, out := s.q.Wait(ctx, w, timer.C)
		switch out {
		case TimedOut:
			s.timedOut.Add(1)
			return
		case ClientGone:
			s.gone.Add(1)
			return
		}
		switch g.Kind {
		case Handoff:
			if _, dup := s.tokens.LoadOrStore(g.Value, struct{}{}); dup {
				s.dupes.Add(1)
			}
			s.mu.Lock()
			s.handed = append(s.handed, handed{g.Value.(int64), s.seqOf[w]})
			s.mu.Unlock()
			s.served.Add(1)
			s.use()
			s.release(model)
			return
		case Flushed:
			s.flushed.Add(1)
			return
		case Reroute:
			for {
				s.mu.Lock()
				if s.free > 0 {
					s.free--
					s.mu.Unlock()
					s.q.Done(w)
					s.served.Add(1)
					s.use()
					s.release(model)
					return
				}
				s.mu.Unlock()
				if !s.q.Park(w) {
					break
				}
			}
		}
	}
}

func (s *simRouter) refresh(models ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range models {
		if s.free > 0 {
			s.q.Wake(m, s.free)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	testwait.Eventually(t, d, cond, what)
}

// Many goroutines hammer arrivals, releases, cancellations and timeouts. Invariants: capacity is never exceeded,
// every request ends exactly once (no lost waiter), no slot is handed out twice, hand-offs follow enqueue order,
// and everything drains.
func TestQueueStressHandoffFIFONoLossNoDouble(t *testing.T) {
	const (
		capacity = 4
		clients  = 400
	)
	q := NewQueue(QueueParams{MaxModelDepth: 64, MaxWait: time.Second, MaxTenantShare: 0.5}, nil)
	s := newSimRouter(capacity, q)
	baseline := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wait := 5 * time.Second // a backstop only; hand-offs normally arrive long before
			switch i % 7 {
			case 3: // some clients leave early
				go func() { runtime.Gosched(); cancel() }()
			case 5: // some time out
				wait = time.Microsecond * time.Duration(1+i%50)
			}
			tenant := []string{"", "A", "B"}[i%3]
			s.request(ctx, "m", tenant, wait, false)
		}(i)
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		t.Fatalf("stuck: depth %d free %d served %d timeouts %d gone %d", q.Depth("m"), s.free, s.served.Load(), s.timedOut.Load(), s.gone.Load())
	}

	if s.peak.Load() > capacity {
		t.Fatalf("peak concurrency %d exceeds capacity %d", s.peak.Load(), capacity)
	}
	if s.dupes.Load() != 0 {
		t.Fatalf("%d slots were handed out twice", s.dupes.Load())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.free != capacity {
		t.Fatalf("slots at rest: %d, want %d (a slot was lost or duplicated)", s.free, capacity)
	}
	if q.Depth("m") != 0 || len(q.Models()) != 0 {
		t.Fatal("waiters left behind")
	}
	sort.Slice(s.handed, func(i, j int) bool { return s.handed[i].token < s.handed[j].token })
	for i := 1; i < len(s.handed); i++ {
		if s.handed[i].seq <= s.handed[i-1].seq {
			t.Fatalf("hand-off #%d went to enqueue #%d after enqueue #%d: not FIFO", s.handed[i].token, s.handed[i].seq, s.handed[i-1].seq)
		}
	}
	if len(s.handed) == 0 {
		t.Fatal("the stress never exercised a hand-off")
	}
	// Every request ended exactly once; sheds at the door are the remainder.
	if ended := s.served.Load() + s.timedOut.Load() + s.gone.Load() + s.flushed.Load(); ended > clients {
		t.Fatalf("%d outcomes for %d requests", ended, clients)
	}
	waitFor(t, 5*time.Second, func() bool { return runtime.NumGoroutine() <= baseline+2 }, "goroutines to return to baseline")
}

// The same, with registry refreshes waking waiters to route on their own (Reroute, Park, missed hand-offs).
func TestQueueStressWithRefreshWakes(t *testing.T) {
	const capacity = 3
	q := NewQueue(QueueParams{MaxModelDepth: 1000, MaxWait: time.Second}, nil)
	s := newSimRouter(capacity, q)
	stop := make(chan struct{})
	var rw sync.WaitGroup
	rw.Add(1)
	go func() {
		defer rw.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.refresh("m", "n")
				runtime.Gosched()
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			model := []string{"m", "n"}[i%2]
			s.request(context.Background(), model, "", 30*time.Second, true)
		}(i)
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		t.Fatalf("stuck: depths %v free %d served %d", q.Depths(), s.free, s.served.Load())
	}
	close(stop)
	rw.Wait()
	// Two models share the sim's single pool: free ends at capacity, nothing waits, every request was served
	// (no timeouts were possible: the wait is far longer than the run).
	if s.free != capacity || len(q.Models()) != 0 {
		t.Fatalf("free %d depths %v", s.free, q.Depths())
	}
	if s.served.Load() != 300 || s.timedOut.Load() != 0 {
		t.Fatalf("served %d timed out %d: a waiter was lost", s.served.Load(), s.timedOut.Load())
	}
	if s.peak.Load() > capacity || s.dupes.Load() != 0 {
		t.Fatalf("peak %d dupes %d", s.peak.Load(), s.dupes.Load())
	}
}

// 10,000 waiters cancel: all leave at once, nothing is left in the queue, and no goroutine is leaked (the package
// starts none).
func TestQueueCancellationsLeaveNoTrace(t *testing.T) {
	q := NewQueue(QueueParams{MaxModelDepth: 100000, MaxWait: time.Minute}, nil)
	baseline := runtime.NumGoroutine()
	var wg sync.WaitGroup
	var cancelled atomic.Int64
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := q.Enqueue("m", "")
			if err != nil {
				t.Error(err)
				return
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, out := q.Wait(ctx, w, nil); out == ClientGone {
				cancelled.Add(1)
			}
		}()
	}
	wg.Wait()
	if cancelled.Load() != 10000 || q.Depth("m") != 0 || len(q.Models()) != 0 {
		t.Fatalf("cancelled %d depth %d", cancelled.Load(), q.Depth("m"))
	}
	waitFor(t, 5*time.Second, func() bool { return runtime.NumGoroutine() <= baseline+2 }, "goroutines to return to baseline")
}

// FIFO is proven for arrivals too: with the model busy, each arrival queues behind the earlier ones and the freed
// slots are handed out in exactly arrival order, deterministically.
func TestQueueArrivalOrderIsServiceOrder(t *testing.T) {
	q, _ := newTestQueue(100, 0, 0)
	var ws []*Waiter
	for i := 0; i < 50; i++ {
		if i > 0 && !q.Busy("m") {
			t.Fatal("arrivals after the first must find a busy queue")
		}
		ws = append(ws, mustEnqueue(t, q, "m", ""))
	}
	for i := range ws {
		if !q.Handoff("m", i) {
			t.Fatal("handoff")
		}
		for j, w := range ws {
			select {
			case g := <-w.C():
				if j != i || g.Value != i {
					t.Fatalf("waiter %d served at step %d", j, i)
				}
			default:
				if j == i {
					t.Fatalf("waiter %d not served at its step", j)
				}
			}
		}
	}
}

// With no periodic refresh, only occasional single wake-ups and the releases themselves move waiters. No request may
// be stranded while a slot is free: every one is served long before its (generous) wait ends.
func TestQueueStressSparseWakesNeverStrandWaiters(t *testing.T) {
	const capacity = 3
	q := NewQueue(QueueParams{MaxModelDepth: 1000, MaxWait: time.Minute}, nil)
	s := newSimRouter(capacity, q)
	stop := make(chan struct{})
	var rw sync.WaitGroup
	rw.Add(1)
	go func() {
		defer rw.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			s.mu.Lock()
			q.Wake("m", 1) // fewer wakes than free slots, at arbitrary moments
			s.mu.Unlock()
			for j := 0; j < 50+i%97; j++ {
				runtime.Gosched()
			}
		}
	}()
	var wg sync.WaitGroup
	const clients = 300
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if i%9 == 4 {
				go func() { runtime.Gosched(); cancel() }()
			}
			s.request(ctx, "m", "", 20*time.Second, true)
		}(i)
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatalf("a waiter was stranded: depth %d free %d served %d gone %d", q.Depth("m"), s.free, s.served.Load(), s.gone.Load())
	}
	close(stop)
	rw.Wait()
	if s.timedOut.Load() != 0 {
		t.Fatalf("%d waiters waited out their time with slots free", s.timedOut.Load())
	}
	if s.served.Load()+s.gone.Load() != clients {
		t.Fatalf("served %d + gone %d != %d", s.served.Load(), s.gone.Load(), clients)
	}
	if s.free != capacity || len(q.Models()) != 0 {
		t.Fatalf("free %d depths %v", s.free, q.Depths())
	}
}
