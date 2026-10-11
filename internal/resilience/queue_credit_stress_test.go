package resilience

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func (q *Queue) anyRouting(model string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	mq := q.models[model]
	if mq == nil {
		return false
	}
	for e := mq.waiters.Front(); e != nil; e = e.Next() {
		if e.Value.(*Waiter).woken {
			return true
		}
	}
	return false
}

type creditRound struct {
	waiters, releasers int
	wakes              bool // occasional Wake(1..3) at arbitrary moments
	leavers            bool // some waiters cancel or time out
	flush              bool // a flush ends the round early
	wait               time.Duration
}

type creditResult struct {
	served, gone, timedOut, flushed int64
	free                            int
	violations                      int64
	remaining                       int
}

// run drives one round: waiters queue up behind an empty pool, releasers free one slot each (hand-off when
// possible, otherwise the slot goes back to the pool), and rare wake-ups send waiters to route on their own.
// A checker holds the router lock between every few operations and asserts the strand invariant: a slot is never
// free while waiters are queued and none of them is routing or about to.
func (c creditRound) run(t *testing.T) creditResult {
	t.Helper()
	q := NewQueue(QueueParams{MaxModelDepth: c.waiters + 1, MaxWait: c.wait}, nil)
	var mu sync.Mutex // the router lock
	free := 0
	var res creditResult
	var tokens atomic.Int64

	release := func() {
		mu.Lock()
		defer mu.Unlock()
		if q.Handoff("m", tokens.Add(1)) {
			return
		}
		free++
	}
	routeOnce := func(w *Waiter) bool { // true: served
		mu.Lock()
		defer mu.Unlock()
		if free > 0 {
			free--
			q.Done(w)
			return true
		}
		return false
	}

	enqueued := make(chan struct{}, c.waiters)
	var wg sync.WaitGroup
	for i := 0; i < c.waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			timeout := time.After(c.wait)
			if c.leavers && i%7 == 3 {
				timeout = time.After(time.Duration(1+i%5) * time.Millisecond)
			}
			mu.Lock()
			w, err := q.Enqueue("m", "")
			mu.Unlock()
			if err != nil {
				t.Error(err)
				return
			}
			enqueued <- struct{}{}
			if c.leavers && i%11 == 5 {
				go func() { runtime.Gosched(); cancel() }()
			}
			for {
				g, out := q.Wait(ctx, w, timeout)
				switch out {
				case TimedOut:
					atomic.AddInt64(&res.timedOut, 1)
					return
				case ClientGone:
					atomic.AddInt64(&res.gone, 1)
					return
				}
				switch g.Kind {
				case Handoff:
					atomic.AddInt64(&res.served, 1)
					return
				case Flushed:
					atomic.AddInt64(&res.flushed, 1)
					return
				case Reroute:
					for {
						if routeOnce(w) {
							atomic.AddInt64(&res.served, 1)
							return
						}
						if !q.Park(w) {
							break
						}
					}
				}
			}
		}(i)
	}
	for i := 0; i < c.waiters; i++ {
		<-enqueued
	}

	stop := make(chan struct{})
	var aux sync.WaitGroup
	aux.Add(1)
	go func() { // the checker
		defer aux.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			if free > 0 && q.Depth("m") > 0 && !q.anyRouting("m") {
				atomic.AddInt64(&res.violations, 1)
			}
			if !q.settled("m") {
				atomic.AddInt64(&res.violations, 1)
			}
			mu.Unlock()
			runtime.Gosched()
		}
	}()
	if c.wakes {
		aux.Add(1)
		go func() {
			defer aux.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				q.Wake("m", 1+i%3)
				mu.Unlock()
				for j := 0; j < 200+i%301; j++ {
					runtime.Gosched()
				}
			}
		}()
	}
	var rel sync.WaitGroup
	per := c.waiters / c.releasers
	for r := 0; r < c.releasers; r++ {
		n := per
		if r == c.releasers-1 {
			n = c.waiters - per*(c.releasers-1)
		}
		rel.Add(1)
		go func() {
			defer rel.Done()
			for i := 0; i < n; i++ {
				release()
				if i%3 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	if c.flush {
		go func() {
			runtime.Gosched()
			mu.Lock()
			q.Flush("m")
			mu.Unlock()
		}()
	}
	rel.Wait()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		mu.Lock()
		t.Errorf("stuck: depth %d free %d served %d gone %d timedOut %d credit %d", q.Depth("m"), free, atomic.LoadInt64(&res.served), res.gone, res.timedOut, q.creditOf("m"))
		mu.Unlock()
	}
	close(stop)
	aux.Wait()
	mu.Lock()
	res.free = free
	res.remaining = q.Depth("m")
	mu.Unlock()
	return res
}

func hammerRounds() (rounds, waiters, releasers int) {
	if v := os.Getenv("SF_HAMMER"); v != "" { // SF_HAMMER=100 -> 100 rounds x 1000 waiters x 20 releasers
		n, _ := strconv.Atoi(v)
		return n, 1000, 20
	}
	return 30, 40, 4
}

// 40 waiters, 40 slots released by 4 goroutines, rare Wake(1..3): nobody may wait while a slot is free, so nobody
// times out and all 40 are served.
func TestQueueNoSlotStrandedWhileWaitersQueued(t *testing.T) {
	rounds, waiters, releasers := hammerRounds()
	t.Logf("%d rounds x %d waiters x %d releasers", rounds, waiters, releasers)
	for r := 0; r < rounds; r++ {
		res := creditRound{waiters: waiters, releasers: releasers, wakes: true, wait: 25 * time.Second}.run(t)
		if res.violations != 0 || res.served != int64(waiters) || res.timedOut != 0 || res.free != 0 || res.remaining != 0 {
			t.Fatalf("round %d: %+v", r, res)
		}
	}
}

// Everything at once: wake-ups, cancellations, timeouts and a flush. Every waiter ends exactly once, the strand
// invariant holds throughout, and every released slot is either used or still in the pool.
func TestQueueHammerAllPaths(t *testing.T) {
	rounds, waiters, releasers := hammerRounds()
	for r := 0; r < rounds; r++ {
		res := creditRound{waiters: waiters, releasers: releasers, wakes: true, leavers: true, flush: r%2 == 0, wait: 25 * time.Second}.run(t)
		if res.violations != 0 {
			t.Fatalf("round %d: strand invariant violated %d times: %+v", r, res.violations, res)
		}
		if ended := res.served + res.gone + res.timedOut + res.flushed; ended != int64(waiters) || res.remaining != 0 {
			t.Fatalf("round %d: %d of %d waiters ended: %+v", r, ended, waiters, res)
		}
		if int64(res.free)+res.served != int64(waiters) && res.flushed == 0 {
			t.Fatalf("round %d: slots lost or duplicated: %+v", r, res)
		}
	}
}
