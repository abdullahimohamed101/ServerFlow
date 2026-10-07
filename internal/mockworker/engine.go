package mockworker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrQueueFull is returned by Acquire when every slot is busy and the
// bounded wait queue is full.
var ErrQueueFull = errors.New("mockworker: queue full")

// recent_tokens_per_second averages over windowBuckets buckets of bucketWidth.
const (
	bucketWidth   = 100 * time.Millisecond
	windowBuckets = 50 // 5 seconds
	// ringSize has one more slot than the window so the filling current bucket
	// never shares a slot with the oldest complete one.
	ringSize = windowBuckets + 1
)

// Engine models a worker's capacity: at most maxActive generations run at
// once, up to maxQueue more wait in strict arrival order, and anything beyond
// that is rejected, so memory use is bounded.
type Engine struct {
	now func() time.Time

	mu           sync.Mutex
	maxActive    int
	maxQueue     int
	active       int
	queue        []*waiter
	queuedTokens int
	completed    int64
	failed       int64
	rejected     int64
	cancelled    int64
	tokens       int64
	tput         throughput
}

type waiter struct {
	ready        chan struct{}
	promptTokens int
	granted      bool
}

// NewEngine returns an engine with the given limits.
func NewEngine(maxActive, maxQueue int) *Engine {
	return newEngine(maxActive, maxQueue, time.Now)
}

func newEngine(maxActive, maxQueue int, now func() time.Time) *Engine {
	return &Engine{now: now, maxActive: maxActive, maxQueue: maxQueue}
}

// Acquire takes a generation slot, waiting in FIFO order if none is free.
// promptTokens feeds the queued_input_tokens statistic while waiting. It
// returns ErrQueueFull when the queue is full, or ctx.Err() if ctx ends first.
// On success the caller must call the returned release exactly when the
// generation ends; calling it more than once is harmless.
func (e *Engine) Acquire(ctx context.Context, promptTokens int) (release func(), err error) {
	e.mu.Lock()
	if e.active < e.maxActive && len(e.queue) == 0 {
		e.active++
		e.mu.Unlock()
		return e.releaser(), nil
	}
	if len(e.queue) >= e.maxQueue {
		e.rejected++
		e.mu.Unlock()
		return nil, ErrQueueFull
	}
	w := &waiter{ready: make(chan struct{}), promptTokens: promptTokens}
	e.queue = append(e.queue, w)
	e.queuedTokens += promptTokens
	e.mu.Unlock()

	select {
	case <-w.ready:
		return e.releaser(), nil
	case <-ctx.Done():
		e.abandon(w)
		return nil, ctx.Err()
	}
}

// abandon is the cancel path of Acquire: a waiter that gave up either leaves
// the queue, or, if the slot was handed to it at the same moment, passes the
// slot on so it is never leaked.
func (e *Engine) abandon(w *waiter) {
	e.mu.Lock()
	if w.granted {
		e.releaseLocked()
	} else {
		e.removeLocked(w)
	}
	e.cancelled++
	e.mu.Unlock()
}

func (e *Engine) releaser() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Lock()
			e.releaseLocked()
			e.mu.Unlock()
		})
	}
}

// releaseLocked frees a slot and hands it straight to the oldest waiter, so a
// new arrival can never overtake the queue.
func (e *Engine) releaseLocked() {
	e.active--
	if len(e.queue) > 0 {
		w := e.queue[0]
		e.queue = e.queue[1:]
		e.queuedTokens -= w.promptTokens
		e.active++
		w.granted = true
		close(w.ready)
	}
}

func (e *Engine) removeLocked(w *waiter) {
	for i, q := range e.queue {
		if q == w {
			e.queue = append(e.queue[:i], e.queue[i+1:]...)
			e.queuedTokens -= w.promptTokens
			return
		}
	}
}

// RecordTokens adds n generated tokens to the recent-throughput window.
func (e *Engine) RecordTokens(n int) {
	e.mu.Lock()
	e.tokens += int64(n)
	e.tput.record(e.now(), int64(n))
	e.mu.Unlock()
}

// RecordCancelled counts a request whose client went away after it was
// admitted (a cancelled waiter is counted by Acquire itself).
func (e *Engine) RecordCancelled() {
	e.mu.Lock()
	e.cancelled++
	e.mu.Unlock()
}

// RecordOutcome counts a finished request as completed or failed.
func (e *Engine) RecordOutcome(ok bool) {
	e.mu.Lock()
	if ok {
		e.completed++
	} else {
		e.failed++
	}
	e.mu.Unlock()
}

// EngineStats is a point-in-time snapshot.
type EngineStats struct {
	Active                int
	QueueDepth            int
	QueuedInputTokens     int
	RecentTokensPerSecond float64
	Completed             int64
	Failed                int64
	Rejected              int64
	Cancelled             int64
	Tokens                int64
}

// Stats returns a consistent snapshot.
func (e *Engine) Stats() EngineStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return EngineStats{
		Active:                e.active,
		QueueDepth:            len(e.queue),
		QueuedInputTokens:     e.queuedTokens,
		RecentTokensPerSecond: e.tput.rate(e.now()),
		Completed:             e.completed,
		Failed:                e.failed,
		Rejected:              e.rejected,
		Cancelled:             e.cancelled,
		Tokens:                e.tokens,
	}
}

// throughput measures recent tokens per second over a sliding window of
// bucketWidth-sized buckets. It averages only complete buckets, so the
// still-filling current one cannot make the reading sag, and divides by the
// time actually covered, so a young window or one resuming after idle is not
// diluted. Not safe for concurrent use; Engine guards it.
type throughput struct {
	slots [ringSize]struct{ idx, n int64 }
	// epoch is the time of the first token ever. Buckets are measured from it
	// with Time.Sub, which uses the monotonic clock when the times carry one,
	// so a wall-clock step (NTP, suspend) cannot corrupt the window.
	epoch   time.Time
	first   int64 // bucket index where the current busy period began
	last    int64 // bucket index of the most recent token
	started bool
}

// bucketOf maps a time to its bucket index, never negative so a clock that
// steps backwards cannot index outside the ring.
func (t *throughput) bucketOf(now time.Time) int64 {
	if d := now.Sub(t.epoch); d > 0 {
		return int64(d / bucketWidth)
	}
	return 0
}

func (t *throughput) record(now time.Time, n int64) {
	if !t.started && t.epoch.IsZero() {
		t.epoch = now
	}
	idx := t.bucketOf(now)
	if !t.started || idx-t.last > windowBuckets {
		// First tokens ever, or the window has fully emptied: start a new
		// busy period rather than averaging over the stale gap.
		t.slots = [ringSize]struct{ idx, n int64 }{}
		t.first, t.started = idx, true
	}
	s := &t.slots[idx%ringSize]
	if s.idx != idx {
		*s = struct{ idx, n int64 }{idx: idx}
	}
	s.n += n
	t.last = idx
}

// rate returns tokens per second over the last complete buckets, at most the
// full window. The bucket in which a busy period began is skipped because it
// is only partly filled. The result is 0 until one full bucket has completed.
func (t *throughput) rate(now time.Time) float64 {
	if !t.started {
		return 0
	}
	cur := t.bucketOf(now)
	start := t.first + 1   // first fully covered bucket of this busy period
	covered := cur - start // complete buckets from start up to the current one
	if covered > windowBuckets {
		covered = windowBuckets
	}
	if covered <= 0 {
		return 0
	}
	var total int64
	for _, s := range t.slots {
		if s.idx < cur && s.idx >= cur-covered {
			total += s.n
		}
	}
	return float64(total) / (float64(covered) * bucketWidth.Seconds())
}
