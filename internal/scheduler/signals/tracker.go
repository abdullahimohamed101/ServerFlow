package signals

import (
	"sort"
	"sync"
	"time"

	"serverflow/pkg/protocol"
)

// Defaults for Config.
const (
	DefaultBucket     = 100 * time.Millisecond
	DefaultMaxWorkers = 4096
	// maxCount bounds the counts taken from a worker's report so that the arithmetic cannot overflow.
	maxCount = 1 << 31
	// maxReportAge bounds the heartbeat age taken from a snapshot before it becomes a duration.
	maxReportAge = 1e6 // seconds
)

// Config sizes a Tracker.
type Config struct {
	// Bucket is the granularity of the start/finish history (default 100 ms).
	Bucket time.Duration
	// Window is how far back a report may be and still be corrected from the history. Older reports fall back
	// to max(local, reported). The gateway uses registry_max_staleness plus one heartbeat interval.
	Window time.Duration
	// MaxWorkers caps the workers tracked; beyond it a worker is untracked and gets the fallback (default 4096).
	MaxWorkers int
}

// Key identifies one incarnation of a worker.
type Key struct {
	WorkerID   string
	Registered int64 // RegisteredAt in Unix nanoseconds
}

// KeyOf is the key of a snapshot.
func KeyOf(w *protocol.WorkerSnapshot) Key {
	return Key{WorkerID: w.WorkerID, Registered: w.RegisteredAt.UnixNano()}
}

// Token proves an entry was added to the ledger and says which incarnation of the tracked worker it went to. It
// is what End and AddCost need; a Token from an entry that has since been pruned or replaced is harmless (the
// calls do nothing). The zero Token is invalid and also harmless: it is what Begin returns for an untracked
// worker.
type Token struct {
	key Key
	gen uint64
}

// Valid reports whether the token refers to a tracked entry (it may still have gone stale since).
func (t Token) Valid() bool { return t.gen != 0 }

// Fallback says why the ledger rule gave way to max(local, reported).
type Fallback uint8

// Fallback values.
const (
	FallbackNone      Fallback = iota // the history answered
	FallbackUntracked                 // the worker is not tracked (unknown, or beyond the cap)
	FallbackHistory                   // the history cannot answer: the report predates it, or is older than the window
)

// Input is one worker as the overlay sees it.
type Input struct {
	Key      Key
	Local    int       // this gateway's in-flight requests on the worker
	Reported int       // the worker-reported active requests
	At       time.Time // when the report was made, on this gateway's clock (fetch time minus heartbeat age)
}

// Result is the overlay's answer for one worker.
type Result struct {
	// Active is the effective active count. It is never below Local.
	Active int
	// Inflight and Work are the ledger: requests and estimated token cost in flight on the worker, as this
	// gateway sent them.
	Inflight int
	Work     int64
	Fallback Fallback
}

// Observed converts the result into the Go-only snapshot field the strategies read.
func (r Result) Observed() protocol.ObservedLoad {
	o := protocol.ObservedLoad{InflightRequests: r.Inflight, WorkAheadTokens: float64(r.Work), EffectiveActive: r.Active}
	if r.Fallback != FallbackNone {
		o.Flags |= protocol.ObservedColdStart
	}
	return o
}

// Observation is a worker in a registry snapshot, as Sync needs it.
type Observation struct {
	Key           Key
	LastHeartbeat time.Time
}

// Stats are cumulative counters and sizes, for tests and metrics.
type Stats struct {
	Tracked           int    // entries now
	Untracked         uint64 // workers that could not be tracked because of the cap (counted per attempt)
	Resets            uint64 // histories reset because a worker's last heartbeat went backwards
	Pruned            uint64 // entries removed because the worker left the snapshot or re-registered
	FallbackUntracked uint64
	FallbackHistory   uint64
}

// Tracker is the ledger and history for all the workers of one gateway. It is safe for concurrent use.
type Tracker struct {
	cfg Config

	mu      sync.Mutex
	epoch   time.Time
	hasEp   bool
	entries map[Key]*entry
	nextGen uint64
	stats   Stats
}

type entry struct {
	gen      uint64
	inflight int
	work     int64
	// validFrom is the earliest report time the history can answer for: reports made before it fall back.
	validFrom time.Time
	lastHB    time.Time
	hist      history
}

// New returns an empty tracker. Zero Config fields take their defaults; a Window shorter than two buckets is
// widened to two.
func New(cfg Config) *Tracker {
	if cfg.Bucket <= 0 {
		cfg.Bucket = DefaultBucket
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = DefaultMaxWorkers
	}
	if cfg.Window < 2*cfg.Bucket {
		cfg.Window = 2 * cfg.Bucket
	}
	return &Tracker{cfg: cfg, entries: map[Key]*entry{}}
}

// ReportTime is the gateway-local instant a report was made: now minus the report's (aged) heartbeat age. now
// must be the instant the snapshot view was aged to, so that the arithmetic stays inside one clock domain (the
// snapshot cache does the same to judge staleness). Ages that are negative, NaN or absurd are clamped.
func ReportTime(now time.Time, heartbeatAgeSeconds float64) time.Time {
	age := heartbeatAgeSeconds
	if !(age > 0) { // also catches NaN
		return now
	}
	if age > maxReportAge {
		age = maxReportAge
	}
	return now.Add(-time.Duration(age * float64(time.Second)))
}

// Sync reconciles the tracker with a registry snapshot, once per refresh and before the snapshot is used: it
// creates entries for workers it has not seen, prunes entries for workers (or incarnations) that are gone, and
// resets the history of a worker whose last heartbeat went backwards (a registry restart that kept RegisteredAt:
// the old reports and the old history no longer line up). The ledger of a worker that is merely reset keeps
// counting: its requests are still in flight.
func (t *Tracker) Sync(now time.Time, members []Observation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setEpoch(now)
	seen := make(map[Key]struct{}, len(members))
	for _, m := range members {
		seen[m.Key] = struct{}{}
		e := t.entries[m.Key]
		if e == nil {
			if e = t.create(m.Key, now); e == nil {
				continue
			}
		}
		if !m.LastHeartbeat.IsZero() && !e.lastHB.IsZero() && m.LastHeartbeat.Before(e.lastHB) {
			e.hist.reset()
			e.validFrom = now
			t.stats.Resets++
		}
		e.lastHB = m.LastHeartbeat
	}
	for k := range t.entries {
		if _, ok := seen[k]; !ok {
			delete(t.entries, k)
			t.stats.Pruned++
		}
	}
}

// create adds an entry unless the cap is reached (then it counts the miss and returns nil).
func (t *Tracker) create(k Key, now time.Time) *entry {
	if len(t.entries) >= t.cfg.MaxWorkers {
		t.stats.Untracked++
		return nil
	}
	t.nextGen++
	e := &entry{gen: t.nextGen, validFrom: now}
	t.entries[k] = e
	return e
}

// Begin records a request sent to the worker: one more in flight, and a start in the history. It returns the
// token End needs. It does not add cost (AddCost does), because on the hand-off path the request that will use
// the slot is not known yet. For an untracked worker it returns the zero token and everything that follows is a
// no-op.
func (t *Tracker) Begin(now time.Time, k Key) Token {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setEpoch(now)
	e := t.entries[k]
	if e == nil {
		if e = t.create(k, now); e == nil {
			return Token{}
		}
	}
	e.inflight++
	e.hist.record(t.bucket(now), true, t.window())
	return Token{key: k, gen: e.gen}
}

// AddCost adds the estimated token cost of the request holding tok to the worker's ledger. It reports whether it
// did (false for an invalid or stale token); the caller then owes End the same cost.
func (t *Tracker) AddCost(tok Token, cost int64) bool {
	if !tok.Valid() || cost <= 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[tok.key]
	if e == nil || e.gen != tok.gen {
		return false
	}
	e.work += cost
	return true
}

// End records the request holding tok as finished: one fewer in flight, cost less work (the cost AddCost took,
// 0 when it took none), and a finish in the history. A stale token does nothing. Callers pair every Begin with
// exactly one End.
func (t *Tracker) End(now time.Time, tok Token, cost int64) {
	if !tok.Valid() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setEpoch(now)
	e := t.entries[tok.key]
	if e == nil || e.gen != tok.gen {
		return
	}
	if e.inflight > 0 {
		e.inflight--
	}
	if cost > 0 {
		e.work -= cost
		if e.work < 0 {
			e.work = 0
		}
	}
	e.hist.record(t.bucket(now), false, t.window())
}

// Overlay is the one function that turns a worker's report and this gateway's own count into the effective active
// count (and reads its ledger). The router calls it for the scheduler's view (reserve) and for the queue's
// free-slot count (freeSlots), each with the view's own now, so the two cannot disagree about a worker.
func (t *Tracker) Overlay(now time.Time, in Input) Result {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setEpoch(now)
	local := clampCount(in.Local)
	reported := clampCount(in.Reported)
	res := Result{Active: max(local, reported)}
	e := t.entries[in.Key]
	if e == nil {
		res.Fallback = FallbackUntracked
		t.stats.FallbackUntracked++
		return res
	}
	res.Inflight, res.Work = e.inflight, e.work
	at := in.At
	if at.After(now) {
		at = now
	}
	if at.Before(e.validFrom) || now.Sub(at) > t.cfg.Window {
		res.Fallback = FallbackHistory
		t.stats.FallbackHistory++
		return res
	}
	b := t.bucket(at)
	started, ok1 := e.hist.since(b, true)
	finished, ok2 := e.hist.since(b+finishLag, false)
	if !ok1 || !ok2 {
		res.Fallback = FallbackHistory
		t.stats.FallbackHistory++
		return res
	}
	// started and finished are bounded by the history's size, which is bounded by the window, and reported by
	// maxCount, so int64 cannot overflow.
	eff := int64(reported) + int64(started) - int64(finished)
	res.Active = int(min(max(eff, int64(local)), maxCount))
	return res
}

// finishLag is how many buckets after the report's bucket a finish must be to count as "after the report". A
// finish in the report's own bucket or the next one may be before the report (the report time is only known to
// about a bucket), and not subtracting it can only over-count.
const finishLag = 2

// Ledger returns the requests and estimated token cost in flight on a worker (0, 0 when untracked).
func (t *Tracker) Ledger(k Key) (inflight int, work int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entries[k]; e != nil {
		return e.inflight, e.work
	}
	return 0, 0
}

// Totals sums the ledger over all tracked workers (leak checks).
func (t *Tracker) Totals() (inflight int, work int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range t.entries {
		inflight += e.inflight
		work += e.work
	}
	return inflight, work
}

// Stats returns a copy of the counters.
func (t *Tracker) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats
	s.Tracked = len(t.entries)
	return s
}

// HistoryRecords is how many history buckets are held across all workers (bounds check).
func (t *Tracker) HistoryRecords() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, e := range t.entries {
		n += e.hist.live()
	}
	return n
}

func (t *Tracker) setEpoch(now time.Time) {
	if !t.hasEp {
		t.epoch, t.hasEp = now, true
	}
}

// bucket is the history bucket of an instant: floor((x - epoch) / Bucket).
func (t *Tracker) bucket(x time.Time) int64 {
	d := x.Sub(t.epoch)
	b := int64(d / t.cfg.Bucket)
	if d < 0 && d%t.cfg.Bucket != 0 {
		b--
	}
	return b
}

func (t *Tracker) window() int64 { return int64(t.cfg.Window/t.cfg.Bucket) + 2 }

func clampCount(n int) int { return min(max(n, 0), maxCount) }

// history is a bounded record of cumulative starts and finishes by bucket. Only buckets that saw an event are
// stored, in time order; each holds the totals up to and including that bucket, so "events since bucket b" is a
// subtraction and a binary search, whatever the number of workers or the request rate.
type history struct {
	recs       []rec // live records are recs[from:]
	from       int
	totS, totF uint64 // totals so far
	baseS      uint64 // totals up to and including droppedIdx (what was trimmed away)
	baseF      uint64
	droppedIdx int64
	hasDropped bool
}

type rec struct {
	idx        int64
	cumS, cumF uint64
}

func (h *history) live() int { return len(h.recs) - h.from }

func (h *history) reset() { *h = history{} }

// record adds one start or finish in bucket idx (an idx older than the last record is treated as the last one,
// so a clock that steps back cannot reorder the records) and trims buckets older than keep buckets.
func (h *history) record(idx int64, start bool, keep int64) {
	if start {
		h.totS++
	} else {
		h.totF++
	}
	if n := len(h.recs); n > h.from {
		last := &h.recs[n-1]
		if idx <= last.idx {
			last.cumS, last.cumF = h.totS, h.totF
			return
		}
	}
	h.recs = append(h.recs, rec{idx: idx, cumS: h.totS, cumF: h.totF})
	for h.from < len(h.recs) && h.recs[h.from].idx < idx-keep {
		r := h.recs[h.from]
		h.baseS, h.baseF, h.droppedIdx, h.hasDropped = r.cumS, r.cumF, r.idx, true
		h.from++
	}
	if h.from > 32 && h.from*2 > len(h.recs) { // compact so the backing array stays bounded
		n := copy(h.recs, h.recs[h.from:])
		h.recs, h.from = h.recs[:n], 0
	}
}

// since counts the starts (or finishes) in buckets at or after pivot. ok is false when the history no longer
// reaches back to pivot.
func (h *history) since(pivot int64, start bool) (n uint64, ok bool) {
	live := h.recs[h.from:]
	i := sort.Search(len(live), func(i int) bool { return live[i].idx >= pivot }) // first record at or after pivot
	var cumS, cumF uint64
	switch {
	case i > 0:
		cumS, cumF = live[i-1].cumS, live[i-1].cumF
	case h.hasDropped && pivot <= h.droppedIdx:
		return 0, false
	default:
		cumS, cumF = h.baseS, h.baseF
	}
	if start {
		return h.totS - cumS, true
	}
	return h.totF - cumF, true
}
