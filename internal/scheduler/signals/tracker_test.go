package signals

import (
	"fmt"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

var t0 = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func key(id string) Key { return Key{WorkerID: id, Registered: t0.UnixNano()} }

func newTracker() *Tracker {
	return New(Config{Window: 12 * time.Second})
}

// seed makes the tracker follow the worker from time 0, so reports made after that can be corrected.
func seed(tr *Tracker, ks ...Key) {
	obs := make([]Observation, len(ks))
	for i, k := range ks {
		obs[i] = Observation{Key: k, LastHeartbeat: t0}
	}
	tr.Sync(at(0), obs)
}

// ov asks the overlay with a report made at reportMs, asked at nowMs.
func ov(tr *Tracker, k Key, nowMs, reportMs, local, reported int) Result {
	return tr.Overlay(at(nowMs), Input{Key: k, Local: local, Reported: reported, At: at(reportMs)})
}

// The overlay cases of the plan (D4), one by one. Times are milliseconds on the fake clock, a bucket is 100 ms, and
// the report is made at 5,000 ms unless the case says otherwise.
func TestOverlayCases(t *testing.T) {
	type ev struct {
		ms    int
		start bool
	}
	cases := []struct {
		name            string
		events          []ev
		local, reported int
		reportMs, nowMs int
		want            int
		wantFallback    Fallback
	}{
		{name: "nothing happened: reported stands", reported: 3, local: 0, reportMs: 5000, nowMs: 5400, want: 3},
		{name: "started before the report, still running: in reported already", events: []ev{{1000, true}}, reported: 1, local: 1, reportMs: 5000, nowMs: 5400, want: 1},
		{name: "started after the report: added", events: []ev{{1000, true}, {5600, true}}, reported: 1, local: 2, reportMs: 5000, nowMs: 5700, want: 2},
		{name: "finished after the report: subtracted (stale-high report no longer hides the worker)", events: []ev{{1000, true}, {1000, true}, {5600, false}, {5700, false}}, reported: 2, local: 0, reportMs: 5000, nowMs: 5800, want: 0},
		{name: "finished before the report: not subtracted", events: []ev{{1000, true}, {4000, false}}, reported: 0, local: 0, reportMs: 5000, nowMs: 5400, want: 0},
		{name: "stale-low report: the local floor covers it", events: []ev{{5600, true}, {5650, true}, {5700, true}}, reported: 0, local: 3, reportMs: 5000, nowMs: 5800, want: 3},
		{name: "never below local even when the arithmetic says so", events: []ev{{1000, true}, {5600, false}}, reported: 1, local: 2, reportMs: 5000, nowMs: 5800, want: 2},
		{name: "request shorter than a bucket, wholly after the report: nets zero", events: []ev{{5610, true}, {5620, false}}, reported: 4, local: 0, reportMs: 5000, nowMs: 5800, want: 4},
		{name: "request shorter than a bucket inside the report's bucket: counted as after for the start, not subtracted", events: []ev{{5010, true}, {5030, false}}, reported: 4, local: 0, reportMs: 5000, nowMs: 5800, want: 5},
		{name: "finish in the bucket after the report's is still not subtracted (report time is only known to a bucket)", events: []ev{{1000, true}, {5150, false}}, reported: 1, local: 0, reportMs: 5000, nowMs: 5800, want: 1},
		{name: "finish two buckets after the report is subtracted", events: []ev{{1000, true}, {5250, false}}, reported: 1, local: 0, reportMs: 5000, nowMs: 5800, want: 0},
		{name: "start in the bucket before the report's is in reported", events: []ev{{4950, true}}, reported: 1, local: 1, reportMs: 5000, nowMs: 5800, want: 1},
		{name: "report older than the window falls back to max", events: []ev{{1000, true}, {18000, false}}, reported: 5, local: 1, reportMs: 4000, nowMs: 17000, want: 5, wantFallback: FallbackHistory},
		{name: "report older than the worker's history falls back to max", events: nil, reported: 3, local: 1, reportMs: -500, nowMs: 800, want: 3, wantFallback: FallbackHistory},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTracker()
			k := key("w")
			seed(tr, k)
			// events are replayed in time order through Begin/End so the history is the real one
			for _, e := range tc.events {
				if e.start {
					tr.Begin(at(e.ms), k)
				} else {
					tr.End(at(e.ms), Token{key: k, gen: 1}, 0)
				}
			}
			got := ov(tr, k, tc.nowMs, tc.reportMs, tc.local, tc.reported)
			if got.Active != tc.want || got.Fallback != tc.wantFallback {
				t.Fatalf("Active=%d fallback=%d, want %d fallback=%d", got.Active, got.Fallback, tc.want, tc.wantFallback)
			}
			if got.Active < tc.local {
				t.Fatalf("Active %d is below local %d", got.Active, tc.local)
			}
		})
	}
}

func TestUntrackedWorkerFallsBackToMaxAndCounts(t *testing.T) {
	tr := newTracker()
	got := ov(tr, key("nobody"), 1000, 500, 2, 5)
	if got.Active != 5 || got.Fallback != FallbackUntracked {
		t.Fatalf("got %+v", got)
	}
	got = ov(tr, key("nobody"), 1000, 500, 7, 5)
	if got.Active != 7 {
		t.Fatalf("local floor: %+v", got)
	}
	if s := tr.Stats(); s.FallbackUntracked != 2 {
		t.Fatalf("fallbacks counted %d, want 2", s.FallbackUntracked)
	}
}

// Missed heartbeats: the report grows older, the window widens, and past the window the rule is max again.
func TestMissedHeartbeatsWidenTheCorrectionThenFallBack(t *testing.T) {
	tr := newTracker()
	k := key("w")
	seed(tr, k)
	tr.Begin(at(100), k) // in the report below
	tok := Token{key: k, gen: 1}
	tr.End(at(9000), tok, 0) // finished long after the report
	// report at 1,000 ms, asked at 9,500: eight seconds old, inside the 12 s window
	if got := ov(tr, k, 9500, 1000, 0, 1); got.Active != 0 || got.Fallback != FallbackNone {
		t.Fatalf("an 8 s old report: %+v", got)
	}
	// the same report asked 13 s later is outside the window: max(local, reported)
	if got := ov(tr, k, 14000, 1000, 0, 1); got.Active != 1 || got.Fallback != FallbackHistory {
		t.Fatalf("a 13 s old report: %+v", got)
	}
}

// Clock skew is not a thing inside one clock domain, but a report time in the future (a worker age of zero asked
// with an older now) and absurd ages must never panic or produce a value below local.
func TestReportTimeAndSkew(t *testing.T) {
	now := at(10_000)
	if got := ReportTime(now, 2.5); !got.Equal(at(7_500)) {
		t.Fatalf("ReportTime = %v", got)
	}
	for _, age := range []float64{-1, 0, 1e300, 1e18} {
		got := ReportTime(now, age)
		if got.After(now) {
			t.Fatalf("age %v: report time %v is in the future", age, got)
		}
	}
	if got := ReportTime(now, nan()); !got.Equal(now) {
		t.Fatalf("NaN age: %v", got)
	}
	tr := newTracker()
	k := key("w")
	seed(tr, k)
	// a report "from the future" is clamped to now
	if got := tr.Overlay(at(1000), Input{Key: k, Local: 2, Reported: 1, At: at(9000)}); got.Active != 2 {
		t.Fatalf("future report: %+v", got)
	}
	// garbage counts are clamped, never overflow
	if got := tr.Overlay(at(1000), Input{Key: k, Local: -5, Reported: 1 << 62, At: at(900)}); got.Active != maxCount {
		t.Fatalf("huge reported: %+v", got)
	}
}

func nan() float64 { var z float64; return z / z }

// A registry restart that keeps RegisteredAt shows as the last heartbeat going backwards: the worker's history is
// reset (reports made before the reset fall back to max), its ledger is not.
func TestHeartbeatGoingBackwardsResetsHistoryNotLedger(t *testing.T) {
	tr := newTracker()
	k := key("w")
	tr.Sync(at(0), []Observation{{Key: k, LastHeartbeat: at(0)}})
	tok := tr.Begin(at(100), k)
	tr.AddCost(tok, 500)
	tr.Sync(at(2000), []Observation{{Key: k, LastHeartbeat: at(1900)}})
	if got := ov(tr, k, 2100, 1900, 1, 1); got.Fallback != FallbackNone {
		t.Fatalf("a normal refresh must not reset: %+v", got)
	}
	tr.Sync(at(3000), []Observation{{Key: k, LastHeartbeat: at(1000)}}) // backwards
	if s := tr.Stats(); s.Resets != 1 {
		t.Fatalf("resets = %d", s.Resets)
	}
	if got := ov(tr, k, 3100, 2900, 1, 4); got.Active != 4 || got.Fallback != FallbackHistory {
		t.Fatalf("a report made before the reset must fall back to max: %+v", got)
	}
	if n, w := tr.Ledger(k); n != 1 || w != 500 {
		t.Fatalf("the ledger survives a history reset: %d, %d", n, w)
	}
	// a report made after the reset is corrected again
	tr.End(at(3300), tok, 500)
	if got := ov(tr, k, 3600, 3100, 0, 1); got.Active != 1 && got.Fallback != FallbackNone {
		t.Fatalf("after the reset: %+v", got)
	}
	// and a heartbeat that merely stays equal is not a restart
	tr.Sync(at(4000), []Observation{{Key: k, LastHeartbeat: at(1000)}})
	if s := tr.Stats(); s.Resets != 1 {
		t.Fatalf("equal heartbeat reset the history: %d", s.Resets)
	}
}

func TestLedgerPairsAndStaleTokensAreHarmless(t *testing.T) {
	tr := newTracker()
	a, b := key("a"), key("b")
	seed(tr, a, b)
	ta := tr.Begin(at(10), a)
	tr.AddCost(ta, 100)
	tb := tr.Begin(at(20), b)
	tr.AddCost(tb, 40)
	if n, w := tr.Totals(); n != 2 || w != 140 {
		t.Fatalf("totals %d %d", n, w)
	}
	tr.End(at(30), ta, 100)
	if n, w := tr.Ledger(a); n != 0 || w != 0 {
		t.Fatalf("a: %d %d", n, w)
	}
	// worker b re-registers: a new key, the old one pruned; the old token must not touch the new entry
	b2 := Key{WorkerID: "b", Registered: t0.Add(time.Hour).UnixNano()}
	tr.Sync(at(40), []Observation{{Key: a, LastHeartbeat: t0}, {Key: b2, LastHeartbeat: t0}})
	tb2 := tr.Begin(at(50), b2)
	tr.AddCost(tb2, 7)
	tr.End(at(60), tb, 40) // the old incarnation's request ends
	if n, w := tr.Ledger(b2); n != 1 || w != 7 {
		t.Fatalf("a stale token changed the new incarnation: %d %d", n, w)
	}
	if tr.AddCost(tb, 5) {
		t.Fatal("AddCost accepted a stale token")
	}
	if tr.AddCost(Token{}, 5) {
		t.Fatal("AddCost accepted the zero token")
	}
	tr.End(at(70), Token{}, 5)
	tr.End(at(80), tb2, 7)
	if n, w := tr.Totals(); n != 0 || w != 0 {
		t.Fatalf("ledger not back to zero: %d %d", n, w)
	}
	// pruned and re-created with the same key: the old generation's token is still stale
	tok := tr.Begin(at(90), a)
	tr.Sync(at(100), nil)
	tr.Sync(at(110), []Observation{{Key: a, LastHeartbeat: t0}})
	tr.End(at(120), tok, 0)
	if n, _ := tr.Ledger(a); n != 0 {
		t.Fatalf("the recreated entry was decremented by a stale token: %d", n)
	}
}

func TestEndCannotDriveTheLedgerNegative(t *testing.T) {
	tr := newTracker()
	k := key("w")
	seed(tr, k)
	tok := tr.Begin(at(1), k)
	tr.End(at(2), tok, 999) // a cost that was never added (a caller bug) must not go below zero
	tr.End(at(3), tok, 0)   // a second End of the same token
	if n, w := tr.Ledger(k); n != 0 || w != 0 {
		t.Fatalf("%d %d", n, w)
	}
}

func TestTrackedWorkerCapFallsBackAndCounts(t *testing.T) {
	tr := New(Config{MaxWorkers: 3, Window: 12 * time.Second})
	var obs []Observation
	for i := 0; i < 5; i++ {
		obs = append(obs, Observation{Key: key(fmt.Sprintf("w%d", i)), LastHeartbeat: t0})
	}
	tr.Sync(at(0), obs)
	s := tr.Stats()
	if s.Tracked != 3 || s.Untracked != 2 {
		t.Fatalf("stats %+v", s)
	}
	tok := tr.Begin(at(5), key("w4")) // beyond the cap
	if tok.Valid() {
		t.Fatal("a token for an untracked worker")
	}
	got := ov(tr, key("w4"), 100, 50, 1, 9)
	if got.Fallback != FallbackUntracked || got.Active != 9 {
		t.Fatalf("%+v", got)
	}
	tr.End(at(6), tok, 10) // harmless
}

// Worker-ID churn: 100,000 distinct workers come and go; the tracker holds only what the latest snapshot lists.
func TestWorkerChurnDoesNotGrow(t *testing.T) {
	tr := New(Config{Window: 12 * time.Second})
	var prev []Observation
	for i := 0; i < 100_000; i++ {
		k := Key{WorkerID: fmt.Sprintf("w-%d", i), Registered: t0.UnixNano()}
		cur := []Observation{{Key: k, LastHeartbeat: t0}}
		if i > 0 {
			cur = append(cur, prev[0])
		}
		now := at(i)
		tr.Sync(now, cur)
		tok := tr.Begin(now, k)
		tr.AddCost(tok, 10)
		tr.End(now, tok, 10)
		prev = cur[:1]
	}
	s := tr.Stats()
	if s.Tracked > 2 {
		t.Fatalf("tracked %d workers after churn", s.Tracked)
	}
	if n, w := tr.Totals(); n != 0 || w != 0 {
		t.Fatalf("leak: %d %d", n, w)
	}
	if tr.HistoryRecords() > 4 {
		t.Fatalf("history records %d", tr.HistoryRecords())
	}
}

// The history of one worker is bounded by the window, whatever the traffic.
func TestHistoryIsBounded(t *testing.T) {
	tr := New(Config{Window: 2 * time.Second})
	k := key("w")
	seed(tr, k)
	for ms := 0; ms < 600_000; ms += 7 {
		tok := tr.Begin(at(ms), k)
		tr.End(at(ms+3), tok, 0)
	}
	limit := int(2*time.Second/DefaultBucket) + 8
	if n := tr.HistoryRecords(); n > limit+40 { // + the compaction slack
		t.Fatalf("%d history records for a 2 s window (limit about %d)", n, limit)
	}
	tr.mu.Lock()
	capacity := cap(tr.entries[k].hist.recs)
	tr.mu.Unlock()
	if capacity > 4*limit+128 {
		t.Fatalf("backing array grew to %d", capacity)
	}
}

func TestObservedConversion(t *testing.T) {
	r := Result{Active: 3, Inflight: 2, Work: 1500}
	o := r.Observed()
	if o.EffectiveActive != 3 || o.InflightRequests != 2 || o.WorkAheadTokens != 1500 || o.Flags != 0 {
		t.Fatalf("%+v", o)
	}
	r.Fallback = FallbackHistory
	if r.Observed().Flags&protocol.ObservedColdStart == 0 {
		t.Fatal("a fallback must be flagged")
	}
}

// A token of an earlier generation of the same key (the entry was pruned and created again) must not add cost to
// the new entry.
func TestStaleTokenOfTheSameKeyCannotAddCost(t *testing.T) {
	tr := newTracker()
	k := key("w")
	seed(tr, k)
	old := tr.Begin(at(10), k)
	tr.Sync(at(20), nil) // the worker leaves the snapshot ...
	tr.Sync(at(30), []Observation{{Key: k, LastHeartbeat: t0}})
	fresh := tr.Begin(at(40), k) // ... and comes back as a new entry
	if tr.AddCost(old, 50) {
		t.Fatal("AddCost accepted a token of the previous generation")
	}
	if n, w := tr.Ledger(k); n != 1 || w != 0 {
		t.Fatalf("the new entry holds %d / %d, want 1 / 0", n, w)
	}
	tr.End(at(50), fresh, 0)
}

// A reset history holds no records (memory), and the ledger survives it.
func TestResetEmptiesTheHistory(t *testing.T) {
	tr := newTracker()
	k := key("w")
	tr.Sync(at(0), []Observation{{Key: k, LastHeartbeat: at(500)}})
	for i := 0; i < 20; i++ {
		tok := tr.Begin(at(100+i*100), k)
		tr.End(at(150+i*100), tok, 0)
	}
	if tr.HistoryRecords() == 0 {
		t.Fatal("no history recorded")
	}
	tr.Sync(at(3000), []Observation{{Key: k, LastHeartbeat: at(100)}})
	if n := tr.HistoryRecords(); n != 0 {
		t.Fatalf("%d history records after a reset", n)
	}
}

// A history that has dropped its oldest buckets answers "unknown" for a pivot at or before the last one it dropped,
// and only for those.
func TestHistorySinceAfterTrimming(t *testing.T) {
	var h history
	keep := int64(5)
	for idx := int64(0); idx < 20; idx++ {
		h.record(idx, true, keep)
		h.record(idx, false, keep)
	}
	if !h.hasDropped {
		t.Fatal("nothing was trimmed")
	}
	if _, ok := h.since(h.droppedIdx, true); ok {
		t.Fatal("a pivot at the last dropped bucket is unknown: its bucket's events were folded into the base")
	}
	if _, ok := h.since(h.droppedIdx-3, true); ok {
		t.Fatal("a pivot before the last dropped bucket is unknown")
	}
	n, ok := h.since(h.droppedIdx+1, true)
	if !ok || n != uint64(19-h.droppedIdx) {
		t.Fatalf("since(droppedIdx+1) = %d, %v; want %d, true", n, ok, 19-h.droppedIdx)
	}
}

// Buckets floor toward minus infinity, so an instant before the origin lands in the bucket below 0, not in bucket 0.
func TestBucketFloorsBelowTheOrigin(t *testing.T) {
	tr := newTracker()
	tr.Sync(at(1000), nil) // the origin is 1,000 ms
	for _, tc := range []struct {
		ms   int
		want int64
	}{{1000, 0}, {1099, 0}, {1100, 1}, {999, -1}, {901, -1}, {900, -1}, {899, -2}} {
		tr.mu.Lock()
		got := tr.bucket(at(tc.ms))
		tr.mu.Unlock()
		if got != tc.want {
			t.Errorf("bucket(%d ms) = %d, want %d", tc.ms, got, tc.want)
		}
	}
}

// A report time after now is taken as now: the events of the current bucket then count as after the report.
func TestAFutureReportTimeIsClampedToNow(t *testing.T) {
	tr := newTracker()
	k := key("w")
	seed(tr, k)
	tr.Begin(at(1050), k) // a start in the bucket of now (and a little later than the view's now)
	got := tr.Overlay(at(1000), Input{Key: k, Local: 0, Reported: 3, At: at(1500)})
	if got.Active != 4 {
		t.Fatalf("Active = %d, want 4 (reported 3 plus the start in now's bucket)", got.Active)
	}
}

// Nonsense counts from a report are treated as zero, never negative.
func TestNegativeCountsAreClamped(t *testing.T) {
	tr := newTracker()
	k := key("w")
	seed(tr, k)
	if got := tr.Overlay(at(1000), Input{Key: k, Local: -5, Reported: -3, At: at(900)}); got.Active != 0 {
		t.Fatalf("history path: %d", got.Active)
	}
	if got := tr.Overlay(at(1000), Input{Key: key("other"), Local: -5, Reported: -3, At: at(900)}); got.Active != 0 {
		t.Fatalf("fallback path: %d", got.Active)
	}
}
