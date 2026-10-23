package signals

import (
	"math/rand/v2"
	"testing"
	"time"
)

// refEntry is the naive reference: the definition of the overlay applied to a plain list of events.
type refEntry struct {
	gen       uint64
	inflight  int
	work      int64
	validFrom int64 // ms
	lastHB    int64
	hasHB     bool
	events    []refEvent
}

type refEvent struct {
	ms    int64
	start bool
}

type refModel struct {
	entries  map[Key]*refEntry
	nextGen  uint64
	epochMs  int64
	windowMs int64
}

func (m *refModel) bucket(ms int64) int64 {
	d := ms - m.epochMs
	b := d / 100
	if d < 0 && d%100 != 0 {
		b--
	}
	return b
}

func (m *refModel) overlay(nowMs int64, k Key, local, reported int, atMs int64) (active int, fb Fallback) {
	e := m.entries[k]
	base := max(local, reported)
	if e == nil {
		return base, FallbackUntracked
	}
	if atMs > nowMs {
		atMs = nowMs
	}
	if atMs < e.validFrom || nowMs-atMs > m.windowMs {
		return base, FallbackHistory
	}
	b := m.bucket(atMs)
	var s, f int
	for _, ev := range e.events {
		if ev.start && m.bucket(ev.ms) >= b {
			s++
		}
		if !ev.start && m.bucket(ev.ms) >= b+2 {
			f++
		}
	}
	return max(local, reported+s-f), FallbackNone
}

type liveTok struct {
	tok  Token
	cost int64
	gen  uint64
	key  Key
}

func TestOverlayMatchesNaiveReference(t *testing.T) {
	for seedN := uint64(1); seedN <= 40; seedN++ {
		rng := rand.New(rand.NewPCG(seedN, 99))
		windowMs := int64(5000)
		tr := New(Config{Window: time.Duration(windowMs) * time.Millisecond})
		m := &refModel{entries: map[Key]*refEntry{}, windowMs: windowMs}
		keys := []Key{key("a"), key("b"), key("c")}
		var toks []liveTok
		nowMs := int64(0)
		tr.Sync(at(0), nil) // fixes the bucket origin at 0 ms, as the reference assumes
		for step := 0; step < 3000; step++ {
			nowMs += int64(rng.IntN(250))
			now := at(int(nowMs))
			switch op := rng.IntN(10); {
			case op == 0: // sync: a random subset, heartbeats mostly advancing, sometimes going back
				var obs []Observation
				present := map[Key]int64{}
				for _, k := range keys {
					if rng.IntN(5) == 0 {
						continue
					}
					hb := nowMs - int64(rng.IntN(300))
					if e := m.entries[k]; e != nil && e.hasHB && rng.IntN(8) != 0 {
						hb = max(hb, e.lastHB)
					}
					present[k] = hb
					obs = append(obs, Observation{Key: k, LastHeartbeat: at(int(hb))})
				}
				tr.Sync(now, obs)
				for k := range m.entries {
					if _, ok := present[k]; !ok {
						delete(m.entries, k)
					}
				}
				for k, hb := range present {
					e := m.entries[k]
					if e == nil {
						m.nextGen++
						e = &refEntry{gen: m.nextGen, validFrom: nowMs}
						m.entries[k] = e
					}
					if e.hasHB && hb < e.lastHB {
						e.events, e.validFrom = nil, nowMs
					}
					e.lastHB, e.hasHB = hb, true
				}
			case op <= 3: // begin (+ cost)
				k := keys[rng.IntN(len(keys))]
				tok := tr.Begin(now, k)
				e := m.entries[k]
				if e == nil {
					m.nextGen++
					e = &refEntry{gen: m.nextGen, validFrom: nowMs}
					m.entries[k] = e
				}
				e.inflight++
				e.events = append(e.events, refEvent{nowMs, true})
				cost := int64(rng.IntN(200))
				if tr.AddCost(tok, cost) {
					e.work += cost
				} else {
					cost = 0
				}
				toks = append(toks, liveTok{tok, cost, e.gen, k})
			case op <= 6: // end a random outstanding token (stale ones too)
				if len(toks) == 0 {
					continue
				}
				i := rng.IntN(len(toks))
				lt := toks[i]
				toks = append(toks[:i], toks[i+1:]...)
				tr.End(now, lt.tok, lt.cost)
				if e := m.entries[lt.key]; e != nil && e.gen == lt.gen {
					e.inflight--
					e.work -= lt.cost
					e.events = append(e.events, refEvent{nowMs, false})
				}
			default: // query
				k := keys[rng.IntN(len(keys))]
				local, reported := rng.IntN(6), rng.IntN(12)
				atMs := nowMs - int64(rng.IntN(int(windowMs)+2000))
				got := tr.Overlay(now, Input{Key: k, Local: local, Reported: reported, At: at(int(atMs))})
				want, fb := m.overlay(nowMs, k, local, reported, atMs)
				if got.Active != want || got.Fallback != fb {
					t.Fatalf("seed %d step %d: key %v local=%d reported=%d now=%d at=%d: got %d (fb %d), want %d (fb %d)", seedN, step, k.WorkerID, local, reported, nowMs, atMs, got.Active, got.Fallback, want, fb)
				}
				if got.Active < local {
					t.Fatalf("below local")
				}
				if e := m.entries[k]; e != nil && (got.Inflight != e.inflight || got.Work != e.work) {
					t.Fatalf("seed %d step %d: ledger %d/%d, want %d/%d", seedN, step, got.Inflight, got.Work, e.inflight, e.work)
				}
			}
		}
	}
}
