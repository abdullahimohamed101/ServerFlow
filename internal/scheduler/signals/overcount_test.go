package signals_test

import (
	"container/heap"
	"math/rand"
	"testing"
	"time"

	"serverflow/internal/scheduler/signals"
)

// Regression guard for the ledger overlay's boundary classification (plan D4(a), ADR-022). With a finish lag of 2
// buckets every request that started and finished around a report stayed a permanent +1 until the next report, and
// at short request lengths the worker looked full for the whole heartbeat interval (99.7% "wrong-full" against 0.9%
// for max in the case below). The simulation is the verifier's: one worker, one gateway, open-loop Poisson arrivals,
// heartbeats and registry reads on a virtual clock, the real Tracker. "Wrong-full" = the estimator says full while
// the gateway's own count is below capacity. Simulator results, not hardware.

type fin struct {
	at  time.Time
	tok signals.Token
}
type finHeap []fin

func (h finHeap) Len() int           { return len(h) }
func (h finHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h finHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *finHeap) Push(x any)        { *h = append(*h, x.(fin)) }
func (h *finHeap) Pop() any          { o := *h; x := o[len(o)-1]; *h = o[:len(o)-1]; return x }

type simRes struct {
	arrivals, refusedLedger, refusedMax, wrongLedger, wrongMax int
	maxOver                                                    int
	sumOver                                                    float64
	oversubscribed                                             int // arrivals admitted while the gateway itself held a full worker
}

// one worker, one gateway; reports at heartbeat times; open-loop Poisson arrivals; admit iff effective < C.

func simulateWorker(seed int64, C int, d time.Duration, rho float64, H time.Duration, lag time.Duration, useLedgerAdmission bool, oracle bool) simRes {
	rng := rand.New(rand.NewSource(seed))
	t0 := time.Unix(1_700_000_000, 0)
	tr := signals.New(signals.Config{Window: 12 * time.Second})
	key := signals.Key{WorkerID: "w", Registered: 1}
	lambda := rho * float64(C) / d.Seconds() // per second
	var fh finHeap
	local := 0
	now := t0
	tr.Sync(now, []signals.Observation{{Key: key}})
	var reported int
	var lastHB time.Time // registry's LastHeartbeat
	var fetched time.Time
	var hbAgeAtFetch float64
	nextHB := t0.Add(H / 3)
	nextFetch := t0
	nextArr := t0.Add(time.Duration(rng.ExpFloat64() / lambda * float64(time.Second)))
	end := t0.Add(120 * time.Second)
	var res simRes
	type pend struct {
		at  time.Time
		val int
	}
	var hbq []pend // heartbeats in flight to registry (lag)
	for now.Before(end) {
		// next event time
		next := nextArr
		if nextHB.Before(next) {
			next = nextHB
		}
		if nextFetch.Before(next) {
			next = nextFetch
		}
		if len(fh) > 0 && fh[0].at.Before(next) {
			next = fh[0].at
		}
		if len(hbq) > 0 && hbq[0].at.Before(next) {
			next = hbq[0].at
		}
		now = next
		switch {
		case len(fh) > 0 && !fh[0].at.After(now):
			f := heap.Pop(&fh).(fin)
			tr.End(now, f.tok, 0)
			local--
		case len(hbq) > 0 && !hbq[0].at.After(now):
			// registry receives
			lastHB = hbq[0].at
			reported = hbq[0].val
			hbq = hbq[1:]
		case !nextHB.After(now):
			// worker samples now, registry gets it after lag
			hbq = append(hbq, pend{now.Add(lag), local})
			nextHB = nextHB.Add(H)
			// receipt time is LastHeartbeat (registry clock at receipt)
		case !nextFetch.After(now):
			fetched = now
			if !lastHB.IsZero() {
				hbAgeAtFetch = fetched.Sub(lastHB).Seconds()
			}
			tr.Sync(now, []signals.Observation{{Key: key, LastHeartbeat: lastHB}})
			nextFetch = nextFetch.Add(100 * time.Millisecond)
		case !nextArr.After(now):
			nextArr = now.Add(time.Duration(rng.ExpFloat64() / lambda * float64(time.Second)))
			if lastHB.IsZero() {
				continue
			}
			res.arrivals++
			age := now.Sub(fetched)
			at := signals.ReportTime(now, hbAgeAtFetch+age.Seconds())
			r := tr.Overlay(now, signals.Input{Key: key, Local: local, Reported: reported, At: at})
			eff := r.Active
			mx := max(local, reported)
			if eff >= C {
				res.refusedLedger++
				if local < C {
					res.wrongLedger++
				}
			}
			if mx >= C {
				res.refusedMax++
				if local < C {
					res.wrongMax++
				}
			}
			if eff-local > res.maxOver {
				res.maxOver = eff - local
			}
			res.sumOver += float64(eff - local)
			admit := eff < C
			if !useLedgerAdmission {
				admit = mx < C
			}
			if oracle {
				admit = local < C
			}
			if admit {
				if local >= C {
					res.oversubscribed++
				}
				tok := tr.Begin(now, key)
				local++
				dur := time.Duration(float64(d) * (0.5 + rng.Float64()))
				heap.Push(&fh, fin{now.Add(dur), tok})
			}
		}
	}
	return res
}

func TestLedgerWrongFullStaysCloseToMax(t *testing.T) {
	const marginPP = 5.0 // percentage points
	for _, c := range []struct {
		slots int
		d     time.Duration
		rho   float64
	}{{8, 20 * time.Millisecond, 0.3}, {8, 100 * time.Millisecond, 0.7}, {32, 100 * time.Millisecond, 0.7}, {8, 500 * time.Millisecond, 0.7}} {
		for _, hb := range []time.Duration{time.Second, 2 * time.Second} {
			r := simulateWorker(7, c.slots, c.d, c.rho, hb, 0, true, true)
			if r.arrivals < 500 {
				t.Fatalf("%+v: only %d arrivals", c, r.arrivals)
			}
			l, m := 100*float64(r.wrongLedger)/float64(r.arrivals), 100*float64(r.wrongMax)/float64(r.arrivals)
			t.Logf("slots=%2d d=%-6v rho=%.1f hb=%v: wrong-full ledger %.1f%% max %.1f%% (mean over-count %.2f)", c.slots, c.d, c.rho, hb, l, m, r.sumOver/float64(r.arrivals))
			// heartbeat 1 s is the case the verifier measured; 2 s is looser because a report stays in force longer
			margin := marginPP
			if hb > time.Second {
				margin = 8
			}
			if l > m+margin {
				t.Errorf("%+v hb=%v: ledger says full wrongly %.1f%% of the time, max %.1f%%: the boundary classification over-counts again", c, hb, l, m)
			}
		}
	}
}

// Whatever the classification does at the boundary, the effective load is never below the gateway's own count, so
// the gateway never admits to a worker it knows is full: no oversubscription from this gateway.
func TestLedgerNeverAdmitsToAWorkerTheGatewayKnowsIsFull(t *testing.T) {
	for _, d := range []time.Duration{2 * time.Millisecond, 20 * time.Millisecond, 200 * time.Millisecond, 2 * time.Second} {
		for _, rho := range []float64{0.5, 0.95, 1.5} {
			r := simulateWorker(3, 8, d, rho, time.Second, 0, true, false)
			if r.oversubscribed != 0 {
				t.Errorf("d=%v rho=%.2f: %d admissions to a worker the gateway held full", d, rho, r.oversubscribed)
			}
		}
	}
}
