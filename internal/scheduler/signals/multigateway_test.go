package signals_test

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"serverflow/internal/scheduler/signals"
)

// Multi-gateway regression guard for the ledger overlay (plan D4(a), ADR-022). The simulation is the independent
// verifier's (adapted from outside the repository): millisecond ticks, G gateways sharing one worker, worker-side
// truth, start/finish network latency, heartbeat jitter, drops and sampling delay, snapshot refresh every ~100 ms,
// bursty open-loop and closed-loop clients, optional untracked foreign load and a registry restart, all with fixed
// seeds. Admission follows the truth (an oracle), so both estimators are judged on exactly the same traffic.
//
// "wrong-free" = the estimator says there is room while the worker truly is full: the exposure of subtracting this
// gateway's own finishes when other gateways' load is invisible. "wrong-full" = it says full while the worker has
// room. Simulator results, not hardware.

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

type estAcc struct {
	n, truthFull, truthNotFull int
	wrongFull, wrongFree       int
	sumSigned, sumAbs          float64
}

func (a *estAcc) add(est, truth, c int) {
	a.n++
	if truth >= c {
		a.truthFull++
		if est < c {
			a.wrongFree++
		}
	} else {
		a.truthNotFull++
		if est >= c {
			a.wrongFull++
		}
	}
	d := float64(est - truth)
	a.sumSigned += d
	a.sumAbs += math.Abs(d)
}

type advCfg struct {
	name    string
	C       int
	lo, hi  float64 // ms duration range, log-uniform
	rho     float64
	G       int
	H       int // ms heartbeat
	closed  bool
	burst   bool
	foreign float64 // fraction of load from untracked source
	drop    float64 // heartbeat drop prob
	restart bool
	policy  string // ledger|max|oracle
	seed    int64
	secs    int
}

type advRes struct {
	arr, grid                                  [3]estAcc // 0 ledger 1 max 2 reportedOnly(not used) -> index: ledger, max
	overSubAdmits, admits, overSubTicks, ticks int
	belowRepGrid, gridN                        int
	maxBelowRep                                int
	lOnly, truthFullN, undercnt2, gridCnt      int
}

func logu(r *rand.Rand, lo, hi float64) float64 {
	return math.Exp(math.Log(lo) + r.Float64()*(math.Log(hi)-math.Log(lo)))
}

func runAdv(cfg advCfg) (res advRes) {
	rng := rand.New(rand.NewSource(cfg.seed))
	base := time.Unix(1_700_000_000, 123456789)
	tickT := func(k int) time.Time { return base.Add(time.Duration(k) * time.Millisecond) }
	T := cfg.secs * 1000
	ev := make([][]func(int), T+20000)
	at := func(k int, f func(int)) {
		if k < len(ev) {
			ev[k] = append(ev[k], f)
		}
	}
	key := signals.Key{WorkerID: "w", Registered: 1}
	trs := make([]*signals.Tracker, cfg.G)
	local := make([]int, cfg.G)
	snapRep := make([]int, cfg.G)
	snapHB := make([]time.Time, cfg.G)
	for g := range trs {
		trs[g] = signals.New(signals.Config{Window: time.Duration(cfg.H*10+cfg.H) * time.Millisecond})
		trs[g].Sync(tickT(0), []signals.Observation{{Key: key}})
	}
	truth := 0
	foreignN := 0
	// registry state
	var regRep int
	var regHB time.Time
	var prevRep int
	var prevHB time.Time
	// mean duration
	mean := (cfg.hi - cfg.lo) / math.Log(cfg.hi/cfg.lo)
	total := cfg.rho * float64(cfg.C) / (mean / 1000) // req/s total
	own := total * (1 - cfg.foreign)
	// heartbeat chain
	var hbTick func(k int)
	hbTick = func(k int) {
		// worker samples (truth includes foreign)
		val := truth
		if rng.Float64() >= cfg.drop {
			d := rng.Intn(51)
			sub := time.Duration(rng.Intn(1_000_000))
			rk := k + d
			at(rk, func(kk int) {
				prevRep, prevHB = regRep, regHB
				regRep, regHB = val, tickT(kk).Add(sub)
			})
		}
		nxt := cfg.H + int(float64(cfg.H)*0.4*(rng.Float64()-0.5)) // +-20%
		at(k+nxt, hbTick)
	}
	at(rng.Intn(cfg.H), hbTick)
	if cfg.restart {
		at(T/2, func(kk int) {
			// registry reloaded older state: last heartbeat goes backwards
			if !prevHB.IsZero() {
				regRep, regHB = prevRep, prevHB.Add(-2*time.Second)
			}
		})
	}
	// worker/gateway request lifecycle
	startReq := func(g int, k int, foreign bool) {
		dur := int(logu(rng, cfg.lo, cfg.hi))
		if dur < 1 {
			dur = 1
		}
		n1, n2 := rng.Intn(4), rng.Intn(4)
		if foreign {
			at(k+n1, func(int) { truth++ })
			at(k+n1+dur, func(int) { truth-- })
			return
		}
		tok := trs[g].Begin(tickT(k), key)
		local[g]++
		at(k+n1, func(int) { truth++ })
		fw := k + n1 + dur
		at(fw, func(int) { truth-- })
		at(fw+n2, func(kk int) {
			trs[g].End(tickT(kk), tok, 0)
			local[g]--
		})
	}
	eval := func(g, k int) (eff, mx int) {
		if snapHB[g].IsZero() {
			return -1, -1
		}
		r := trs[g].Overlay(tickT(k), signals.Input{Key: key, Local: local[g], Reported: snapRep[g], At: snapHB[g]})
		return r.Active, max(local[g], snapRep[g])
	}
	// snapshot refresh per gateway
	for g := 0; g < cfg.G; g++ {
		g := g
		var rf func(k int)
		rf = func(k int) {
			snapRep[g], snapHB[g] = regRep, regHB
			if !regHB.IsZero() {
				trs[g].Sync(tickT(k), []signals.Observation{{Key: key, LastHeartbeat: regHB}})
			}
			at(k+100+rng.Intn(21)-10, rf)
		}
		at(rng.Intn(100), rf)
	}
	admit := func(g, k int) bool {
		eff, mx := eval(g, k)
		if eff < 0 {
			return false
		}
		var ok bool
		switch cfg.policy {
		case "ledger":
			ok = eff < cfg.C
		case "max":
			ok = mx < cfg.C
		default:
			ok = truth < cfg.C
		}
		res.arr[0].add(eff, truth, cfg.C)
		res.arr[1].add(mx, truth, cfg.C)
		if ok {
			res.admits++
			if truth >= cfg.C {
				res.overSubAdmits++
			}
		}
		return ok
	}
	// load generation
	for g := 0; g < cfg.G; g++ {
		g := g
		if cfg.closed {
			// closed loop: N clients per gateway, think time sized to hit rho
			n := int(math.Max(4, 3*float64(cfg.C)/float64(cfg.G)*2))
			think := float64(n) / (own / float64(cfg.G)) * 1000 // ms mean think (open approx.)
			think -= mean
			if think < 1 {
				think = 1
			}
			var cl func(k int)
			cl = func(k int) {
				if k >= T {
					return
				}
				if admit(g, k) {
					startReq(g, k, false)
				}
				at(k+1+int(rng.ExpFloat64()*think), cl)
			}
			for i := 0; i < n; i++ {
				at(rng.Intn(500), cl)
			}
			continue
		}
		hi := true
		lam := func() float64 {
			l := own / float64(cfg.G)
			if cfg.burst {
				if hi {
					return l * 3
				}
				return l * 0.25
			}
			return l
		}
		if cfg.burst {
			var sw func(k int)
			sw = func(k int) { hi = !hi; at(k+int(rng.ExpFloat64()*1500)+1, sw) }
			at(1000, sw)
		}
		var ar func(k int)
		ar = func(k int) {
			if k >= T {
				return
			}
			if admit(g, k) {
				startReq(g, k, false)
			}
			at(k+1+int(rng.ExpFloat64()*1000/lam()), ar)
		}
		at(rng.Intn(100)+500, ar)
	}
	if cfg.foreign > 0 {
		var fr func(k int)
		fr = func(k int) {
			if k >= T {
				return
			}
			if truth < cfg.C+2 { // foreign obeys capacity roughly
				startReq(0, k, true)
			}
			at(k+1+int(rng.ExpFloat64()*1000/(total*cfg.foreign)), fr)
		}
		at(100, fr)
	}
	_ = foreignN
	for k := 0; k < T; k++ {
		for i := 0; i < len(ev[k]); i++ { // may append to same tick
			ev[k][i](k)
		}
		ev[k] = nil
		if k > 3000 && k%3 == 0 {
			res.ticks++
			if truth > cfg.C {
				res.overSubTicks++
			}
			for g := 0; g < cfg.G; g++ {
				eff, mx := eval(g, k)
				if eff < 0 {
					continue
				}
				res.grid[0].add(eff, truth, cfg.C)
				res.grid[1].add(mx, truth, cfg.C)
				res.gridN++
				res.gridCnt++
				if truth >= cfg.C {
					res.truthFullN++
					if eff < cfg.C && mx >= cfg.C {
						res.lOnly++
					}
				}
				if eff < truth-1 && eff >= local[g] && truth > mx {
					res.undercnt2++
				}
				if eff < snapRep[g] {
					res.belowRepGrid++
					if snapRep[g]-eff > res.maxBelowRep {
						res.maxBelowRep = snapRep[g] - eff
					}
				}
			}
		}
	}
	return
}

func TestMultiGatewayLedgerIsNotMuchWorseThanMax(t *testing.T) {
	const secs = 60
	var cfgs []advCfg
	for _, G := range []int{2, 3} {
		for _, d := range [][2]float64{{20, 300}, {200, 3000}} {
			for _, rho := range []float64{0.7, 0.95} {
				cfgs = append(cfgs, advCfg{name: "open", C: 8, lo: d[0], hi: d[1], rho: rho, G: G, H: 1000, drop: 0.05, secs: secs})
				cfgs = append(cfgs, advCfg{name: "burst", C: 8, lo: d[0], hi: d[1], rho: rho, G: G, H: 1000, burst: true, drop: 0.05, secs: secs})
			}
		}
	}
	cfgs = append(cfgs,
		advCfg{name: "c32", C: 32, lo: 200, hi: 3000, rho: 0.9, G: 3, H: 1000, burst: true, drop: 0.05, secs: secs},
		advCfg{name: "foreign50", C: 8, lo: 20, hi: 300, rho: 0.9, G: 2, H: 1000, foreign: 0.5, drop: 0.05, secs: secs},
		advCfg{name: "restart", C: 8, lo: 20, hi: 300, rho: 0.9, G: 2, H: 1000, restart: true, drop: 0.05, secs: secs},
	)
	var l, m estAcc
	for _, c := range cfgs {
		c.policy = "oracle"
		for seed := int64(1); seed <= 3; seed++ {
			c.seed = seed
			r := runAdv(c)
			mergeAcc(&l, r.grid[0])
			mergeAcc(&m, r.grid[1])
		}
	}
	lf, mf := pct(l.wrongFree, l.truthFull), pct(m.wrongFree, m.truthFull)
	lw, mw := pct(l.wrongFull, l.truthNotFull), pct(m.wrongFull, m.truthNotFull)
	t.Logf("%d configurations x 3 seeds, %d samples: wrong-free ledger %.1f%% max %.1f%%; wrong-full ledger %.1f%% max %.1f%%", len(cfgs), l.n, lf, mf, lw, mw)
	if l.truthFull < 1000 || l.truthNotFull < 1000 {
		t.Fatalf("too few samples (%d full, %d not full): the simulation is not exercising the cases", l.truthFull, l.truthNotFull)
	}
	if lf > mf+3 {
		t.Errorf("ledger says free for a truly full worker %.1f%% of the time against max's %.1f%%: more than 3 points worse", lf, mf)
	}
	if lw > mw+5 {
		t.Errorf("ledger says full for a worker with room %.1f%% of the time against max's %.1f%%: more than 5 points worse", lw, mw)
	}
}

func mergeAcc(dst *estAcc, src estAcc) {
	dst.n += src.n
	dst.truthFull += src.truthFull
	dst.truthNotFull += src.truthNotFull
	dst.wrongFull += src.wrongFull
	dst.wrongFree += src.wrongFree
}
