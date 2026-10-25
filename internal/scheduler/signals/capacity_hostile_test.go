package signals

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

// A worker that reports an absurd rate (ADR-022, limits): its peak is cut to peerClamp times the median of the
// other workers' peaks, so it can inflate its capacity by at most that factor over the honest median.

const hostile = 1e9

func capsOf(tr *Tracker, ids ...string) []float64 {
	out := make([]float64, len(ids))
	for i, id := range ids {
		out[i], _ = capOf(tr, id)
	}
	return out
}

func TestHostileAmongHonestWorkersIsCutToKTimesTheHonestMedian(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 2, 100), rep("b", 1, 2, 110), rep("h", 1, 2, hostile), rep("c", 1, 2, 90)})
	// the others' peaks are 90, 100, 110: median 100
	near(t, "hostile", capsOf(tr, "h")[0], 1000) // K = 10 (ADR-022)
	for i, id := range []string{"a", "b", "c"} {
		near(t, id, capsOf(tr, id)[0], []float64{100, 110, 90}[i])
	}
}

func TestHostileAmongTwoIsCutToKTimesTheHonestOne(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 2, 100), rep("h", 1, 2, hostile)})
	near(t, "hostile", capsOf(tr, "h")[0], peerClamp*100)
	// the honest worker's estimate is its peak, or the floor under the target (the median of the two peaks)
	near(t, "honest", capsOf(tr, "a")[0], 0.25*(100+peerClamp*100)/2)
}

// With no other peak there is nothing to compare with: only maxTPS applies. The residual of the design.
func TestHostileAloneIsOnlyBoundedByMaxTPS(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("h", 1, 2, hostile)})
	near(t, "hostile alone", capsOf(tr, "h")[0], maxTPS)
}

// The lie was stored when the worker was alone; the first honest peer's sample cuts it.
func TestHostileThatArrivesFirstIsCutWhenAPeerHasASample(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("h", 1, 2, hostile)})
	near(t, "alone", capsOf(tr, "h")[0], maxTPS)
	tr.Sync(at(1000), []Observation{rep("h", 2, 2, hostile), rep("a", 1, 2, 100)})
	near(t, "after a peer", capsOf(tr, "h")[0], peerClamp*100)
	near(t, "honest", capsOf(tr, "a")[0], 0.25*(100+peerClamp*100)/2)
	// a peer without a sample (idle) is not a reference
	tr = capTracker()
	tr.Sync(at(0), []Observation{rep("h", 1, 2, hostile), rep("i", 1, 0, 100)})
	near(t, "idle peer", capsOf(tr, "h")[0], maxTPS)
}

// A hostile worker that goes away and registers again (a new incarnation: a new key, no stored peak) is cut again.
func TestHostileThatReRegistersIsCutAgain(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("h", 1, 2, hostile), rep("a", 1, 2, 100), rep("b", 1, 2, 100)})
	tr.Sync(at(1000), []Observation{rep("a", 2, 2, 100), rep("b", 2, 2, 100)}) // h is gone
	tr.Sync(at(2000), []Observation{rep("h2", 1, 2, hostile), rep("a", 3, 2, 100), rep("b", 3, 2, 100)})
	near(t, "re-registered", capsOf(tr, "h2")[0], peerClamp*100)
}

// A worker that is genuinely faster than K times the median is cut too: the price of the bound, and why it is a
// named constant.
func TestHonestWorkerWithinKIsUntouched(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 2, 100), rep("b", 1, 2, 100), rep("big", 1, 2, 950)})
	near(t, "big", capsOf(tr, "big")[0], 950)
}

// The clamp does not depend on the order of the workers in the snapshot.
func TestPeerClampIgnoresTheOrderOfTheWorkers(t *testing.T) {
	obs := []Observation{rep("a", 1, 2, 100), rep("b", 1, 2, 110), rep("h", 1, 2, hostile), rep("c", 1, 2, 90), rep("h2", 1, 2, 5e8)}
	var want []float64
	for p := 0; p < 24; p++ {
		rng := rand.New(rand.NewPCG(uint64(p), 7))
		o := append([]Observation(nil), obs...)
		rng.Shuffle(len(o), func(i, j int) { o[i], o[j] = o[j], o[i] })
		tr := capTracker()
		tr.Sync(at(0), o)
		got := capsOf(tr, "a", "b", "h", "c", "h2")
		if want == nil {
			want = got
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("permutation %d: %v differs from %v", p, got, want)
			}
		}
	}
}

// naiveClamp is the definition, by brute force: each peak is cut to K times the median of the others' peaks, the
// others' peaks being the ones before any is cut.
func naiveClamp(peaks map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for id, p := range peaks {
		var others []float64
		for id2, p2 := range peaks {
			if id2 != id {
				others = append(others, p2)
			}
		}
		if len(others) == 0 {
			out[id] = p
			continue
		}
		sort.Float64s(others)
		n := len(others)
		m := others[n/2]
		if n%2 == 0 {
			m = (others[n/2-1] + others[n/2]) / 2
		}
		out[id] = min(p, peerClamp*m)
	}
	return out
}

// Random fleets, with liars among them, over several syncs and two models, against the naive definition. Time does
// not advance, so the decay does not move a peak; the estimate is the peak raised to the floor under the model's
// target, and (as in sample) a new sample is never below the previous estimate.
func TestPeerClampMatchesTheNaiveReference(t *testing.T) {
	for seed := uint64(1); seed <= 300; seed++ {
		rng := rand.New(rand.NewPCG(seed, 99))
		tr := capTracker()
		peaks := map[string]map[string]float64{"m": {}, "n": {}} // the reference's stored peaks per model
		est := map[string]float64{}
		n := 1 + rng.IntN(8)
		for step := 1; step <= 4; step++ {
			var obs []Observation
			for i := 0; i < n; i++ {
				id := fmt.Sprintf("w%d", i)
				model := "m"
				if i%3 == 2 {
					model = "n"
				}
				o := rep(id, step, 1+rng.IntN(3), 0)
				o.Model = model
				switch rng.IntN(5) {
				case 0: // idle: no sample
					o.ReportedActive = 0
					o.ReportedTPS = 50
				case 1: // a liar
					o.ReportedTPS = hostile * (1 + rng.Float64())
				default:
					o.ReportedTPS = 20 + rng.Float64()*200
				}
				obs = append(obs, o)
				if o.ReportedActive >= 1 {
					v := min(o.ReportedTPS, maxTPS)
					if _, ok := peaks[model][id]; ok {
						v = max(v, est[id])
					}
					peaks[model][id] = v
				}
			}
			rng.Shuffle(len(obs), func(i, j int) { obs[i], obs[j] = obs[j], obs[i] })
			tr.Sync(at(0), obs)
			for _, ps := range peaks {
				want := naiveClamp(ps)
				var vals []float64
				for id, w := range want {
					ps[id] = w
					vals = append(vals, w)
				}
				if len(vals) == 0 {
					continue
				}
				tgt := refMedian(vals)
				for id, w := range want {
					est[id] = max(w, 0.25*tgt)
					r, prior := capOf(tr, id)
					if prior || r < est[id]*(1-1e-9) || r > est[id]*(1+1e-9) {
						t.Fatalf("seed %d step %d: %s capacity %v (prior=%v), reference %v", seed, step, id, r, prior, est[id])
					}
				}
			}
		}
	}
}
