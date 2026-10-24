package signals

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// The capacity estimate and the work-ahead terms (ADR-022, D5 and D6).

const halflife = 60 * time.Second

func capTracker() *Tracker {
	return New(Config{Window: 12 * time.Second, CapacityHalflife: halflife, CapacityFloor: 0.25})
}

// rep is an observation of a worker that is reporting tps with active requests, heartbeat number hb (seconds).
func rep(id string, hb int, active int, tps float64) Observation {
	return Observation{
		Key: key(id), LastHeartbeat: t0.Add(time.Duration(hb) * time.Second), Model: "m",
		ReportedActive: active, ReportedTPS: tps, Fresh: true,
	}
}

func near(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6*math.Max(1, math.Abs(want)) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func capOf(tr *Tracker, id string) (float64, bool) { return tr.Capacity(key(id)) }

// A reported rate is demand when the worker is lightly loaded: the estimate is the peak, not the latest.
func TestCapacityIsThePeakNotTheLatestSample(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 4, 100)})
	tr.Sync(at(1000), []Observation{rep("a", 2, 1, 30)}) // the load dropped: the report says 30
	if r, prior := capOf(tr, "a"); prior || math.Abs(r-100) > 1e-9 {
		t.Fatalf("capacity %v prior=%v: a lightly loaded worker is not a slow one, want the peak 100", r, prior)
	}
	tr.Sync(at(2000), []Observation{rep("a", 3, 4, 160)})
	if r, _ := capOf(tr, "a"); r != 160 {
		t.Fatalf("a higher sample raises the peak: %v", r)
	}
}

func TestCapacitySamplingRules(t *testing.T) {
	cases := []struct {
		name string
		obs  Observation
		want bool // a sample was taken
	}{
		{"fresh and busy", rep("a", 1, 2, 80), true},
		{"idle: nothing to measure", rep("a", 1, 0, 80), false},
		{"not fresh (not offered by the registry)", func() Observation { o := rep("a", 1, 2, 80); o.Fresh = false; return o }(), false},
		{"no heartbeat yet", func() Observation { o := rep("a", 1, 2, 80); o.LastHeartbeat = time.Time{}; return o }(), false},
		{"zero rate", rep("a", 1, 2, 0), false},
		{"below the smallest meaningful rate", rep("a", 1, 2, 0.0005), false},
		{"negative", rep("a", 1, 2, -5), false},
		{"NaN", rep("a", 1, 2, math.NaN()), false},
		{"+Inf", rep("a", 1, 2, math.Inf(1)), false},
		{"-Inf", rep("a", 1, 2, math.Inf(-1)), false},
		{"absurdly large is clamped, not trusted", rep("a", 1, 2, 1e300), true},
	}
	for _, tc := range cases {
		tr := capTracker()
		tr.Sync(at(0), []Observation{tc.obs})
		r, prior := capOf(tr, "a")
		if tc.want == prior {
			t.Errorf("%s: sampled=%v but prior=%v (capacity %v)", tc.name, tc.want, prior, r)
		}
		if tc.name == "absurdly large is clamped, not trusted" && r != maxTPS {
			t.Errorf("clamp: got %v, want %v", r, float64(maxTPS))
		}
		if !tc.want && r != 0 {
			t.Errorf("%s: an unsampled lone worker has unknown capacity, got %v", tc.name, r)
		}
	}
}

// One heartbeat is one sample, however many refreshes see it.
func TestTheSameReportIsSampledOnce(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 2, 100), rep("b", 1, 2, 100)})
	// a's next refreshes show the same heartbeat with a lower rate (a stale copy): ignored
	tr.Sync(at(100), []Observation{rep("a", 1, 2, 10), rep("b", 2, 2, 100)})
	if r, _ := capOf(tr, "a"); r < 99 {
		t.Fatalf("a repeated report lowered or replaced the estimate: %v", r)
	}
}

// Hand-computed. Peaks 200 (a) and 100 (b): the decay target is their median, 150. One half-life later, with no new
// sample, a is 150 + 50 x 0.5 = 175 and b is 150 - 50 x 0.5 = 125; after many half-lives both are 150. The fleet
// prior (what a worker with no sample gets) is the median of the estimates: 150.
func TestCapacityDecaysTowardTheFleetMedianWithTheHalflife(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 3, 200), rep("b", 1, 3, 100), {Key: key("c"), Model: "m", LastHeartbeat: t0.Add(time.Second)}})
	if r, _ := capOf(tr, "a"); r != 200 {
		t.Fatalf("a at the sample: %v", r)
	}
	idle := func(id string, hb int) Observation {
		return Observation{Key: key(id), Model: "m", LastHeartbeat: t0.Add(time.Duration(hb) * time.Second), Fresh: true}
	}
	tr.Sync(at(60_000), []Observation{idle("a", 2), idle("b", 2), idle("c", 2)})
	ra, _ := capOf(tr, "a")
	rb, _ := capOf(tr, "b")
	near(t, "a after one half-life", ra, 175)
	near(t, "b after one half-life", rb, 125)
	rc, prior := capOf(tr, "c")
	if !prior {
		t.Fatal("c never had a sample: its capacity is the prior")
	}
	near(t, "the fleet prior", rc, 150)
	tr.Sync(at(60_000+20*60_000), []Observation{idle("a", 3), idle("b", 3), idle("c", 3)})
	ra, _ = capOf(tr, "a")
	rb, _ = capOf(tr, "b")
	near(t, "a after twenty half-lives", ra, 150)
	near(t, "b after twenty half-lives", rb, 150)
}

// The decay is continuous: there is no moment at which an old estimate snaps to the prior.
func TestCapacityDecayHasNoSnap(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 3, 400), rep("b", 1, 3, 100), rep("c", 1, 3, 100)})
	var prev float64
	for ms := 0; ms <= 600_000; ms += 1000 {
		tr.Sync(at(ms), []Observation{{Key: key("a"), Model: "m", LastHeartbeat: t0.Add(time.Second)}, {Key: key("b"), Model: "m", LastHeartbeat: t0.Add(time.Second)}, {Key: key("c"), Model: "m", LastHeartbeat: t0.Add(time.Second)}})
		r, _ := capOf(tr, "a")
		if ms > 0 && math.Abs(r-prev) > 5 { // a step of 1 s moves a 400 -> 100 decay by under 1.2%
			t.Fatalf("at %d ms the estimate jumped from %v to %v", ms, prev, r)
		}
		prev = r
	}
}

// A very low sample is floored at a quarter of the prior: a worker is never estimated infinitely slow.
func TestCapacityFloorKeepsALowEstimateFinite(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 3, 1), rep("b", 1, 3, 1000), rep("c", 1, 3, 1000)})
	r, prior := capOf(tr, "a")
	if prior {
		t.Fatal("a has a sample")
	}
	near(t, "the floored estimate", r, 250) // 0.25 x the target (the median peak, 1000)
	// and a still recovers by decay once it stops being sampled
	idle := func(id string) Observation {
		return Observation{Key: key(id), Model: "m", LastHeartbeat: t0.Add(time.Second), Fresh: true}
	}
	tr.Sync(at(10*60_000), []Observation{idle("a"), idle("b"), idle("c")})
	r, _ = capOf(tr, "a")
	if r < 990 {
		t.Fatalf("a never recovered: %v", r)
	}
}

// Bounded exile: from any low estimate that is not refreshed, the estimate is within 10% of the prior after four
// half-lives.
func TestAnUnrefreshedLowEstimateRecoversWithinFourHalflives(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("slow", 1, 4, 1), rep("x", 1, 4, 100), rep("y", 1, 4, 100), rep("z", 1, 4, 100)})
	idle := func(id string) Observation {
		return Observation{Key: key(id), Model: "m", LastHeartbeat: t0.Add(time.Second), Fresh: true}
	}
	tr.Sync(at(4*60_000), []Observation{idle("slow"), idle("x"), idle("y"), idle("z")})
	if r, _ := capOf(tr, "slow"); r < 90 {
		t.Fatalf("after four half-lives the slow worker is estimated %v against a prior of 100", r)
	}
}

func TestACapacitySampleStartsFromTheDecayedPeak(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 3, 200), rep("b", 1, 3, 100)})
	// one half-life later a is 175; a new sample of 120 is below that: the peak is the decayed 175, not 200 and not 120
	tr.Sync(at(60_000), []Observation{{Key: key("a"), Model: "m", LastHeartbeat: t0.Add(time.Second)}, {Key: key("b"), Model: "m", LastHeartbeat: t0.Add(time.Second)}})
	tr.Sync(at(60_001), []Observation{rep("a", 2, 3, 120), {Key: key("b"), Model: "m", LastHeartbeat: t0.Add(time.Second)}})
	r, _ := capOf(tr, "a")
	near(t, "peak after a lower sample", r, 175)
}

func TestWorkersOfOtherModelsAreOtherFleets(t *testing.T) {
	tr := capTracker()
	a := rep("a", 1, 3, 100)
	b := rep("b", 1, 3, 900)
	b.Model = "other"
	cold := Observation{Key: key("c"), Model: "m", LastHeartbeat: t0.Add(time.Second)}
	coldOther := Observation{Key: key("d"), Model: "other", LastHeartbeat: t0.Add(time.Second)}
	tr.Sync(at(0), []Observation{a, b, cold, coldOther})
	if r, _ := capOf(tr, "c"); r != 100 {
		t.Fatalf("c takes its own fleet's prior: %v", r)
	}
	if r, _ := capOf(tr, "d"); r != 900 {
		t.Fatalf("d takes its own fleet's prior: %v", r)
	}
	// a fleet without any sample has no prior
	tr.Sync(at(1000), []Observation{{Key: key("e"), Model: "none", LastHeartbeat: t0.Add(time.Second)}})
	if r, prior := capOf(tr, "e"); r != 0 || !prior {
		t.Fatalf("no sample anywhere: %v %v", r, prior)
	}
}

func TestAReRegisteredWorkerStartsCold(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 3, 500), rep("b", 1, 3, 100)})
	reborn := rep("a", 1, 0, 0)
	reborn.Key = Key{WorkerID: "a", Registered: t0.Add(time.Hour).UnixNano()}
	tr.Sync(at(1000), []Observation{reborn, rep("b", 2, 0, 0)})
	r, prior := tr.Capacity(reborn.Key)
	if !prior || r != 100 {
		t.Fatalf("a re-registered worker is cold and takes the prior (b's 100): %v %v", r, prior)
	}
	if _, prior := capOf(tr, "a"); !prior {
		t.Fatal("the old incarnation was not pruned")
	}
}

// Load: WorkAhead = ledger + foreign x mean cost + queued x input weight. Hand-computed.
func TestLoadAddsTheLedgerTheForeignLoadAndTheQueuedTokens(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 3, 100)})
	tok := tr.Begin(at(100), key("a"))
	tr.AddCost(tok, 700)
	tr.ObserveCost("m", 500)
	// reported 5 on the worker (a report older than the history: the max rule), local 1: foreign = 5 - 1 = 4
	in := LoadInput{Input: Input{Key: key("a"), Local: 1, Reported: 5, At: at(-5000)}, Model: "m", QueuedTokens: 300, Cost: 123, InputWeight: 0.5}
	o := tr.Load(at(200), in)
	// ledger 700 + 4 x mean 500 + 300 x 0.5
	if o.WorkAheadTokens != 700+4*500+150 {
		t.Fatalf("work ahead %v, want 2850", o.WorkAheadTokens)
	}
	if o.EffectiveActive != 5 || o.InflightRequests != 1 || o.CapacityTPS != 100 {
		t.Fatalf("%+v", o)
	}
	if o.Flags&protocol.ObservedLedger == 0 || o.Flags&protocol.ObservedPriorCapacity != 0 || o.Flags&protocol.ObservedUntracked != 0 {
		t.Fatalf("flags %b", o.Flags)
	}
	// a tracked worker without a sample gets the fleet prior and says so
	tr.Sync(at(300), []Observation{rep("a", 1, 3, 100), {Key: key("cold"), Model: "m", LastHeartbeat: t0}})
	o = tr.Load(at(400), LoadInput{Input: Input{Key: key("cold"), At: at(0)}, Model: "m", Cost: 1, InputWeight: 1})
	if o.CapacityTPS != 100 || o.Flags&protocol.ObservedPriorCapacity == 0 || o.Flags&protocol.ObservedUntracked != 0 {
		t.Fatalf("cold tracked worker: %+v", o)
	}
	// before a mean exists the request's own cost stands in
	o = tr.Load(at(200), LoadInput{Input: in.Input, Model: "unseen", QueuedTokens: 0, Cost: 123, InputWeight: 1})
	if o.WorkAheadTokens != 700+4*123 {
		t.Fatalf("cold mean: %v, want 1192", o.WorkAheadTokens)
	}
}

func TestLoadClampsGarbageAndFlagsUntrackedWorkers(t *testing.T) {
	tr := capTracker()
	tr.Sync(at(0), []Observation{rep("a", 1, 3, 100)})
	base := LoadInput{Input: Input{Key: key("a"), Local: 0, Reported: 0, At: at(0)}, Model: "m", Cost: 10, InputWeight: 1}
	for _, tc := range []struct {
		name string
		mut  func(*LoadInput)
		want float64
	}{
		{"negative queued tokens", func(l *LoadInput) { l.QueuedTokens = -9 }, 0},
		{"huge queued tokens are capped at 2^31-1", func(l *LoadInput) { l.QueuedTokens = math.MaxInt64 }, float64(maxCount)},
		{"NaN input weight reads as 1", func(l *LoadInput) { l.QueuedTokens, l.InputWeight = 10, math.NaN() }, 10},
		{"negative input weight reads as 1", func(l *LoadInput) { l.QueuedTokens, l.InputWeight = 10, -4 }, 10},
		{"infinite input weight reads as 1", func(l *LoadInput) { l.QueuedTokens, l.InputWeight = 10, math.Inf(1) }, 10},
		{"a huge input weight is capped", func(l *LoadInput) { l.QueuedTokens, l.InputWeight = 10, 1e300 }, 10 * 1e4},
		{"negative reported and local counts", func(l *LoadInput) { l.Reported, l.Local = -3, -2 }, 0},
		{"huge reported count is clamped before it is priced", func(l *LoadInput) { l.Reported, l.Cost = math.MaxInt, 1 }, float64(maxCount)},
	} {
		in := base
		tc.mut(&in)
		o := tr.Load(at(100), in)
		if o.WorkAheadTokens != tc.want || math.IsNaN(o.WorkAheadTokens) {
			t.Errorf("%s: work ahead %v, want %v", tc.name, o.WorkAheadTokens, tc.want)
		}
	}
	o := tr.Load(at(100), LoadInput{Input: Input{Key: key("ghost"), At: at(0)}, Model: "m", Cost: 10, InputWeight: 1})
	if o.Flags&protocol.ObservedUntracked == 0 || o.Flags&protocol.ObservedPriorCapacity == 0 || o.Flags&protocol.ObservedLedger == 0 || o.CapacityTPS != 0 {
		t.Fatalf("an untracked worker: %+v", o)
	}
}

func TestMeanCostIsAnEWMAAndIsBoundedAndPruned(t *testing.T) {
	tr := New(Config{Window: 12 * time.Second, MaxModels: 3})
	tr.ObserveCost("m", 100)
	tr.ObserveCost("m", 200)
	if m, _ := tr.MeanCost("m"); math.Abs(m-110) > 1e-9 { // 100 + 0.1 x 100
		t.Fatalf("mean %v, want 110", m)
	}
	tr.ObserveCost("m", 0)
	tr.ObserveCost("m", -5)
	if m, _ := tr.MeanCost("m"); math.Abs(m-110) > 1e-9 {
		t.Fatalf("a free request moved the mean: %v", m)
	}
	for i := 0; i < 100; i++ {
		tr.ObserveCost(fmt.Sprintf("model-%d", i), 50)
	}
	tr.mu.Lock()
	n := len(tr.meanCost)
	tr.mu.Unlock()
	if n != 3 {
		t.Fatalf("%d models tracked, cap is 3", n)
	}
	// a model whose workers are gone is forgotten at the next sync
	tr.Sync(at(0), []Observation{{Key: key("a"), Model: "m", LastHeartbeat: t0}})
	if _, ok := tr.MeanCost("m"); !ok {
		t.Fatal("m still has a worker")
	}
	if _, ok := tr.MeanCost("model-0"); ok {
		t.Fatal("a model without workers kept its mean")
	}
	tr.Sync(at(1000), nil)
	if _, ok := tr.MeanCost("m"); ok {
		t.Fatal("pruned model kept its mean")
	}
}

// Worker-ID churn: nothing grows beyond the caps.
func TestCapacityStateIsBoundedUnderChurn(t *testing.T) {
	tr := New(Config{Window: 12 * time.Second, MaxWorkers: 50, MaxModels: 10})
	for i := 0; i < 20000; i++ {
		id := fmt.Sprintf("w%d", i)
		o := rep(id, 1, 2, 100)
		o.Model = fmt.Sprintf("m%d", i%40)
		tr.Sync(at(i), []Observation{o})
		tr.ObserveCost(o.Model, 10)
	}
	st := tr.Stats()
	if st.Tracked > 50 {
		t.Fatalf("%d workers tracked", st.Tracked)
	}
	tr.mu.Lock()
	n := len(tr.meanCost)
	tr.mu.Unlock()
	if n > 10 {
		t.Fatalf("%d model means", n)
	}
}

// --- a naive reference of the capacity rules, run against random sequences -------------------------------------

// refCapacity keeps, for each worker, the samples it was given (those that pass the rules) and recomputes the
// estimate from scratch at every step with the plan's definition: peak decayed toward the median of the peaks,
// floored, and the prior as the median of the estimates.
type refWorker struct {
	peak   float64
	peakAt time.Time
	has    bool
	lastHB time.Time
	est    float64
}

func refMedian(v []float64) float64 {
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}

func TestCapacityMatchesANaiveReferenceOnRandomSequences(t *testing.T) {
	rng := rand.New(rand.NewPCG(71, 72))
	for trial := 0; trial < 60; trial++ {
		tr := capTracker()
		ids := []string{"a", "b", "c", "d", "e"}
		ref := map[string]*refWorker{}
		hb := map[string]int{}
		now := 0
		for step := 0; step < 120; step++ {
			now += 500 + rng.IntN(120_000)
			var obs []Observation
			present := []string{}
			for _, id := range ids {
				if rng.IntN(6) == 0 {
					continue // the worker is out of this snapshot
				}
				present = append(present, id)
				if rng.IntN(2) == 0 {
					hb[id]++
				}
				o := rep(id, hb[id], rng.IntN(3), float64(rng.IntN(500))-20)
				o.Fresh = rng.IntN(8) != 0
				if hb[id] == 0 {
					o.LastHeartbeat = time.Time{}
				}
				obs = append(obs, o)
			}
			tr.Sync(at(now), obs)

			// the reference
			inSnap := map[string]bool{}
			for _, id := range present {
				inSnap[id] = true
			}
			for id := range ref {
				if !inSnap[id] {
					delete(ref, id)
				}
			}
			for _, o := range obs {
				id := o.Key.WorkerID
				w := ref[id]
				if w == nil {
					w = &refWorker{}
					ref[id] = w
				}
				if o.Fresh && o.ReportedActive >= 1 && !o.LastHeartbeat.IsZero() && !o.LastHeartbeat.Equal(w.lastHB) {
					w.lastHB = o.LastHeartbeat
					if o.ReportedTPS >= minTPS {
						v := math.Min(o.ReportedTPS, maxTPS)
						if w.has && w.est > v {
							v = w.est
						}
						w.peak, w.peakAt, w.has = v, at(now), true
					}
				}
			}
			// the peer clamp (ADR-022): each peak is cut to peerClamp times the median of the others' peaks
			stored := map[string]float64{}
			for id, w := range ref {
				if w.has {
					stored[id] = w.peak
				}
			}
			for id, v := range naiveClamp(stored) {
				ref[id].peak = v
			}
			var peaks []float64
			for _, w := range ref {
				if w.has {
					peaks = append(peaks, w.peak)
				}
			}
			var ests []float64
			if len(peaks) > 0 {
				target := refMedian(peaks)
				for _, w := range ref {
					if w.has {
						f := math.Pow(0.5, at(now).Sub(w.peakAt).Seconds()/halflife.Seconds())
						w.est = math.Max(target+(w.peak-target)*f, 0.25*target)
						ests = append(ests, w.est)
					}
				}
			}
			for _, id := range present {
				w := ref[id]
				got, prior := capOf(tr, id)
				switch {
				case w.has:
					if prior || math.Abs(got-w.est) > 1e-6*math.Max(1, w.est) {
						t.Fatalf("trial %d step %d: %s has %v (prior=%v), reference %v", trial, step, id, got, prior, w.est)
					}
				case len(ests) > 0:
					want := refMedian(ests)
					if !prior || math.Abs(got-want) > 1e-6*math.Max(1, want) {
						t.Fatalf("trial %d step %d: cold %s has %v (prior=%v), reference prior %v", trial, step, id, got, prior, want)
					}
				default:
					if !prior || got != 0 {
						t.Fatalf("trial %d step %d: no sample anywhere but %s has %v", trial, step, id, got)
					}
				}
			}
		}
	}
}
