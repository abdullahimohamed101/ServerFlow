package engine

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// --- extreme values ---------------------------------------------------------------------------------

func TestExtremeRequestValuesNeverCorruptTheEngine(t *testing.T) {
	ints := []int{math.MinInt, math.MinInt64, -1, 0, 1, 2, 1 << 20, 1<<31 - 1, 1 << 40, math.MaxInt64 - 1, math.MaxInt64, math.MaxInt}
	times := []time.Duration{0, time.Second, 1 << 60, math.MaxInt64 - 5, math.MaxInt64}
	p := quiet()
	p.KVBudgetTokens = 1000
	p.OutputFraction = 0.9
	for _, in := range ints {
		for _, mx := range ints {
			e := newEngine(t, p, 4, 4)
			for _, at := range times {
				_, _, err := e.Submit(at, Request{InputTokens: in, MaxTokens: mx})
				if cerr := e.CheckInvariants(); cerr != nil {
					t.Fatalf("Submit(%v, input %d, max_tokens %d) = %v left the engine inconsistent: %v", at, in, mx, err, cerr)
				}
				if in > 999 || in < 1 || mx < 0 {
					if err == nil {
						t.Fatalf("Submit(input %d, max_tokens %d) was accepted", in, mx)
					}
				}
			}
			runUntil(t, e, 10*time.Hour)
			if err := e.CheckInvariants(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestExtremeTimesSaturateInsteadOfWrapping(t *testing.T) {
	e := newEngine(t, quiet(), 2, 2)
	near := time.Duration(math.MaxInt64 - int64(time.Second))
	info, _, err := e.Submit(near, Request{InputTokens: 1, MaxTokens: 3})
	if err != nil {
		t.Fatal(err)
	}
	evs := runUntil(t, e, math.MaxInt64)
	if got := len(tokenTimes(evs, info.ID)); got != 3 {
		t.Errorf("%d tokens at the end of time, want 3", got)
	}
	prev := near
	for _, ev := range evs {
		if ev.At < prev {
			t.Fatalf("event %+v before its predecessor: the end time wrapped", ev)
		}
		prev = ev.At
	}
	if err := e.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Advance(0); !errors.Is(err, ErrTimeRegression) {
		t.Errorf("going back from the end of time: %v", err)
	}
}

func TestExtremeParametersSaturateInsteadOfOverflowing(t *testing.T) {
	p := quiet()
	p.StepBaseMs, p.PrefillPerTokenMs, p.PrefillFixedMs = maxStepMs, maxStepMs, maxStepMs
	p.StepNoiseCV, p.PrefillNoiseCV, p.OutputCV = maxCV, maxCV, maxCV
	p.KVBudgetTokens = 1 << 26
	e := newEngine(t, p, 2, 2)
	info, _ := mustSubmit(t, e, 0, Request{InputTokens: 1 << 25, MaxTokens: 3})
	evs := runUntil(t, e, 1000*24*time.Hour)
	prev := time.Duration(0)
	for _, ev := range evs {
		if ev.At < prev {
			t.Fatalf("time went backwards at %+v", ev)
		}
		prev = ev.At
	}
	if first := tokenTimes(evs, info.ID)[0]; first != maxIteration {
		t.Errorf("a prefill beyond a day must saturate at a day, got %v", first)
	}
}

func TestRandomExtremeScriptsKeepInvariants(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		cfg := randomConfig(rng, int64(seed))
		pick := func() int { // input tokens: extremes included
			return []int{0, 1, 1, 2, 50, 449, 450, 451, 1 << 20, math.MaxInt64, -1}[rng.IntN(11)]
		}
		pickMax := func() int { // max_tokens: kept small enough to run to completion
			return []int{0, 0, 1, 7, 100, 2000, -1}[rng.IntN(7)]
		}
		var steps []step
		at := time.Duration(0)
		for range 40 {
			at += time.Duration(rng.IntN(20)) * time.Millisecond
			steps = append(steps, step{at: at, cancel: -1, req: Request{InputTokens: pick(), MaxTokens: pickMax()}})
		}
		cfg.MaxQueue = 50
		play(t, cfg, steps) // fails on any inconsistency, panic or livelock
	}
}

// --- mutation survivors: clamps, queues, rounding, windows -------------------------------------------

func TestNoiseIsClampedOnBothSides(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	lo, hi := math.Inf(1), 0.0
	for range 400000 {
		f := lognormal(r, maxCV)
		lo, hi = math.Min(lo, f), math.Max(hi, f)
	}
	if hi != 20 || lo != 1.0/20 {
		t.Errorf("extremes of the noise factor = [%v, %v], want exactly [0.05, 20]", lo, hi)
	}
}

func TestPreemptedRequestGoesBackToTheFront(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 120
	e := newEngine(t, p, 3, 8)
	for range 4 { // three take the slots, the fourth waits for a slot
		mustSubmit(t, e, 0, Request{InputTokens: 30, MaxTokens: 60})
	}
	seen := false
	for steps := 0; steps < maxTestSteps; steps++ {
		at, ok := e.NextEvent()
		if !ok {
			break
		}
		evs, err := e.Advance(at)
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range evs {
			if ev.Kind != EvPreempt {
				continue
			}
			seen = true
			for i, w := range e.waiting {
				if w.id == ev.ID && i != 0 {
					t.Fatalf("preempted request %d waits at position %d, want the front", ev.ID, i)
				}
			}
		}
	}
	if !seen {
		t.Fatal("scenario produced no preemption")
	}
}

func TestPreemptionVictimIsTheNewestSequence(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 100
	e := newEngine(t, p, 8, 8)
	var ids []int64
	for range 3 {
		info, _ := mustSubmit(t, e, 0, Request{InputTokens: 20, MaxTokens: 80})
		ids = append(ids, info.ID)
	}
	var first *Event
	for steps := 0; steps < maxTestSteps && first == nil; steps++ {
		at, ok := e.NextEvent()
		if !ok {
			break
		}
		evs, _ := e.Advance(at)
		for i := range evs {
			if evs[i].Kind == EvPreempt {
				first = &evs[i]
				break
			}
		}
	}
	if first == nil {
		t.Fatal("no preemption")
	}
	if first.ID != ids[2] {
		t.Errorf("preempted request %d, want the newest, %d", first.ID, ids[2])
	}
}

// A sequence that is alone for decode but shares the engine with a prefilling one is preempted, not
// ended: only a truly alone sequence ends with "length" on KV exhaustion.
func TestRunningSequenceWithAPrefillingNeighbourIsPreemptedNotEnded(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 42
	p.PrefillChunkTokens = 10
	e := newEngine(t, p, 4, 4)
	a, _ := mustSubmit(t, e, 0, Request{InputTokens: 20, MaxTokens: 15})
	mustSubmit(t, e, 0, Request{InputTokens: 20, MaxTokens: 3})
	evs := runUntil(t, e, time.Hour)
	var done *Event
	for i := range evs {
		if evs[i].ID == a.ID && evs[i].Kind == EvDone {
			done = &evs[i]
		}
	}
	st := e.Stats()
	if done == nil || done.Index != 15 || st.KVExhausted != 0 || st.Preemptions == 0 {
		t.Errorf("done %+v, stats %+v: want A preempted and completed with all 15 tokens", done, st)
	}
}

func TestIterationsAlwaysTakeTime(t *testing.T) {
	p := quiet()
	p.StepBaseMs, p.StepPerSeqMs = 1e-9, 0
	p.PrefillFixedMs, p.PrefillPerTokenMs = 1e-9, 0
	e := newEngine(t, p, 2, 2)
	info, evs := mustSubmit(t, e, 0, Request{InputTokens: 1, MaxTokens: 20})
	evs = append(evs, runUntil(t, e, time.Hour)...)
	ts := tokenTimes(evs, info.ID)
	if len(ts) != 20 {
		t.Fatalf("%d tokens", len(ts))
	}
	if ts[0] < minIteration {
		t.Errorf("a prefill took %v; every iteration must take at least %v", ts[0], minIteration)
	}
	for i := 1; i < len(ts); i++ {
		if ts[i]-ts[i-1] < minIteration {
			t.Fatalf("iterations %d and %d are %v apart; virtual time must advance by at least %v", i-1, i, ts[i]-ts[i-1], minIteration)
		}
	}
}

func TestRecentRateWindowIsFiveSeconds(t *testing.T) {
	var w rateWindow
	w.record(0, 1)
	w.record(150*time.Millisecond, 1000) // bucket 1
	if got, want := w.rate(5100*time.Millisecond), 200.0; got != want {
		t.Errorf("rate at 5.1s = %v, want %v (bucket 1 is the oldest of the 50 buckets)", got, want)
	}
	if got := w.rate(5200 * time.Millisecond); got != 0 {
		t.Errorf("rate at 5.2s = %v, want 0: bucket 1 is older than the 5 s window", got)
	}
}

func TestUtilisationCountsPrefillingSequencesToo(t *testing.T) {
	e := newEngine(t, quiet(), 4, 4)
	mustSubmit(t, e, 0, Request{InputTokens: 5000, MaxTokens: 5}) // a 52ms prefill is under way
	st := e.Stats()
	if st.Prefilling != 1 || st.Running != 0 || st.GPUUtilization != 25 {
		t.Errorf("stats = %+v, want 1 prefilling, 0 running, 25%% utilisation", st)
	}
}

func TestPlannedOutputIsBoundedWhateverMaxTokensIs(t *testing.T) {
	p := quiet()
	p.OutputFraction = 0.9
	e := newEngine(t, p, 1, 4)
	info, _ := mustSubmit(t, e, 0, Request{InputTokens: 1, MaxTokens: math.MaxInt64})
	if info.OutputTokens < 1 || info.OutputTokens > maxOutput {
		t.Errorf("planned output %d for max_tokens MaxInt64, want 1..%d", info.OutputTokens, maxOutput)
	}
}

func TestMillisecondsSaturateAtADay(t *testing.T) {
	for _, v := range []float64{1e30, math.MaxFloat64, 1e15} {
		if got := ms(v); got != maxIteration {
			t.Errorf("ms(%v) = %v, want %v", v, got, maxIteration)
		}
	}
	if got := ms(1.5); got != 1500*time.Microsecond {
		t.Errorf("ms(1.5) = %v", got)
	}
}
