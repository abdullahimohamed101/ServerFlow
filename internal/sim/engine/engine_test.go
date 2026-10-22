package engine

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// quiet is a noise-free, easy-to-compute world: decode step 10ms + 1ms*b, prefill 2ms + 0.01ms/token.
func quiet() Params {
	return Params{
		Name: "test", Provenance: "test",
		StepBaseMs: 10, StepPerSeqMs: 1,
		PrefillFixedMs: 2, PrefillPerTokenMs: 0.01,
		KVBudgetTokens: 1 << 20, KVBytesPerToken: 1 << 20, WeightsMB: 100,
	}
}

// newEngine builds an engine whose natural output is long, so max_tokens decides each request's length.
func newEngine(t testing.TB, p Params, seqs, queue int) *Engine {
	return newEngineMean(t, p, seqs, queue, 100000)
}

func newEngineMean(t testing.TB, p Params, seqs, queue, mean int) *Engine {
	t.Helper()
	e, err := New(Config{Params: p, Seed: 1, MaxSeqs: seqs, MaxQueue: queue, OutputMeanTokens: mean})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// runUntil advances the engine event by event until the given time (or idle) and returns all events.
func runUntil(t testing.TB, e *Engine, until time.Duration) []Event {
	t.Helper()
	var all []Event
	for {
		at, ok := e.NextEvent()
		if !ok || at > until {
			break
		}
		evs, err := e.Advance(at)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, evs...)
	}
	evs, err := e.Advance(max(until, e.Now()))
	if err != nil {
		t.Fatal(err)
	}
	return append(all, evs...)
}

func mustSubmit(t testing.TB, e *Engine, at time.Duration, r Request) (Info, []Event) {
	t.Helper()
	info, evs, err := e.Submit(at, r)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return info, evs
}

func tokenTimes(evs []Event, id int64) []time.Duration {
	var out []time.Duration
	for _, ev := range evs {
		if ev.ID == id && ev.Kind == EvToken {
			out = append(out, ev.At)
		}
	}
	return out
}

func approx(t *testing.T, what string, got, want time.Duration) {
	t.Helper()
	if d := got - want; d > time.Microsecond || d < -time.Microsecond {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

func TestDecodeStepCostGrowsWithBatchAndAggregateSaturates(t *testing.T) {
	var prevPer time.Duration
	var prevAgg float64
	for _, b := range []int{1, 2, 4, 8, 16, 32} {
		e := newEngine(t, quiet(), 64, 0)
		var ids []int64
		var all []Event
		for range b {
			info, evs := mustSubmit(t, e, 0, Request{InputTokens: 1, MaxTokens: 200})
			ids, all = append(ids, info.ID), append(all, evs...)
		}
		all = append(all, runUntil(t, e, time.Hour)...)
		// Inter-token gaps of request 0 once every prefill is done are one full-batch decode step.
		times := tokenTimes(all, ids[0])
		if len(times) != 200 {
			t.Fatalf("b=%d: %d tokens", b, len(times))
		}
		gap := times[100] - times[99]
		approx(t, fmt.Sprintf("b=%d step", b), gap, ms(10+float64(b)))
		if gap <= prevPer {
			t.Errorf("b=%d: per-request interval %v did not grow from %v", b, gap, prevPer)
		}
		agg := float64(b) / gap.Seconds()
		if agg <= prevAgg {
			t.Errorf("b=%d: aggregate rate %v did not grow from %v", b, agg, prevAgg)
		}
		if agg > 1000/1.0*1.0001 { // asymptote 1/t1 = 1000 tok/s
			t.Errorf("b=%d: aggregate %v exceeds the 1/t1 asymptote", b, agg)
		}
		prevPer, prevAgg = gap, agg
	}
	if prevAgg < 0.75*1000 {
		t.Errorf("at b=32 the aggregate rate %v is not close to saturation (1000)", prevAgg)
	}
}

func TestPrefillTimeScalesWithInputLength(t *testing.T) {
	var prev time.Duration
	for _, n := range []int{1, 100, 1000, 8000} {
		e := newEngine(t, quiet(), 4, 0)
		info, evs := mustSubmit(t, e, 0, Request{InputTokens: n, MaxTokens: 5})
		evs = append(evs, runUntil(t, e, time.Hour)...)
		first := tokenTimes(evs, info.ID)[0]
		approx(t, fmt.Sprintf("ttft(n=%d)", n), first, ms(2+0.01*float64(n)))
		if first <= prev {
			t.Errorf("ttft did not grow with input length: %v after %v", first, prev)
		}
		prev = first
	}
}

func TestBlockingPrefillStallsTheBatchAndChunkingBoundsTheStall(t *testing.T) {
	gapOfA := func(chunk int) time.Duration {
		p := quiet()
		p.PrefillChunkTokens = chunk
		e := newEngine(t, p, 8, 0)
		a, evs := mustSubmit(t, e, 0, Request{InputTokens: 1, MaxTokens: 500})
		// B arrives well after A is decoding, with a 4000-token prompt (40ms of prefill).
		evs = append(evs, runUntil(t, e, 200*time.Millisecond)...)
		_, more := mustSubmit(t, e, e.Now(), Request{InputTokens: 4000, MaxTokens: 5})
		evs = append(append(evs, more...), runUntil(t, e, 2*time.Second)...)
		var worst time.Duration
		ts := tokenTimes(evs, a.ID)
		for i := 1; i < len(ts); i++ {
			worst = max(worst, ts[i]-ts[i-1])
		}
		return worst
	}
	blocking, chunked := gapOfA(0), gapOfA(500)
	if blocking < ms(40) {
		t.Errorf("blocking prefill stalled the batch for only %v, want >= 40ms", blocking)
	}
	if chunked >= blocking {
		t.Errorf("chunked prefill gap %v should be below the blocking gap %v", chunked, blocking)
	}
}

func TestKVBudgetQueuesBeforeTheConcurrencyCap(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 150
	e := newEngine(t, p, 8, 4)
	a, _ := mustSubmit(t, e, 0, Request{InputTokens: 100, MaxTokens: 20})
	b, _ := mustSubmit(t, e, 0, Request{InputTokens: 100, MaxTokens: 20})
	st := e.Stats()
	if st.Active != 1 || st.QueueDepth != 1 || st.QueuedInputTokens != 100 {
		t.Fatalf("stats = active %d queue %d queued tokens %d, want 1, 1, 100 (KV, not the %d-seq cap, is binding)", st.Active, st.QueueDepth, st.QueuedInputTokens, 8)
	}
	evs := runUntil(t, e, time.Hour)
	var doneA, startB time.Duration
	for _, ev := range evs {
		if ev.ID == a.ID && ev.Kind == EvDone {
			doneA = ev.At
		}
		if ev.ID == b.ID && ev.Kind == EvStart {
			startB = ev.At
		}
	}
	if startB < doneA || startB == 0 {
		t.Errorf("B started at %v, before A finished at %v: KV was over-committed", startB, doneA)
	}
	if err := e.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestPreemptionKeepsTokenOrderAndFinishesEverything(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 120
	e := newEngine(t, p, 8, 8)
	var infos []Info
	var all []Event
	for range 3 {
		info, evs := mustSubmit(t, e, 0, Request{InputTokens: 30, MaxTokens: 60})
		infos, all = append(infos, info), append(all, evs...)
	}
	all = append(all, runUntil(t, e, time.Hour)...)
	if e.Stats().Preemptions == 0 {
		t.Fatal("expected KV growth to force a preemption")
	}
	for _, info := range infos {
		next := 0
		done := false
		for _, ev := range all {
			if ev.ID != info.ID {
				continue
			}
			switch ev.Kind {
			case EvToken:
				if ev.Index != next {
					t.Fatalf("request %d: token %d after %d", info.ID, ev.Index, next-1)
				}
				next++
			case EvDone:
				done = true
				if ev.Index != 60 || ev.Finish != "length" {
					t.Errorf("request %d done with %d tokens, finish %q", info.ID, ev.Index, ev.Finish)
				}
			}
		}
		if !done {
			t.Errorf("request %d never finished", info.ID)
		}
	}
	if err := e.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestAloneRequestOutgrowingTheKVBudgetEndsWithLength(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 50
	e := newEngine(t, p, 2, 0)
	info, evs := mustSubmit(t, e, 0, Request{InputTokens: 40, MaxTokens: 1000})
	evs = append(evs, runUntil(t, e, time.Hour)...)
	var done *Event
	for i := range evs {
		if evs[i].ID == info.ID && evs[i].Kind == EvDone {
			done = &evs[i]
		}
	}
	if done == nil || done.Finish != "length" || done.Index >= 1000 || done.Index < 1 {
		t.Fatalf("done = %+v, want an early finish \"length\"", done)
	}
	if e.Stats().KVExhausted != 1 || e.Stats().Active != 0 {
		t.Errorf("stats = %+v", e.Stats())
	}
}

func TestSubmitErrors(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 100
	e := newEngine(t, p, 1, 1)
	if _, _, err := e.Submit(0, Request{InputTokens: 100}); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversized prompt: %v", err)
	}
	if _, _, err := e.Submit(0, Request{InputTokens: 0}); err == nil {
		t.Error("zero input accepted")
	}
	mustSubmit(t, e, 0, Request{InputTokens: 10, MaxTokens: 50}) // takes the only slot
	mustSubmit(t, e, 0, Request{InputTokens: 10, MaxTokens: 50}) // waits (queue of 1)
	if _, _, err := e.Submit(0, Request{InputTokens: 10}); !errors.Is(err, ErrQueueFull) {
		t.Errorf("queue overflow: %v", err)
	}
	if got := e.Stats().Rejected; got != 2 {
		t.Errorf("rejected = %d, want 2", got)
	}
	if _, err := e.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Advance(time.Millisecond); !errors.Is(err, ErrTimeRegression) {
		t.Errorf("going back in time: %v", err)
	}
	if _, _, err := e.Submit(time.Millisecond, Request{InputTokens: 1}); !errors.Is(err, ErrTimeRegression) {
		t.Errorf("submit in the past: %v", err)
	}
}

func TestCancelFreesKVAndIsIdempotent(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 150
	e := newEngine(t, p, 4, 4)
	a, _ := mustSubmit(t, e, 0, Request{InputTokens: 100, MaxTokens: 1000})
	b, _ := mustSubmit(t, e, 0, Request{InputTokens: 100, MaxTokens: 5})
	if e.Stats().QueueDepth != 1 {
		t.Fatal("B should be waiting for KV")
	}
	ok, evs, err := e.Cancel(30*time.Millisecond, a.ID)
	if err != nil || !ok {
		t.Fatalf("cancel: %v %v", ok, err)
	}
	if ok, _, _ := e.Cancel(30*time.Millisecond, a.ID); ok {
		t.Error("second cancel reported a live request")
	}
	evs = append(evs, runUntil(t, e, time.Hour)...)
	for _, ev := range evs {
		if ev.ID == a.ID && ev.At > 30*time.Millisecond {
			t.Errorf("cancelled request produced %+v after the cancel", ev)
		}
	}
	if len(tokenTimes(evs, b.ID)) != 5 {
		t.Errorf("B did not complete after A was cancelled")
	}
	if st := e.Stats(); st.KVUsedTokens != 0 || st.Cancelled != 1 {
		t.Errorf("stats = %+v", st)
	}
}

func TestOutputLengthIsSeededAndIndependentOfMaxTokens(t *testing.T) {
	p := quiet()
	p.OutputCV = 0.5
	draw := func(seed int64, maxTokens int) (lens []int) {
		e, err := New(Config{Params: p, Seed: seed, MaxSeqs: 1, MaxQueue: 5000, OutputMeanTokens: 200})
		if err != nil {
			t.Fatal(err)
		}
		for range 3000 {
			info, _, err := e.Submit(0, Request{InputTokens: 1, MaxTokens: maxTokens})
			if err != nil {
				t.Fatal(err)
			}
			lens = append(lens, info.OutputTokens)
		}
		return lens
	}
	a, again, other, capped := draw(7, 0), draw(7, 0), draw(8, 0), draw(7, 100)
	same := func(x, y []int) bool {
		for i := range x {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}
	if !same(a, again) {
		t.Error("same seed produced different lengths")
	}
	if same(a, other) {
		t.Error("different seeds produced the same lengths")
	}
	for i := range a {
		if want := min(a[i], 100); capped[i] != want {
			t.Fatalf("request %d: natural %d but with max_tokens=100 got %d (length must not depend on max_tokens)", i, a[i], capped[i])
		}
	}
	var sum, sq float64
	for _, v := range a {
		sum += float64(v)
		sq += float64(v) * float64(v)
	}
	mean := sum / float64(len(a))
	cv := math.Sqrt(sq/float64(len(a))-mean*mean) / mean
	if math.Abs(mean-200) > 10 || math.Abs(cv-0.5) > 0.05 {
		t.Errorf("mean %v cv %v, want about 200 and 0.5", mean, cv)
	}
}

func TestOutputFractionMakesMaxTokensAKnownOverestimate(t *testing.T) {
	p := quiet()
	p.OutputFraction = 0.3
	e := newEngineMean(t, p, 1, 10, 10)
	info, _ := mustSubmit(t, e, 0, Request{InputTokens: 1, MaxTokens: 1000})
	if info.OutputTokens != 300 || info.Finish != "stop" {
		t.Errorf("info = %+v, want 300 tokens, stop", info)
	}
	// Without max_tokens the fraction has nothing to scale: the mean applies.
	info, _ = mustSubmit(t, e, 0, Request{InputTokens: 1})
	if info.OutputTokens != 10 {
		t.Errorf("no max_tokens: %d tokens, want the mean 10", info.OutputTokens)
	}
}

func TestFinishReasonLengthWhenMaxTokensEndsTheOutput(t *testing.T) {
	e := newEngineMean(t, quiet(), 1, 10, 10)
	for _, c := range []struct {
		max    int
		n      int
		finish string
	}{{0, 10, "stop"}, {11, 10, "stop"}, {10, 10, "length"}, {4, 4, "length"}} {
		info, _ := mustSubmit(t, e, 0, Request{InputTokens: 1, MaxTokens: c.max})
		if info.OutputTokens != c.n || info.Finish != c.finish {
			t.Errorf("max_tokens %d: %+v, want %d %s", c.max, info, c.n, c.finish)
		}
	}
}

func TestNoiseHasTheConfiguredCoefficientOfVariation(t *testing.T) {
	for _, cv := range []float64{0.1, 0.3, 0.6} {
		p := quiet()
		p.StepNoiseCV, p.StepBaseMs, p.StepPerSeqMs = cv, 10, 0
		e2, err := New(Config{Params: p, Seed: 3, MaxSeqs: 1, MaxQueue: 0, OutputMeanTokens: 20000})
		if err != nil {
			t.Fatal(err)
		}
		inf, evs2, err := e2.Submit(0, Request{InputTokens: 1})
		if err != nil {
			t.Fatal(err)
		}
		evs2 = append(evs2, runUntil(t, e2, 100*time.Hour)...)
		ts := tokenTimes(evs2, inf.ID)
		var gaps []float64
		for i := 2; i < len(ts); i++ {
			gaps = append(gaps, (ts[i] - ts[i-1]).Seconds())
		}
		var sum, sq float64
		for _, g := range gaps {
			sum += g
			sq += g * g
		}
		mean := sum / float64(len(gaps))
		got := math.Sqrt(sq/float64(len(gaps))-mean*mean) / mean
		if math.Abs(got-cv) > 0.03 {
			t.Errorf("configured cv %v, measured %v over %d steps", cv, got, len(gaps))
		}
		if math.Abs(mean-0.010) > 0.0005 {
			t.Errorf("noise must keep the mean: got mean step %v, want 10ms", mean)
		}
	}
}

func TestStatsReportBatchFillMemoryAndRate(t *testing.T) {
	p := quiet()
	p.WeightsMB, p.KVBytesPerToken = 1000, 1<<20 // 1 MiB per KV token
	e := newEngine(t, p, 8, 0)
	if st := e.Stats(); st.GPUUtilization != 0 || st.GPUMemoryUsedMB != 1000 || st.RecentTokensPerSecond != 0 {
		t.Fatalf("idle stats = %+v", st)
	}
	for range 4 {
		mustSubmit(t, e, 0, Request{InputTokens: 50, MaxTokens: 100000})
	}
	runUntil(t, e, 3*time.Second)
	st := e.Stats()
	if st.GPUUtilization != 50 {
		t.Errorf("util = %v, want 50 (4 of 8 slots)", st.GPUUtilization)
	}
	if want := int64(1000 + st.KVUsedTokens); st.GPUMemoryUsedMB != want {
		t.Errorf("memory = %d, want weights + KV = %d", st.GPUMemoryUsedMB, want)
	}
	agg := 4 / (14 * time.Millisecond).Seconds() // b=4: 10+4 ms
	if math.Abs(st.RecentTokensPerSecond-agg)/agg > 0.05 {
		t.Errorf("recent tokens/s = %v, want about %v", st.RecentTokensPerSecond, agg)
	}
	e.Advance(2 * time.Hour) //nolint:errcheck // far ahead: the window must fall to idle
}

func TestRecentRateFallsToZeroAfterIdle(t *testing.T) {
	e := newEngine(t, quiet(), 4, 0)
	mustSubmit(t, e, 0, Request{InputTokens: 1, MaxTokens: 500})
	runUntil(t, e, 2*time.Second)
	if e.Stats().RecentTokensPerSecond == 0 {
		t.Fatal("rate should be positive while a request has been generating")
	}
	// Past the end of the request and the 5 s window.
	if _, err := e.Advance(time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := e.Stats().RecentTokensPerSecond; got != 0 {
		t.Errorf("idle rate = %v, want 0", got)
	}
}

// --- determinism and randomized properties --------------------------------------------------------

type step struct {
	at     time.Duration
	cancel int // index of an earlier submission to cancel, or -1
	req    Request
}

func script(rng *rand.Rand, n int) []step {
	var out []step
	at := time.Duration(0)
	for range n {
		at += time.Duration(rng.IntN(40)) * time.Millisecond
		s := step{at: at, cancel: -1, req: Request{InputTokens: 1 + rng.IntN(400), MaxTokens: rng.IntN(120)}}
		if rng.IntN(6) == 0 && len(out) > 0 {
			s.cancel = rng.IntN(len(out))
		}
		out = append(out, s)
	}
	return out
}

// play runs a script and returns every event, checking invariants after each call.
func play(t testing.TB, cfg Config, steps []step) ([]Event, *Engine, []int64) {
	t.Helper()
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var all []Event
	var ids []int64
	check := func(where string) {
		if err := e.CheckInvariants(); err != nil {
			t.Fatalf("%s: %v", where, err)
		}
	}
	for i, s := range steps {
		// Interleave engine events with the scripted calls in time order.
		for {
			at, ok := e.NextEvent()
			if !ok || at > s.at {
				break
			}
			evs, err := e.Advance(at)
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, evs...)
			check("advance")
		}
		if s.cancel >= 0 && ids[s.cancel] != 0 {
			_, evs, err := e.Cancel(s.at, ids[s.cancel])
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, evs...)
			check("cancel")
		}
		info, evs, err := e.Submit(s.at, s.req)
		all = append(all, evs...)
		switch {
		case err == nil:
			ids = append(ids, info.ID)
		case errors.Is(err, ErrQueueFull) || errors.Is(err, ErrTooLarge):
			ids = append(ids, 0)
		default:
			t.Fatalf("step %d: %v", i, err)
		}
		check("submit")
	}
	all = append(all, runUntil(t, e, 1000*time.Hour)...)
	check("drained")
	return all, e, ids
}

func digest(evs []Event) [32]byte {
	h := sha256.New()
	for _, ev := range evs {
		_, _ = fmt.Fprintf(h, "%d %d %d %d %s\n", ev.At, ev.ID, ev.Kind, ev.Index, ev.Finish)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func randomConfig(rng *rand.Rand, seed int64) Config {
	p := quiet()
	p.StepNoiseCV, p.PrefillNoiseCV, p.OutputCV = rng.Float64()*0.5, rng.Float64()*0.5, rng.Float64()
	p.PrefillChunkTokens = []int{0, 0, 64, 512}[rng.IntN(4)]
	p.KVBudgetTokens = []int{450, 800, 2000, 100000}[rng.IntN(4)]
	if rng.IntN(3) == 0 {
		p.OutputFraction = 0.2 + rng.Float64()*0.6
	}
	return Config{Params: p, Seed: seed, MaxSeqs: 1 + rng.IntN(8), MaxQueue: rng.IntN(12), OutputMeanTokens: 1 + rng.IntN(80)}
}

func TestSameSeedSameTraceDifferentSeedDifferentTrace(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := randomConfig(rng, 5)
	cfg.Params.StepNoiseCV, cfg.Params.OutputCV = 0.3, 0.4
	steps := script(rng, 80)
	a, _, _ := play(t, cfg, steps)
	b, _, _ := play(t, cfg, steps)
	if digest(a) != digest(b) {
		t.Fatal("the same config and script produced different traces")
	}
	cfg.Seed = 6
	c, _, _ := play(t, cfg, steps)
	if digest(a) == digest(c) {
		t.Fatal("a different seed produced the identical trace despite noise")
	}
}

func TestRandomizedProperties(t *testing.T) {
	for seed := int64(1); seed <= 300; seed++ {
		rng := rand.New(rand.NewPCG(uint64(seed), 99))
		cfg := randomConfig(rng, seed)
		steps := script(rng, 60)
		evs, e, ids := play(t, cfg, steps)

		// Time never goes backwards in the event stream.
		for i := 1; i < len(evs); i++ {
			if evs[i].At < evs[i-1].At {
				t.Fatalf("seed %d: event %d at %v after %v", seed, i, evs[i].At, evs[i-1].At)
			}
		}
		// Per request: start before tokens, contiguous token indices, at most one Done, nothing after it.
		type track struct {
			started, done bool
			next          int
			doneIndex     int
		}
		tr := map[int64]*track{}
		for _, ev := range evs {
			x := tr[ev.ID]
			if x == nil {
				x = &track{}
				tr[ev.ID] = x
			}
			if x.done {
				t.Fatalf("seed %d: request %d has %+v after Done", seed, ev.ID, ev)
			}
			switch ev.Kind {
			case EvStart:
				x.started = true
			case EvToken:
				if !x.started || ev.Index != x.next {
					t.Fatalf("seed %d: request %d token %d (started %v, want %d)", seed, ev.ID, ev.Index, x.started, x.next)
				}
				x.next++
			case EvDone:
				x.done, x.doneIndex = true, ev.Index
				if ev.Index != x.next {
					t.Fatalf("seed %d: request %d Done after %d tokens but %d were emitted", seed, ev.ID, ev.Index, x.next)
				}
			}
		}
		cancelled := 0
		for _, id := range ids {
			if id == 0 {
				continue
			}
			x := tr[id]
			if x != nil && !x.done {
				cancelled++ // a cancelled request is the only way to never finish
			}
		}
		st := e.Stats()
		if int64(cancelled) > st.Cancelled {
			t.Fatalf("seed %d: %d requests never finished but only %d were cancelled", seed, cancelled, st.Cancelled)
		}
		if st.Active != 0 || st.QueueDepth != 0 || st.KVUsedTokens != 0 {
			t.Fatalf("seed %d: not drained: %+v", seed, st)
		}
		if st.Submitted != st.Completed+st.Cancelled+st.KVExhausted {
			t.Fatalf("seed %d: conservation: %+v", seed, st)
		}
		if st.Tokens < 0 {
			t.Fatal("negative tokens")
		}
	}
}

func TestEveryRequestThatIsNotCancelledFinishesWithItsPlannedLengthOrKVEnd(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	cfg := randomConfig(rng, 3)
	cfg.Params.KVBudgetTokens = 100000
	var steps []step
	for i := range 40 {
		steps = append(steps, step{at: time.Duration(i) * time.Millisecond, cancel: -1, req: Request{InputTokens: 1 + rng.IntN(300), MaxTokens: rng.IntN(100)}})
	}
	cfg.MaxQueue = 100
	evs, _, ids := play(t, cfg, steps)
	infoOf := map[int64]int{}
	e, _ := New(cfg)
	for _, s := range steps {
		info, _, err := e.Submit(0, s.req)
		if err == nil {
			infoOf[info.ID] = info.OutputTokens
		}
	}
	for _, id := range ids {
		if id == 0 {
			continue
		}
		for _, ev := range evs {
			if ev.ID == id && ev.Kind == EvDone && ev.Index != infoOf[id] {
				t.Errorf("request %d finished with %d tokens, planned %d", id, ev.Index, infoOf[id])
			}
		}
	}
}

func FuzzEngine(f *testing.F) {
	f.Add(int64(1), uint8(3), uint16(300), uint16(20))
	f.Fuzz(func(t *testing.T, seed int64, seqs uint8, kv uint16, outMean uint16) {
		cfg := Config{Params: quiet(), Seed: seed, MaxSeqs: 1 + int(seqs%16), MaxQueue: 8, OutputMeanTokens: 1 + int(outMean%500)}
		cfg.Params.KVBudgetTokens = 2 + int(kv)
		cfg.Params.StepNoiseCV, cfg.Params.OutputCV = 0.4, 0.8
		rng := rand.New(rand.NewPCG(uint64(seed), 5))
		play(t, cfg, script(rng, 40))
	})
}

// Chunked prefill pays the fixed cost once per prefill, not once per chunk.
func TestChunkedPrefillChargesTheFixedCostOnce(t *testing.T) {
	p := quiet()
	p.PrefillChunkTokens = 500
	e := newEngine(t, p, 4, 0)
	info, evs := mustSubmit(t, e, 0, Request{InputTokens: 2000, MaxTokens: 3})
	evs = append(evs, runUntil(t, e, time.Hour)...)
	approx(t, "ttft of a chunked prefill on an idle worker", tokenTimes(evs, info.ID)[0], ms(2+0.01*2000))
}

// A request that fits exactly is admitted: the boundary is "prompt + 1 <= free", not "<".
func TestAdmissionAndDecodeGrowthAcceptAnExactFit(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 100
	e := newEngine(t, p, 8, 4)
	mustSubmit(t, e, 0, Request{InputTokens: 49, MaxTokens: 2})
	mustSubmit(t, e, 0, Request{InputTokens: 49, MaxTokens: 2})
	// Run to the point where both have been admitted (the second at the end of the first's prefill).
	for at, ok := e.NextEvent(); ok && e.Stats().Active < 2; at, ok = e.NextEvent() {
		if _, err := e.Advance(at); err != nil {
			t.Fatal(err)
		}
	}
	if st := e.Stats(); st.Active != 2 || st.QueueDepth != 0 || st.KVUsedTokens != 100 {
		t.Fatalf("two requests needing 50 of 100 KV tokens each should both run: %+v", st)
	}
}

func TestDecodeStepThatExactlyFillsTheBudgetDoesNotPreempt(t *testing.T) {
	p := quiet()
	p.KVBudgetTokens = 102 // two sequences at 50 tokens each, plus one token each for the step
	e := newEngine(t, p, 8, 4)
	mustSubmit(t, e, 0, Request{InputTokens: 49, MaxTokens: 2})
	mustSubmit(t, e, 0, Request{InputTokens: 49, MaxTokens: 2})
	runUntil(t, e, time.Hour)
	if got := e.Stats().Preemptions; got != 0 {
		t.Errorf("preemptions = %d, want 0: the step fits exactly", got)
	}
	if got := e.Stats().Completed; got != 2 {
		t.Errorf("completed = %d, want 2", got)
	}
}
