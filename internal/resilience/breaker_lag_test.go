package resilience

import (
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

// A model of the gateway's verdict timing against the real Breakers (the verifier's simulation of finding F1).
//
// One worker of two receives 10 requests a second for 120 s (1,200 requests); each request fails with probability f
// before its first byte (a 503: a fault, known at once) and otherwise streams for `lag` seconds. The question is how
// often a mostly healthy worker is ejected. With the success counted at the first byte (the gateway's semantics) the
// length of the stream cannot matter. With the success counted when the stream completes (an earlier design of this
// PR) faults pile up with no success to balance them while long streams are in flight, and a healthy worker is
// ejected: the legacy mode below reproduces that and proves the test can see it.

const (
	lagRequests = 1200
	lagRate     = 10 * time.Second / 100 // one request every 100 ms
	lagTTFB     = 200 * time.Millisecond
)

type lagEvent struct {
	at   time.Duration
	kind int // 0 acquire, 1 first byte, 2 completion
	req  int
}

// lagTripped runs one simulated window and reports whether the breaker of the faulted-at-random worker opened.
func lagTripped(seed uint64, faultRate float64, lag time.Duration, legacy bool) bool {
	clk := newFakeClock()
	rnd := rand.New(rand.NewPCG(seed, 17))
	tripped := false
	bs := NewBreakers(testParams2(), Options{Now: clk.Now, Rand: func() float64 { return 0.5 }, OnChange: func(c Change) {
		if c.To == Open && c.Key.ID == "w1" {
			tripped = true
		}
	}})
	bs.Sync([]Member{{Key: KeyOf("w1", time.Unix(1, 0)), Eligible: true}, {Key: KeyOf("w2", time.Unix(1, 0)), Eligible: true}})
	k := KeyOf("w1", time.Unix(1, 0))
	fault := make([]bool, lagRequests)
	var evs []lagEvent
	for i := range fault {
		fault[i] = rnd.Float64() < faultRate
		at := time.Duration(i) * lagRate
		evs = append(evs, lagEvent{at, 0, i}, lagEvent{at + lagTTFB, 1, i})
		if !fault[i] {
			evs = append(evs, lagEvent{at + lagTTFB + lag, 2, i})
		}
	}
	sort.SliceStable(evs, func(a, b int) bool { return evs[a].at < evs[b].at })
	tickets := map[int]Ticket{}
	base := clk.Now()
	for _, e := range evs {
		clk.t = base.Add(e.at) // single goroutine: no lock needed
		switch e.kind {
		case 0:
			if tk, ok := bs.Acquire(k); ok {
				tickets[e.req] = tk
			}
		case 1:
			tk, ok := tickets[e.req]
			if !ok {
				continue
			}
			switch {
			case fault[e.req]:
				bs.Record(tk, Fault)
			case !legacy:
				bs.Record(tk, OK) // the gateway: the ok is counted at the first byte
			}
		case 2:
			if tk, ok := tickets[e.req]; ok && legacy {
				bs.Record(tk, OK) // the earlier design: counted when the stream completes
			}
		}
		if tripped {
			return true
		}
	}
	return tripped
}

func testParams2() BreakerParams {
	p := testParams()
	p.MaxOpenFraction = 0.5
	return p
}

func lagRateOver(runs int, faultRate float64, lag time.Duration, legacy bool) int {
	n := 0
	for s := 0; s < runs; s++ {
		if lagTripped(uint64(s)+1, faultRate, lag, legacy) {
			n++
		}
	}
	return n
}

func TestSuccessLagDoesNotEjectAMostlyHealthyWorker(t *testing.T) {
	const runs = 200
	const margin = 6 // trips out of 200 that a lag may add over lag 0 (3 percentage points)
	faultRates := []float64{0.02, 0.05, 0.10}
	lags := []time.Duration{0, 10 * time.Second, 60 * time.Second}
	for _, f := range faultRates {
		base := lagRateOver(runs, f, 0, false)
		for _, lag := range lags[1:] {
			got := lagRateOver(runs, f, lag, false)
			t.Logf("fault rate %.0f%%, stream length %v: %d/%d trips (lag 0: %d/%d)", f*100, lag, got, runs, base, runs)
			if got > base+margin {
				t.Errorf("fault rate %.0f%%, lag %v: %d trips, lag 0 has %d: the success lag biases the breaker towards ejecting", f*100, lag, got, base)
			}
		}
	}
}

// The same model with the success counted at completion (the earlier design) does eject healthy workers on long
// streams; this keeps the test above honest about what it can detect.
func TestTheModelSeesTheBiasOfCountingSuccessAtCompletion(t *testing.T) {
	for _, f := range []float64{0.02, 0.05, 0.10} {
		for _, lag := range []time.Duration{0, 10 * time.Second, 60 * time.Second} {
			t.Logf("completion-counted success, fault rate %.0f%%, lag %v: %d/200 trips", f*100, lag, lagRateOver(200, f, lag, true))
		}
	}
	const runs = 100
	base := lagRateOver(runs, 0.05, 0, true)
	long := lagRateOver(runs, 0.05, 60*time.Second, true)
	t.Logf("success counted at completion, fault rate 5%%: lag 0 %d/%d trips, lag 60 s %d/%d", base, runs, long, runs)
	if long < base+30 {
		t.Fatalf("the legacy timing should eject far more often with long streams: %d against %d of %d", long, base, runs)
	}
}
