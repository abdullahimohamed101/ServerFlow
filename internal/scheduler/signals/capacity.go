package signals

import (
	"math"
	"sort"
	"time"

	"serverflow/pkg/protocol"
)

// Capacity and work-ahead (ADR-022, D5 and D6). The two work-aware strategies divide the work ahead of a request
// by the rate the worker can serve it, and "rate" is not the latest reported tokens per second: that figure is
// demand (a lightly loaded worker reports little, an empty one nothing), so dividing by it makes idle workers
// look slow. The estimate used is a decayed peak of the reported figure, sampled only while the worker had
// work, with a fleet prior for workers that have no sample.

const (
	// maxTPS bounds a reported tokens-per-second sample so a worker that reports an absurd figure cannot make
	// itself look infinitely fast. It is far above any engine's aggregate rate.
	maxTPS = 1e7
	// minTPS is the smallest sample that counts: below it the report says nothing (a rate of 1e-300 would make
	// a worker's predicted wait overflow).
	minTPS = 1e-3
	// costAlpha is the weight of a new request in the per-model mean cost.
	costAlpha = 0.1
)

// sample takes the worker's reported rate as a capacity sample when it is fresh, the worker had work, and the
// report is a new one (the same heartbeat is sampled once). The new peak is the larger of the decayed old peak
// and the sample.
func (t *Tracker) sample(e *entry, m Observation, now time.Time) {
	if !m.Fresh || m.ReportedActive < 1 || m.LastHeartbeat.IsZero() || m.LastHeartbeat.Equal(e.capHB) {
		return
	}
	e.capHB = m.LastHeartbeat
	v := m.ReportedTPS
	if !(v >= minTPS) { // also catches NaN
		return
	}
	v = math.Min(v, maxTPS)
	if e.hasPeak && e.capR > v { // capR is the decayed peak as of the last Sync
		v = e.capR
	}
	e.peak, e.peakAt, e.hasPeak = v, now, true
}

// recomputeCapacity derives every worker's capacity estimate as of now. Per model: the decay target is the
// median of the stored peaks of the workers that have one; each such worker's estimate decays from its peak
// toward it with the configured half-life, never below CapacityFloor times the target (so it is never zero, and
// never infinitely slow); the fleet prior, which workers without a sample use, is the median of those estimates.
// With one sampled worker the target is its own peak, so it keeps it ("if no peer has a sample the worker keeps
// its last peak"). The decay is continuous, so there is no moment at which an old estimate snaps to the prior.
func (t *Tracker) recomputeCapacity(now time.Time) {
	byModel := map[string][]*entry{}
	for _, e := range t.entries {
		if e.model != "" {
			byModel[e.model] = append(byModel[e.model], e)
		}
		e.capR, e.capPrior = 0, true // workers without a model stay unknown
	}
	for _, es := range byModel {
		peaks := make([]float64, 0, len(es))
		for _, e := range es {
			if e.hasPeak {
				peaks = append(peaks, e.peak)
			}
		}
		if len(peaks) == 0 {
			continue // nobody has a sample: capacity is unknown for the whole fleet
		}
		target := median(peaks)
		rs := peaks[:0]
		for _, e := range es {
			if e.hasPeak {
				e.capR = t.decay(e.peak, target, now.Sub(e.peakAt))
				e.capPrior = false
				rs = append(rs, e.capR)
			}
		}
		prior := median(rs)
		for _, e := range es {
			if !e.hasPeak {
				e.capR = prior
			}
		}
	}
}

// decay moves peak toward target by the elapsed time: target + (peak - target) * 2^(-elapsed/halflife), floored
// at CapacityFloor * target.
func (t *Tracker) decay(peak, target float64, elapsed time.Duration) float64 {
	if elapsed < 0 {
		elapsed = 0
	}
	f := math.Exp2(-elapsed.Seconds() / t.cfg.CapacityHalflife.Seconds())
	return math.Max(target+(peak-target)*f, t.cfg.CapacityFloor*target)
}

// median of a non-empty slice (it reorders the slice): the middle value, or the mean of the two middle ones.
func median(v []float64) float64 {
	sort.Float64s(v)
	n := len(v)
	if n%2 == 1 {
		return v[n/2]
	}
	return (v[n/2-1] + v[n/2]) / 2
}

// pruneModels forgets the mean cost of models no worker in the tracker serves any more.
func (t *Tracker) pruneModels() {
	if len(t.meanCost) == 0 {
		return
	}
	live := make(map[string]struct{}, len(t.meanCost))
	for _, e := range t.entries {
		live[e.model] = struct{}{}
	}
	for m := range t.meanCost {
		if _, ok := live[m]; !ok {
			delete(t.meanCost, m)
		}
	}
}

// ObserveCost folds the work cost of a request this gateway routed into the model's mean. The mean stands in for
// the size of the requests other gateways and clients have on a worker, which this gateway cannot see (D5). The
// first request of a model sets it; models beyond MaxModels are not tracked.
func (t *Tracker) ObserveCost(model string, cost int64) {
	if cost <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m, ok := t.meanCost[model]
	switch {
	case ok:
		t.meanCost[model] = m + costAlpha*(float64(cost)-m)
	case len(t.meanCost) < t.cfg.MaxModels:
		t.meanCost[model] = float64(cost)
	}
}

// LoadInput is a worker as the work-aware strategies need it: the overlay input plus the model, the worker's
// reported queued input tokens, this request's work cost (the mean's cold value) and the input weight.
type LoadInput struct {
	Input
	Model        string
	QueuedTokens int64
	Cost         int64
	InputWeight  float64
}

// Load is Overlay plus everything a work-aware strategy reads, as one protocol.ObservedLoad (D5):
//
//	WorkAhead = ledger work + foreign * mean cost + queued tokens * input weight
//
// where foreign is the load other gateways and clients appear to add (the effective active count minus this
// gateway's own in-flight slots, never negative) and mean cost is the model's mean request cost, or this
// request's cost before there is one. The queued-tokens term can double count requests this gateway also holds
// in its ledger, but only on a worker that is queueing, which is the direction wanted.
func (t *Tracker) Load(now time.Time, in LoadInput) protocol.ObservedLoad {
	t.mu.Lock()
	defer t.mu.Unlock()
	res := t.overlayLocked(now, in.Input)
	mean := float64(max(in.Cost, 0))
	if m, ok := t.meanCost[in.Model]; ok {
		mean = m
	}
	foreign := max(res.Active-clampCount(in.Local), 0)
	queued := float64(min(max(in.QueuedTokens, 0), maxCount))
	iw := in.InputWeight
	if !(iw >= 0) || math.IsInf(iw, 0) {
		iw = 1
	}
	o := res.Observed()
	o.WorkAheadTokens += float64(foreign)*mean + queued*math.Min(iw, 1e4)
	o.CapacityTPS = res.Capacity
	o.Flags |= protocol.ObservedLedger
	if res.Fallback == FallbackUntracked {
		o.Flags |= protocol.ObservedUntracked
	}
	if res.CapacityPrior {
		o.Flags |= protocol.ObservedPriorCapacity
	}
	return o
}

// Capacity returns a worker's capacity estimate and whether it is the fleet prior (or unknown) rather than the
// worker's own sample. For tests and diagnostics.
func (t *Tracker) Capacity(k Key) (tps float64, prior bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entries[k]; e != nil {
		return e.capR, e.capPrior || e.capR <= 0
	}
	return 0, true
}

// MeanCost returns the model's mean request cost (0, false before the first request).
func (t *Tracker) MeanCost(model string) (float64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m, ok := t.meanCost[model]
	return m, ok
}
