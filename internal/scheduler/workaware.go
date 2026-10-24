package scheduler

import (
	"context"
	"math"
	"sort"

	"serverflow/internal/cost"
	"serverflow/pkg/protocol"
)

// The work-aware strategies (ADR-022, D5). Both read protocol.ObservedLoad, the gateway's own view of the work on
// each worker, and are pure arithmetic over the snapshot: no clock, no I/O, no state but the shared rotor.
//
//	least-tokens  minimises  WorkAhead_w                       the estimated token work ahead of the request
//	least-work    minimises  (WorkAhead_w + c) / R_w           a predicted wait in seconds
//
// where c is the request's own work cost (cost.Work of its estimate) and R_w the worker's estimated token rate.
// A score within a relative 1e-9 of the minimum is a tie, and tied workers take turns through the rotor in worker
// ID order, as for the Phase 5 strategies.
//
// What they do when a signal is missing, in this order:
//   - no candidate carries a ledger observation (the load overlay is "max"): the choice is least-active's, on
//     the effective active count, and the returned copy is flagged ObservedDegraded;
//   - least-work and a candidate has no usable rate: the median rate of the candidates that have one stands in
//     (the router has normally supplied the fleet prior already); if no candidate has a rate the choice is
//     least-tokens' and the copy is flagged ObservedDegraded;
//   - a candidate's work ahead (or its score) is NaN, infinite or negative, or it has no ledger observation
//     while others do: it scores the median of the valid scores, so garbage neither wins nor is exiled, and the
//     copy is flagged ObservedInvalid when it is chosen.
//
// Selection starts from eligible(), so the section 15 invariants are the shared ones.
type workAware struct {
	rot      *rotor
	work     bool // least-work; else least-tokens
	weights  cost.Weights
	fallback *leastLoaded // the rule used when there is no ledger observation
}

// tieEps is the relative distance within which two scores are a tie.
const tieEps = 1e-9

func (s *workAware) SelectWorker(ctx context.Context, req *protocol.InferenceRequest, workers []protocol.WorkerSnapshot) (*protocol.WorkerSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, why := eligible(req, workers)
	if why != nil {
		return nil, why
	}
	observed := false
	for _, j := range idx {
		if workers[j].Observed.Flags&protocol.ObservedLedger != 0 {
			observed = true
			break
		}
	}
	if !observed {
		chosen := s.fallback.chooseWith(s.rot, req.Model, workers, idx)
		chosen.Observed.Flags |= protocol.ObservedDegraded
		return chosen, nil
	}

	sc := scorer{work: s.work}
	if s.work {
		sc.c = float64(cost.Work(req.EstInputTokens, req.EstOutputTokens, s.weights))
		sc.prepare(workers, idx)
	}

	// Pass 1: the minimum of the valid scores, and how many scores are invalid.
	best, nValid, nInvalid := math.Inf(1), 0, 0
	for _, j := range idx {
		if v, ok := sc.score(&workers[j]); ok {
			nValid++
			best = math.Min(best, v)
		} else {
			nInvalid++
		}
	}
	neutral := math.Inf(1) // the score of an invalid candidate; stays +Inf (never a tie by itself) when none is valid
	if nInvalid > 0 && nValid > 0 {
		vals := make([]float64, 0, nValid) // the rare path: an invalid or unobserved candidate
		for _, j := range idx {
			if v, ok := sc.score(&workers[j]); ok {
				vals = append(vals, v)
			}
		}
		neutral = medianOf(vals)
	}
	allInvalid := nValid == 0

	// Pass 2: the tied candidates, compacted in place (the write index never passes the read index).
	n := 0
	for _, j := range idx {
		v, ok := sc.score(&workers[j])
		if !ok {
			v = neutral
		}
		if allInvalid || v <= best+tieEps*math.Abs(best) {
			idx[n] = j
			n++
		}
	}
	tied := idx[:n]
	chosen := workers[tied[s.rot.next(req.Model, len(tied), func(i int) string { return workers[tied[i]].WorkerID })]]
	if sc.degraded {
		chosen.Observed.Flags |= protocol.ObservedDegraded
	}
	if _, ok := sc.score(&chosen); !ok {
		chosen.Observed.Flags |= protocol.ObservedInvalid
	}
	return &chosen, nil
}

// scorer turns a snapshot into the strategy's score.
type scorer struct {
	work     bool    // score the predicted wait; else the work ahead
	c        float64 // the request's own work cost
	prior    float64 // the rate that stands in for an unknown one
	degraded bool    // least-work had no rate to divide by anywhere and scores like least-tokens
}

// prepare finds the stand-in rate: the median of the rates the candidates have. With none, least-work scores the
// work ahead alone.
func (sc *scorer) prepare(workers []protocol.WorkerSnapshot, idx []int) {
	known := 0
	for _, j := range idx {
		if validRate(workers[j].Observed.CapacityTPS) {
			known++
		}
	}
	switch {
	case known == 0:
		sc.work, sc.degraded = false, true
	case known < len(idx):
		rates := make([]float64, 0, known)
		for _, j := range idx {
			if validRate(workers[j].Observed.CapacityTPS) {
				rates = append(rates, workers[j].Observed.CapacityTPS)
			}
		}
		sc.prior = medianOf(rates)
	}
}

func validRate(r float64) bool { return r > 0 && !math.IsInf(r, 0) } // false for NaN

// score is the candidate's score and whether it is valid. Lower is better.
func (sc *scorer) score(w *protocol.WorkerSnapshot) (float64, bool) {
	if w.Observed.Flags&protocol.ObservedLedger == 0 {
		return 0, false
	}
	ahead := w.Observed.WorkAheadTokens
	if !(ahead >= 0) || math.IsInf(ahead, 0) {
		return 0, false
	}
	if !sc.work {
		return ahead, true
	}
	r := w.Observed.CapacityTPS
	if !validRate(r) {
		r = sc.prior
	}
	v := (ahead + sc.c) / r
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}

// medianOf is the median of a non-empty slice; it reorders the slice.
func medianOf(v []float64) float64 {
	sort.Float64s(v)
	n := len(v)
	if n%2 == 1 {
		return v[n/2]
	}
	return (v[n/2-1] + v[n/2]) / 2
}
