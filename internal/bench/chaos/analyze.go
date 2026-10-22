package chaos

import (
	"fmt"
	"math"
	"sort"
	"time"

	"serverflow/internal/bench/collect"
	"serverflow/internal/bench/report"
)

// Recovery needs this many consecutive buckets, each with at least RecoveryMinRequests requests, whose
// success ratio is at least the baseline's (Phase 15 D19).
const (
	RecoveryBuckets     = 5
	RecoveryMinRequests = 5
)

// Windows are the three measurement windows, as offsets from the start of the run. They follow what the
// conductor did (when the fault was really applied and cleared), not what was planned.
type Windows struct{ Baseline, Fault, Recovery collect.Window }

func windowsOf(p Params, tl Timeline) Windows {
	applied, cleared := p.Warmup+p.Baseline, p.Warmup+p.Baseline+p.Fault
	if tl.Applied {
		applied = tl.AppliedAt
	}
	if tl.Cleared {
		cleared = tl.ClearedAt
	}
	cleared = max(cleared, applied)
	return Windows{
		Baseline: collect.Window{Start: p.Warmup, End: applied},
		Fault:    collect.Window{Start: applied, End: cleared},
		Recovery: collect.Window{Start: cleared, End: p.Total()},
	}
}

// delta is the change in the gateway's counters between two ticks (zero when a tick is missing).
func delta(tl Timeline, from, to int) Counters {
	if from < 0 || to < 0 || from >= len(tl.Ticks) || to >= len(tl.Ticks) || to < from {
		return Counters{}
	}
	return tl.Ticks[to].Sub(tl.Ticks[from].Counters)
}

// maxQueueDepth is the deepest the admission queue was at any tick from one tick to another (0 on a build without it).
func maxQueueDepth(tl Timeline, from, to int) float64 {
	deepest := 0.0
	for i := max(from, 0); i <= to && i < len(tl.Ticks); i++ {
		deepest = math.Max(deepest, tl.Ticks[i].QueueDepth)
	}
	return deepest
}

func tickAt(p Params, offset time.Duration) int { return int(offset / p.Bucket) }

func ptr(v float64) *float64 { return &v }

// summarizeWindow describes the requests sent inside a window and the gateway counters over it.
func summarizeWindow(name string, w collect.Window, recs []collect.Record, d Counters, depthMax float64) report.ChaosWindow {
	s := collect.SummarizeRecords(recs, w)
	cw := report.ChaosWindow{Name: name, StartSeconds: w.Start.Seconds(), EndSeconds: w.End.Seconds(),
		Sent: s.Sent, Succeeded: s.Succeeded, Failed: s.Failed, Retried: s.Retried,
		AttemptsDelta: d.Attempts, RetriesDelta: d.Retries, NoCapacityDelta: d.NoCapacity, IneligibleDelta: d.Ineligible,
		QueuedDelta: d.Queued, ShedDelta: d.Shed, QueueDepthMax: depthMax,
		ErrorClasses: s.ErrorClasses, StatusClasses: s.StatusClasses}
	if d.Queued > 0 {
		cw.QueueWaitMeanMs = ptr(d.QueueWaitSum / d.Queued * 1000)
	}
	if s.Sent > 0 {
		cw.SuccessRate = float64(s.Succeeded) / float64(s.Sent)
	}
	if d.Requests > 0 {
		cw.AttemptsPerRequest = ptr(d.Attempts / d.Requests)
	}
	if s.Latency != nil {
		cw.LatencyP50, cw.LatencyP95, cw.LatencyP99 = ptr(s.Latency.P50), ptr(s.Latency.P95), ptr(s.Latency.P99)
	}
	var fail []float64
	for _, r := range recs {
		if w.Contains(r.Intended) && !r.OK() {
			fail = append(fail, float64(r.Latency())/float64(time.Millisecond))
		}
	}
	if len(fail) > 0 {
		sort.Float64s(fail)
		v, _ := collect.Percentile(fail, 50)
		cw.FailureP50 = ptr(v)
	}
	return cw
}

// series builds the per-bucket series from the end of the warm-up: successes and failures by completion
// time, and the faulted worker's selections and the breaker's probes from consecutive ticks.
func series(p Params, tl Timeline, recs []collect.Record) []report.ChaosBucket {
	first := tickAt(p, p.Warmup)
	last := tickAt(p, p.Total())
	if last <= first {
		return nil
	}
	out := make([]report.ChaosBucket, last-first)
	for i := range out {
		out[i].StartSeconds = (time.Duration(first+i) * p.Bucket).Seconds()
		d := delta(tl, first+i, first+i+1)
		out[i].FaultySelections, out[i].Probes = d.FaultySelections, d.Probes
	}
	for _, r := range recs {
		j := tickAt(p, r.Done) - first
		if j < 0 || j >= len(out) || r.Done < p.Warmup {
			continue
		}
		if r.OK() {
			out[j].Succeeded++
		} else {
			out[j].Failed++
		}
	}
	return out
}

// IsolationMinBuckets is how many buckets in a row the faulted worker must go without a non-probe selection,
// to the end of the fault window, to count as isolated. One quiet bucket is not isolation: on master a frozen
// worker whose slots are all held by hung requests is skipped by the scheduler until they time out, so it
// is quiet every other bucket.
const IsolationMinBuckets = 3

// timeToIsolate is the seconds from the fault's start to the first bucket of the final stretch, at least
// IsolationMinBuckets long and reaching the end of the fault window, in which the faulted worker got no
// non-probe selections while the gateway was making attempts. Probes are told apart by the breaker's probe
// counter (zero on a build without a breaker). The second result says why it is nil.
func timeToIsolate(p Params, tl Timeline, singleWorker bool) (*float64, string) {
	switch {
	case !singleWorker:
		return nil, "not defined: no single faulted worker"
	case !tl.Applied || !tl.Cleared:
		return nil, "the fault window is incomplete"
	}
	k := tl.ClearTick
	for t := tl.ClearTick - 1; t >= tl.ApplyTick; t-- {
		d := delta(tl, t, t+1)
		if math.Max(d.FaultySelections-d.Probes, 0) > 0 || d.Attempts <= 0 {
			break
		}
		k = t
	}
	if tl.ClearTick-k < IsolationMinBuckets {
		return nil, fmt.Sprintf("the worker was still selected within the last %d buckets of the fault window", IsolationMinBuckets)
	}
	return ptr((tl.Ticks[k].At - tl.AppliedAt).Seconds()), ""
}

// timeToRecover is the seconds from the end of the fault until RecoveryBuckets consecutive buckets with
// enough requests had a success ratio of at least the baseline's.
func timeToRecover(p Params, tl Timeline, s []report.ChaosBucket, baselineRate float64) (*float64, string) {
	if !tl.Cleared {
		return nil, "the fault was never cleared"
	}
	first := tickAt(p, p.Warmup)
	start := int(math.Ceil(float64(tl.ClearedAt) / float64(p.Bucket)))
	run := 0
	for j := start - first; j < len(s); j++ {
		if j < 0 {
			continue
		}
		b := s[j]
		n := b.Succeeded + b.Failed
		if n >= RecoveryMinRequests && float64(b.Succeeded)/float64(n) >= baselineRate {
			run++
			if run == RecoveryBuckets {
				at := time.Duration(first+j-RecoveryBuckets+1) * p.Bucket
				return ptr(math.Max((at - tl.ClearedAt).Seconds(), 0)), ""
			}
			continue
		}
		run = 0
	}
	return nil, "the recovery window never held 5 consecutive buckets at the baseline success ratio with at least 5 requests each"
}
