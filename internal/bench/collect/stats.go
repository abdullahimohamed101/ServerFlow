// Package collect holds the measurement arithmetic of the benchmark harness:
// percentiles, the Jain fairness index, per-request records and their summary,
// and the worker-side sampler. The arithmetic is pure so it can be tested against
// hand-computed fixtures without a network.
package collect

import (
	"math"
	"sort"
)

// Percentile returns the nearest-rank percentile p (0 < p <= 100) of sorted, which
// must be in ascending order: the smallest sample such that at least p percent of the
// samples are at or below it. Exact, not estimated. ok is false for no samples or a
// p outside (0, 100].
func Percentile(sorted []float64, p float64) (v float64, ok bool) {
	n := len(sorted)
	if n == 0 || math.IsNaN(p) || p <= 0 || p > 100 {
		return 0, false
	}
	rank := int(math.Ceil(p * float64(n) / 100)) // p*n first: it is exact for whole p
	rank = min(max(rank, 1), n)
	return sorted[rank-1], true
}

// Dist summarizes a set of samples. Percentiles are nearest-rank.
type Dist struct {
	Count int     `json:"count"`
	Min   float64 `json:"min"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

// Summarize returns the distribution of samples, or nil when there are none (so
// callers must say "not measured" rather than report a zero). It does not modify
// samples.
func Summarize(samples []float64) *Dist {
	if len(samples) == 0 {
		return nil
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	var sum float64
	for _, v := range s {
		sum += v
	}
	d := &Dist{Count: len(s), Min: s[0], Max: s[len(s)-1], Mean: sum / float64(len(s))}
	d.P50, _ = Percentile(s, 50)
	d.P95, _ = Percentile(s, 95)
	d.P99, _ = Percentile(s, 99)
	return d
}

// Jain returns the Jain fairness index (sum x)^2 / (n * sum x^2) of xs: 1 when every
// value is equal, 1/n when one value holds everything. ok is false when it is
// undefined: no values, all zeros, or a negative or non-finite value.
func Jain(xs []float64) (v float64, ok bool) {
	if len(xs) == 0 {
		return 0, false
	}
	var sum, sumSq float64
	for _, x := range xs {
		if math.IsNaN(x) || math.IsInf(x, 0) || x < 0 {
			return 0, false
		}
		sum += x
		sumSq += x * x
	}
	if sumSq == 0 {
		return 0, false
	}
	return sum * sum / (float64(len(xs)) * sumSq), true
}

// Mean returns the arithmetic mean of xs; ok is false for none.
func Mean(xs []float64) (float64, bool) {
	if len(xs) == 0 {
		return 0, false
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs)), true
}
