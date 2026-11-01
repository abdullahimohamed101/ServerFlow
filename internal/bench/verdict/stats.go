package verdict

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
)

// Fixed constants of the statistics (Phase 14 D26).
const (
	// Level is the confidence level of every interval.
	Level = 0.95
	// Alpha is the significance level of Holm and of the equivalence tests' family (1 - Level).
	Alpha = 0.05
	// DefaultResamples is the bootstrap size when a hypothesis does not set one.
	DefaultResamples = 10000
	// DefaultBootstrapSeed is the resampling seed when a hypothesis does not set one.
	DefaultBootstrapSeed = 20260101
	// MinPairs is the fewest pairs a verdict is computed from. The plan's real-time cells have five repeats;
	// a median interval from fewer is mostly luck.
	MinPairs = 5
	// MaxResamples bounds a hypothesis's bootstrap size (memory and time).
	MaxResamples = 1_000_000
	minResamples = 1000
)

// ErrNonFinite is returned for NaN or infinite data.
var ErrNonFinite = errors.New("verdict: data contains NaN or an infinity")

func finite(xs []float64) error {
	for _, x := range xs {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return ErrNonFinite
		}
	}
	return nil
}

// Quantile returns the q-quantile (0 <= q <= 1) of xs, which must be sorted ascending, by linear
// interpolation between order statistics: with h = (n-1)q it is x[floor h] + (h - floor h)(x[floor h + 1] - x[floor h]).
func Quantile(sorted []float64, q float64) (float64, error) {
	n := len(sorted)
	if n == 0 {
		return 0, errors.New("verdict: quantile of no data")
	}
	if !(q >= 0 && q <= 1) {
		return 0, fmt.Errorf("verdict: quantile %v is outside [0, 1]", q)
	}
	if err := finite(sorted); err != nil {
		return 0, err
	}
	h := float64(n-1) * q
	lo := int(math.Floor(h))
	if lo >= n-1 {
		return sorted[n-1], nil
	}
	return sorted[lo] + (h-float64(lo))*(sorted[lo+1]-sorted[lo]), nil
}

// Median returns the median of xs (the mean of the middle two for an even count). xs is not modified.
func Median(xs []float64) (float64, error) {
	if len(xs) == 0 {
		return 0, errors.New("verdict: median of no data")
	}
	if err := finite(xs); err != nil {
		return 0, err
	}
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	return Quantile(c, 0.5)
}

// Mean returns the arithmetic mean of xs.
func Mean(xs []float64) (float64, error) {
	if len(xs) == 0 {
		return 0, errors.New("verdict: mean of no data")
	}
	if err := finite(xs); err != nil {
		return 0, err
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs)), nil
}

// PairedRelative returns the relative differences (b_i - a_i) / a_i. Every a_i must be positive and finite
// and every b_i finite.
func PairedRelative(a, b []float64) ([]float64, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("verdict: %d baseline values but %d treatment values", len(a), len(b))
	}
	out := make([]float64, len(a))
	for i := range a {
		if math.IsNaN(a[i]) || math.IsInf(a[i], 0) || math.IsNaN(b[i]) || math.IsInf(b[i], 0) {
			return nil, ErrNonFinite
		}
		if !(a[i] > 0) {
			return nil, fmt.Errorf("verdict: baseline value %v (pair %d) is not positive, so a relative difference is undefined", a[i], i+1)
		}
		out[i] = (b[i] - a[i]) / a[i]
	}
	return out, nil
}

// Interval is a two-sided interval at a confidence level.
type Interval struct{ Lo, Hi, Level float64 }

// Bootstrap is the result of BootstrapMedian.
type Bootstrap struct {
	// Estimate is the median of the data.
	Estimate float64
	Interval Interval
	// P is the two-sided p-value for H0 "the median is zero" (see the package comment).
	P         float64
	Resamples int
}

// BootstrapMedian computes the percentile bootstrap interval and p-value of the median of xs. It is a pure
// function of its arguments: the same data, resample count and seed always give the same result.
func BootstrapMedian(xs []float64, level float64, resamples int, seed uint64) (Bootstrap, error) {
	if len(xs) < 2 {
		return Bootstrap{}, fmt.Errorf("verdict: a bootstrap needs at least 2 values, got %d", len(xs))
	}
	if err := finite(xs); err != nil {
		return Bootstrap{}, err
	}
	if !(level > 0 && level < 1) {
		return Bootstrap{}, fmt.Errorf("verdict: confidence level %v is outside (0, 1)", level)
	}
	if resamples < minResamples || resamples > MaxResamples {
		return Bootstrap{}, fmt.Errorf("verdict: resamples must be between %d and %d, got %d", minResamples, MaxResamples, resamples)
	}
	est, _ := Median(xs)
	n := len(xs)
	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	reps := make([]float64, resamples)
	scratch := make([]float64, n)
	for b := range reps {
		for i := range scratch {
			scratch[i] = xs[rng.IntN(n)]
		}
		sort.Float64s(scratch)
		reps[b], _ = Quantile(scratch, 0.5)
	}
	sort.Float64s(reps)
	alpha := (1 - level) / 2
	lo, _ := Quantile(reps, alpha)
	hi, _ := Quantile(reps, 1-alpha)
	var le, ge int
	for _, r := range reps {
		if r <= 0 {
			le++
		}
		if r >= 0 {
			ge++
		}
	}
	d := float64(resamples + 1)
	p := 2 * math.Min(float64(le+1)/d, float64(ge+1)/d)
	return Bootstrap{Estimate: est, Interval: Interval{Lo: lo, Hi: hi, Level: level}, P: math.Min(1, p), Resamples: resamples}, nil
}

// InsideMargin reports whether the interval lies entirely inside (-margin, +margin); the bounds themselves
// do not count as inside.
func InsideMargin(iv Interval, margin float64) bool { return iv.Lo > -margin && iv.Hi < margin }

// OutsideMargin reports whether the interval lies entirely outside [-margin, +margin].
func OutsideMargin(iv Interval, margin float64) bool { return iv.Lo > margin || iv.Hi < -margin }

// HolmResult is one hypothesis's outcome of the Holm procedure, in the order the p-values were given.
type HolmResult struct {
	P        float64
	Adjusted float64
	Reject   bool
}

// Holm applies the Holm step-down procedure at level alpha (see the package comment). Ties are broken by
// position, so the result is deterministic.
func Holm(p []float64, alpha float64) ([]HolmResult, error) {
	if !(alpha > 0 && alpha < 1) {
		return nil, fmt.Errorf("verdict: alpha %v is outside (0, 1)", alpha)
	}
	for i, v := range p {
		if math.IsNaN(v) || v < 0 || v > 1 {
			return nil, fmt.Errorf("verdict: p-value %d is %v, not in [0, 1]", i+1, v)
		}
	}
	m := len(p)
	idx := make([]int, m)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(x, y int) bool { return p[idx[x]] < p[idx[y]] })
	out := make([]HolmResult, m)
	running := 0.0
	for rank, i := range idx {
		adj := math.Min(1, float64(m-rank)*p[i])
		running = math.Max(running, adj)
		out[i] = HolmResult{P: p[i], Adjusted: running, Reject: running < alpha}
	}
	return out, nil
}

// TOSTResult is the parametric two one-sided t-tests on the mean of paired differences.
type TOSTResult struct {
	N          int
	Mean, SE   float64
	PLower     float64 // H0: mean <= -margin
	PUpper     float64 // H0: mean >= +margin
	P          float64 // max(PLower, PUpper)
	Equivalent bool    // P < alpha
}

// TOST runs the two one-sided t-tests that the mean of d lies inside (-margin, +margin), each at level alpha.
// With t_L = (mean + margin) / se and t_U = (mean - margin) / se, df = n-1:
//
//	PLower = P(T >= t_L)      PUpper = P(T <= t_U)      P = max(PLower, PUpper)
//
// Zero variance (se = 0) is decided by the sample mean alone: a mean strictly inside the margin gives P = 0,
// otherwise P = 1.
func TOST(d []float64, margin, alpha float64) (TOSTResult, error) {
	n := len(d)
	if n < 2 {
		return TOSTResult{}, fmt.Errorf("verdict: TOST needs at least 2 differences, got %d", n)
	}
	if !(margin > 0) || math.IsInf(margin, 0) {
		return TOSTResult{}, fmt.Errorf("verdict: TOST margin must be positive and finite, got %v", margin)
	}
	if !(alpha > 0 && alpha < 0.5) {
		return TOSTResult{}, fmt.Errorf("verdict: TOST alpha must be in (0, 0.5), got %v", alpha)
	}
	mean, err := Mean(d)
	if err != nil {
		return TOSTResult{}, err
	}
	var ss float64
	for _, x := range d {
		ss += (x - mean) * (x - mean)
	}
	se := math.Sqrt(ss / float64(n-1) / float64(n))
	r := TOSTResult{N: n, Mean: mean, SE: se}
	if se == 0 {
		r.PLower, r.PUpper = 1, 1
		if mean > -margin {
			r.PLower = 0
		}
		if mean < margin {
			r.PUpper = 0
		}
	} else {
		df := float64(n - 1)
		r.PLower = 1 - tCDF((mean+margin)/se, df)
		r.PUpper = tCDF((mean-margin)/se, df)
	}
	r.P = math.Max(r.PLower, r.PUpper)
	r.Equivalent = r.P < alpha
	return r, nil
}

// tCDF is the cumulative distribution function of Student's t with df degrees of freedom, through the
// regularised incomplete beta function: for t >= 0, F = 1 - I_x(df/2, 1/2) / 2 with x = df / (df + t^2).
func tCDF(t, df float64) float64 {
	switch {
	case math.IsNaN(t):
		return math.NaN()
	case math.IsInf(t, 1):
		return 1
	case math.IsInf(t, -1):
		return 0
	}
	x := df / (df + t*t)
	tail := 0.5 * betaInc(df/2, 0.5, x)
	if t >= 0 {
		return 1 - tail
	}
	return tail
}

// betaInc is the regularised incomplete beta function I_x(a, b) for a, b > 0 and 0 <= x <= 1, by the
// continued fraction of Lentz's method (the symmetry relation is used when x is large).
func betaInc(a, b, x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	la, _ := math.Lgamma(a)
	lb, _ := math.Lgamma(b)
	lab, _ := math.Lgamma(a + b)
	front := math.Exp(lab - la - lb + a*math.Log(x) + b*math.Log1p(-x))
	if x < (a+1)/(a+b+2) {
		return front * betaCF(a, b, x) / a
	}
	return 1 - front*betaCF(b, a, 1-x)/b
}

func betaCF(a, b, x float64) float64 {
	const (
		maxIter = 500
		eps     = 1e-15
		tiny    = 1e-300
	)
	qab, qap, qam := a+b, a+1, a-1
	c := 1.0
	d := 1 - qab*x/qap
	if math.Abs(d) < tiny {
		d = tiny
	}
	d = 1 / d
	h := d
	for m := 1; m <= maxIter; m++ {
		fm := float64(m)
		m2 := 2 * fm
		aa := fm * (b - fm) * x / ((qam + m2) * (a + m2))
		d = 1 + aa*d
		if math.Abs(d) < tiny {
			d = tiny
		}
		c = 1 + aa/c
		if math.Abs(c) < tiny {
			c = tiny
		}
		d = 1 / d
		h *= d * c
		aa = -(a + fm) * (qab + fm) * x / ((a + m2) * (qap + m2))
		d = 1 + aa*d
		if math.Abs(d) < tiny {
			d = tiny
		}
		c = 1 + aa/c
		if math.Abs(c) < tiny {
			c = tiny
		}
		d = 1 / d
		del := d * c
		h *= del
		if math.Abs(del-1) < eps {
			break
		}
	}
	return h
}
