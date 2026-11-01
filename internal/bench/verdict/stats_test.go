package verdict

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
)

func near(t *testing.T, what string, got, want, tol float64) {
	t.Helper()
	if math.IsNaN(got) || math.Abs(got-want) > tol {
		t.Errorf("%s = %.10g, want %.10g +/- %g", what, got, want, tol)
	}
}

func TestQuantileHandComputed(t *testing.T) {
	xs := []float64{1, 2, 3, 4}
	for _, c := range []struct{ q, want float64 }{
		{0, 1}, {1, 4}, {0.5, 2.5}, {0.25, 1.75}, {0.95, 3.85}, {1.0 / 3, 2}, {0.75, 3.25},
	} {
		got, err := Quantile(xs, c.q)
		if err != nil {
			t.Fatal(err)
		}
		near(t, "quantile", got, c.want, 1e-12)
	}
	if v, _ := Quantile([]float64{7}, 0.3); v != 7 {
		t.Errorf("a single value is every quantile, got %v", v)
	}
	if _, err := Quantile(nil, 0.5); err == nil {
		t.Error("no data")
	}
	if _, err := Quantile(xs, 1.01); err == nil {
		t.Error("q > 1")
	}
	if _, err := Quantile(xs, -0.01); err == nil {
		t.Error("q < 0")
	}
	if _, err := Quantile(xs, math.NaN()); err == nil {
		t.Error("NaN q")
	}
	if _, err := Quantile([]float64{1, math.NaN()}, 0.5); !errors.Is(err, ErrNonFinite) {
		t.Errorf("%v", err)
	}
}

func TestMedianAndMean(t *testing.T) {
	in := []float64{3, 1, 2}
	if m, _ := Median(in); m != 2 {
		t.Error(m)
	}
	if in[0] != 3 || in[1] != 1 || in[2] != 2 {
		t.Error("Median must not reorder its argument")
	}
	if m, _ := Median([]float64{4, 1, 3, 2}); m != 2.5 {
		t.Error(m)
	}
	if m, _ := Median([]float64{5, 5, 5, 9}); m != 5 {
		t.Errorf("ties: %v", m)
	}
	if m, _ := Mean([]float64{1, 2, 6}); m != 3 {
		t.Error(m)
	}
	for _, f := range []func([]float64) (float64, error){Median, Mean} {
		if _, err := f(nil); err == nil {
			t.Error("empty")
		}
		if _, err := f([]float64{1, math.Inf(1)}); !errors.Is(err, ErrNonFinite) {
			t.Error("inf")
		}
	}
}

func TestPairedRelative(t *testing.T) {
	got, err := PairedRelative([]float64{100, 200, 50}, []float64{90, 250, 50})
	if err != nil {
		t.Fatal(err)
	}
	for i, w := range []float64{-0.1, 0.25, 0} {
		near(t, "diff", got[i], w, 1e-15)
	}
	for name, ab := range map[string][2][]float64{
		"length":      {{1, 2}, {1}},
		"zero base":   {{0, 1}, {1, 1}},
		"neg base":    {{-1, 1}, {1, 1}},
		"nan base":    {{math.NaN(), 1}, {1, 1}},
		"inf treated": {{1, 1}, {math.Inf(1), 1}},
		"nan treated": {{1, 1}, {1, math.NaN()}},
	} {
		if _, err := PairedRelative(ab[0], ab[1]); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}

func TestBootstrapIsDeterministicPerSeed(t *testing.T) {
	xs := []float64{0.12, 0.05, 0.2, -0.03, 0.15, 0.09, 0.11, 0.3}
	a, err := BootstrapMedian(xs, 0.95, 2000, 7)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := BootstrapMedian(xs, 0.95, 2000, 7)
	if a != b {
		t.Fatalf("same seed, different result: %+v %+v", a, b)
	}
	c, _ := BootstrapMedian(xs, 0.95, 2000, 8)
	if c == a {
		t.Fatal("a different seed must resample differently")
	}
	// The estimate is the data's median whatever the seed, and the input is untouched.
	want, _ := Median(xs)
	if a.Estimate != want || c.Estimate != want {
		t.Fatal("estimate")
	}
	if xs[0] != 0.12 || xs[3] != -0.03 {
		t.Fatal("input reordered")
	}
	if a.Resamples != 2000 || a.Interval.Level != 0.95 {
		t.Fatalf("%+v", a)
	}
	if !(a.Interval.Lo <= a.Estimate && a.Estimate <= a.Interval.Hi) {
		t.Fatalf("estimate outside its interval: %+v", a)
	}
	if !(a.Interval.Lo >= -0.03 && a.Interval.Hi <= 0.3) {
		t.Fatalf("a bootstrap median cannot leave the data's range: %+v", a)
	}
}

func TestBootstrapHandComputedCases(t *testing.T) {
	const B = 10000
	// All values positive: every resample median is positive, so no replicate is <= 0, and
	// p = 2 * min((0+1)/(B+1), (B+1)/(B+1)) = 2/(B+1) exactly.
	for _, xs := range [][]float64{{1, 2, 3, 4, 5, 6}, {0.5, 0.5, 0.5, 0.5}} {
		r, err := BootstrapMedian(xs, 0.95, B, 1)
		if err != nil {
			t.Fatal(err)
		}
		near(t, "p of all-positive data", r.P, 2.0/(B+1), 1e-15)
	}
	// All negative: the mirror image.
	r, _ := BootstrapMedian([]float64{-1, -2, -3, -4}, 0.95, B, 1)
	near(t, "p of all-negative data", r.P, 2.0/(B+1), 1e-15)
	// All zero: every replicate is both <= 0 and >= 0, so min(1, 2) = 1.
	r, _ = BootstrapMedian([]float64{0, 0, 0, 0, 0}, 0.95, B, 1)
	if r.P != 1 || r.Interval.Lo != 0 || r.Interval.Hi != 0 || r.Estimate != 0 {
		t.Fatalf("%+v", r)
	}
	// Zero variance, non-zero value: the interval collapses onto it.
	r, _ = BootstrapMedian([]float64{0.2, 0.2, 0.2}, 0.9, B, 5)
	if r.Interval.Lo != 0.2 || r.Interval.Hi != 0.2 || r.Estimate != 0.2 || r.Interval.Level != 0.9 {
		t.Fatalf("%+v", r)
	}
	// Two points {0, 1}: a resample's median is 0, 0.5 or 1 with probabilities 1/4, 1/2, 1/4, so the
	// 2.5% and 97.5% quantiles of the replicates are 0 and 1, the estimate is 0.5, and
	// P(replicate <= 0) = 1/4 gives p = 2 * 1/4 = 0.5 (the sampling error of a share near 1/4 with
	// B = 20000 is 0.003).
	r, _ = BootstrapMedian([]float64{0, 1}, 0.95, 20000, 3)
	if r.Estimate != 0.5 || r.Interval.Lo != 0 || r.Interval.Hi != 1 {
		t.Fatalf("%+v", r)
	}
	near(t, "p of {0,1}", r.P, 0.5, 0.02)
	// A lower level gives a narrower (or equal) interval from the same replicates and seed.
	wide, _ := BootstrapMedian([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.99, B, 4)
	narrow, _ := BootstrapMedian([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.5, B, 4)
	if !(narrow.Interval.Lo >= wide.Interval.Lo && narrow.Interval.Hi <= wide.Interval.Hi) || narrow.Interval == wide.Interval {
		t.Fatalf("%+v %+v", narrow, wide)
	}
}

func TestBootstrapRefusesBadInput(t *testing.T) {
	ok := []float64{1, 2, 3}
	for name, f := range map[string]func() error{
		"one value":      func() error { _, e := BootstrapMedian([]float64{1}, 0.95, 2000, 1); return e },
		"no value":       func() error { _, e := BootstrapMedian(nil, 0.95, 2000, 1); return e },
		"nan":            func() error { _, e := BootstrapMedian([]float64{1, math.NaN()}, 0.95, 2000, 1); return e },
		"inf":            func() error { _, e := BootstrapMedian([]float64{1, math.Inf(-1)}, 0.95, 2000, 1); return e },
		"level 0":        func() error { _, e := BootstrapMedian(ok, 0, 2000, 1); return e },
		"level 1":        func() error { _, e := BootstrapMedian(ok, 1, 2000, 1); return e },
		"level nan":      func() error { _, e := BootstrapMedian(ok, math.NaN(), 2000, 1); return e },
		"few resamples":  func() error { _, e := BootstrapMedian(ok, 0.95, 999, 1); return e },
		"many resamples": func() error { _, e := BootstrapMedian(ok, 0.95, MaxResamples+1, 1); return e },
	} {
		if f() == nil {
			t.Errorf("%s must be refused", name)
		}
	}
	if _, err := BootstrapMedian(ok, 0.95, 1000, 1); err != nil {
		t.Errorf("the smallest allowed size: %v", err)
	}
	if _, err := BootstrapMedian([]float64{1, 2}, 0.95, 1000, 1); err != nil {
		t.Errorf("two values are enough for the function (MinPairs is the verdict's rule): %v", err)
	}
}

// Coverage of the percentile bootstrap interval for the median of a known distribution. The true median
// of N(0,1) is 0. The percentile interval is known to undercover slightly at small n (about 93% at n = 25
// for the median); with 600 replications the sampling error of the observed coverage is about 1%, and B =
// 1000 resamples adds a little more. The band [0.88, 0.99] is therefore about -7 and +4 standard errors around
// 0.93: wide enough not to flake (the run is deterministic anyway, fixed seeds), tight enough to fail if the
// interval is a wrong quantile (a 90% or 99% interval would fall outside it) or is not centred.
func TestBootstrapCoverageOnAKnownDistribution(t *testing.T) {
	const reps, n = 600, 25
	rng := rand.New(rand.NewPCG(2024, 1))
	covered, below, above := 0, 0, 0
	for i := 0; i < reps; i++ {
		xs := make([]float64, n)
		for j := range xs {
			xs[j] = rng.NormFloat64()
		}
		r, err := BootstrapMedian(xs, 0.95, 1000, uint64(i+1))
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case r.Interval.Lo > 0:
			above++
		case r.Interval.Hi < 0:
			below++
		default:
			covered++
		}
	}
	cov := float64(covered) / reps
	if cov < 0.88 || cov > 0.99 {
		t.Fatalf("coverage %.3f outside [0.88, 0.99] (missed below %d, above %d)", cov, below, above)
	}
	// The misses are on both sides: a one-sided error would mean a biased interval.
	if below == 0 || above == 0 {
		t.Errorf("misses should fall on both sides, got below %d above %d", below, above)
	}
}

func TestInsideAndOutsideMargin(t *testing.T) {
	iv := func(lo, hi float64) Interval { return Interval{Lo: lo, Hi: hi, Level: 0.95} }
	for _, c := range []struct {
		iv           Interval
		m            float64
		inside, outs bool
	}{
		{iv(-0.02, 0.02), 0.05, true, false},
		{iv(-0.05, 0.02), 0.05, false, false}, // touching the bound is not inside
		{iv(-0.02, 0.05), 0.05, false, false},
		{iv(0.04, 0.09), 0.05, false, false}, // straddles the upper bound
		{iv(0.051, 0.09), 0.05, false, true},
		{iv(0.05, 0.09), 0.05, false, false}, // touching the bound is not outside
		{iv(-0.09, -0.051), 0.05, false, true},
		{iv(-0.09, -0.05), 0.05, false, false},
		{iv(0, 0), 0.05, true, false},
	} {
		if got := InsideMargin(c.iv, c.m); got != c.inside {
			t.Errorf("InsideMargin(%+v, %v) = %v", c.iv, c.m, got)
		}
		if got := OutsideMargin(c.iv, c.m); got != c.outs {
			t.Errorf("OutsideMargin(%+v, %v) = %v", c.iv, c.m, got)
		}
	}
}

func TestHolmTextbookCases(t *testing.T) {
	// p = 0.01, 0.04, 0.03, 0.005, m = 4. Sorted 0.005, 0.01, 0.03, 0.04 times 4, 3, 2, 1 gives
	// 0.02, 0.03, 0.06, 0.04; the running maximum is 0.02, 0.03, 0.06, 0.06.
	res, err := Holm([]float64{0.01, 0.04, 0.03, 0.005}, 0.05)
	if err != nil {
		t.Fatal(err)
	}
	want := []HolmResult{{0.01, 0.03, true}, {0.04, 0.06, false}, {0.03, 0.06, false}, {0.005, 0.02, true}}
	for i := range want {
		near(t, "adjusted", res[i].Adjusted, want[i].Adjusted, 1e-15)
		if res[i].Reject != want[i].Reject || res[i].P != want[i].P {
			t.Errorf("result %d = %+v, want %+v", i, res[i], want[i])
		}
	}
	// 0.01 .. 0.05, m = 5: 0.05, 0.08, 0.09, 0.08, 0.05 -> running max 0.05, 0.08, 0.09, 0.09, 0.09; at alpha
	// = 0.05 the smallest adjusted value equals alpha exactly and is NOT rejected (strict inequality).
	res, _ = Holm([]float64{0.01, 0.02, 0.03, 0.04, 0.05}, 0.05)
	for i, w := range []float64{0.05, 0.08, 0.09, 0.09, 0.09} {
		near(t, "adjusted", res[i].Adjusted, w, 1e-15)
		if res[i].Reject {
			t.Errorf("p %d must not be rejected at the boundary", i)
		}
	}
	if res, _ = Holm([]float64{0.01, 0.02, 0.03, 0.04, 0.05}, 0.0501); !res[0].Reject || res[1].Reject {
		t.Errorf("just above the boundary only the first is rejected: %+v", res)
	}
	// Capped at 1 and made monotone: 0.6 * 2 = 1.2 -> 1; 0.7 * 1 = 0.7 -> running max 1.
	res, _ = Holm([]float64{0.6, 0.7}, 0.05)
	if res[0].Adjusted != 1 || res[1].Adjusted != 1 {
		t.Errorf("%+v", res)
	}
	// One test: Holm is the test itself.
	res, _ = Holm([]float64{0.049}, 0.05)
	if !res[0].Reject || res[0].Adjusted != 0.049 {
		t.Errorf("%+v", res)
	}
	res, _ = Holm([]float64{0.05}, 0.05)
	if res[0].Reject {
		t.Error("p = alpha is not rejected")
	}
	// Two tests, the first strong: 0.001 * 2 = 0.002 rejected; 0.2 not.
	res, _ = Holm([]float64{0.001, 0.2}, 0.05)
	if !res[0].Reject || res[1].Reject || res[0].Adjusted != 0.002 {
		t.Errorf("%+v", res)
	}
	// Step-down: the second rejection uses the smaller multiplier. 0.02 and 0.04: sorted x2, x1 -> 0.04, 0.04.
	res, _ = Holm([]float64{0.04, 0.02}, 0.05)
	if !res[0].Reject || !res[1].Reject || res[0].Adjusted != 0.04 || res[1].Adjusted != 0.04 {
		t.Errorf("%+v", res)
	}
	if res, err = Holm(nil, 0.05); err != nil || len(res) != 0 {
		t.Errorf("an empty family is empty: %v %v", res, err)
	}
	// Ties are adjusted alike and deterministically.
	res, _ = Holm([]float64{0.02, 0.02, 0.02}, 0.05)
	for _, r := range res {
		near(t, "tie", r.Adjusted, 0.06, 1e-15)
	}
}

func TestHolmRefusesBadInput(t *testing.T) {
	for _, p := range [][]float64{{math.NaN()}, {-0.1}, {1.1}, {0.5, math.Inf(1)}} {
		if _, err := Holm(p, 0.05); err == nil {
			t.Errorf("%v must be refused", p)
		}
	}
	for _, a := range []float64{0, 1, -1, math.NaN()} {
		if _, err := Holm([]float64{0.5}, a); err == nil {
			t.Errorf("alpha %v must be refused", a)
		}
	}
}

func TestHolmProperties(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 22))
	for trial := 0; trial < 3000; trial++ {
		m := 1 + rng.IntN(8)
		p := make([]float64, m)
		for i := range p {
			p[i] = rng.Float64()
			if rng.IntN(4) == 0 {
				p[i] *= 0.05
			}
			if rng.IntN(10) == 0 {
				p[i] = math.Round(p[i]*20) / 20 // ties
			}
		}
		res, err := Holm(p, 0.05)
		if err != nil {
			t.Fatal(err)
		}
		bonf := 0.05 / float64(m)
		for i, r := range res {
			if r.Adjusted < r.P || r.Adjusted > 1 {
				t.Fatalf("adjusted %v outside [p=%v, 1]", r.Adjusted, r.P)
			}
			if p[i] < bonf && !r.Reject {
				t.Fatalf("Holm must reject whatever Bonferroni does: p=%v m=%d", p[i], m)
			}
			if r.Reject && p[i] >= 0.05 {
				t.Fatalf("rejected p=%v >= alpha", p[i])
			}
			for j, s := range res {
				if p[i] <= p[j] && r.Adjusted > s.Adjusted {
					t.Fatalf("adjusted values must be monotone in p: %v %v", p, res)
				}
			}
			// Monotone in the data: a smaller p-value never turns a rejection into a non-rejection.
			q := append([]float64(nil), p...)
			q[i] *= 0.5
			res2, _ := Holm(q, 0.05)
			for k := range res {
				if res[k].Reject && !res2[k].Reject {
					t.Fatalf("lowering p[%d] un-rejected %d: %v -> %v", i, k, p, q)
				}
			}
		}
	}
}

// Under the global null the p-values are uniform and independent, and Holm's family-wise error rate is
// 1 - (1 - alpha/m)^m = 0.0489 for m = 10 (the same as Bonferroni). With 40,000 trials the standard error is
// 0.0011, so the bound 0.0543 is +4 standard errors; the lower bound shows the procedure is not vacuously
// conservative.
func TestHolmControlsTheFamilyWiseErrorRateUnderTheNull(t *testing.T) {
	rng := rand.New(rand.NewPCG(99, 100))
	const trials, m = 40000, 10
	anyReject := 0
	for i := 0; i < trials; i++ {
		p := make([]float64, m)
		for j := range p {
			p[j] = rng.Float64()
		}
		res, _ := Holm(p, 0.05)
		for _, r := range res {
			if r.Reject {
				anyReject++
				break
			}
		}
	}
	fwer := float64(anyReject) / trials
	if fwer > 0.0543 || fwer < 0.040 {
		t.Fatalf("FWER %.4f outside [0.040, 0.0543]; expected about 0.0489", fwer)
	}
}

// Five true nulls (uniform p) mixed with five strong effects: the chance to reject any true null stays
// within alpha; the strong effects are found almost always.
func TestHolmControlsTheErrorRateWithSomeTrueEffects(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	const trials = 30000
	falseRejects, strongFound := 0, 0
	for i := 0; i < trials; i++ {
		p := make([]float64, 10)
		for j := 0; j < 5; j++ {
			p[j] = rng.Float64()
		}
		for j := 5; j < 10; j++ {
			p[j] = rng.Float64() * 1e-6
		}
		res, _ := Holm(p, 0.05)
		for j := 0; j < 5; j++ {
			if res[j].Reject {
				falseRejects++
				break
			}
		}
		if res[5].Reject && res[9].Reject {
			strongFound++
		}
	}
	// Holm bounds the chance of any false rejection by alpha (Bonferroni over the five true nulls would give a
	// bound of 0.05 too, and the actual rate is about 0.049). 30,000 trials: SE <= 0.0013, so 0.0545 is +3.5 SE.
	if r := float64(falseRejects) / trials; r > 0.0545 {
		t.Fatalf("false rejection rate %.4f above alpha + 4 SE", r)
	}
	if strongFound < trials*99/100 {
		t.Fatalf("the strong effects were found only %d times", strongFound)
	}
}

func TestTCDFAgainstTablesAndClosedForms(t *testing.T) {
	// Values from numerical integration of the density (Simpson, 2e6 steps) and standard t tables.
	for _, c := range []struct{ t, df, want float64 }{
		{2.262157, 9, 0.975}, {1.833113, 9, 0.95}, {2.570582, 5, 0.975}, {2.0, 10, 0.9633059826146757},
		{-1.0, 3, 0.1955011094778853}, {0.5, 1, 0.64758361765043}, {0, 4, 0.5},
	} {
		near(t, "tCDF", tCDF(c.t, c.df), c.want, 2e-8)
	}
	// df = 1 is the Cauchy distribution: F(t) = 1/2 + atan(t)/pi. df = 2: F(t) = 1/2 + t / (2 sqrt(2 + t^2)).
	for _, x := range []float64{-30, -5, -1.5, -0.2, 0.2, 1, 3, 12, 100} {
		near(t, "df=1", tCDF(x, 1), 0.5+math.Atan(x)/math.Pi, 1e-12)
		near(t, "df=2", tCDF(x, 2), 0.5+x/(2*math.Sqrt(2+x*x)), 1e-12)
		near(t, "symmetry", tCDF(x, 7)+tCDF(-x, 7), 1, 1e-12)
	}
	// Large df tends to the normal: Phi(1.96) = 0.9750021.
	near(t, "df large", tCDF(1.96, 1e7), 0.9750021, 1e-5)
	if tCDF(math.Inf(1), 3) != 1 || tCDF(math.Inf(-1), 3) != 0 || !math.IsNaN(tCDF(math.NaN(), 3)) {
		t.Error("infinities and NaN")
	}
	// Monotone.
	prev := -1.0
	for x := -8.0; x <= 8; x += 0.25 {
		v := tCDF(x, 6)
		if v < prev {
			t.Fatalf("not monotone at %v", x)
		}
		prev = v
	}
}

func TestTOSTHandComputed(t *testing.T) {
	// d = -0.01, 0.01, 0.02, 0, 0.03: mean 0.01, sum of squares 1e-3, se = sqrt(1e-3/4/5) = 0.0070711,
	// margin 0.03: t_L = 5.65685, t_U = -2.82843, df = 4. One-sided p-values from numerical integration of
	// the t density: P(T >= 5.65685) = 0.0024063392, P(T <= -2.82843) = 0.0237103278.
	d := []float64{-0.01, 0.01, 0.02, 0, 0.03}
	r, err := TOST(d, 0.03, 0.025)
	if err != nil {
		t.Fatal(err)
	}
	near(t, "mean", r.Mean, 0.01, 1e-15)
	near(t, "se", r.SE, math.Sqrt(5e-5), 1e-15)
	near(t, "PLower", r.PLower, 0.0024063392, 1e-9)
	near(t, "PUpper", r.PUpper, 0.0237103278, 1e-9)
	near(t, "P", r.P, 0.0237103278, 1e-9)
	if !r.Equivalent || r.N != 5 {
		t.Errorf("%+v", r)
	}
	if r2, _ := TOST(d, 0.03, 0.02); r2.Equivalent {
		t.Error("P = 0.0237 is not below alpha = 0.02")
	}
	// A wider margin only helps; a narrower one hurts.
	if r3, _ := TOST(d, 0.02, 0.025); r3.Equivalent {
		t.Errorf("margin 0.02: %+v", r3)
	}
	if r3, _ := TOST(d, 0.06, 0.025); !r3.Equivalent || r3.P >= r.P {
		t.Errorf("margin 0.06: %+v", r3)
	}
	// df = 2 closed form as an independent oracle: d = 0.01, 0.02, 0.03, mean 0.02, se = sqrt(1e-4/3).
	d3 := []float64{0.01, 0.02, 0.03}
	r, _ = TOST(d3, 0.05, 0.025)
	se := math.Sqrt(1e-4 / 3)
	f := func(x float64) float64 { return 0.5 + x/(2*math.Sqrt(2+x*x)) }
	near(t, "PLower df2", r.PLower, 1-f((0.02+0.05)/se), 1e-12)
	near(t, "PUpper df2", r.PUpper, f((0.02-0.05)/se), 1e-12)
	near(t, "P df2", r.P, math.Max(r.PLower, r.PUpper), 0)
}

func TestTOSTProperties(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for trial := 0; trial < 500; trial++ {
		n := 2 + rng.IntN(15)
		d := make([]float64, n)
		neg := make([]float64, n)
		scaled := make([]float64, n)
		for i := range d {
			d[i] = rng.NormFloat64() * 0.03
			neg[i] = -d[i]
			scaled[i] = d[i] * 7
		}
		margin := 0.01 + rng.Float64()*0.08
		a, err := TOST(d, margin, 0.025)
		if err != nil {
			t.Fatal(err)
		}
		// Symmetric: negating the data swaps the two one-sided tests and leaves the verdict alone.
		b, _ := TOST(neg, margin, 0.025)
		near(t, "swap lower/upper", a.PLower, b.PUpper, 1e-12)
		near(t, "swap upper/lower", a.PUpper, b.PLower, 1e-12)
		near(t, "P", a.P, b.P, 1e-12)
		if a.Equivalent != b.Equivalent {
			t.Fatal("equivalence must not depend on the sign of the data")
		}
		// Scale-invariant: scaling data and margin together changes nothing.
		c, _ := TOST(scaled, margin*7, 0.025)
		near(t, "scale", a.P, c.P, 1e-9)
		if a.P < 0 || a.P > 1 || a.PLower < 0 || a.PUpper > 1 {
			t.Fatalf("%+v", a)
		}
	}
}

func TestTOSTEdgeCases(t *testing.T) {
	// Zero variance: decided by the mean alone.
	in, _ := TOST([]float64{0.01, 0.01, 0.01}, 0.05, 0.025)
	if in.P != 0 || !in.Equivalent || in.SE != 0 {
		t.Errorf("%+v", in)
	}
	out, _ := TOST([]float64{0.06, 0.06, 0.06}, 0.05, 0.025)
	if out.P != 1 || out.Equivalent || out.PUpper != 1 || out.PLower != 0 {
		t.Errorf("%+v", out)
	}
	onEdge, _ := TOST([]float64{0.05, 0.05}, 0.05, 0.025)
	if onEdge.Equivalent || onEdge.PUpper != 1 {
		t.Errorf("a mean on the margin is not inside it: %+v", onEdge)
	}
	lowEdge, _ := TOST([]float64{-0.05, -0.05}, 0.05, 0.025)
	if lowEdge.Equivalent || lowEdge.PLower != 1 || lowEdge.PUpper != 0 {
		t.Errorf("a mean on the lower margin is not inside it: %+v", lowEdge)
	}
	low, _ := TOST([]float64{-0.07, -0.07}, 0.05, 0.025)
	if low.PLower != 1 || low.PUpper != 0 || low.Equivalent {
		t.Errorf("%+v", low)
	}
	for name, f := range map[string]func() error{
		"n = 1":      func() error { _, e := TOST([]float64{0.1}, 0.05, 0.025); return e },
		"n = 0":      func() error { _, e := TOST(nil, 0.05, 0.025); return e },
		"margin 0":   func() error { _, e := TOST([]float64{0, 0}, 0, 0.025); return e },
		"margin neg": func() error { _, e := TOST([]float64{0, 0}, -1, 0.025); return e },
		"margin nan": func() error { _, e := TOST([]float64{0, 0}, math.NaN(), 0.025); return e },
		"margin inf": func() error { _, e := TOST([]float64{0, 0}, math.Inf(1), 0.025); return e },
		"alpha 0":    func() error { _, e := TOST([]float64{0, 0}, 0.1, 0); return e },
		"alpha 0.5":  func() error { _, e := TOST([]float64{0, 0}, 0.1, 0.5); return e },
		"data nan":   func() error { _, e := TOST([]float64{0, math.NaN()}, 0.1, 0.025); return e },
		"data inf":   func() error { _, e := TOST([]float64{0, math.Inf(1)}, 0.1, 0.025); return e },
	} {
		if f() == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}
