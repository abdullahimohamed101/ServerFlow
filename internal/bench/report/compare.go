package report

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"text/tabwriter"
)

// Headline is one number two runs are compared on.
type headline struct {
	key, label, unit string
	// points means the delta is shown in percentage points of a rate, not as a percent.
	points bool
	get    func(Result) *float64
}

func f(v float64) *float64 { return &v }

var headlines = []headline{
	{key: "throughput", label: "throughput", unit: "req/s", get: func(r Result) *float64 { return f(r.Summary.RequestsPerSecond) }},
	{key: "input_tps", label: "input tokens/s", unit: "tok/s", get: func(r Result) *float64 { return f(r.Summary.InputTokensPerSecond) }},
	{key: "output_tps", label: "output tokens/s", unit: "tok/s", get: func(r Result) *float64 { return f(r.Summary.OutputTokensPerSecond) }},
	{key: "latency_p50", label: "latency p50", unit: "ms", get: func(r Result) *float64 {
		if d := r.Summary.Latency; d != nil {
			return f(d.P50)
		}
		return nil
	}},
	{key: "latency_p95", label: "latency p95", unit: "ms", get: func(r Result) *float64 {
		if d := r.Summary.Latency; d != nil {
			return f(d.P95)
		}
		return nil
	}},
	{key: "latency_p99", label: "latency p99", unit: "ms", get: func(r Result) *float64 {
		if d := r.Summary.Latency; d != nil {
			return f(d.P99)
		}
		return nil
	}},
	{key: "ttft_p95", label: "TTFT p95", unit: "ms", get: func(r Result) *float64 {
		if d := r.Summary.TTFT; d != nil {
			return f(d.P95)
		}
		return nil
	}},
	{key: "queue_avg", label: "queue depth avg", unit: "requests", get: func(r Result) *float64 {
		if q := r.Queue; q != nil {
			return f(q.AvgTotalQueue)
		}
		return nil
	}},
	{key: "queue_p95", label: "queue depth p95", unit: "requests", get: func(r Result) *float64 {
		if q := r.Queue; q != nil {
			return f(q.P95TotalQueue)
		}
		return nil
	}},
	{key: "request_jain", label: "worker balance (Jain)", unit: "index", get: func(r Result) *float64 { return r.Imbalance.RequestJain }},
	{key: "error_rate", label: "error rate", unit: "%", points: true, get: func(r Result) *float64 { return f(r.Summary.ErrorRate * 100) }},
}

// MinRepeats is how many runs each side needs before the spread of the repeats may be used
// to judge a difference. With fewer, the range of a side is mostly luck.
const MinRepeats = 3

// MaxValidErrorRate is the error rate above which compare warns that a run's numbers describe
// only its survivors.
const MaxValidErrorRate = 0.05

// Delta is the change from A to B in one number. For repeat groups of at least MinRepeats runs
// on both sides, A and B are the medians of the groups and the ranges are shown; otherwise they
// are the two runs named. Pct is nil when it is undefined (a value is not measured, or A is
// zero); Abs is B minus A. A positive delta means B is higher, not better.
type Delta struct {
	Key, Label, Unit string
	A, B             *float64
	AMin, AMax       *float64
	BMin, BMax       *float64
	Abs, Pct         *float64
	Points           bool
	// Spread is one of "ranges overlap", "ranges do not overlap", "insufficient repeats", or
	// "n/a" (the two values are equal or zero, so there is nothing to judge).
	Spread string
}

// Difference is a metadata field that differs between two runs.
type Difference struct {
	Field, A, B string
	// Expected differences (the scheduler, in a scheduler comparison) are listed but do not
	// make the comparison unfair.
	Expected bool
}

// Comparison is the table of deltas between two runs.
type Comparison struct {
	A, B string
	// NA and NB are how many runs stand behind each side; Grouped says the table compares
	// medians of repeat groups (both sides have at least MinRepeats runs).
	NA, NB      int
	Grouped     bool
	Deltas      []Delta
	Differences []Difference
	// Warnings are problems with the runs themselves (invalid, high error rate) and are
	// printed before the table.
	Warnings []string
}

// Unfair reports whether anything other than the expected differences differs.
func (c Comparison) Unfair() bool {
	for _, d := range c.Differences {
		if !d.Expected {
			return true
		}
	}
	return false
}

// PercentDelta returns (b-a)/a*100, and false when it is undefined because a is zero.
func PercentDelta(a, b float64) (float64, bool) {
	if a == 0 || math.IsNaN(a) || math.IsNaN(b) {
		return 0, false
	}
	return (b - a) / a * 100, true
}

// Compare compares two single runs.
func Compare(a, b Result) Comparison { return CompareGroups(a, b, []Result{a}, []Result{b}) }

// CompareGroups compares runs a and b. When both repeat groups (ga, gb) have at least MinRepeats
// runs it compares their medians and says whether the two groups' ranges overlap; otherwise it
// compares a with b and says "insufficient repeats". The rule is a plain range check, not a
// significance test: for two groups of 3 drawn from the same distribution the ranges are
// disjoint 10% of the time (2/C(6,3)), per metric, so "do not overlap" on one of many metrics
// is weak evidence.
func CompareGroups(a, b Result, ga, gb []Result) Comparison {
	c := Comparison{A: a.RunID, B: b.RunID, NA: len(ga), NB: len(gb), Differences: differences(a.Metadata, b.Metadata)}
	c.Grouped = len(ga) >= MinRepeats && len(gb) >= MinRepeats
	c.Warnings = warnings(ga, gb)
	for _, h := range headlines {
		d := Delta{Key: h.key, Label: h.label, Unit: h.unit, Points: h.points, Spread: "insufficient repeats"}
		if c.Grouped {
			amed, amin, amax, ok1 := groupStats(h, ga)
			bmed, bmin, bmax, ok2 := groupStats(h, gb)
			if ok1 {
				d.A, d.AMin, d.AMax = &amed, &amin, &amax
			}
			if ok2 {
				d.B, d.BMin, d.BMax = &bmed, &bmin, &bmax
			}
			if ok1 && ok2 {
				d.Spread = rangeVerdict(amed, bmed, amin, amax, bmin, bmax)
			}
		} else {
			d.A, d.B = h.get(a), h.get(b)
		}
		if d.A != nil && d.B != nil {
			abs := *d.B - *d.A
			d.Abs = &abs
			if !h.points {
				if p, ok := PercentDelta(*d.A, *d.B); ok {
					d.Pct = &p
				}
			}
		}
		c.Deltas = append(c.Deltas, d)
	}
	return c
}

// groupStats returns the median, minimum and maximum of a headline across a group, over the
// runs that measured it. ok is false when fewer than MinRepeats runs did.
func groupStats(h headline, g []Result) (med, mn, mx float64, ok bool) {
	var xs []float64
	for _, r := range g {
		if v := h.get(r); v != nil {
			xs = append(xs, *v)
		}
	}
	if len(xs) < MinRepeats {
		return 0, 0, 0, false
	}
	sort.Float64s(xs)
	n := len(xs)
	med = xs[n/2]
	if n%2 == 0 {
		med = (xs[n/2-1] + xs[n/2]) / 2
	}
	return med, xs[0], xs[n-1], true
}

func rangeVerdict(amed, bmed, amin, amax, bmin, bmax float64) string {
	if amed == bmed || (amed == 0 && bmed == 0) {
		return "n/a"
	}
	if bmin > amax || bmax < amin {
		return "ranges do not overlap"
	}
	return "ranges overlap"
}

// warnings lists runs whose numbers should not be taken at face value.
func warnings(groups ...[]Result) []string {
	var out []string
	seen := map[string]bool{}
	for _, g := range groups {
		for _, r := range g {
			if seen[r.RunID] {
				continue
			}
			seen[r.RunID] = true
			switch {
			case !r.Valid:
				why := "no reason recorded"
				if len(r.InvalidReasons) > 0 {
					why = strings.Join(r.InvalidReasons, "; ")
				}
				out = append(out, fmt.Sprintf("%s is INVALID: %s", r.RunID, why))
			case r.Summary.ErrorRate > MaxValidErrorRate:
				out = append(out, fmt.Sprintf("%s has an error rate of %.2f%%: its latency, TTFT and balance describe only the requests that succeeded", r.RunID, r.Summary.ErrorRate*100))
			}
		}
	}
	return out
}

func differences(a, b Metadata) []Difference {
	var out []Difference
	add := func(field string, expected bool, av, bv string) {
		if av != bv {
			out = append(out, Difference{Field: field, A: av, B: bv, Expected: expected})
		}
	}
	add("scheduler", true, a.Scheduler, b.Scheduler)
	add("scheduler source", false, a.SchedulerSource, b.SchedulerSource)
	add("workload", false, a.Workload, b.Workload)
	add("seed", false, fmt.Sprint(a.Seed), fmt.Sprint(b.Seed))
	add("plan digest", false, a.PlanDigest, b.PlanDigest)
	add("workers", false, workerCount(a), workerCount(b))
	add("worker profile", false, profile(a), profile(b))
	add("target", false, a.Target+" "+a.TargetHost, b.Target+" "+b.TargetHost)
	add("models", false, strings.Join(a.Models, ","), strings.Join(b.Models, ","))
	add("mode", false, a.Mode, b.Mode)
	add("concurrency", false, fmt.Sprint(a.Concurrency), fmt.Sprint(b.Concurrency))
	add("rate", false, fmt.Sprint(a.Rate), fmt.Sprint(b.Rate))
	add("duration", false, fmt.Sprint(a.DurationSecs), fmt.Sprint(b.DurationSecs))
	add("warm-up", false, fmt.Sprint(a.WarmupSecs), fmt.Sprint(b.WarmupSecs))
	add("stream ratio", false, fmt.Sprint(a.StreamRatio), fmt.Sprint(b.StreamRatio))
	add("git commit", false, a.GitCommit, b.GitCommit)
	add("git tree", false, a.GitTree, b.GitTree)
	add("machine", false, a.Machine, b.Machine)
	add("go version", false, a.GoVersion, b.GoVersion)
	add("harness version", false, a.HarnessVersion, b.HarnessVersion)
	return out
}

func workerCount(m Metadata) string {
	if m.WorkerCount == nil {
		return Unknown
	}
	return fmt.Sprint(*m.WorkerCount)
}

func profile(m Metadata) string {
	if m.Cluster == nil {
		return Unknown
	}
	parts := make([]string, 0, len(m.Cluster.Workers))
	for _, w := range m.Cluster.Workers {
		parts = append(parts, fmt.Sprintf("%s:%g/%dms", w.Model, w.TokensPerSecond, w.TTFTMillis))
	}
	return m.Cluster.Profile + " [" + strings.Join(parts, " ") + "]"
}

// Write prints the comparison as a table. It states deltas and whether the runs are
// comparable; it does not declare a winner.
func (c Comparison) Write(w io.Writer) error {
	var b strings.Builder
	for _, warn := range c.Warnings {
		fmt.Fprintf(&b, "WARNING: %s\n", warn)
	}
	if len(c.Warnings) > 0 {
		b.WriteString("\n")
	}
	if c.Grouped {
		fmt.Fprintf(&b, "Comparing %s (A) with %s (B): medians of %d and %d repeats, with the min-max range of each in brackets. ", c.A, c.B, c.NA, c.NB)
	} else {
		fmt.Fprintf(&b, "Comparing %s (A) with %s (B). ", c.A, c.B)
	}
	b.WriteString("Throughput is requests that completed inside the measurement window per second of window. Deltas are B relative to A; positive means B is higher, not better.\n\n")
	tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "METRIC\tA\tB\tDELTA\tCHANGE\tSPREAD")
	for _, d := range c.Deltas {
		change := "n/a"
		switch {
		case d.Pct != nil:
			change = fmt.Sprintf("%+.2f%%", *d.Pct)
		case d.Points && d.Abs != nil:
			change = fmt.Sprintf("%+.2f pp", *d.Abs)
		}
		abs := "n/a"
		if d.Abs != nil {
			abs = fmt.Sprintf("%+.4g %s", *d.Abs, d.Unit)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", d.Label, cell(d.A, d.AMin, d.AMax), cell(d.B, d.BMin, d.BMax), abs, change, d.Spread)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if !c.Grouped {
		fmt.Fprintf(&b, "\nSPREAD: insufficient repeats (it needs at least %d runs on each side, made with --repeat; this compares %d and %d). Two single runs cannot show whether a difference is noise.\n", MinRepeats, c.NA, c.NB)
	} else {
		b.WriteString("\nSPREAD compares the min-max ranges of the two groups. It is not a significance test: with 3 runs per side and no real difference the ranges are disjoint about 10% of the time, per metric.\n")
	}
	b.WriteString("\n")
	if len(c.Differences) == 0 {
		b.WriteString("Metadata: the runs differ in nothing that is recorded.\n")
	} else {
		if c.Unfair() {
			b.WriteString("WARNING: these runs differ in more than the scheduler, so the deltas may not be a fair comparison:\n")
		} else {
			b.WriteString("Metadata that differs (expected for this comparison):\n")
		}
		for _, d := range c.Differences {
			mark := "  "
			if !d.Expected {
				mark = "! "
			}
			fmt.Fprintf(&b, "%s%s: %s -> %s\n", mark, d.Field, d.A, d.B)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// cell formats a value, with its range when it has one.
func cell(v, mn, mx *float64) string {
	if v == nil {
		return "not measured"
	}
	if mn == nil || mx == nil {
		return fmt.Sprintf("%.4g", *v)
	}
	return fmt.Sprintf("%.4g [%.4g-%.4g]", *v, *mn, *mx)
}
