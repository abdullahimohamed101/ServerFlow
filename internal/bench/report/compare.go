package report

import (
	"fmt"
	"io"
	"math"
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

// Delta is the change from run A to run B in one number. Pct is nil when it is undefined
// (a value is not measured, or A is zero); Abs is B minus A. A positive delta means B is
// higher, not better.
type Delta struct {
	Key, Label, Unit string
	A, B             *float64
	Abs, Pct         *float64
	Points           bool
	// Spread says whether the difference exceeds run-to-run spread: "outside spread",
	// "within spread", or "no repeat data".
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
	A, B        string
	Deltas      []Delta
	Differences []Difference
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

// CompareGroups compares runs a and b and, from their repeat groups, says for each number
// whether the difference exceeds the spread across repeats (the two groups' ranges do
// not overlap).
func CompareGroups(a, b Result, ga, gb []Result) Comparison {
	c := Comparison{A: a.RunID, B: b.RunID, Differences: differences(a.Metadata, b.Metadata)}
	repeated := len(ga) > 1 || len(gb) > 1
	for _, h := range headlines {
		d := Delta{Key: h.key, Label: h.label, Unit: h.unit, A: h.get(a), B: h.get(b), Points: h.points}
		if d.A != nil && d.B != nil {
			abs := *d.B - *d.A
			d.Abs = &abs
			if !h.points {
				if p, ok := PercentDelta(*d.A, *d.B); ok {
					d.Pct = &p
				}
			}
		}
		d.Spread = "no repeat data"
		if repeated && d.A != nil && d.B != nil {
			d.Spread = spread(h, ga, gb)
		}
		c.Deltas = append(c.Deltas, d)
	}
	return c
}

func spread(h headline, ga, gb []Result) string {
	lo := func(g []Result) (mn, mx float64, ok bool) {
		mn, mx = math.Inf(1), math.Inf(-1)
		for _, r := range g {
			if v := h.get(r); v != nil {
				mn, mx, ok = math.Min(mn, *v), math.Max(mx, *v), true
			}
		}
		return
	}
	amin, amax, ok1 := lo(ga)
	bmin, bmax, ok2 := lo(gb)
	if !ok1 || !ok2 {
		return "no repeat data"
	}
	if bmin > amax || bmax < amin {
		return "outside spread"
	}
	return "within spread"
}

func differences(a, b Metadata) []Difference {
	var out []Difference
	add := func(field string, expected bool, av, bv string) {
		if av != bv {
			out = append(out, Difference{Field: field, A: av, B: bv, Expected: expected})
		}
	}
	add("scheduler", true, a.Scheduler, b.Scheduler)
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
	fmt.Fprintf(w, "Comparing %s (A) with %s (B). Deltas are B relative to A; positive means B is higher, not better.\n\n", c.A, c.B)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "METRIC\tA\tB\tDELTA\tCHANGE\tSPREAD")
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
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", d.Label, num(d.A), num(d.B), abs, change, d.Spread)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(w)
	if len(c.Differences) == 0 {
		fmt.Fprintln(w, "Metadata: the runs differ in nothing that is recorded.")
		return nil
	}
	if c.Unfair() {
		fmt.Fprintln(w, "WARNING: these runs differ in more than the scheduler, so the deltas may not be a fair comparison:")
	} else {
		fmt.Fprintln(w, "Metadata that differs (expected for this comparison):")
	}
	for _, d := range c.Differences {
		mark := "  "
		if !d.Expected {
			mark = "! "
		}
		fmt.Fprintf(w, "%s%s: %s -> %s\n", mark, d.Field, d.A, d.B)
	}
	return nil
}

func num(p *float64) string {
	if p == nil {
		return "not measured"
	}
	return fmt.Sprintf("%.4g", *p)
}
