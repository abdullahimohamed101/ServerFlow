package chaos

import (
	"fmt"
	"sort"
	"strings"

	"serverflow/internal/bench/report"
)

// SpreadSummary prints, for the valid runs of one scenario, the minimum, median and maximum of the
// pre-registered primary measures and of the descriptive ones. Invalid runs are listed with their reasons
// and left out: they are never compared. With fewer than report.MinRepeats valid runs the ranges are
// mostly luck and the summary says so.
func SpreadSummary(sc Scenario, rs []report.Result) string {
	var b strings.Builder
	var valid []report.Result
	for _, r := range rs {
		if r.Valid && r.Chaos != nil {
			valid = append(valid, r)
		}
	}
	fmt.Fprintf(&b, "%s: %d run(s), %d valid.\n", sc.Name, len(rs), len(valid))
	for _, r := range rs {
		if !r.Valid {
			fmt.Fprintf(&b, "  %s is INVALID and left out: %s\n", report.Clean(r.RunID), report.Clean(strings.Join(r.InvalidReasons, "; ")))
		}
	}
	if len(valid) == 0 {
		return b.String()
	}
	if len(valid) < report.MinRepeats {
		fmt.Fprintf(&b, "  Only %d valid run(s): fewer than %d, so the range below is mostly luck and supports no comparison.\n", len(valid), report.MinRepeats)
	}
	mixed := map[string]bool{}
	for _, r := range valid {
		mixed[r.Metadata.Resilience] = true
	}
	if len(mixed) > 1 {
		fmt.Fprintf(&b, "  WARNING: these runs mix resilience settings (%s): they are not one configuration and the range below means nothing.\n", strings.Join(sortedKeys(mixed), ", "))
	}
	fmt.Fprintf(&b, "  resilience: %s. Min / median / max across the valid runs:\n", valid[0].Metadata.Resilience)
	row := func(name string, primary bool, get func(report.ChaosMeasures) *float64, format string) {
		var xs []float64
		for _, r := range valid {
			if v := get(r.Chaos.Measures); v != nil {
				xs = append(xs, *v)
			}
		}
		tag := ""
		if primary {
			tag = " (primary)"
		}
		label := name + tag
		if len(xs) == 0 {
			fmt.Fprintf(&b, "    %-40s not reached in any run\n", label)
			return
		}
		sort.Float64s(xs)
		note := ""
		if len(xs) < len(valid) {
			note = fmt.Sprintf("  [reached in %d of %d runs]", len(xs), len(valid))
		}
		fmt.Fprintf(&b, "    %-40s "+format+" / "+format+" / "+format+"%s\n", label, xs[0], median(xs), xs[len(xs)-1], note)
	}
	row("fault-window success rate (%)", true, func(m report.ChaosMeasures) *float64 { return ptr(100 * m.FaultSuccessRate) }, "%.2f")
	row("fault-window attempts per request", true, func(m report.ChaosMeasures) *float64 { return m.FaultAttemptsPerRequest }, "%.3f")
	row("time to isolate (s)", false, func(m report.ChaosMeasures) *float64 { return m.TimeToIsolate }, "%.1f")
	row("time to recover (s)", false, func(m report.ChaosMeasures) *float64 { return m.TimeToRecover }, "%.1f")
	row("fault-window success latency p95 (ms)", false, func(m report.ChaosMeasures) *float64 { return m.FaultSuccessLatencyP95 }, "%.0f")
	row("fault-window success latency p99 (ms)", false, func(m report.ChaosMeasures) *float64 { return m.FaultSuccessLatencyP99 }, "%.0f")
	b.WriteString("  Only the two primary measures support a claim, and only when `benchmark compare` of two groups reports ranges that do not overlap.\n")
	return b.String()
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func sortedKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
