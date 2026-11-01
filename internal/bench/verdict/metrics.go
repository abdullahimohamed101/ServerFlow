package verdict

import (
	"sort"

	"serverflow/internal/bench/report"
)

// metrics are the result fields a hypothesis can be about, with the units of result.json (milliseconds,
// requests per second, a fraction for the error rate).
var metrics = map[string]func(report.Result) (float64, bool){
	"latency_p50": func(r report.Result) (float64, bool) {
		if d := r.Summary.Latency; d != nil {
			return d.P50, true
		}
		return 0, false
	},
	"latency_p95": func(r report.Result) (float64, bool) {
		if d := r.Summary.Latency; d != nil {
			return d.P95, true
		}
		return 0, false
	},
	"latency_p99": func(r report.Result) (float64, bool) {
		if d := r.Summary.Latency; d != nil {
			return d.P99, true
		}
		return 0, false
	},
	"ttft_p95": func(r report.Result) (float64, bool) {
		if d := r.Summary.TTFT; d != nil {
			return d.P95, true
		}
		return 0, false
	},
	"throughput": func(r report.Result) (float64, bool) { return r.Summary.RequestsPerSecond, true },
	"error_rate": func(r report.Result) (float64, bool) { return r.Summary.ErrorRate, true },
}

// MetricNames lists the metrics a hypothesis may name.
func MetricNames() []string {
	out := make([]string, 0, len(metrics))
	for k := range metrics {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
