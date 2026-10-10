package telemetry

import (
	"sort"

	dto "github.com/prometheus/client_model/go"
)

// AllowedLabelNames is the closed list of label names any ServerFlow series may carry (ADR-017). Adding a name
// is a deliberate act: it is reviewed here, and a test in every binary's package fails on any other label.
// Never labels: request ID, attempt ID, API key or key ID, path, raw error text, client IP, user-supplied
// strings, trace ID. "tenant" exists only on tenant_requests_total, behind metrics.tenant_labels.
var AllowedLabelNames = map[string]bool{
	"model": true, "status": true, "outcome": true, "reason": true, "limit": true, "strategy": true,
	"result": true, "kind": true, "state": true, "cache": true, "worker_id": true, "direction": true, "tenant": true,
	// Prometheus' own and the Go and process collectors'.
	"le": true, "quantile": true, "version": true, "commit": true, "go_version": true,
}

// LabelNamesOutsideAllowlist returns "family{label}" for every label of every family that is not allowed,
// sorted. Empty means the registry obeys the cardinality rules for label names.
func LabelNamesOutsideAllowlist(fams []*dto.MetricFamily) []string {
	bad := map[string]bool{}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if !AllowedLabelNames[l.GetName()] {
					bad[f.GetName()+"{"+l.GetName()+"}"] = true
				}
			}
		}
	}
	out := make([]string, 0, len(bad))
	for b := range bad {
		out = append(out, b)
	}
	sort.Strings(out)
	return out
}

// SeriesCount is the number of time series (children) across all families.
func SeriesCount(fams []*dto.MetricFamily) int {
	n := 0
	for _, f := range fams {
		n += len(f.GetMetric())
	}
	return n
}

// SampleCount is the number of samples one scrape carries, which is what Prometheus' sample_limit counts: a
// histogram series expands into one sample per bucket plus its sum and count, a summary into its quantiles plus
// sum and count.
func SampleCount(fams []*dto.MetricFamily) int {
	n := 0
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			switch {
			case m.GetHistogram() != nil:
				n += len(m.GetHistogram().GetBucket()) + 3 // buckets, +Inf, sum, count
			case m.GetSummary() != nil:
				n += len(m.GetSummary().GetQuantile()) + 2
			default:
				n++
			}
		}
	}
	return n
}
