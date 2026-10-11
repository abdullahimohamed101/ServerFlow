package chaos

import (
	"github.com/prometheus/client_golang/prometheus"
)

// readCounters sums the gateway series the harness uses. Series that do not exist (a build without the
// breaker has no circuit_breaker_probes_total) contribute 0, so the same harness runs on master and
// on a build with the feature.
func readCounters(g prometheus.Gatherer, faulty string) (Counters, error) {
	fams, err := g.Gather()
	if err != nil {
		return Counters{}, err
	}
	var c Counters
	for _, f := range fams {
		var dst *float64
		switch f.GetName() {
		case "inference_attempts_total":
			dst = &c.Attempts
		case "inference_requests_total":
			dst = &c.Requests
		case "inference_retries_total":
			dst = &c.Retries
		case "scheduler_no_capacity_total":
			dst = &c.NoCapacity
		case "scheduler_ineligible_selections_total":
			dst = &c.Ineligible
		case "circuit_breaker_probes_total":
			dst = &c.Probes
		case "scheduler_selections_total":
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "worker_id" && l.GetValue() == faulty && faulty != "" {
						c.FaultySelections += m.GetCounter().GetValue()
					}
				}
			}
			continue
		default:
			continue
		}
		for _, m := range f.GetMetric() {
			*dst += m.GetCounter().GetValue()
		}
	}
	return c, nil
}
