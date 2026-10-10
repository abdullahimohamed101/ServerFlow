package telemetry

// Histogram bucket edges shared by the gateway and the workers, so a request's latency at the gateway and on
// the worker aggregate on the same boundaries (ADR-017). Classic histograms only; changing an edge breaks
// aggregation across versions, so revisit them with real vLLM latencies in Phase 13, not casually.
var (
	// DurationBuckets are for whole-request durations in seconds.
	DurationBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}
	// TTFTBuckets are for time to first token in seconds.
	TTFTBuckets = []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	// InternalLatencyBuckets are for the gateway's own work (scheduler decision, overhead), from 50 microseconds
	// to one second. 10 ms and 25 ms are edges so the p95 SLO thresholds are not interpolated across a wide bucket.
	InternalLatencyBuckets = []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1}
)
