package gateway

import "github.com/prometheus/client_golang/prometheus"

// TracingStats are the counters of the span export pipeline, read on every scrape.
type TracingStats struct {
	// Exported is spans the collector accepted. Dropped is spans lost before export (the queue was full or the
	// process was shutting down). FailedSpans is spans in batches the collector did not accept, and Failures the
	// number of such batches.
	Exported, Dropped, FailedSpans, Failures int64
}

// WithTracingStats exports tracing_spans_exported_total, tracing_spans_dropped_total and
// tracing_export_failures_total, read from stats at scrape time. The series exist only when tracing is on, so
// a gateway without tracing has no tracing series. The gateway does not import OpenTelemetry; cmd/gateway
// passes a function over the tracing provider.
func WithTracingStats(stats func() TracingStats) Option {
	return func(s *Server) {
		if stats == nil {
			return
		}
		reg := s.metrics.reg
		counter := func(name, help string, f func(TracingStats) int64) {
			_ = reg.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help},
				func() float64 { return float64(f(stats())) }))
		}
		counter("tracing_spans_exported_total", "Spans the trace collector accepted.", func(t TracingStats) int64 { return t.Exported })
		counter("tracing_spans_dropped_total", "Spans dropped before export because the export queue was full or the gateway was shutting down.",
			func(t TracingStats) int64 { return t.Dropped })
		counter("tracing_export_failures_total", "Span batches the trace collector did not accept (or could not be reached for).",
			func(t TracingStats) int64 { return t.Failures })
	}
}
