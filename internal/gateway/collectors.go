package gateway

import "github.com/prometheus/client_golang/prometheus"

// WithExtraCollectors registers additional Prometheus collectors on the gateway's own registry, so a component
// that is wired in from outside (the event publisher, Phase 12) shows up on /metrics next to the gateway's series.
// Like every Option it must be applied before the server serves; registering the same collector twice panics.
func WithExtraCollectors(cs ...prometheus.Collector) Option {
	return func(s *Server) { s.metrics.reg.MustRegister(cs...) }
}
