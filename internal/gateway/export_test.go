package gateway

import (
	"log/slog"

	"serverflow/internal/config"
)

// NewWithUpstreamForTest builds a static-mode Server around a caller-supplied Upstream, for tests in the
// external gateway_test package (which may import packages that import gateway).
func NewWithUpstreamForTest(cfg config.GatewayConfig, log *slog.Logger, up Upstream, opts ...Option) *Server {
	s := newWithUpstream(cfg, log, up)
	for _, o := range opts {
		o(s)
	}
	return s
}
