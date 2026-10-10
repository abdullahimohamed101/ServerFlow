package kafka

import (
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// health logs one line when the brokers become unreachable and one when a connection succeeds again, however
// many connection attempts fail in between. It logs the broker list, never credentials.
type health struct {
	cfg  Config
	log  *slog.Logger
	down atomic.Bool
}

func newHealth(cfg Config) *health {
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &health{cfg: cfg, log: log.With("component", "kafka")}
}

// OnBrokerConnect implements kgo.HookBrokerConnect.
func (h *health) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		if h.down.CompareAndSwap(false, true) {
			h.log.Warn("kafka is unreachable; events are buffered and then dropped, requests are not affected",
				"brokers", h.cfg.Brokers, "error", h.cfg.safe(err).Error())
		}
		return
	}
	if h.down.CompareAndSwap(true, false) {
		h.log.Info("kafka connection recovered", "brokers", h.cfg.Brokers)
	}
}
