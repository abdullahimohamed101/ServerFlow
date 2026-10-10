package postgres

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PoolStats is a point-in-time view of the connection pool.
type PoolStats struct {
	Acquired, Idle, Total, Max int32
	AcquireCount               int64
	AcquireDuration            time.Duration // total time callers spent waiting for a connection
	EmptyAcquireCount          int64         // acquires that found the pool empty and had to wait
}

// PoolStats reports the pool's current usage.
func (s *Store) PoolStats() PoolStats {
	st := s.pool.Stat()
	return PoolStats{
		Acquired: st.AcquiredConns(), Idle: st.IdleConns(), Total: st.TotalConns(), Max: st.MaxConns(),
		AcquireCount: st.AcquireCount(), AcquireDuration: st.AcquireDuration(), EmptyAcquireCount: st.EmptyAcquireCount(),
	}
}

// Collector exports the pool's usage at scrape time (ADR-017).
func (s *Store) Collector() prometheus.Collector { return NewPoolCollector(s.PoolStats) }

// NewPoolCollector exports whatever stats returns; Store.Collector uses the real pool.
func NewPoolCollector(stats func() PoolStats) prometheus.Collector {
	return &poolCollector{stats: stats}
}

type poolCollector struct{ stats func() PoolStats }

var (
	connsDesc   = prometheus.NewDesc("postgres_pool_connections", "Connections in the PostgreSQL pool, by state (acquired, idle, total or max).", []string{"state"}, nil)
	acquiresDsc = prometheus.NewDesc("postgres_pool_acquires_total", "Connections acquired from the PostgreSQL pool.", nil, nil)
	waitDesc    = prometheus.NewDesc("postgres_pool_acquire_wait_seconds_total", "Total seconds callers spent waiting for a PostgreSQL connection.", nil, nil)
	emptyDesc   = prometheus.NewDesc("postgres_pool_empty_acquires_total", "Acquires that found the pool exhausted and had to wait: the pool is too small or the database too slow.", nil, nil)
)

func (*poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- connsDesc
	ch <- acquiresDsc
	ch <- waitDesc
	ch <- emptyDesc
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.stats()
	for state, v := range map[string]int32{"acquired": st.Acquired, "idle": st.Idle, "total": st.Total, "max": st.Max} {
		ch <- prometheus.MustNewConstMetric(connsDesc, prometheus.GaugeValue, float64(v), state)
	}
	ch <- prometheus.MustNewConstMetric(acquiresDsc, prometheus.CounterValue, float64(st.AcquireCount))
	ch <- prometheus.MustNewConstMetric(waitDesc, prometheus.CounterValue, st.AcquireDuration.Seconds())
	ch <- prometheus.MustNewConstMetric(emptyDesc, prometheus.CounterValue, float64(st.EmptyAcquireCount))
}
