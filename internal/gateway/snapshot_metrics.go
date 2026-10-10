package gateway

import "github.com/prometheus/client_golang/prometheus"

// snapshotCollector exports how fresh the gateway's view of the worker registry is, read at scrape time. Stale
// routing data is the failure spec section 63 rule 10 warns about, so the age is always exported: before the
// first successful refresh it counts from the cache's creation, so an alert on it fires for a gateway that has
// never reached the control plane.
type snapshotCollector struct {
	cache    *snapshotCache
	age      *prometheus.Desc
	failures *prometheus.Desc
}

func newSnapshotCollector(c *snapshotCache) *snapshotCollector {
	return &snapshotCollector{
		cache: c,
		age: prometheus.NewDesc("gateway_registry_snapshot_age_seconds",
			"Seconds since the gateway last fetched the worker registry (since start before the first fetch). Above gateway.registry_max_staleness the gateway fails closed.", nil, nil),
		failures: prometheus.NewDesc("gateway_registry_refresh_failures_total",
			"Failed attempts to refresh the worker registry snapshot.", nil, nil),
	}
}

func (c *snapshotCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.age
	ch <- c.failures
}

func (c *snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	since := c.cache.created
	if s := c.cache.cur.Load(); s != nil {
		since = s.fetched
	}
	age := c.cache.now().Sub(since).Seconds()
	if age < 0 {
		age = 0
	}
	ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, age)
	ch <- prometheus.MustNewConstMetric(c.failures, prometheus.CounterValue, float64(c.cache.refreshFailures.Load()))
}
