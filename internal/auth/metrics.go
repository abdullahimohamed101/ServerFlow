package auth

import "github.com/prometheus/client_golang/prometheus"

// CacheStats reports how many key lookups the cache answered (hits, including remembered unknown prefixes) and
// how many went on to the store or a stale copy (misses).
func (a *Authenticator) CacheStats() (hits, misses int64) {
	return a.cacheHits.Load(), a.cacheMisses.Load()
}

// Collector exports the key cache's hit and miss counts at scrape time (ADR-017). The only cache the gateway has
// is this one, so cache="auth_key".
func (a *Authenticator) Collector() prometheus.Collector { return &cacheCollector{a: a} }

type cacheCollector struct{ a *Authenticator }

var (
	hitsDesc   = prometheus.NewDesc("cache_hits_total", "Cache lookups answered from the cache, by cache.", []string{"cache"}, nil)
	missesDesc = prometheus.NewDesc("cache_misses_total", "Cache lookups that needed the backing store, by cache.", []string{"cache"}, nil)
)

func (*cacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- hitsDesc
	ch <- missesDesc
}

func (c *cacheCollector) Collect(ch chan<- prometheus.Metric) {
	h, m := c.a.CacheStats()
	ch <- prometheus.MustNewConstMetric(hitsDesc, prometheus.CounterValue, float64(h), "auth_key")
	ch <- prometheus.MustNewConstMetric(missesDesc, prometheus.CounterValue, float64(m), "auth_key")
}
