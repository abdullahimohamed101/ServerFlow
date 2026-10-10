package mockworker

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"serverflow/internal/telemetry"
)

// workerMetrics is the mock worker's own Prometheus surface (ADR-017 D11): counters read from the engine at
// scrape time, plus duration, time-to-first-token and queue histograms observed as requests finish. It is
// served on the worker's existing listener because the mock worker is a test double that is never public.
// Tokens per second on the dashboards comes from these counters, not from parsing response bodies.
type workerMetrics struct {
	reg         *prometheus.Registry
	duration    prometheus.Histogram
	ttft        prometheus.Histogram
	queue       prometheus.Histogram
	inputTokens atomic.Int64
}

func newWorkerMetrics(s *Server) *workerMetrics {
	m := &workerMetrics{
		reg: prometheus.NewRegistry(),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "worker_request_duration_seconds", Help: "Time the worker spent on a chat request, from arrival to the end of the response.",
			Buckets: telemetry.DurationBuckets,
		}),
		ttft: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "worker_ttft_seconds", Help: "Time from the start of generation to the first streamed token.",
			Buckets: telemetry.TTFTBuckets,
		}),
		queue: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "worker_queue_duration_seconds", Help: "Time a request waited for a generation slot.",
			Buckets: telemetry.DurationBuckets,
		}),
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		telemetry.BuildInfoCollector(),
		m.duration, m.ttft, m.queue,
		&engineCollector{s: s, m: m},
	)
	return m
}

func (m *workerMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// observe records one finished chat request.
func (m *workerMetrics) observe(rl *chatLog, total time.Duration) {
	m.duration.Observe(total.Seconds())
	if rl.queued {
		m.queue.Observe(rl.queueDur.Seconds())
	}
	if rl.stream && rl.ttftDur > 0 {
		m.ttft.Observe(rl.ttftDur.Seconds())
	}
}

type engineCollector struct {
	s *Server
	m *workerMetrics
}

var (
	requestsDesc = prometheus.NewDesc("worker_requests_total", "Chat requests by result: ok, failed, rejected (queue full) or cancelled (client left).", []string{"result"}, nil)
	inputDesc    = prometheus.NewDesc("worker_input_tokens_total", "Estimated prompt tokens of requests that got a generation slot.", nil, nil)
	outputDesc   = prometheus.NewDesc("worker_output_tokens_total", "Output tokens generated.", nil, nil)
)

func (*engineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- requestsDesc
	ch <- inputDesc
	ch <- outputDesc
}

func (c *engineCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.s.engine.Stats()
	for result, v := range map[string]int64{"ok": st.Completed, "failed": st.Failed, "rejected": st.Rejected, "cancelled": st.Cancelled} {
		ch <- prometheus.MustNewConstMetric(requestsDesc, prometheus.CounterValue, float64(v), result)
	}
	ch <- prometheus.MustNewConstMetric(inputDesc, prometheus.CounterValue, float64(c.m.inputTokens.Load()))
	ch <- prometheus.MustNewConstMetric(outputDesc, prometheus.CounterValue, float64(st.Tokens))
}
