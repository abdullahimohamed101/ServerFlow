package gateway

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metrics holds the gateway's Prometheus instruments on a private
// registry so each Server (and each test) is isolated. Label values are
// bounded: model is only set to a validated configured model, otherwise
// "unknown".
type metrics struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	active   prometheus.Gauge
	duration *prometheus.HistogramVec
	ttft     *prometheus.HistogramVec
	attempts *prometheus.CounterVec
	retries  *prometheus.CounterVec
}

func newMetrics() *metrics {
	m := &metrics{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inference_requests_total",
			Help: "Inference requests handled by the gateway, by model and HTTP status.",
		}, []string{"model", "status"}),
		active: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "inference_requests_active",
			Help: "Inference requests currently in flight.",
		}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "inference_request_duration_seconds",
			Help:    "Total inference request duration as seen by the gateway.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"model"}),
		ttft: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "inference_ttft_seconds",
			Help:    "Time from request accepted to the first streamed chunk of a streaming response.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"model"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inference_attempts_total",
			Help: "Attempts to serve a request on a worker (registry mode), by model and outcome.",
		}, []string{"model", "outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inference_retries_total",
			Help: "Attempts that were abandoned for a retry on another worker, by model and failure class.",
		}, []string{"model", "reason"}),
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.requests, m.active, m.duration, m.ttft, m.attempts, m.retries,
	)
	return m
}

func (m *metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// observe records a finished inference request.
func (m *metrics) observe(info *reqInfo, status int, d time.Duration) {
	model := info.model
	if model == "" {
		model = "unknown"
	}
	m.requests.WithLabelValues(model, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(model).Observe(d.Seconds())
	if info.ttft > 0 {
		m.ttft.WithLabelValues(model).Observe(info.ttft.Seconds())
	}
}

// observeAttempt counts a finished attempt. outcome is one of the fixed attempt outcomes.
func (m *metrics) observeAttempt(model, outcome string) {
	if model == "" {
		model = "unknown"
	}
	m.attempts.WithLabelValues(model, outcome).Inc()
}

// observeRetry counts an attempt abandoned for a retry. reason is an attempt error class.
func (m *metrics) observeRetry(model, reason string) {
	if model == "" {
		model = "unknown"
	}
	m.retries.WithLabelValues(model, reason).Inc()
}
