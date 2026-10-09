package gateway

import (
	"net/http"
	"strconv"
	"time"

	"serverflow/internal/ratelimit"

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
	// authRejects counts requests refused by authentication, by status only: tenants and
	// reasons are unbounded or sensitive and stay out of the labels.
	authRejects *prometheus.CounterVec
	// rateRejects counts requests refused by rate limiting, by which limit (requests, tokens, concurrency,
	// model, unavailable). Tenants stay out of the labels (unbounded).
	rateRejects  *prometheus.CounterVec
	rateBypassed prometheus.Counter
	rateDecision prometheus.Histogram
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
		authRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "auth_rejections_total",
			Help: "Requests refused by API key authentication, by HTTP status (401, 403, 500 or 503).",
		}, []string{"status"}),
		rateRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rate_limit_rejections_total",
			Help: "Requests refused by rate limiting, by limit: requests, tokens, concurrency, model or unavailable (the check could not be made).",
		}, []string{"limit"}),
		rateBypassed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rate_limit_bypassed_total",
			Help: "Requests admitted without a rate limit check because Redis was unavailable and redis.on_failure is open.",
		}),
		rateDecision: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "rate_limit_decision_seconds",
			Help:    "Time the rate limiter took to decide a request.",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25},
		}),
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.requests, m.active, m.duration, m.ttft, m.attempts, m.retries, m.authRejects,
		m.rateRejects, m.rateBypassed, m.rateDecision,
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

// observeAuthReject counts a request refused by authentication.
func (m *metrics) observeAuthReject(status int) {
	m.authRejects.WithLabelValues(strconv.Itoa(status)).Inc()
}

// observeRateReject counts a request refused by rate limiting. limit is one of the fixed limit names.
func (m *metrics) observeRateReject(limit ratelimit.Limit) {
	m.rateRejects.WithLabelValues(string(limit)).Inc()
}

// observeRateDecision records how long the limiter took.
func (m *metrics) observeRateDecision(d time.Duration) { m.rateDecision.Observe(d.Seconds()) }

// registerLimiterStats exports the limiter's lease bookkeeping. Registering twice (a second limiter on the same
// server) replaces nothing: the first registration stays, which is fine because a server has one limiter.
func (m *metrics) registerLimiterStats(st limiterStats) {
	_ = m.reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "rate_limit_local_leases",
		Help: "Concurrency leases this gateway currently tracks (requests in flight that hold a slot).",
	}, func() float64 { return float64(st.LocalLeases()) }))
	_ = m.reg.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "rate_limit_dropped_releases_total",
		Help: "Concurrency slot releases that were not sent to Redis (queue full or Redis unavailable); the slot is held until lease_ttl.",
	}, func() float64 { return float64(st.DroppedReleases()) }))
}
