package gateway

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"serverflow/internal/api"
	"serverflow/internal/config"
	"serverflow/internal/ratelimit"
	"serverflow/internal/telemetry"
)

// metrics holds the gateway's Prometheus instruments on a private registry so each Server (and each test) is
// isolated. It is the first Observer (ADR-016): the lifecycle events drive the instruments. State that is not a
// request event (the registry snapshot, Redis, PostgreSQL, the key cache, the limiter's leases) is read at
// scrape time by collectors registered through WithCollector, never from the request path.
//
// Label values are bounded (ADR-017). model is only ever a model the registry or the configured list confirmed,
// else "unknown", and at most MaxModels distinct values are kept (the rest are "other"); worker_id is capped the
// same way; tenant exists only when metrics.tenant_labels is on. The series, labels and buckets are pinned by
// TestMetricsSeriesGolden* and the label allowlist test.
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

	// Phase 10 additions.
	failures        *prometheus.CounterVec   // model, reason: requests that ended with a gateway error code
	overhead        *prometheus.HistogramVec // model: request start to the first dispatch
	decisions       *prometheus.CounterVec   // strategy, model, result
	decisionDur     *prometheus.HistogramVec // strategy
	noCapacity      *prometheus.CounterVec   // model, reason
	selections      *prometheus.CounterVec   // model, worker_id
	selectionSeries atomic.Int64             // children of selections created, against maxSelectionSeries
	ineligible      prometheus.Counter
	tenantReqs      *prometheus.CounterVec // nil unless metrics.tenant_labels

	models  *telemetry.LabelGuard
	workers *telemetry.LabelGuard
	tenants *telemetry.LabelGuard

	// strategy is the scheduler strategy in registry mode, "" otherwise. It is set before the server serves.
	strategy string
	// decisionDurChild is the strategy's histogram, resolved once.
	decisionDurChild prometheus.Observer

	authRej [4]prometheus.Counter // 401, 403, 500, 503

	mu       sync.RWMutex
	perModel map[string]*modelInst
}

// Scheduler decision results.
const (
	resultSelected   = "selected"
	resultNoCapacity = "no_capacity"
	resultNoModel    = "no_model"
	resultError      = "error"
)

// Attempt outcome slots, in the order of modelInst.attempts.
var attemptOutcomes = [4]string{outcomeOK, outcomeFailed, outcomeRetried, outcomeClientClosed}

var authRejectStatuses = [4]int{401, 403, 500, 503}

// Rate limit names that limiter decisions can carry; the series are created at start so rate() works from the
// first scrape (ADR-017 D6).
var rateLimitNames = []string{
	string(ratelimit.LimitRequests), string(ratelimit.LimitTokens), string(ratelimit.LimitConcurrency),
	string(ratelimit.LimitModel), string(ratelimit.LimitUnavailable),
}

// Tenant request outcomes (only with metrics.tenant_labels).
const (
	tenantOK           = "ok"
	tenantRateLimited  = "rate_limited"
	tenantClientClosed = "client_closed"
	tenantClientError  = "client_error"
	tenantServerError  = "server_error"
)

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
			Buckets: telemetry.DurationBuckets,
		}, []string{"model"}),
		ttft: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "inference_ttft_seconds",
			Help:    "Time from request accepted to the first streamed chunk of a streaming response.",
			Buckets: telemetry.TTFTBuckets,
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
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inference_failures_total",
			Help: "Inference requests that ended with a gateway error, by model and API error code.",
		}, []string{"model", "reason"}),
		overhead: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "inference_gateway_overhead_seconds",
			Help:    "Time from request accepted to the first dispatch to a worker (authentication, rate limit, parsing and worker selection), by model.",
			Buckets: telemetry.InternalLatencyBuckets,
		}, []string{"model"}),
		decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_decisions_total",
			Help: "Scheduling decisions in registry mode, by strategy, model and result (selected, no_capacity, no_model or error).",
		}, []string{"strategy", "model", "result"}),
		decisionDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_decision_duration_seconds",
			Help:    "Time the scheduler took to choose a worker, by strategy.",
			Buckets: telemetry.InternalLatencyBuckets,
		}, []string{"strategy"}),
		noCapacity: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_no_capacity_total",
			Help: "Requests refused because no worker could be chosen, by model and reason (no_capacity or worker_unavailable).",
		}, []string{"model", "reason"}),
		selections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_selections_total",
			Help: "Workers chosen by the scheduler, by model and worker (worker_id is capped; later workers are \"other\").",
		}, []string{"model", "worker_id"}),
		ineligible: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_ineligible_selections_total",
			Help: "Selections of a worker the registry view did not judge eligible (not READY, or past the heartbeat bound). Must stay 0: the scheduler never routes to a known-unhealthy worker.",
		}),
		perModel: map[string]*modelInst{},
	}
	m.configure(config.Default().Metrics)
	for i, st := range authRejectStatuses {
		m.authRej[i] = m.authRejects.WithLabelValues(strconv.Itoa(st))
	}
	for _, l := range rateLimitNames {
		m.rateRejects.WithLabelValues(l)
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		telemetry.BuildInfoCollector(),
		m.requests, m.active, m.duration, m.ttft, m.attempts, m.retries, m.authRejects,
		m.rateRejects, m.rateBypassed, m.rateDecision,
		m.failures, m.overhead, m.decisions, m.decisionDur, m.noCapacity, m.selections, m.ineligible,
	)
	return m
}

// configure applies the metrics section: the label caps and the optional tenant series. It must run before the
// server serves.
func (m *metrics) configure(c config.MetricsConfig) {
	m.models = telemetry.NewLabelGuard(c.MaxModels, "other")
	m.workers = telemetry.NewLabelGuard(c.MaxWorkersLabel, "other")
	m.tenants = telemetry.NewLabelGuard(c.MaxTenants, "other")
	if c.TenantLabels && m.tenantReqs == nil {
		m.tenantReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tenant_requests_total",
			Help: "Inference requests by tenant and outcome, for the first metrics.max_tenants tenants (later tenants are \"other\"). Present only with metrics.tenant_labels.",
		}, []string{"tenant", "outcome"})
		m.reg.MustRegister(m.tenantReqs)
	}
}

// setStrategy marks the gateway as scheduling (registry mode) and resolves the strategy's histogram.
func (m *metrics) setStrategy(strategy string) {
	m.strategy = strategy
	m.decisionDurChild = m.decisionDur.WithLabelValues(strategy)
}

func (m *metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// modelInst holds the instruments of one model label, resolved once so the request path does a map lookup and
// no label formatting or allocation. Counters that must not exist before their first event (an outcome that has
// never happened must not appear as a zero series) are created lazily.
type modelInst struct {
	label    string
	duration prometheus.Observer
	ttft     prometheus.Observer
	overhead prometheus.Observer

	attempts [len(attemptOutcomes)]counterSlot
	requests [500]counterSlot // HTTP status 100-599
	// decisions is indexed by decisionIndex.
	decisions [4]counterSlot

	retries    counterCache
	failures   counterCache
	noCapacity counterCache
	selections counterCache
}

func newModelInst(m *metrics, label string) *modelInst {
	return &modelInst{
		label:    label,
		duration: m.duration.WithLabelValues(label),
		ttft:     m.ttft.WithLabelValues(label),
		overhead: m.overhead.WithLabelValues(label),
	}
}

// counterSlot is a lazily resolved counter: the first caller creates it, later callers load it. The create path
// (vec.WithLabelValues) is idempotent, so two racing first callers get the same child.
type counterSlot struct{ p atomic.Pointer[counterRef] }

type counterRef struct{ c prometheus.Counter }

func (s *counterSlot) load() prometheus.Counter {
	if r := s.p.Load(); r != nil {
		return r.c
	}
	return nil
}

func (s *counterSlot) store(c prometheus.Counter) { s.p.Store(&counterRef{c}) }

// counterCache maps one label value to its counter.
type counterCache struct {
	mu sync.RWMutex
	m  map[string]prometheus.Counter
}

func (c *counterCache) load(key string) prometheus.Counter {
	c.mu.RLock()
	x := c.m[key]
	c.mu.RUnlock()
	return x
}

func (c *counterCache) store(key string, x prometheus.Counter) {
	c.mu.Lock()
	if c.m == nil {
		c.m = map[string]prometheus.Counter{}
	}
	c.m[key] = x
	c.mu.Unlock()
}

// pair resolves the (model, value) child of vec through the cache.
func (c *counterCache) pair(vec *prometheus.CounterVec, model, value string) prometheus.Counter {
	if x := c.load(value); x != nil {
		return x
	}
	x := vec.WithLabelValues(model, value)
	c.store(value, x)
	return x
}

// maxSelectionSeries bounds scheduler_selections_total across all models: each (model, worker) pair is a series,
// and capping models and workers separately would still allow models x workers of them (66 x 257 = 16,962 at the
// default caps, close to Prometheus' sample_limit). Pairs beyond this budget count under worker_id="other".
const maxSelectionSeries = 1024

// budgeted is pair for a series family with a global budget: while the budget lasts each new value gets its own
// child; after that, new values share the model's "other" child.
func (c *counterCache) budgeted(vec *prometheus.CounterVec, model, value string, used *atomic.Int64) prometheus.Counter {
	if x := c.load(value); x != nil {
		return x
	}
	if used.Add(1) > maxSelectionSeries {
		used.Add(-1)
		return c.pair(vec, model, "other")
	}
	return c.pair(vec, model, value)
}

// model returns the instruments for a model. An empty (unconfirmed) model is "unknown"; a model beyond the cap
// is "other".
func (m *metrics) model(raw string) *modelInst {
	label := "unknown"
	if raw != "" {
		label = m.models.Value(raw)
	}
	m.mu.RLock()
	mi := m.perModel[label]
	m.mu.RUnlock()
	if mi != nil {
		return mi
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if mi = m.perModel[label]; mi == nil {
		mi = newModelInst(m, label)
		m.perModel[label] = mi
	}
	return mi
}

func decisionIndex(result string) int {
	switch result {
	case resultSelected:
		return 0
	case resultNoCapacity:
		return 1
	case resultNoModel:
		return 2
	}
	return 3
}

func (m *metrics) decision(mi *modelInst, result string) {
	slot := &mi.decisions[decisionIndex(result)]
	c := slot.load()
	if c == nil {
		c = m.decisions.WithLabelValues(m.strategy, mi.label, result)
		slot.store(c)
	}
	c.Inc()
}

// metrics is the first Observer (ADR-016).
var _ Observer = (*metrics)(nil)

// RequestStarted counts the request as in flight.
func (m *metrics) RequestStarted(ctx context.Context, _ RequestStart) context.Context {
	m.active.Inc()
	return ctx
}

// RequestAdmitted records how long the limiter took and whether it let the request through unchecked.
func (m *metrics) RequestAdmitted(_ context.Context, e Admission) {
	if e.RateLimitChecked {
		m.rateDecision.Observe(e.RateLimitDuration.Seconds())
	}
	if e.RateLimitBypassed {
		m.rateBypassed.Inc()
	}
}

// RequestRejected counts authentication and rate limit refusals, and in registry mode the scheduling refusals.
func (m *metrics) RequestRejected(_ context.Context, e Rejection) {
	switch e.Kind {
	case RejectAuth:
		for i, st := range authRejectStatuses {
			if e.Status == st {
				m.authRej[i].Inc()
				return
			}
		}
		m.authRejects.WithLabelValues(strconv.Itoa(e.Status)).Inc()
	case RejectRateLimit:
		m.rateDecision.Observe(e.DecisionDuration.Seconds())
		m.rateRejects.WithLabelValues(e.Reason).Inc()
	case RejectCapacity, RejectModel, RejectInternal:
		if m.strategy == "" {
			return // static mode has no scheduler
		}
		m.schedulingRefused(e)
	}
}

// schedulingRefused records a registry-mode refusal in which no worker was chosen.
func (m *metrics) schedulingRefused(e Rejection) {
	mi := m.model(e.Model)
	switch {
	case e.Kind == RejectCapacity && e.Reason == "no_capacity":
		m.decision(mi, resultNoCapacity)
		mi.noCapacity.pair(m.noCapacity, mi.label, "no_capacity").Inc()
	case e.Kind == RejectCapacity:
		// The registry view was missing or too old: the scheduler could not decide at all.
		m.decision(mi, resultError)
		mi.noCapacity.pair(m.noCapacity, mi.label, "worker_unavailable").Inc()
	case e.Kind == RejectModel && e.Reason == "model_not_found":
		m.decision(m.model(""), resultNoModel)
	case e.Kind == RejectInternal:
		m.decision(mi, resultError)
	default:
		return // forbidden model: a policy refusal, not a scheduling decision
	}
	if e.DecisionDuration > 0 && m.decisionDurChild != nil {
		m.decisionDurChild.Observe(e.DecisionDuration.Seconds())
	}
}

// AttemptStarted records the gateway overhead of the first dispatch and, in registry mode, the scheduling
// decision that chose the worker.
func (m *metrics) AttemptStarted(ctx context.Context, e AttemptStart) context.Context {
	mi := m.model(e.Model)
	if e.Number <= 1 {
		mi.overhead.Observe(e.SinceRequestStart.Seconds())
	}
	if e.WorkerID == "" {
		return ctx // static mode: no scheduler
	}
	m.decision(mi, resultSelected)
	if m.decisionDurChild != nil {
		m.decisionDurChild.Observe(e.SelectDuration.Seconds())
	}
	mi.selections.budgeted(m.selections, mi.label, m.workers.Value(e.WorkerID), &m.selectionSeries).Inc()
	if !e.WorkerEligible || e.WorkerState != "READY" {
		m.ineligible.Inc()
	}
	return ctx
}

// FirstToken has no series of its own: the TTFT histogram is fed from the completion.
func (m *metrics) FirstToken(context.Context, FirstToken) {}

// AttemptEnded counts a finished attempt on a registry worker, and an attempt abandoned for a retry. A
// static-mode attempt (empty worker ID) is not counted: inference_attempts_total is a registry-mode series.
func (m *metrics) AttemptEnded(_ context.Context, e AttemptEnd) {
	if e.WorkerID == "" {
		return
	}
	mi := m.model(e.Model)
	slot := -1
	for i, o := range attemptOutcomes {
		if e.Outcome == o {
			slot = i
			break
		}
	}
	if slot >= 0 {
		c := mi.attempts[slot].load()
		if c == nil {
			c = m.attempts.WithLabelValues(mi.label, e.Outcome)
			mi.attempts[slot].store(c)
		}
		c.Inc()
	} else {
		m.attempts.WithLabelValues(mi.label, e.Outcome).Inc()
	}
	if e.WillRetry {
		mi.retries.pair(m.retries, mi.label, e.Class).Inc()
	}
}

// RequestCompleted records a finished inference request that reached the handler.
func (m *metrics) RequestCompleted(_ context.Context, e Completion) {
	m.active.Dec()
	if !e.Handled {
		return
	}
	mi := m.model(e.Model)
	if e.Status >= 100 && e.Status <= 599 {
		slot := &mi.requests[e.Status-100]
		c := slot.load()
		if c == nil {
			c = m.requests.WithLabelValues(mi.label, strconv.Itoa(e.Status))
			slot.store(c)
		}
		c.Inc()
	} else {
		m.requests.WithLabelValues(mi.label, strconv.Itoa(e.Status)).Inc()
	}
	mi.duration.Observe(e.Duration.Seconds())
	if e.TTFT > 0 {
		mi.ttft.Observe(e.TTFT.Seconds())
	}
	if e.ErrorCode != "" {
		mi.failures.pair(m.failures, mi.label, failureReason(e.ErrorCode)).Inc()
	}
	if m.tenantReqs != nil && e.TenantID != "" {
		m.tenantReqs.WithLabelValues(m.tenants.Value(e.TenantID), tenantOutcome(e.Status)).Inc()
	}
}

// failureReason keeps the reason label to the gateway's fixed set of error codes.
func failureReason(code string) string {
	if _, ok := knownErrorCodes[code]; ok {
		return code
	}
	return "OTHER"
}

var knownErrorCodes = map[string]struct{}{
	api.CodeInvalidRequest: {}, api.CodeModelNotFound: {}, api.CodeNoCapacity: {}, api.CodeUpstreamTimeout: {},
	api.CodeWorkerUnavailable: {}, api.CodeInferenceFailed: {}, api.CodeInternalError: {}, api.CodeUnauthorized: {},
	api.CodeForbidden: {}, api.CodeAuthUnavailable: {}, api.CodeRateLimited: {}, api.CodeRateLimitUnavail: {},
}

func tenantOutcome(status int) string {
	switch {
	case status == 499:
		return tenantClientClosed
	case status == http.StatusTooManyRequests:
		return tenantRateLimited
	case status >= 500:
		return tenantServerError
	case status >= 400:
		return tenantClientError
	}
	return tenantOK
}

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
