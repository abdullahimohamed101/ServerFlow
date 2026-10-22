package gateway

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/resilience"
)

// The Phase 15 series (ADR-017, ADR-020). They are registered only when the feature is effective (resilience.mode
// on in registry mode), so with the feature off or static workers /metrics is exactly what it was before. Labels
// are the existing allow-listed names, with the same caps as every other series: model through the model guard
// (at most metrics.max_models, the rest "other"), worker_id through the worker guard; the state gauge exports
// breakers that are not closed, so a healthy fleet has no per-worker series at all.
type resilienceMetrics struct {
	transitions *prometheus.CounterVec // model, state
	probes      *prometheus.CounterVec // model, result
	denied      *prometheus.CounterVec // model, reason
	suppressed  *prometheus.CounterVec // model
	deadline    *prometheus.CounterVec // model
	// Admission (PR E): refusals by reason, and the wait of the requests that waited.
	rejections *prometheus.CounterVec   // model, reason
	queueWait  *prometheus.HistogramVec // model
}

// queueWaitBuckets cover the queue's range: from the first 5 ms to the 60 s ceiling of resilience.queue.max_wait.
var queueWaitBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 3, 5, 10, 30, 60}

func (m *metrics) enableResilience(rs *resilienceState) {
	rm := &resilienceMetrics{
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "circuit_breaker_transitions_total",
			Help: "Circuit breaker state changes, by model and the state entered (open, half_open or closed). Present only with resilience.mode on.",
		}, []string{"model", "state"}),
		probes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "circuit_breaker_probes_total",
			Help: "Probe attempts (live requests a half-open breaker lets through), by model and result (ok, fault or neutral).",
		}, []string{"model", "result"}),
		denied: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "retries_denied_total",
			Help: "Retries that were wanted but not made, by model and reason (budget or deadline). The failed worker's response was relayed instead.",
		}, []string{"model", "reason"}),
		suppressed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "breaker_trips_suppressed_total",
			Help: "Circuit breaker trips held back by the ejection cap (resilience.breaker.max_open_fraction), by model.",
		}, []string{"model"}),
		deadline: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inference_deadline_exceeded_total",
			Help: "Requests whose first-byte deadline (resilience.retry.first_byte_deadline) elapsed before any output, by model. The client got 504 UPSTREAM_TIMEOUT.",
		}, []string{"model"}),
		rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "admission_rejections_total",
			Help: "Requests shed by admission control with 503 NO_CAPACITY, by model and reason (global_limit, queue_full, queue_timeout or draining). global_limit is refused before the body is read, so its model is unknown.",
		}, []string{"model", "reason"}),
		queueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "inference_queue_duration_seconds",
			Help:    "Time requests spent waiting in the admission queue for a worker slot, by model. Only requests that waited are observed, whatever the outcome; the gateway overhead histogram excludes this time.",
			Buckets: queueWaitBuckets,
		}, []string{"model"}),
	}
	m.res = rm
	m.reg.MustRegister(rm.transitions, rm.probes, rm.denied, rm.suppressed, rm.deadline, rm.rejections, rm.queueWait,
		newBreakerCollector(m, rs), newQueueCollector(m, rs))
}

// overloadShed counts a request shed by admission control.
func (m *metrics) overloadShed(e Rejection) {
	if m.res == nil {
		return
	}
	mi := m.model(e.Model)
	mi.rejections.pair(m.res.rejections, mi.label, e.Reason).Inc()
}

// queueWaited records the wait of a request that waited in the admission queue.
func (m *metrics) queueWaited(e QueueWait) {
	if m.res == nil {
		return
	}
	mi := m.model(e.Model)
	ref := mi.queueWait.Load()
	if ref == nil {
		ref = &observerRef{o: m.res.queueWait.WithLabelValues(mi.label)}
		mi.queueWait.Store(ref)
	}
	ref.o.Observe(e.Waited.Seconds())
}

// queueCollector exports admission_queue_depth{model}, read at scrape time: the requests waiting for a worker slot.
// Every model the registry serves reads its depth (0 when empty), so an absent series never means "empty".
type queueCollector struct {
	m     *metrics
	rs    *resilienceState
	depth *prometheus.Desc
}

func newQueueCollector(m *metrics, rs *resilienceState) *queueCollector {
	return &queueCollector{m: m, rs: rs, depth: prometheus.NewDesc("admission_queue_depth",
		"Requests waiting in the admission queue for a worker slot, by model (0 when none).", []string{"model"}, nil)}
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.depth }

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	depth := map[string]int{}
	if models := c.rs.models.Load(); models != nil {
		for _, mm := range *models {
			depth[c.m.models.Value(mm)] += 0
		}
	}
	for model, n := range c.rs.waitq.Depths() {
		depth[c.m.models.Value(model)] += n
	}
	names := make([]string, 0, len(depth))
	for model := range depth {
		names = append(names, model)
	}
	sort.Strings(names)
	for _, model := range names {
		ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(depth[model]), model)
	}
}

// breakerChange counts a breaker transition, or a trip the ejection cap held back.
func (m *metrics) breakerChange(e BreakerChange) {
	if m.res == nil {
		return
	}
	label := m.models.Value(e.Model)
	if e.Model == "" {
		label = "unknown"
	}
	if e.Reason == resilience.ReasonSuppressed {
		m.res.suppressed.WithLabelValues(label).Inc()
		return
	}
	m.res.transitions.WithLabelValues(label, e.To).Inc()
}

// resilienceAttemptEnded counts what an ended attempt says about the resilience features. It does nothing, and
// allocates nothing, for an attempt that was not a probe, was not denied a retry and did not hit the deadline.
func (m *metrics) resilienceAttemptEnded(mi *modelInst, e *AttemptEnd) {
	if m.res == nil {
		return
	}
	if e.Probe {
		result := e.Verdict
		if result != VerdictOK && result != VerdictFault {
			result = VerdictNeutral
		}
		mi.probes.pair(m.res.probes, mi.label, result).Inc()
	}
	if e.RetryDenied != "" {
		mi.denied.pair(m.res.denied, mi.label, e.RetryDenied).Inc()
	}
	if e.Class == classDeadline {
		mi.deadlineCounter(m).Inc()
	}
}

func (mi *modelInst) deadlineCounter(m *metrics) prometheus.Counter {
	if c := mi.deadlineHits.load(); c != nil {
		return c
	}
	c := m.res.deadline.WithLabelValues(mi.label)
	mi.deadlineHits.store(c)
	return c
}

// breakerCollector exports the breakers that are not closed, read at scrape time:
// circuit_breaker_state{worker_id,model} (1 half-open, 2 open) and circuit_breakers_open{model}, the number of
// breakers that are open or half-open per model (a model with none reads 0).
type breakerCollector struct {
	m     *metrics
	rs    *resilienceState
	state *prometheus.Desc
	open  *prometheus.Desc
}

func newBreakerCollector(m *metrics, rs *resilienceState) *breakerCollector {
	return &breakerCollector{
		m: m, rs: rs,
		state: prometheus.NewDesc("circuit_breaker_state",
			"State of a circuit breaker that is not closed: 1 half-open (probing), 2 open. A closed breaker has no series. worker_id is capped like the other worker labels (later workers are \"other\", with the highest state).", []string{"worker_id", "model"}, nil),
		open: prometheus.NewDesc("circuit_breakers_open",
			"Circuit breakers that are open or half-open, by model (0 when none).", []string{"model"}, nil),
	}
}

func (c *breakerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.state
	ch <- c.open
}

func (c *breakerCollector) Collect(ch chan<- prometheus.Metric) {
	infos := c.rs.breakers.Snapshot()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key.ID < infos[j].Key.ID })
	type pair struct{ worker, model string }
	states := map[pair]float64{}
	openBy := map[string]int{}
	for _, in := range infos {
		model := "unknown"
		if mm := c.rs.modelOf(in.Key.ID); mm != "" {
			model = c.m.models.Value(mm)
		}
		k := pair{c.m.workers.Value(in.Key.ID), model}
		states[k] = max(states[k], float64(in.State))
		openBy[model]++
	}
	keys := make([]pair, 0, len(states))
	for k := range states {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].model != keys[j].model {
			return keys[i].model < keys[j].model
		}
		return keys[i].worker < keys[j].worker
	})
	for _, k := range keys {
		ch <- prometheus.MustNewConstMetric(c.state, prometheus.GaugeValue, states[k], k.worker, k.model)
	}
	// Every model the registry serves reads 0 or its count, folded through the same model guard.
	served := map[string]struct{}{}
	if models := c.rs.models.Load(); models != nil {
		for _, mm := range *models {
			served[c.m.models.Value(mm)] = struct{}{}
		}
	}
	for model := range openBy {
		served[model] = struct{}{}
	}
	names := make([]string, 0, len(served))
	for model := range served {
		names = append(names, model)
	}
	sort.Strings(names)
	for _, model := range names {
		ch <- prometheus.MustNewConstMetric(c.open, prometheus.GaugeValue, float64(openBy[model]), model)
	}
}
