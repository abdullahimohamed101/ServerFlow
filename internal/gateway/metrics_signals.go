package gateway

import (
	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/telemetry"
)

// The work-aware scheduling series (ADR-022, D24, the minimum the plan assigns to PR C1). They exist only when
// scheduler.strategy is least-tokens or least-work, so every other configuration scrapes exactly the series it
// did before (the golden series files pin that). No new label name: model (capped like the others) and reason.

// Fallback reasons of scheduler_signal_fallbacks_total.
const (
	// fallbackColdStart: the decision used a fleet prior for the chosen worker's capacity (it has no sample yet), or
	// a simpler rule because no worker had a rate or the ledger had no observation.
	fallbackColdStart = "cold_start"
	// fallbackUntracked: the ledger does not follow the chosen worker (the tracked-worker cap), so its work-ahead and
	// capacity terms were missing.
	fallbackUntracked = "untracked"
	// fallbackInvalid: a signal of the chosen worker was NaN, infinite, negative or missing and scored neutrally.
	fallbackInvalid = "invalid"
)

type signalMetrics struct {
	predictedWait *prometheus.HistogramVec // model
	fallbacks     *prometheus.CounterVec   // model, reason
}

// enableSignals registers the series. It must run before the server serves.
func (m *metrics) enableSignals() {
	m.sig = &signalMetrics{
		predictedWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "scheduler_predicted_wait_seconds",
			Help: "The wait a work-aware strategy predicted for the worker it chose at decision time, (work ahead + the request's estimated cost) / the worker's estimated token rate, by model. A model-based estimate, not a measurement; absent for decisions without a capacity estimate.",
			// Predicted waits run from milliseconds (an idle fast worker) to minutes (a long queue of long requests).
			Buckets: telemetry.DurationBuckets,
		}, []string{"model"}),
		fallbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_signal_fallbacks_total",
			Help: "Decisions in which a work-aware strategy lacked a signal for the chosen worker, by model and reason (cold_start: a fleet prior or simpler rule stood in; untracked: the ledger does not follow the worker; invalid: a signal was unusable and scored neutrally).",
		}, []string{"model", "reason"}),
	}
	m.reg.MustRegister(m.sig.predictedWait, m.sig.fallbacks)
}

// signalsObserved records what a work-aware strategy knew at the moment it chose a worker.
func (m *metrics) signalsObserved(mi *modelInst, s SchedulerSignals) {
	if s.Predicted && s.PredictedWait >= 0 {
		ref := mi.predictedWait.Load()
		if ref == nil {
			ref = &observerRef{o: m.sig.predictedWait.WithLabelValues(mi.label)}
			mi.predictedWait.Store(ref)
		}
		ref.o.Observe(s.PredictedWait)
	}
	if s.ColdStart {
		mi.fallbacks.pair(m.sig.fallbacks, mi.label, fallbackColdStart).Inc()
	}
	if s.Untracked {
		mi.fallbacks.pair(m.sig.fallbacks, mi.label, fallbackUntracked).Inc()
	}
	if s.Invalid {
		mi.fallbacks.pair(m.sig.fallbacks, mi.label, fallbackInvalid).Inc()
	}
}
