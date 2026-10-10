package registry

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/telemetry"
	"serverflow/pkg/protocol"
)

// Heartbeat results, in the order of Registry.heartbeats.
const (
	heartbeatOK = iota
	heartbeatInvalid
	heartbeatUnknownWorker
	heartbeatStaleRegistration
	heartbeatIllegalTransition
	numHeartbeatResults
)

var heartbeatResultNames = [numHeartbeatResults]string{"ok", "invalid", "unknown_worker", "stale_registration", "illegal_transition"}

func heartbeatResult(err error) int {
	switch {
	case err == nil:
		return heartbeatOK
	case errors.Is(err, ErrUnknownWorker):
		return heartbeatUnknownWorker
	case errors.Is(err, ErrStaleRegistration):
		return heartbeatStaleRegistration
	case errors.Is(err, ErrIllegalTransition):
		return heartbeatIllegalTransition
	}
	return heartbeatInvalid
}

// Collector exports the registry's view of the fleet at scrape time (ADR-017): one series per live worker for
// each per-worker gauge, taken from the last heartbeat, so a deregistered or evicted worker's series vanish on
// the next scrape. The snapshot is copied under the registry's read lock and released before anything is
// emitted; a scrape never holds the lock a heartbeat needs.
type Collector struct {
	r      *Registry
	models *telemetry.LabelGuard

	active, queueDepth, queuedTokens, queueCapacity, tokensPerSecond, heartbeatAge, health *prometheus.Desc
	gpuUtil, gpuMem                                                                        *prometheus.Desc
	workers, registrations, heartbeats                                                     *prometheus.Desc
}

// NewCollector returns the registry's Prometheus collector. maxModels caps the distinct model label values
// (the rest are "other"); the worker_id label is bounded by the registry's MaxWorkers.
func NewCollector(r *Registry, maxModels int) *Collector {
	w := []string{"worker_id", "model"}
	desc := func(name, help string, labels []string) *prometheus.Desc {
		return prometheus.NewDesc(name, help, labels, nil)
	}
	return &Collector{
		r: r, models: telemetry.NewLabelGuard(maxModels, "other"),
		active:          desc("worker_active_requests", "Requests the worker reported running in its last heartbeat.", w),
		queueDepth:      desc("worker_queue_depth", "Requests waiting in the worker's queue, from its last heartbeat.", w),
		queuedTokens:    desc("worker_queued_tokens", "Input tokens waiting in the worker's queue, from its last heartbeat.", w),
		queueCapacity:   desc("worker_queue_capacity", "The worker's queue size as registered; queue depth above it would be an unbounded queue.", w),
		tokensPerSecond: desc("worker_tokens_per_second", "Recent output tokens per second the worker reported in its last heartbeat.", w),
		heartbeatAge:    desc("worker_heartbeat_age_seconds", "Seconds since the control plane last heard from the worker.", w),
		health: desc("worker_health", "1 when the worker is eligible for routing (READY and recently heard from), else 0; the state label is the effective state.",
			[]string{"worker_id", "model", "state"}),
		gpuUtil: desc("gpu_utilization_percent", "GPU utilisation the worker reported (only workers that report one).", w),
		gpuMem:  desc("gpu_memory_used_bytes", "GPU memory in use the worker reported (only workers that report it).", w),
		workers: desc("registry_workers", "Registered workers by model and effective state.", []string{"model", "state"}),
		registrations: desc("registry_registrations_total",
			"Successful worker registrations (a restarted worker registering again counts again).", nil),
		heartbeats: desc("registry_heartbeats_total", "Heartbeats received, by result.", []string{"result"}),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.active, c.queueDepth, c.queuedTokens, c.queueCapacity, c.tokensPerSecond, c.heartbeatAge,
		c.health, c.gpuUtil, c.gpuMem, c.workers, c.registrations, c.heartbeats} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snaps := c.r.List(ListFilter{}) // copied under the read lock, released before emitting
	type key struct {
		model string
		state protocol.WorkerState
	}
	byState := map[key]int{}
	gauge := func(d *prometheus.Desc, v float64, lv ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, lv...)
	}
	for i := range snaps {
		s := &snaps[i]
		model := c.models.Value(s.Model)
		byState[key{model, s.State}]++
		gauge(c.active, float64(s.Metrics.ActiveRequests), s.WorkerID, model)
		gauge(c.queueDepth, float64(s.Metrics.QueueDepth), s.WorkerID, model)
		gauge(c.queuedTokens, float64(s.Metrics.QueuedInputTokens), s.WorkerID, model)
		gauge(c.queueCapacity, float64(s.QueueSize), s.WorkerID, model)
		gauge(c.tokensPerSecond, s.Metrics.RecentTokensPerSecond, s.WorkerID, model)
		gauge(c.heartbeatAge, s.HeartbeatAgeSeconds, s.WorkerID, model)
		healthy := 0.0
		if s.Eligible {
			healthy = 1
		}
		gauge(c.health, healthy, s.WorkerID, model, string(s.State))
		if s.Metrics.GPUUtilization != nil {
			gauge(c.gpuUtil, *s.Metrics.GPUUtilization, s.WorkerID, model)
		}
		if s.Metrics.GPUMemoryUsedMB != nil {
			gauge(c.gpuMem, float64(*s.Metrics.GPUMemoryUsedMB)*1024*1024, s.WorkerID, model)
		}
	}
	for k, n := range byState {
		gauge(c.workers, float64(n), k.model, string(k.state))
	}
	ch <- prometheus.MustNewConstMetric(c.registrations, prometheus.CounterValue, float64(c.r.registrations.Load()))
	for i := range heartbeatResultNames {
		ch <- prometheus.MustNewConstMetric(c.heartbeats, prometheus.CounterValue, float64(c.r.heartbeats[i].Load()), heartbeatResultNames[i])
	}
}
