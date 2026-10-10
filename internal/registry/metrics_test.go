package registry

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"serverflow/internal/telemetry"
	"serverflow/pkg/protocol"
)

func gather(t testing.TB, c prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*dto.MetricFamily{}
	for _, f := range fams {
		out[f.GetName()] = f
	}
	return out
}

func series(f *dto.MetricFamily, labels map[string]string) *dto.Metric {
	if f == nil {
		return nil
	}
next:
	for _, m := range f.GetMetric() {
		for k, v := range labels {
			ok := false
			for _, l := range m.GetLabel() {
				if l.GetName() == k && l.GetValue() == v {
					ok = true
				}
			}
			if !ok {
				continue next
			}
		}
		return m
	}
	return nil
}

func val(t *testing.T, fams map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	m := series(fams[name], labels)
	if m == nil {
		t.Fatalf("no series %s%v", name, labels)
	}
	if m.Gauge != nil {
		return m.GetGauge().GetValue()
	}
	return m.GetCounter().GetValue()
}

func TestCollectorShowsHeartbeatValuesPerWorkerAndDropsGoneWorkers(t *testing.T) {
	r, clock := newTest(t)
	util, mem := 87.5, int64(2048)
	a := mustRegister(t, r, "w-a", "qwen-7b")
	b := mustRegister(t, r, "w-b", "qwen-7b")
	if err := r.Heartbeat("w-a", a, protocol.Heartbeat{RegistrationID: a, State: protocol.StateReady,
		Metrics: protocol.Metrics{ActiveRequests: 3, QueueDepth: 2, QueuedInputTokens: 40, RecentTokensPerSecond: 55.5, GPUUtilization: &util, GPUMemoryUsedMB: &mem}}); err != nil {
		t.Fatal(err)
	}
	mustBeat(t, r, "w-b", b, protocol.StateWarming)
	clock.Advance(3 * time.Second)

	c := NewCollector(r, 64)
	f := gather(t, c)
	la := map[string]string{"worker_id": "w-a", "model": "qwen-7b"}
	for name, want := range map[string]float64{
		"worker_active_requests": 3, "worker_queue_depth": 2, "worker_queued_tokens": 40, "worker_queue_capacity": 8,
		"worker_tokens_per_second": 55.5, "worker_heartbeat_age_seconds": 3, "gpu_utilization_percent": 87.5, "gpu_memory_used_bytes": 2048 * 1024 * 1024,
	} {
		if got := val(t, f, name, la); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if series(f["gpu_utilization_percent"], map[string]string{"worker_id": "w-b"}) != nil {
		t.Error("a worker without GPU numbers must not export GPU series")
	}
	if got := val(t, f, "worker_health", map[string]string{"worker_id": "w-a", "state": "READY"}); got != 1 {
		t.Errorf("w-a health %v", got)
	}
	if got := val(t, f, "worker_health", map[string]string{"worker_id": "w-b", "state": "WARMING"}); got != 0 {
		t.Errorf("w-b is warming, not eligible: %v", got)
	}
	if got := val(t, f, "registry_workers", map[string]string{"model": "qwen-7b", "state": "READY"}); got != 1 {
		t.Errorf("registry_workers %v", got)
	}
	if got := val(t, f, "registry_registrations_total", nil); got != 2 {
		t.Errorf("registrations %v", got)
	}
	if got := val(t, f, "registry_heartbeats_total", map[string]string{"result": "ok"}); got != 2 {
		t.Errorf("heartbeats %v", got)
	}

	if err := r.Deregister("w-b", b); err != nil {
		t.Fatal(err)
	}
	f = gather(t, c)
	if series(f["worker_health"], map[string]string{"worker_id": "w-b"}) != nil || series(f["worker_queue_depth"], map[string]string{"worker_id": "w-b"}) != nil {
		t.Error("a deregistered worker's series must disappear on the next scrape")
	}
	if n := len(f["worker_queue_depth"].GetMetric()); n != 1 {
		t.Errorf("%d queue series for one worker", n)
	}
}

func TestWorkerHealthFollowsStateTransitions(t *testing.T) {
	r, clock := newTest(t)
	reg := mustRegister(t, r, "w1", "m")
	mustBeat(t, r, "w1", reg, protocol.StateWarming)
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	c := NewCollector(r, 64)
	state := func() (string, float64) {
		f := gather(t, c)
		m := f["worker_health"].GetMetric()[0]
		for _, l := range m.GetLabel() {
			if l.GetName() == "state" {
				return l.GetValue(), m.GetGauge().GetValue()
			}
		}
		return "", -1
	}
	if s, v := state(); s != "READY" || v != 1 {
		t.Fatalf("%s %v", s, v)
	}
	clock.Advance(5 * time.Second) // suspect: not eligible, still READY as reported
	if s, v := state(); s != "READY" || v != 0 {
		t.Fatalf("suspect: %s %v", s, v)
	}
	clock.Advance(6 * time.Second) // 11s: unhealthy
	if s, v := state(); s != "UNHEALTHY" || v != 0 {
		t.Fatalf("unhealthy: %s %v", s, v)
	}
	clock.Advance(25 * time.Second) // 36s: lost
	if s, v := state(); s != "LOST" || v != 0 {
		t.Fatalf("lost: %s %v", s, v)
	}
	mustBeat(t, r, "w1", reg, protocol.StateReady) // a healed partition resumes
	if s, v := state(); s != "READY" || v != 1 {
		t.Fatalf("healed: %s %v", s, v)
	}
}

func TestRejectedHeartbeatsAreCountedByResult(t *testing.T) {
	r, _ := newTest(t)
	reg := mustRegister(t, r, "w1", "m")
	_ = r.Heartbeat("nobody", reg, hb(reg, protocol.StateReady))
	_ = r.Heartbeat("w1", "reg_wrong", hb("reg_wrong", protocol.StateReady))
	mustBeat(t, r, "w1", reg, protocol.StateReady)
	_ = r.Heartbeat("w1", reg, hb(reg, protocol.StateLoadingModel))     // illegal from READY
	_ = r.Heartbeat("w1", reg, protocol.Heartbeat{RegistrationID: reg}) // invalid: no state
	f := gather(t, NewCollector(r, 64))
	for res, want := range map[string]float64{"ok": 1, "unknown_worker": 1, "stale_registration": 1, "illegal_transition": 1, "invalid": 1} {
		if got := val(t, f, "registry_heartbeats_total", map[string]string{"result": res}); got != want {
			t.Errorf("%s = %v, want %v", res, got, want)
		}
	}
}

func TestCollectorLabelsObeyTheAllowlistAndModelCap(t *testing.T) {
	r, _ := newTest(t, func(c *Config) { c.MaxWorkers = 100 })
	for i := 0; i < 20; i++ {
		mustRegister(t, r, fmt.Sprintf("w%d", i), fmt.Sprintf("model-%d", i))
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewCollector(r, 4))
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
		t.Fatalf("labels outside the allowlist: %v", bad)
	}
	models := map[string]bool{}
	for _, f := range fams {
		if f.GetName() == "worker_queue_depth" {
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "model" {
						models[l.GetValue()] = true
					}
				}
			}
		}
	}
	if len(models) != 5 || !models["other"] {
		t.Fatalf("model label values %v, want 4 + other", models)
	}
}

// A scrape must never block heartbeats: run both at once under -race and check both make progress.
func TestScrapesAndHeartbeatsRunTogether(t *testing.T) {
	r, _ := newTest(t, func(c *Config) { c.MaxWorkers = 200 })
	regs := map[string]string{}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("w%d", i)
		regs[id] = mustRegister(t, r, id, "m")
		mustBeat(t, r, id, regs[id], protocol.StateReady)
	}
	c := NewCollector(r, 64)
	pr := prometheus.NewRegistry()
	pr.MustRegister(c)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				for id, reg := range regs {
					_ = r.Heartbeat(id, reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateReady, Metrics: protocol.Metrics{ActiveRequests: n % 7}})
					n++
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		if _, err := pr.Gather(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if n := testutil.CollectAndCount(c, "worker_active_requests"); n != 100 {
		t.Fatalf("%d series", n)
	}
}

func BenchmarkCollectWorkers1000(b *testing.B) {
	r, _ := newTest(b, func(c *Config) { c.MaxWorkers = 1000 })
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("worker-%04d", i)
		reg, err := r.Register(info(id, fmt.Sprintf("model-%d", i%8)))
		if err != nil {
			b.Fatal(err)
		}
		if err := r.Heartbeat(id, reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateReady, Metrics: protocol.Metrics{ActiveRequests: i % 5, QueueDepth: i % 3, RecentTokensPerSecond: 50}}); err != nil {
			b.Fatal(err)
		}
	}
	pr := prometheus.NewRegistry()
	pr.MustRegister(NewCollector(r, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fams, err := pr.Gather()
		if err != nil || len(fams) == 0 {
			b.Fatal(err)
		}
	}
}

// A slow consumer of the collector's output must not hold up heartbeats: the collector copies the fleet under the
// registry's read lock and releases it before emitting anything. The consumer here takes 100 ms per series; if the
// lock were held while emitting, a heartbeat (which needs the write lock) would wait for all of them.
func TestSlowScrapeDoesNotBlockHeartbeats(t *testing.T) {
	r, _ := newTest(t, func(c *Config) { c.MaxWorkers = 50 })
	regs := map[string]string{}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("w%d", i)
		regs[id] = mustRegister(t, r, id, "m")
		mustBeat(t, r, id, regs[id], protocol.StateReady)
	}
	c := NewCollector(r, 64)
	ch := make(chan prometheus.Metric)
	done := make(chan struct{})
	go func() {
		c.Collect(ch)
		close(done)
	}()
	<-ch // the collector has its snapshot and is now emitting; stall the consumer
	start := time.Now()
	beat := make(chan error, 1)
	go func() { beat <- r.Heartbeat("w0", regs["w0"], hb(regs["w0"], protocol.StateReady)) }()
	select {
	case err := <-beat:
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 50*time.Millisecond {
			t.Fatalf("a heartbeat waited %v behind a stalled scrape", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a heartbeat is blocked behind a stalled scrape: the collector holds the registry lock while emitting")
	}
	for {
		select {
		case <-ch: // drain
		case <-done:
			return
		}
	}
}

// The control plane's series ceiling at its default cap of 1,000 workers (control_plane.max_workers), every worker
// reporting GPU numbers and serving a model of its own: the per-worker gauges dominate: about nine series per worker (nine or ten with the GPU gauges, eight without). ADR-017 states the
// ceiling; above about 2,200 workers with GPU gauges (about 2,500 without) the 20,000 sample_limit on the scrape job would drop the whole scrape, so a
// larger max_workers needs a larger sample_limit.
const (
	controlPlaneSeriesCeiling = 10500
	prometheusSampleCap       = 20000
)

func TestControlPlaneSeriesCeilingAtTheDefaultWorkerCap(t *testing.T) {
	r, _ := newTest(t, func(c *Config) { c.MaxWorkers = 1000 })
	util, mem := 50.0, int64(1024)
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("worker-%04d", i)
		reg, err := r.Register(info(id, fmt.Sprintf("model-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Heartbeat(id, reg, protocol.Heartbeat{RegistrationID: reg, State: protocol.StateReady,
			Metrics: protocol.Metrics{GPUUtilization: &util, GPUMemoryUsedMB: &mem}}); err != nil {
			t.Fatal(err)
		}
	}
	pr := NewMetricsRegistry(r, 64)
	fams, err := pr.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := telemetry.SeriesCount(fams)
	t.Logf("%d series, %d samples with 1,000 workers", n, telemetry.SampleCount(fams))
	if n > controlPlaneSeriesCeiling || n >= prometheusSampleCap {
		t.Fatalf("%d series; the stated ceiling is %d (Prometheus sample_limit %d)", n, controlPlaneSeriesCeiling, prometheusSampleCap)
	}
	if n < 9000 {
		t.Fatalf("only %d series: the test no longer exercises the per-worker gauges", n)
	}
}
