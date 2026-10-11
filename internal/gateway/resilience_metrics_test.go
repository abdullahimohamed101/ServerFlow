package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/resilience"
	"serverflow/internal/telemetry"
	"serverflow/pkg/protocol"
)

// The Phase 15 series (D14): present only when the feature is effective, bounded, and fed by events.

var resilienceFamilies = map[string]bool{
	"circuit_breaker_transitions_total": true, "circuit_breaker_probes_total": true, "retries_denied_total": true,
	"breaker_trips_suppressed_total": true, "inference_deadline_exceeded_total": true,
	"circuit_breaker_state": true, "circuit_breakers_open": true,
}

// additionsCatalogue is seriesCatalogue restricted to the families PR D adds.
func additionsCatalogue(t *testing.T, s *Server) string {
	t.Helper()
	var keep []string
	for _, l := range strings.Split(strings.TrimSpace(seriesCatalogue(t, s)), "\n") {
		if resilienceFamilies[strings.SplitN(l, " ", 2)[0]] {
			keep = append(keep, l)
		}
	}
	return strings.Join(keep, "\n") + "\n"
}

func hasResilienceFamily(t *testing.T, s *Server) []string {
	t.Helper()
	fams, err := s.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, f := range fams {
		if resilienceFamilies[f.GetName()] {
			found = append(found, f.GetName())
		}
	}
	return found
}

// driveResilienceEvents feeds every event a resilience series is made from, as the gateway would.
func driveResilienceEvents(m *metrics) {
	m.BreakerTransition(bg, BreakerChange{WorkerID: "w-1", Model: "qwen-7b", To: "open", Reason: resilience.ReasonConsecutive, Failures: 5, CoolDown: 10 * time.Second})
	m.BreakerTransition(bg, BreakerChange{WorkerID: "w-1", Model: "qwen-7b", To: "half_open", Reason: resilience.ReasonCoolDown})
	m.BreakerTransition(bg, BreakerChange{WorkerID: "w-1", Model: "qwen-7b", To: "closed", Reason: resilience.ReasonProbesPassed})
	m.BreakerTransition(bg, BreakerChange{WorkerID: "w-2", Model: "qwen-7b", To: "closed", Reason: resilience.ReasonSuppressed, Failures: 5})
	for _, v := range []string{VerdictOK, VerdictFault, VerdictNeutral, ""} {
		m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w-1", Model: "qwen-7b", Outcome: AttemptOK, Probe: true, Verdict: v})
	}
	m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w-1", Model: "qwen-7b", Outcome: AttemptFailed, RetryDenied: RetryDeniedBudget})
	m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w-1", Model: "qwen-7b", Outcome: AttemptFailed, RetryDenied: RetryDeniedDeadline})
	m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w-1", Model: "qwen-7b", Outcome: AttemptFailed, Class: classDeadline})
}

// With the feature off, in static mode, or with static workers and mode on, no resilience series exists: /metrics
// is what it was before the feature. Registry mode with mode on registers them.
func TestResilienceSeriesExistOnlyWhenEffective(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mk := func(mode, source string) *Server {
		cfg := config.Default()
		cfg.Gateway.WorkerSource = source
		cfg.Gateway.ControlPlaneURL = "http://127.0.0.1:1"
		cfg.Resilience.Mode = mode
		if source == config.WorkerSourceRegistry {
			s, err := NewRegistry(cfg, log)
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
		return NewFromConfig(cfg, log)
	}
	for _, c := range []struct {
		mode, source string
		want         bool
	}{
		{config.ResilienceOff, config.WorkerSourceRegistry, false},
		{config.ResilienceOn, config.WorkerSourceStatic, false},
		{config.ResilienceOff, config.WorkerSourceStatic, false},
		{config.ResilienceOn, config.WorkerSourceRegistry, true},
	} {
		s := mk(c.mode, c.source)
		driveResilienceEvents(s.metrics) // events change nothing when the series are not registered
		got := hasResilienceFamily(t, s)
		if c.want != (s.res != nil) {
			t.Fatalf("%s/%s: res=%v", c.mode, c.source, s.res != nil)
		}
		if c.want && len(got) == 0 {
			t.Fatalf("%s/%s: no resilience series", c.mode, c.source)
		}
		if !c.want && len(got) != 0 {
			t.Fatalf("%s/%s: %v registered with the feature not effective", c.mode, c.source, got)
		}
	}
}

// The registry-mode series gain exactly the PR D families when the feature is on (names, types, help, labels).
func TestMetricsSeriesGoldenRegistryResilienceAdditions(t *testing.T) {
	good := newScriptWorker(t, "w-good", okJSON("w-good"))
	other := newScriptWorker(t, "w-other", okJSON("w-other"))
	e := newResEnv(t, []protocol.WorkerSnapshot{good.snapshot("qwen-7b"), other.snapshot("qwen-7b")})
	// A breaker that is not closed, so the state gauge has a series.
	k := e.key("w-good")
	tk, _ := e.rs.breakers.Acquire(k)
	for i := 0; i < 3; i++ {
		e.rs.breakers.Record(tk, resilience.Fault)
	}
	driveResilienceEvents(e.gw.metrics)
	got := additionsCatalogue(t, e.gw)
	for name := range resilienceFamilies {
		if !*updateGolden && !strings.Contains(got, name+" ") {
			t.Fatalf("the scenario did not exercise %s:\n%s", name, got)
		}
	}
	checkGolden(t, "testdata/metrics_series_resilience.golden", got)
}

// circuit_breaker_state exports breakers that are not closed, and nothing for a healthy fleet.
func TestTheBreakerStateGaugeExportsOnlyNonClosedBreakers(t *testing.T) {
	_, a, b, ws := twoWorkers(t)
	_ = a
	_ = b
	e := newResEnv(t, ws)
	if _, ok := sample(t, e.gw.metrics, "circuit_breaker_state"); ok {
		t.Fatal("a healthy fleet has no per-worker breaker series")
	}
	want(t, e.gw.metrics, 0, "circuit_breakers_open", "model=qwen-7b")
	if v, ok := sample(t, e.gw.metrics, "circuit_breakers_open", "model=qwen-7b"); !ok || v != 0 {
		t.Fatalf("circuit_breakers_open for a served model reads 0, got %v %v", v, ok)
	}
	for i := 0; i < 8; i++ {
		e.do(t)
	}
	want(t, e.gw.metrics, 2, "circuit_breaker_state", "worker_id=a-flaky", "model=qwen-7b")
	want(t, e.gw.metrics, 1, "circuit_breakers_open", "model=qwen-7b")
	if _, ok := sample(t, e.gw.metrics, "circuit_breaker_state", "worker_id=b-good"); ok {
		t.Fatal("a closed breaker has no series")
	}
	want(t, e.gw.metrics, 1, "circuit_breaker_transitions_total", "model=qwen-7b", "state=open")
	e.advance(t, 10*time.Second)
	want(t, e.gw.metrics, 1, "circuit_breaker_state", "worker_id=a-flaky", "model=qwen-7b")
	// The probe counter follows probe attempts' verdicts.
	for i := 0; i < 3; i++ {
		e.do(t)
	}
	if v, _ := sample(t, e.gw.metrics, "circuit_breaker_probes_total", "model=qwen-7b", "result=fault"); v != 1 {
		t.Fatalf("probe faults %v", v)
	}
}

// With 1,000 workers and 1,000 models (every breaker open, every event kind) the new series stay within the ceiling
// stated in ADR-017 and add no label name.
const resilienceSeriesCeiling = 1500

func TestResilienceSeriesCeilingWithAThousandWorkersAndModels(t *testing.T) {
	cfg := config.Default()
	cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	cfg.Resilience.Mode = config.ResilienceOn
	cfg.Resilience.Breaker.ConsecutiveFailures = 1
	cfg.Resilience.Breaker.MaxOpenFraction = 1
	m := registryMetrics("least-active")
	m.configure(config.Default().Metrics)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	rs := newResilienceState(cfg, log, resilienceDeps{})
	cache := newSnapshotCache(&fakeLister{}, time.Second, 10*time.Second, 5*time.Second, log)
	m.enableResilience(rs, cache)
	var members []resilience.Member
	models := map[string]string{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("worker-%d", i)
		members = append(members, resilience.Member{Key: resilience.KeyOf(id, time.Time{}), Eligible: true})
		models[id] = fmt.Sprintf("model-%d", i)
	}
	rs.models.Store(&models)
	rs.breakers.Sync(members)
	for _, mb := range members {
		if tk, ok := rs.breakers.Acquire(mb.Key); ok {
			rs.breakers.Record(tk, resilience.Fault)
		}
	}
	for i := 0; i < 1000; i++ {
		model := fmt.Sprintf("model-%d", i)
		for _, to := range []string{"open", "half_open", "closed"} {
			m.BreakerTransition(bg, BreakerChange{WorkerID: "w", Model: model, To: to, Reason: "x"})
		}
		m.BreakerTransition(bg, BreakerChange{WorkerID: "w", Model: model, To: "closed", Reason: resilience.ReasonSuppressed})
		for _, v := range []string{VerdictOK, VerdictFault, VerdictNeutral} {
			m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w", Model: model, Outcome: AttemptOK, Probe: true, Verdict: v})
		}
		m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w", Model: model, Outcome: AttemptFailed, RetryDenied: RetryDeniedBudget})
		m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w", Model: model, Outcome: AttemptFailed, RetryDenied: RetryDeniedDeadline})
		m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w", Model: model, Outcome: AttemptFailed, Class: classDeadline})
	}
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var mine []string
	n := 0
	for _, f := range fams {
		if resilienceFamilies[f.GetName()] {
			n += len(f.GetMetric())
			mine = append(mine, fmt.Sprintf("%s=%d", f.GetName(), len(f.GetMetric())))
		}
	}
	sort.Strings(mine)
	t.Logf("%d resilience series with 1,000 workers x 1,000 models: %v", n, mine)
	if n > resilienceSeriesCeiling {
		t.Fatalf("%d resilience series, ceiling %d", n, resilienceSeriesCeiling)
	}
	if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
		t.Fatalf("labels outside the allowlist: %v", bad)
	}
	if total := telemetry.SeriesCount(fams); total > gatewaySeriesCeiling+resilienceSeriesCeiling {
		t.Fatalf("%d series in all", total)
	}
}

// Nothing a hostile worker or client controls reaches a resilience metric label: a 503 body, worker error text and
// request text stay out of /metrics and out of the breaker log lines.
func TestHostileTextNeverReachesResilienceMetricsOrLogs(t *testing.T) {
	canary := "CANARY-SECRET-sk-live-0123456789"
	a := newScriptWorker(t, "a-1", raw503([]byte(`{"error":{"code":"`+canary+`","message":"`+canary+`"}}`)))
	b := newScriptWorker(t, "b-2", raw503([]byte(`{"error":{"code":"queue_full","message":"`+canary+`"}}`)))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")})
	for i := 0; i < 12; i++ {
		e.chat(t, "qwen-7b")
	}
	body := scrape(t, e.gw)
	logs := e.logs.String()
	for name, text := range map[string]string{"metrics": body, "logs": logs} {
		if strings.Contains(text, canary) {
			t.Fatalf("worker error text reached the %s", name)
		}
	}
	for _, c := range e.rec.transitions() {
		if strings.Contains(fmt.Sprint(c), canary) {
			t.Fatal("worker text reached a transition event")
		}
	}
	_ = http.StatusOK
	_ = context.Background
}
