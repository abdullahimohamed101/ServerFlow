package gateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/telemetry"
)

// seriesCeiling is the most series one gateway may export under hostile traffic (ADR-017 D5).
const seriesCeiling = 2000

func TestGatewayLabelNamesAreAllowlisted(t *testing.T) {
	cfg := config.Default().Metrics
	cfg.TenantLabels = true
	for name, m := range map[string]*metrics{"static": newMetrics(), "registry": registryMetrics("round-robin")} {
		m.configure(cfg)
		driveOK(m, "w1", true)
		m.RequestCompleted(bg, Completion{Model: "m", Status: 200, TenantID: "ten_a", ErrorCode: "NO_CAPACITY", Handled: true})
		fams, err := m.reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
			t.Errorf("%s: labels outside the allowlist: %v", name, bad)
		}
	}
}

func TestNoTenantLabelAnywhereByDefault(t *testing.T) {
	m := registryMetrics("round-robin")
	for i := 0; i < 50; i++ {
		m.RequestCompleted(bg, Completion{Model: "m", Status: 200, TenantID: fmt.Sprintf("ten_%d", i), Handled: true})
	}
	fams, _ := m.reg.Gather()
	for _, f := range fams {
		for _, l := range labelNames(f) {
			if l == "tenant" {
				t.Fatalf("%s carries a tenant label with metrics.tenant_labels off", f.GetName())
			}
		}
	}
}

// TestHostileTrafficKeepsSeriesBounded sends thousands of requests whose models, keys, paths and request IDs are
// all distinct and invented, then checks the number of series and the label values.
func TestHostileTrafficKeepsSeriesBounded(t *testing.T) {
	e := newRegistryAuthEnv(t)
	cfg := config.Default().Metrics
	cfg.TenantLabels, cfg.MaxTenants = true, 5
	e.gw.metrics.configure(cfg)
	h := e.gw.Handler()
	valid := e.store.add("acme", nil)
	do := func(method, path, body string, hdr ...string) int {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	for i := 0; i < 2500; i++ {
		do(http.MethodPost, chatCompletionsPath, chatBody(fmt.Sprintf("bogus-model-%d", i)), "Authorization", "Bearer "+valid)
		do(http.MethodPost, chatCompletionsPath, chatBody("qwen-7b"), "Authorization", fmt.Sprintf("Bearer sf_bogus%016d_%032d", i, i))
		do(http.MethodGet, fmt.Sprintf("/no/such/path/%d", i), "")
		do(http.MethodPost, chatCompletionsPath, chatBody("qwen-7b"), "Authorization", "Bearer "+valid, "X-Request-ID", fmt.Sprintf("req-%d", i))
	}
	fams, err := e.gw.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if n := telemetry.SeriesCount(fams); n > seriesCeiling {
		t.Fatalf("%d series after hostile traffic, ceiling %d", n, seriesCeiling)
	}
	if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
		t.Fatalf("labels outside the allowlist: %v", bad)
	}
	body := scrape(t, e.gw)
	if strings.Contains(body, "bogus") || strings.Contains(body, "req-") || strings.Contains(body, "/no/such") {
		t.Fatal("client-supplied text reached a metric label")
	}
	want(t, e.gw.metrics, 2500, "inference_requests_total", "model=unknown", "status=404")
}

// TestOverflowValuesAreOther drives the observer directly with far more models, workers and tenants than the
// caps allow (as a confirmed-model flood or an autoscaled fleet would) and checks the overflow label.
func TestOverflowValuesAreOther(t *testing.T) {
	m := registryMetrics("round-robin")
	cfg := config.Default().Metrics
	cfg.TenantLabels, cfg.MaxModels, cfg.MaxWorkersLabel, cfg.MaxTenants = true, 8, 6, 4
	m.configure(cfg)
	for i := 0; i < 10000; i++ {
		model, worker, tenant := fmt.Sprintf("model-%d", i), fmt.Sprintf("worker-%d", i), fmt.Sprintf("ten_%d", i)
		m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: worker, Model: model, WorkerState: "READY", WorkerEligible: true})
		m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: worker, Model: model, Outcome: AttemptOK})
		m.RequestCompleted(bg, Completion{Model: model, Status: 200, TenantID: tenant, Handled: true})
	}
	fams, _ := m.reg.Gather()
	if n := telemetry.SeriesCount(fams); n > seriesCeiling {
		t.Fatalf("%d series", n)
	}
	want(t, m, 9992, "inference_requests_total", "model=other", "status=200")        // all but the first 8 models
	want(t, m, 9992, "scheduler_selections_total", "worker_id=other", "model=other") // ... and the first 6 workers
	want(t, m, 9996, "tenant_requests_total", "tenant=other", "outcome=ok")          // ... and the first 4 tenants
}

// The series ceilings ADR-017 states, for a fleet with far more models and workers than the caps allow: every one of
// 1,000 models is served by every one of 1,000 workers and every request outcome, status and failure class occurs
// for each. The gateway must stay under gatewaySeriesCeiling, which in turn is well under Prometheus' sample_limit
// of 20,000 (a scrape over the limit is dropped whole, so the ceiling is a promise about not blinding monitoring).
const (
	gatewaySeriesCeiling = 4500
	prometheusSampleCap  = 20000
)

func TestGatewaySeriesCeilingWithAThousandModelsAndWorkers(t *testing.T) {
	m := registryMetrics("least-active")
	cfg := config.Default().Metrics
	cfg.TenantLabels = true
	m.configure(cfg)
	statuses := []int{200, 400, 401, 403, 404, 413, 429, 499, 500, 502, 503, 504}
	codes := []string{"NO_CAPACITY", "WORKER_UNAVAILABLE", "INFERENCE_FAILED", "UPSTREAM_TIMEOUT", "INTERNAL_ERROR", "RATE_LIMITED"}
	classes := []string{"connect", "reset", "empty_stream", "status_502", "status_503"}
	for i := 0; i < 1000; i++ {
		model := fmt.Sprintf("model-%d", i)
		for j := 0; j < 1000; j++ {
			worker := fmt.Sprintf("worker-%d", j)
			m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: worker, Model: model, WorkerState: "READY", WorkerEligible: true, SelectDuration: time.Microsecond})
			m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: worker, Model: model, Outcome: AttemptRetried, Class: classes[j%len(classes)], WillRetry: true})
		}
		for k, st := range statuses {
			m.RequestCompleted(bg, Completion{Model: model, Status: st, ErrorCode: codes[k%len(codes)], TTFT: time.Millisecond, Duration: time.Second,
				TenantID: fmt.Sprintf("ten_%d", i), Handled: true})
		}
		for _, o := range attemptOutcomes {
			m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w", Model: model, Outcome: o})
		}
		m.RequestRejected(bg, Rejection{Kind: RejectCapacity, Reason: "no_capacity", Model: model, Status: 503})
		m.RequestRejected(bg, Rejection{Kind: RejectCapacity, Reason: "worker_unavailable", Model: model, Status: 503})
	}
	for _, st := range []int{401, 403, 500, 503} {
		m.RequestRejected(bg, Rejection{Kind: RejectAuth, Status: st})
	}
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := telemetry.SeriesCount(fams)
	t.Logf("%d series with 1,000 models x 1,000 workers", n)
	if n > gatewaySeriesCeiling || n >= prometheusSampleCap {
		t.Fatalf("%d series; the stated ceiling is %d (Prometheus sample_limit %d)", n, gatewaySeriesCeiling, prometheusSampleCap)
	}
	if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
		t.Fatal(bad)
	}
	for _, f := range fams {
		if f.GetName() == "scheduler_selections_total" && len(f.GetMetric()) > maxSelectionSeries+66+1 {
			t.Fatalf("%d selection series, budget %d", len(f.GetMetric()), maxSelectionSeries)
		}
	}
}
