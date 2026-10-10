package gateway

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"serverflow/internal/api"
	"serverflow/internal/config"
)

// sample returns the value of the series name{labels...} in m's registry (counter or gauge value, histogram
// sample count), and whether it exists. Labels are given as name=value pairs; a series must match all of them.
func sample(t *testing.T, m *metrics, name string, labels ...string) (float64, bool) {
	t.Helper()
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
	next:
		for _, mm := range f.GetMetric() {
			for _, want := range labels {
				k, v, _ := strings.Cut(want, "=")
				found := false
				for _, l := range mm.GetLabel() {
					if l.GetName() == k && l.GetValue() == v {
						found = true
					}
				}
				if !found {
					continue next
				}
			}
			switch {
			case mm.GetCounter() != nil:
				return mm.GetCounter().GetValue(), true
			case mm.GetGauge() != nil:
				return mm.GetGauge().GetValue(), true
			case mm.GetHistogram() != nil:
				return float64(mm.GetHistogram().GetSampleCount()), true
			}
		}
	}
	return 0, false
}

func want(t *testing.T, m *metrics, v float64, name string, labels ...string) {
	t.Helper()
	got, ok := sample(t, m, name, labels...)
	if v == 0 && !ok {
		return
	}
	if !ok || got != v {
		t.Errorf("%s%v = %v (exists %v), want %v", name, labels, got, ok, v)
	}
}

func registryMetrics(strategy string) *metrics {
	m := newMetrics()
	m.setStrategy(strategy)
	return m
}

var bg = context.Background()

// ok drives the event sequence of one successful registry-mode request.
func driveOK(m *metrics, worker string, stream bool) {
	m.RequestStarted(bg, RequestStart{})
	m.RequestAdmitted(bg, Admission{Model: "qwen-7b", RateLimitChecked: true, RateLimitDuration: time.Millisecond})
	m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: worker, Model: "qwen-7b", Strategy: "least-active",
		SelectDuration: 30 * time.Microsecond, SinceRequestStart: 2 * time.Millisecond, WorkerState: "READY", WorkerEligible: true})
	var ttft time.Duration
	if stream {
		ttft = 40 * time.Millisecond
		m.FirstToken(bg, FirstToken{TTFT: ttft})
	}
	m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: worker, Model: "qwen-7b", Outcome: AttemptOK})
	m.RequestCompleted(bg, Completion{Model: "qwen-7b", Status: 200, Duration: 90 * time.Millisecond, Attempts: 1, TTFT: ttft, Handled: true})
}

func TestSuccessAndStreamEffects(t *testing.T) {
	m := registryMetrics("least-active")
	driveOK(m, "w1", false)
	driveOK(m, "w2", true)
	driveOK(m, "w2", true)
	want(t, m, 3, "inference_requests_total", "model=qwen-7b", "status=200")
	want(t, m, 3, "inference_request_duration_seconds", "model=qwen-7b")
	want(t, m, 2, "inference_ttft_seconds", "model=qwen-7b")
	want(t, m, 3, "inference_gateway_overhead_seconds", "model=qwen-7b")
	want(t, m, 3, "inference_attempts_total", "model=qwen-7b", "outcome=ok")
	want(t, m, 3, "scheduler_decisions_total", "strategy=least-active", "model=qwen-7b", "result=selected")
	want(t, m, 3, "scheduler_decision_duration_seconds", "strategy=least-active")
	want(t, m, 1, "scheduler_selections_total", "model=qwen-7b", "worker_id=w1")
	want(t, m, 2, "scheduler_selections_total", "model=qwen-7b", "worker_id=w2")
	want(t, m, 0, "inference_requests_active")
	want(t, m, 0, "scheduler_ineligible_selections_total")
	want(t, m, 0, "inference_failures_total")
	if _, ok := sample(t, m, "inference_attempts_total", "outcome=failed"); ok {
		t.Error("an outcome that never happened must not appear")
	}
}

func TestRetryOnASecondWorker(t *testing.T) {
	m := registryMetrics("round-robin")
	m.RequestStarted(bg, RequestStart{})
	m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: "w1", Model: "qwen-7b", WorkerState: "READY", WorkerEligible: true, SinceRequestStart: time.Millisecond})
	m.AttemptStarted(bg, AttemptStart{Number: 2, WorkerID: "w2", Model: "qwen-7b", WorkerState: "READY", WorkerEligible: true, SinceRequestStart: 50 * time.Millisecond})
	m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w1", Model: "qwen-7b", Outcome: AttemptRetried, Class: "connect", WillRetry: true})
	m.AttemptEnded(bg, AttemptEnd{Number: 2, WorkerID: "w2", Model: "qwen-7b", Outcome: AttemptOK})
	m.RequestCompleted(bg, Completion{Model: "qwen-7b", Status: 200, Attempts: 2, Handled: true})
	want(t, m, 1, "inference_retries_total", "model=qwen-7b", "reason=connect")
	want(t, m, 1, "inference_attempts_total", "outcome=retried")
	want(t, m, 1, "inference_attempts_total", "outcome=ok")
	want(t, m, 2, "scheduler_decisions_total", "result=selected")
	// Only the first dispatch counts as gateway overhead; the retry's age includes the failed attempt.
	want(t, m, 1, "inference_gateway_overhead_seconds", "model=qwen-7b")
}

func TestSchedulingRefusals(t *testing.T) {
	m := registryMetrics("least-active")
	refuse := func(kind, reason string, status int, model, code string) {
		m.RequestStarted(bg, RequestStart{})
		m.RequestAdmitted(bg, Admission{})
		m.RequestRejected(bg, Rejection{Kind: kind, Reason: reason, Status: status, Model: model, DecisionDuration: 15 * time.Microsecond})
		m.RequestCompleted(bg, Completion{Model: model, Status: status, ErrorCode: code, Handled: true})
	}
	refuse(RejectCapacity, "no_capacity", 503, "qwen-7b", api.CodeNoCapacity)
	refuse(RejectCapacity, "worker_unavailable", 503, "", api.CodeWorkerUnavailable)
	refuse(RejectModel, "model_not_found", 404, "", api.CodeModelNotFound)
	refuse(RejectModel, "model_forbidden", 403, "", api.CodeForbidden) // policy, not a scheduling decision
	want(t, m, 1, "scheduler_decisions_total", "model=qwen-7b", "result=no_capacity")
	want(t, m, 1, "scheduler_decisions_total", "model=unknown", "result=error")
	want(t, m, 1, "scheduler_decisions_total", "model=unknown", "result=no_model")
	if n := sumSeries(t, m, "scheduler_decisions_total"); n != 3 {
		t.Errorf("%v scheduling decisions, want 3: a forbidden model is a policy refusal, not a decision", n)
	}
	want(t, m, 1, "scheduler_no_capacity_total", "model=qwen-7b", "reason=no_capacity")
	want(t, m, 1, "scheduler_no_capacity_total", "model=unknown", "reason=worker_unavailable")
	want(t, m, 3, "scheduler_decision_duration_seconds", "strategy=least-active")
	want(t, m, 1, "inference_failures_total", "model=qwen-7b", "reason=NO_CAPACITY")
	want(t, m, 1, "inference_failures_total", "model=unknown", "reason=WORKER_UNAVAILABLE")
	want(t, m, 1, "inference_failures_total", "model=unknown", "reason=FORBIDDEN")
	want(t, m, 1, "inference_requests_total", "model=unknown", "status=404")
	want(t, m, 0, "inference_requests_active")
}

func TestAuthAndRateLimitRefusalsAndDisconnect(t *testing.T) {
	m := registryMetrics("round-robin")
	m.RequestStarted(bg, RequestStart{})
	m.RequestRejected(bg, Rejection{Kind: RejectAuth, Reason: "missing", Status: 401})
	m.RequestCompleted(bg, Completion{Status: 401}) // Handled=false: refused authentication
	m.RequestStarted(bg, RequestStart{})
	m.RequestAdmitted(bg, Admission{})
	m.RequestRejected(bg, Rejection{Kind: RejectRateLimit, Reason: "requests", Status: 429, DecisionDuration: time.Millisecond})
	m.RequestCompleted(bg, Completion{Status: 429, ErrorCode: api.CodeRateLimited, Handled: true})
	m.RequestStarted(bg, RequestStart{})
	m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: "w1", Model: "qwen-7b", WorkerState: "READY", WorkerEligible: true})
	m.AttemptEnded(bg, AttemptEnd{Number: 1, WorkerID: "w1", Model: "qwen-7b", Outcome: AttemptClientClosed})
	m.RequestCompleted(bg, Completion{Model: "qwen-7b", Status: 499, Handled: true})
	want(t, m, 1, "auth_rejections_total", "status=401")
	want(t, m, 1, "rate_limit_rejections_total", "limit=requests")
	want(t, m, 1, "inference_requests_total", "model=unknown", "status=429")
	want(t, m, 1, "inference_requests_total", "model=qwen-7b", "status=499")
	want(t, m, 1, "inference_attempts_total", "outcome=client_closed")
	want(t, m, 0, "inference_requests_active")
	if v, ok := sample(t, m, "inference_requests_total", "status=401"); ok {
		t.Errorf("a refused authentication is not an inference request: %v", v)
	}
	want(t, m, 0, "scheduler_decisions_total", "result=no_capacity")
}

func TestOnlyAnIneligibleSelectionIncrementsTheGuardCounter(t *testing.T) {
	m := registryMetrics("round-robin")
	m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: "w1", Model: "m", WorkerState: "READY", WorkerEligible: true})
	want(t, m, 0, "scheduler_ineligible_selections_total")
	m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: "w2", Model: "m", WorkerState: "FAILED", WorkerEligible: false})
	m.AttemptStarted(bg, AttemptStart{Number: 1, WorkerID: "w3", Model: "m", WorkerState: "READY", WorkerEligible: false})
	want(t, m, 2, "scheduler_ineligible_selections_total")
}

func TestStaticModeHasNoSchedulerSeries(t *testing.T) {
	m := newMetrics() // no strategy
	m.RequestStarted(bg, RequestStart{})
	m.AttemptStarted(bg, AttemptStart{Number: 1, Model: "qwen-7b", SinceRequestStart: time.Millisecond})
	m.AttemptEnded(bg, AttemptEnd{Number: 1, Model: "qwen-7b", Outcome: AttemptOK})
	m.RequestRejected(bg, Rejection{Kind: RejectModel, Reason: "model_not_found", Status: 404})
	m.RequestCompleted(bg, Completion{Model: "qwen-7b", Status: 200, Handled: true})
	for _, n := range []string{"scheduler_decisions_total", "scheduler_selections_total", "scheduler_decision_duration_seconds", "inference_attempts_total"} {
		if _, ok := sample(t, m, n); ok {
			t.Errorf("%s exists in static mode", n)
		}
	}
	want(t, m, 1, "inference_gateway_overhead_seconds", "model=qwen-7b")
}

func TestSeriesExistBeforeTraffic(t *testing.T) {
	m := registryMetrics("least-active")
	for _, st := range []string{"401", "403", "500", "503"} {
		if _, ok := sample(t, m, "auth_rejections_total", "status="+st); !ok {
			t.Errorf("auth_rejections_total{status=%s} missing at start", st)
		}
	}
	for _, l := range []string{"requests", "tokens", "concurrency", "model", "unavailable"} {
		if _, ok := sample(t, m, "rate_limit_rejections_total", "limit="+l); !ok {
			t.Errorf("rate_limit_rejections_total{limit=%s} missing at start", l)
		}
	}
	for _, n := range []string{"scheduler_ineligible_selections_total", "rate_limit_bypassed_total", "inference_requests_active", "rate_limit_decision_seconds"} {
		if _, ok := sample(t, m, n); !ok {
			t.Errorf("%s missing at start", n)
		}
	}
	if _, ok := sample(t, m, "scheduler_decision_duration_seconds", "strategy=least-active"); !ok {
		t.Error("the strategy's decision histogram must exist at start")
	}
}

func TestTenantSeriesOnlyWhenEnabledAndCapped(t *testing.T) {
	off := newMetrics()
	off.RequestCompleted(bg, Completion{Model: "m", Status: 200, TenantID: "ten_a", Handled: true})
	if _, ok := sample(t, off, "tenant_requests_total"); ok {
		t.Fatal("tenant series exist with tenant_labels off")
	}
	on := newMetrics()
	cfg := config.Default().Metrics
	cfg.TenantLabels, cfg.MaxTenants = true, 3
	on.configure(cfg)
	for i := 0; i < 20; i++ {
		on.RequestCompleted(bg, Completion{Model: "m", Status: 200, TenantID: fmt.Sprintf("ten_%d", i), Handled: true})
	}
	on.RequestCompleted(bg, Completion{Model: "m", Status: 429, TenantID: "ten_0", Handled: true})
	on.RequestCompleted(bg, Completion{Model: "m", Status: 200, Handled: true}) // no tenant (auth off): not counted
	want(t, on, 1, "tenant_requests_total", "tenant=ten_0", "outcome=ok")
	want(t, on, 1, "tenant_requests_total", "tenant=ten_0", "outcome=rate_limited")
	want(t, on, 17, "tenant_requests_total", "tenant=other", "outcome=ok")
	if n := countSeries(t, on, "tenant_requests_total"); n != 5 {
		t.Fatalf("%d tenant series, want 3 tenants + other + one extra outcome", n)
	}
}

func countSeries(t *testing.T, m *metrics, name string) int {
	t.Helper()
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			return len(f.GetMetric())
		}
	}
	return 0
}

func labelNames(f *dto.MetricFamily) []string {
	set := map[string]bool{}
	for _, mm := range f.GetMetric() {
		for _, l := range mm.GetLabel() {
			set[l.GetName()] = true
		}
	}
	var out []string
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func sumSeries(t *testing.T, m *metrics, name string) float64 {
	t.Helper()
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, f := range fams {
		if f.GetName() == name {
			for _, mm := range f.GetMetric() {
				sum += mm.GetCounter().GetValue()
			}
		}
	}
	return sum
}
