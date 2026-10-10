package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/mockworker"
	"serverflow/internal/ratelimit"
	"serverflow/internal/registry"
	"serverflow/internal/registry/server"
	"serverflow/internal/telemetry"
	"serverflow/internal/telemetry/metricsserver"
	"serverflow/pkg/protocol"
)

const (
	cpToken      = "control-plane-token-0123456789"
	scrapeToken  = "scrape-token-0123456789-abcdef"
	goodModel    = "qwen-7b"
	parkedModel  = "llama-8b"
	limitedKeyID = "limited"
)

// keyStore is a minimal in-memory auth.KeyStore.
type keyStore struct {
	mu   sync.Mutex
	recs map[string]auth.KeyRecord
}

func (s *keyStore) LookupKey(_ context.Context, prefix string) (auth.KeyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.recs[prefix]; ok {
		return r, nil
	}
	return auth.KeyRecord{}, auth.ErrNotFound
}

func (s *keyStore) add(tenant string) string {
	key, prefix, hash := auth.GenerateKey()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recs == nil {
		s.recs = map[string]auth.KeyRecord{}
	}
	s.recs[prefix] = auth.KeyRecord{KeyID: "key_" + tenant, TenantID: tenant, Prefix: prefix, SecretHash: hash, KeyStatus: auth.KeyActive,
		Policy: auth.TenantPolicy{Status: auth.TenantActive}}
	return key
}

// limiter refuses the tenant "limited" and admits everyone else.
type limiter struct{}

func (limiter) Allow(_ context.Context, r ratelimit.Request) (ratelimit.Decision, error) {
	if r.TenantID == limitedKeyID {
		return ratelimit.Decision{Limit: ratelimit.LimitRequests, RetryAfter: time.Second}, nil
	}
	return ratelimit.Decision{Allowed: true, Release: func() {}}, nil
}

// exported is what a running ServerFlow cluster exposes, family name -> type, gathered over HTTP the way
// Prometheus would.
type exported map[string]famInfo

// famInfo is a family's type and the label names its series carry.
type famInfo struct {
	Type   string
	Labels map[string]bool
}

func parse(t *testing.T, body io.Reader) exported {
	t.Helper()
	tp := expfmt.NewTextParser(model.LegacyValidation) // our metric and label names are plain ASCII
	fams, err := tp.TextToMetricFamilies(body)
	if err != nil {
		t.Fatal(err)
	}
	out := exported{}
	for n, f := range fams {
		fi := famInfo{Type: f.GetType().String(), Labels: map[string]bool{}}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				fi.Labels[l.GetName()] = true
			}
		}
		out[n] = fi
	}
	return out
}

func httpGet(t *testing.T, url, bearer string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// runCluster starts a control plane, three mock workers (one refusing every request with a 503, one parked
// where it is not eligible), and a gateway with authentication and a rate limiter, drives every kind of traffic
// the dashboards and alerts depend on, and returns the metric families each component exposes over HTTP.
func runCluster(t *testing.T) (gw, cp, worker exported, gwBody string) {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	reg, err := registry.New(registry.Config{Suspect: 5 * time.Second, Unhealthy: 10 * time.Second, Lost: 30 * time.Second,
		Retention: time.Minute, MaxWorkers: 10, HeartbeatInterval: 100 * time.Millisecond}, log)
	if err != nil {
		t.Fatal(err)
	}
	cpSrv := httptest.NewServer(server.New(reg, server.Config{Token: cpToken}, log).Handler())
	t.Cleanup(cpSrv.Close)

	startWorker := func(id, model string, mutate func(*mockworker.Config)) *httptest.Server {
		cfg := mockworker.DefaultConfig()
		cfg.Model, cfg.WorkerID, cfg.TTFT, cfg.TokensPerSecond, cfg.OutputTokens, cfg.Seed = model, id, 5*time.Millisecond, 2000, 4, 1
		if mutate != nil {
			mutate(&cfg)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(mockworker.New(cfg, log).Handler())
		t.Cleanup(ts.Close)
		return ts
	}
	register := func(id, model, url string, state protocol.WorkerState, m protocol.Metrics) {
		rid, err := reg.Register(protocol.WorkerInfo{WorkerID: id, Model: model, Address: url, MaxConcurrency: 4, QueueSize: 8})
		if err != nil {
			t.Fatal(err)
		}
		if err := reg.Heartbeat(id, rid, protocol.Heartbeat{RegistrationID: rid, State: state, Metrics: m}); err != nil {
			t.Fatal(err)
		}
	}
	util, mem := 61.5, int64(4096)
	good := startWorker("w-good", goodModel, nil)
	bad := startWorker("w-bad", goodModel, func(c *mockworker.Config) { c.FailureRate, c.FailureMode = 1, mockworker.ModeUnavailable })
	parked := startWorker("w-parked", parkedModel, nil)
	register("w-good", goodModel, good.URL, protocol.StateReady, protocol.Metrics{ActiveRequests: 1, QueueDepth: 1, RecentTokensPerSecond: 40, GPUUtilization: &util, GPUMemoryUsedMB: &mem})
	register("w-bad", goodModel, bad.URL, protocol.StateReady, protocol.Metrics{})
	register("w-parked", parkedModel, parked.URL, protocol.StateWarming, protocol.Metrics{})

	store := &keyStore{}
	authn := auth.New(store, auth.Config{CacheTTL: time.Minute, NegativeTTL: time.Minute, CacheSize: 100, StaleGrace: time.Minute})
	cfg := config.Default()
	cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	cfg.Gateway.ControlPlaneURL, cfg.ControlPlane.Token = cpSrv.URL, cpToken
	cfg.Gateway.RegistryRefresh, cfg.Gateway.RegistryMaxStaleness = 25*time.Millisecond, 2*time.Second
	cfg.Worker.SuspectTimeout = 5 * time.Second
	cfg.Scheduler.Strategy = "round-robin"
	cfg.Auth.Mode = config.AuthModeRequired
	cfg.RateLimit.Mode = config.RateLimitRequired
	cfg.Metrics.TenantLabels = true
	gws, err := gateway.NewRegistry(cfg, log, gateway.WithAuthenticator(authn), gateway.WithLimiter(limiter{}), gateway.WithCollector(authn.Collector()))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gws.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	base := "http://" + ln.Addr().String()

	chat := func(key, model string, stream bool) int {
		body := fmt.Sprintf(`{"model":%q,"stream":%v,"messages":[{"role":"user","content":"hello there"}]}`, model, stream)
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	key, limited := store.add("acme"), store.add(limitedKeyID)

	deadline := time.Now().Add(10 * time.Second)
	for chat(key, goodModel, false) != 200 { // the gateway needs a first snapshot
		if time.Now().After(deadline) {
			t.Fatal("the gateway never became ready")
		}
		time.Sleep(25 * time.Millisecond)
	}
	for i := 0; i < 6; i++ { // round-robin starts some of these on the refusing worker, which are retried
		chat(key, goodModel, i%2 == 0)
	}
	for _, tc := range []struct {
		what string
		got  int
		want int
	}{
		{"no key", chat("", goodModel, false), 401},
		{"rate limited tenant", chat(limited, goodModel, false), 429},
		{"unknown model", chat(key, "no-such-model", false), 404},
		{"model with no eligible worker", chat(key, parkedModel, false), 503},
	} {
		if tc.got != tc.want {
			t.Fatalf("%s: status %d, want %d", tc.what, tc.got, tc.want)
		}
	}
	// A client that leaves mid-request.
	cctx, ccancel := context.WithTimeout(context.Background(), time.Millisecond)
	req, _ := http.NewRequestWithContext(cctx, http.MethodPost, base+"/v1/chat/completions", strings.NewReader(`{"model":"`+goodModel+`","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	req.Header.Set("Authorization", "Bearer "+key)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
	}
	ccancel()

	// Scrape the gateway through the real metrics listener, with a token, as Prometheus would.
	mc := config.MetricsConfig{Token: scrapeToken}
	ms, err := metricsserver.Listen("127.0.0.1:0", mc, gws.MetricsGatherer(), log)
	if err != nil {
		t.Fatal(err)
	}
	mctx, mcancel := context.WithCancel(context.Background())
	go func() { _ = ms.Serve(mctx) }()
	t.Cleanup(mcancel)
	if r := httpGet(t, "http://"+ms.Addr()+"/metrics", ""); r.StatusCode != 401 {
		t.Fatalf("a scrape without the token: %d", r.StatusCode)
	}
	resp := httpGet(t, "http://"+ms.Addr()+"/metrics", scrapeToken)
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("gateway scrape: %d", resp.StatusCode)
	}
	gw = parse(t, strings.NewReader(string(raw)))
	if !strings.Contains(string(raw), `scheduler_ineligible_selections_total 0`) {
		t.Errorf("the scheduler must not have routed to an ineligible worker:\n%s", grep(string(raw), "scheduler_"))
	}

	// The control plane, from the same registry the binary builds.
	cms, err := metricsserver.Listen("127.0.0.1:0", config.MetricsConfig{}, registry.NewMetricsRegistry(reg, 64), log)
	if err != nil {
		t.Fatal(err)
	}
	cctx2, ccancel2 := context.WithCancel(context.Background())
	go func() { _ = cms.Serve(cctx2) }()
	t.Cleanup(ccancel2)
	cresp := httpGet(t, "http://"+cms.Addr()+"/metrics", "")
	cp = parse(t, cresp.Body)
	_ = cresp.Body.Close()

	// A mock worker, on its own listener.
	wresp := httpGet(t, good.URL+"/metrics", "")
	worker = parse(t, wresp.Body)
	_ = wresp.Body.Close()
	return gw, cp, worker, string(raw)
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

// exemptions are metrics the dashboards or rules use that this test cannot make a process export without the
// database and Redis servers. They are checked by TestRedisAndPostgresFamiliesAreExported when those servers are
// configured (the integration job), so a rename still fails in CI. Prometheus' own series are not ours.
var exemptions = map[string]string{
	"redis_up":                           "needs a Redis client; verified by TestRedisAndPostgresFamiliesAreExported",
	"postgres_pool_empty_acquires_total": "needs a PostgreSQL pool; verified by TestRedisAndPostgresFamiliesAreExported",
	"up":                                 "written by Prometheus for every scrape",
	"ALERTS":                             "written by Prometheus for every alert",
}

// TestEveryMetricUsedIsExportedOrRecorded is the exported-by-a-running-server check (ADR-017 D14.3): every
// metric a dashboard, a recording rule or an alert names must be a family a running component exposes over
// HTTP, or a recording rule defined in the repository.
func TestEveryMetricUsedIsExportedOrRecorded(t *testing.T) {
	gw, cp, wk, _ := runCluster(t)
	families := map[string]bool{}
	for _, e := range []exported{gw, cp, wk} {
		for n := range e {
			families[n] = true
		}
	}
	recorded := map[string]bool{}
	for _, n := range recordingNames(t) {
		recorded[n] = true
	}

	used := map[string][]string{} // metric -> where
	add := func(where, expr string) {
		for _, n := range MetricNames(expr) {
			used[n] = append(used[n], where)
		}
	}
	for where, e := range dashboardExprs(t) {
		add(where, e)
	}
	for _, f := range []string{"recording", "alerts"} {
		for _, g := range loadRules(t, "observability/prometheus/rules/"+f+".yml").Groups {
			for _, r := range g.Rules {
				add(f+".yml "+r.Record+r.Alert, r.Expr)
			}
		}
	}
	var missing []string
	for n, where := range used {
		if recorded[n] {
			continue
		}
		if _, ok := Family(n, families); ok {
			continue
		}
		if reason, ok := exemptions[n]; ok {
			t.Logf("exempt: %s (%s)", n, reason)
			continue
		}
		sort.Strings(where)
		missing = append(missing, fmt.Sprintf("%s (used by %s)", n, where[0]))
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("metrics used by dashboards or rules that no component exports and no recording rule defines:\n  %s", strings.Join(missing, "\n  "))
	}
	if len(exemptions) > 5 {
		t.Errorf("the exemption list is meant to stay short, has %d", len(exemptions))
	}
}

// TestRunningClusterExportsTheAdditions checks the Phase 10 series by name and type on each component, so a
// dropped or renamed series fails here as well as in the gateway golden file.
func TestRunningClusterExportsTheAdditions(t *testing.T) {
	gw, cp, wk, body := runCluster(t)
	for name, typ := range map[string]string{
		"inference_failures_total": "COUNTER", "inference_gateway_overhead_seconds": "HISTOGRAM",
		"scheduler_decisions_total": "COUNTER", "scheduler_decision_duration_seconds": "HISTOGRAM",
		"scheduler_no_capacity_total": "COUNTER", "scheduler_selections_total": "COUNTER",
		"scheduler_ineligible_selections_total": "COUNTER", "gateway_registry_snapshot_age_seconds": "GAUGE",
		"gateway_registry_refresh_failures_total": "COUNTER", "cache_hits_total": "COUNTER", "cache_misses_total": "COUNTER",
		"serverflow_build_info": "GAUGE", "tenant_requests_total": "COUNTER", "inference_retries_total": "COUNTER",
		"inference_requests_total": "COUNTER", "auth_rejections_total": "COUNTER", "rate_limit_rejections_total": "COUNTER",
		"go_goroutines": "GAUGE", "process_cpu_seconds_total": "COUNTER",
	} {
		if gw[name].Type != typ {
			t.Errorf("gateway: %s is %q, want %s", name, gw[name].Type, typ)
		}
	}
	for name, typ := range map[string]string{
		"worker_active_requests": "GAUGE", "worker_queue_depth": "GAUGE", "worker_queued_tokens": "GAUGE", "worker_queue_capacity": "GAUGE",
		"worker_tokens_per_second": "GAUGE", "worker_heartbeat_age_seconds": "GAUGE", "worker_health": "GAUGE",
		"registry_workers": "GAUGE", "registry_registrations_total": "COUNTER", "registry_heartbeats_total": "COUNTER",
		"gpu_utilization_percent": "GAUGE", "gpu_memory_used_bytes": "GAUGE", "serverflow_build_info": "GAUGE",
	} {
		if cp[name].Type != typ {
			t.Errorf("control plane: %s is %q, want %s", name, cp[name].Type, typ)
		}
	}
	for name, typ := range map[string]string{
		"worker_requests_total": "COUNTER", "worker_input_tokens_total": "COUNTER", "worker_output_tokens_total": "COUNTER",
		"worker_request_duration_seconds": "HISTOGRAM", "worker_ttft_seconds": "HISTOGRAM", "worker_queue_duration_seconds": "HISTOGRAM",
		"serverflow_build_info": "GAUGE",
	} {
		if wk[name].Type != typ {
			t.Errorf("mock worker: %s is %q, want %s", name, wk[name].Type, typ)
		}
	}
	for _, want := range []string{
		`inference_requests_total{model="qwen-7b",status="200"}`, `inference_requests_total{model="unknown",status="404"}`,
		`inference_requests_total{model="unknown",status="429"}`, `auth_rejections_total{status="401"} 1`,
		`scheduler_decisions_total{model="llama-8b",result="no_capacity",strategy="round-robin"} 1`,
		`scheduler_no_capacity_total{model="llama-8b",reason="no_capacity"} 1`,
		`scheduler_decisions_total{model="unknown",result="no_model",strategy="round-robin"} 1`,
		`tenant_requests_total{outcome="rate_limited",tenant="limited"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the gateway scrape lacks %s\n%s", want, grep(body, "inference_requests_total"))
		}
	}
	if !strings.Contains(body, `scheduler_selections_total{model="qwen-7b",worker_id="w-good"}`) {
		t.Errorf("selection distribution missing:\n%s", grep(body, "scheduler_selections"))
	}
	if !strings.Contains(body, "inference_retries_total{model=\"qwen-7b\",reason=\"status_503\"}") {
		t.Errorf("the refusing worker's 503s should have been retried:\n%s", grep(body, "retries"))
	}
}

// TestRedisAndPostgresFamiliesAreExported covers the exempt families when the integration servers are configured.
func TestRedisAndPostgresFamiliesAreExported(t *testing.T) {
	if os.Getenv("SERVERFLOW_TEST_REDIS_ADDR") == "" || os.Getenv("SERVERFLOW_TEST_POSTGRES_DSN") == "" {
		t.Skip("SERVERFLOW_TEST_REDIS_ADDR or SERVERFLOW_TEST_POSTGRES_DSN is not set; skipping the Redis and PostgreSQL families check")
	}
	redisAndPostgres(t)
}

var _ = telemetry.AllowedLabelNames

// TestLabelsUsedByDashboardsAndRulesExist checks the label names inside panel and rule queries (by/without/on clauses
// and label matchers) against the labels the components really export. A misspelt label ("workerid") is valid PromQL
// that matches nothing, so the metric-name check cannot see it. For each expression the allowed names are those of the
// families it uses (a recording rule contributes the labels of its own expression), the labels Prometheus adds to
// every target (job, instance, component, stack), the mock workers' worker_id from the targets file, and the
// histogram and ALERTS labels.
func TestLabelsUsedByDashboardsAndRulesExist(t *testing.T) {
	gw, cp, wk, _ := runCluster(t)
	labels := map[string]map[string]bool{}
	add := func(e exported, extra ...string) {
		for n, fi := range e {
			set := labels[n]
			if set == nil {
				set = map[string]bool{}
				labels[n] = set
			}
			for l := range fi.Labels {
				set[l] = true
			}
			for _, l := range extra {
				set[l] = true
			}
		}
	}
	add(gw)
	add(cp)
	add(wk, "worker_id") // the targets file labels the mock workers' targets
	labels["ALERTS"] = map[string]bool{"alertname": true, "alertstate": true, "severity": true}
	labels["up"] = map[string]bool{}
	targetLabels := []string{"job", "instance", "component", "stack", "le"}

	recExpr := map[string]string{}
	for _, g := range loadRules(t, "observability/prometheus/rules/recording.yml").Groups {
		for _, r := range g.Rules {
			recExpr[r.Record] = r.Expr
		}
	}
	var allowed func(name string, depth int) map[string]bool
	allowed = func(name string, depth int) map[string]bool {
		out := map[string]bool{}
		if expr, ok := recExpr[name]; ok && depth < 4 {
			for _, l := range LabelNames(expr) {
				out[l] = true
			}
			for _, m := range MetricNames(expr) {
				for l := range allowed(m, depth+1) {
					out[l] = true
				}
			}
			return out
		}
		if f, ok := Family(name, familySet(labels)); ok {
			for l := range labels[f] {
				out[l] = true
			}
		}
		return out
	}
	check := func(where, expr string) {
		ok := map[string]bool{}
		for _, l := range targetLabels {
			ok[l] = true
		}
		for _, m := range MetricNames(expr) {
			for l := range allowed(m, 0) {
				ok[l] = true
			}
		}
		for _, l := range LabelNames(expr) {
			if !ok[l] {
				t.Errorf("%s: label %q is not exported by any metric this expression uses", where, l)
			}
		}
	}
	for where, e := range dashboardExprs(t) {
		check(where, e)
	}
	for _, f := range []string{"recording", "alerts"} {
		for _, g := range loadRules(t, "observability/prometheus/rules/"+f+".yml").Groups {
			for _, r := range g.Rules {
				check(f+".yml "+r.Record+r.Alert, r.Expr)
			}
		}
	}
}

func familySet(m map[string]map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for n := range m {
		out[n] = true
	}
	return out
}
