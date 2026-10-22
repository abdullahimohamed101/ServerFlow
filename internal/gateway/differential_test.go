package gateway_test

// The Phase 15 default-flip differential (plan acceptance 4 and 24, decisions D1 and D25).
//
// The same 39 request shapes are driven through a gateway in process against a fake control plane and fake workers, and what a
// client and an operator can observe is written down: every status, header and body, the normalised log lines, and the /metrics
// series with their values. The outputs of master (commit 16dda6b, which already contains Phase 15 PR A) are recorded under
// testdata/differential/master; they were produced by this very file copied onto that tree (see the recording command below),
// so the harness is the same on both sides.
//
//   - With resilience.mode off, and with static workers whatever the mode, the output must be identical to master, byte for byte.
//   - With the shipped default (mode on, registry workers) the output may differ from master in exactly the documented ways: the
//     new series (D25 item 11), the verdict/probe/retry_denied fields on attempt log lines (an addition the differential found:
//     see "Additional visible details" in docs/operations/resilience.md), and the shapes the D25 table names. Any other
//     difference fails the test, and a table entry that does not differ fails it too (the list cannot go stale).
//
// To record master (needs a checkout of master with this file copied into internal/gateway):
//
//	SF_DIFF_RECORD=/path/to/dir go test -run TestDifferentialRecord -count=1 ./internal/gateway
//
// The test is skipped with -short (it runs 39 shapes three times, about a minute).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/pkg/protocol"
)

type diffWorker struct {
	id       string
	mode     string
	conc     int
	active   int // reported active requests
	state    protocol.WorkerState
	eligible bool
	model    string
	hangFor  time.Duration
}

type diffShape struct {
	name     string
	workers  []diffWorker
	requests []diffReq
	static   bool
	cpDown   bool
	setup    func(cfg *config.Config)
}

type diffReq struct {
	method, path string
	body         string
	stream       bool
	cancelAfter  time.Duration
	big          int
	model        string
}

// diffOut is what one shape produced. It is the file format of the recorded master outputs.
type diffOut struct {
	Requests []diffResult `json:"requests"`
	Series   []string     `json:"series"`
	Logs     []string     `json:"logs"`
}

type diffResult struct {
	Request string            `json:"request"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body"`
	Error   string            `json:"error,omitempty"`
}

func diffHandler(w diffWorker) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			rw.WriteHeader(404)
			return
		}
		b, _ := io.ReadAll(r.Body)
		stream := bytes.Contains(b, []byte(`"stream":true`))
		switch w.mode {
		case "ok":
			if stream {
				rw.Header().Set("Content-Type", "text/event-stream")
				rw.WriteHeader(200)
				for i := 0; i < 3; i++ {
					fmt.Fprintf(rw, "data: {\"worker\":%q,\"i\":%d}\n\n", w.id, i)
					rw.(http.Flusher).Flush()
				}
				fmt.Fprint(rw, "data: [DONE]\n\n")
				return
			}
			rw.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(rw, `{"worker":%q,"usage":{"prompt_tokens":3,"completion_tokens":2}}`, w.id)
		case "503", "503-queue_full", "503-draining", "503-mock":
			code := map[string]string{"503": "x", "503-queue_full": "queue_full", "503-draining": "draining", "503-mock": "mock_failure"}[w.mode]
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(503)
			fmt.Fprintf(rw, `{"error":{"message":"busy","type":"server_error","code":%q}}`, code)
		case "502", "500", "504", "400", "404", "429":
			var n int
			fmt.Sscanf(w.mode, "%d", &n)
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(n)
			fmt.Fprintf(rw, `{"error":{"message":"m","type":"t","code":"c%d"}}`, n)
		case "reset":
			hj, _ := rw.(http.Hijacker)
			c, _, _ := hj.Hijack()
			_ = c.Close()
		case "empty-stream":
			rw.Header().Set("Content-Type", "text/event-stream")
			rw.WriteHeader(200)
			rw.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		case "midstream":
			rw.Header().Set("Content-Type", "text/event-stream")
			rw.WriteHeader(200)
			fmt.Fprintf(rw, "data: {\"worker\":%q,\"i\":0}\n\n", w.id)
			rw.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		case "midstream-json":
			rw.Header().Set("Content-Type", "application/json")
			rw.Header().Set("Content-Length", "500")
			rw.WriteHeader(200)
			fmt.Fprint(rw, `{"partial":`)
			rw.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		case "hang":
			select {
			case <-time.After(w.hangFor):
			case <-r.Context().Done():
			}
		case "slow-first-byte":
			rw.Header().Set("Content-Type", "text/event-stream")
			rw.WriteHeader(200)
			rw.(http.Flusher).Flush()
			select {
			case <-time.After(w.hangFor):
			case <-r.Context().Done():
				return
			}
			fmt.Fprint(rw, "data: [DONE]\n\n")
		case "redirect":
			rw.Header().Set("Location", "http://example.invalid/")
			rw.WriteHeader(302)
		}
	})
}

// diffShapes are the request shapes. r13 is the one a fault-free run can tell apart from master (D25 item 1).
func diffShapes() []diffShape {
	ok := func(id string) diffWorker {
		return diffWorker{id: id, mode: "ok", conc: 4, state: protocol.StateReady, eligible: true, model: "qwen-7b"}
	}
	with := func(w diffWorker, m string) diffWorker { w.mode = m; return w }
	two := func(a, b string) []diffWorker { return []diffWorker{with(ok("a-1"), a), with(ok("b-2"), b)} }
	chat := diffReq{method: "POST", path: "/v1/chat/completions"}
	stream := diffReq{method: "POST", path: "/v1/chat/completions", stream: true}
	n := func(r diffReq, k int) []diffReq {
		out := make([]diffReq, k)
		for i := range out {
			out[i] = r
		}
		return out
	}
	full := func(id string) diffWorker { w := ok(id); w.active = w.conc; return w }
	drain := func(id string) diffWorker { w := ok(id); w.state, w.eligible = protocol.StateDraining, false; return w }
	hang := func(id string, d time.Duration) diffWorker { w := ok(id); w.mode, w.hangFor = "hang", d; return w }
	return []diffShape{
		{name: "r01-ok-json", workers: two("ok", "ok"), requests: n(chat, 4)},
		{name: "r02-ok-stream", workers: two("ok", "ok"), requests: n(stream, 3)},
		{name: "r03-unknown-model", workers: two("ok", "ok"), requests: []diffReq{{method: "POST", path: "/v1/chat/completions", model: "nope"}}},
		{name: "r04-bad-json", workers: two("ok", "ok"), requests: []diffReq{{method: "POST", path: "/v1/chat/completions", body: "{not json"}}},
		{name: "r05-missing-fields", workers: two("ok", "ok"), requests: []diffReq{{method: "POST", path: "/v1/chat/completions", body: `{"model":"qwen-7b"}`}}},
		{name: "r06-retry-reset", workers: two("reset", "ok"), requests: n(chat, 3)},
		{name: "r07-retry-502", workers: two("502", "ok"), requests: n(chat, 3)},
		{name: "r08-retry-503", workers: two("503", "ok"), requests: n(chat, 3)},
		{name: "r09-retry-503-queue-full", workers: two("503-queue_full", "ok"), requests: n(chat, 3)},
		{name: "r10-single-503", workers: []diffWorker{with(ok("a-1"), "503")}, requests: n(chat, 3)},
		{name: "r11-both-503", workers: two("503", "503"), requests: n(chat, 3)},
		{name: "r12-retry-empty-stream", workers: two("empty-stream", "ok"), requests: n(stream, 3)},
		{name: "r13-all-full", workers: []diffWorker{full("a-1"), full("b-2")}, requests: n(chat, 2)},
		{name: "r14-draining-only", workers: []diffWorker{drain("a-1")}, requests: n(chat, 2)},
		{name: "r15-500", workers: two("500", "ok"), requests: n(chat, 4)},
		{name: "r16-504", workers: two("504", "ok"), requests: n(chat, 3)},
		{name: "r17-400", workers: two("400", "400"), requests: n(chat, 2)},
		{name: "r18-404-429", workers: two("404", "429"), requests: n(chat, 2)},
		{name: "r19-midstream-stream", workers: []diffWorker{with(ok("a-1"), "midstream")}, requests: n(stream, 2)},
		{name: "r20-midstream-json", workers: []diffWorker{with(ok("a-1"), "midstream-json")}, requests: n(chat, 2)},
		{name: "r21-header-timeout", workers: []diffWorker{hang("a-1", 3*time.Second), ok("b-2")}, requests: n(chat, 2)},
		{name: "r22-first-byte-stall", workers: []diffWorker{func() diffWorker { w := ok("a-1"); w.mode, w.hangFor = "slow-first-byte", 3*time.Second; return w }(), ok("b-2")}, requests: n(stream, 2)},
		{name: "r23-client-disconnect", workers: []diffWorker{hang("a-1", 3*time.Second)}, requests: []diffReq{{method: "POST", path: "/v1/chat/completions", cancelAfter: 150 * time.Millisecond}}},
		{name: "r24-redirect", workers: two("redirect", "ok"), requests: n(chat, 2)},
		{name: "r25-models", workers: two("ok", "ok"), requests: []diffReq{{method: "GET", path: "/v1/models"}}},
		{name: "r26-healthz-readyz", workers: two("ok", "ok"), requests: []diffReq{{method: "GET", path: "/healthz"}, {method: "GET", path: "/readyz"}}},
		{name: "r27-method-and-path", workers: two("ok", "ok"), requests: []diffReq{{method: "GET", path: "/v1/chat/completions"}, {method: "POST", path: "/nope"}, {method: "DELETE", path: "/v1/models"}}},
		{name: "r28-body-too-large", workers: two("ok", "ok"), requests: []diffReq{{method: "POST", path: "/v1/chat/completions", big: (1 << 20) + 4096}}},
		{name: "r29-no-snapshot", workers: two("ok", "ok"), cpDown: true, requests: []diffReq{chat, {method: "GET", path: "/readyz"}}},
		{name: "r30-no-workers", workers: nil, requests: []diffReq{chat, {method: "GET", path: "/v1/models"}}},
		{name: "r31-max-attempts-1", workers: two("503", "ok"), requests: n(chat, 2), setup: func(c *config.Config) { c.Gateway.MaxAttempts = 1 }},
		{name: "r32-max-attempts-3", workers: []diffWorker{with(ok("a-1"), "503"), with(ok("b-2"), "502"), ok("c-3")}, requests: n(chat, 3), setup: func(c *config.Config) { c.Gateway.MaxAttempts = 3 }},
		{name: "r33-mixed-models", workers: []diffWorker{ok("a-1"), func() diffWorker { w := ok("b-2"); w.model = "llama-8b"; return w }()}, requests: []diffReq{chat, {method: "POST", path: "/v1/chat/completions", model: "llama-8b"}}},
		{name: "r34-reported-full-one", workers: []diffWorker{full("a-1"), ok("b-2")}, requests: n(chat, 4)},
		{name: "r35-lost-model", workers: []diffWorker{func() diffWorker { w := ok("a-1"); w.state, w.eligible = protocol.StateFailed, false; return w }()}, requests: n(chat, 2)},
		{name: "s01-static-ok", static: true, workers: []diffWorker{ok("static")}, requests: []diffReq{chat, stream}},
		{name: "s02-static-503", static: true, workers: []diffWorker{with(ok("static"), "503")}, requests: n(chat, 2)},
		{name: "s03-static-unknown-model", static: true, workers: []diffWorker{ok("static")}, requests: []diffReq{{method: "POST", path: "/v1/chat/completions", model: "nope"}}},
		{name: "s04-static-midstream", static: true, workers: []diffWorker{with(ok("static"), "midstream")}, requests: n(stream, 2)},
	}
}

var (
	diffReID   = regexp.MustCompile(`(req|att|ev)_[0-9a-f]{8,}`)
	diffRePort = regexp.MustCompile(`127\.0\.0\.1:\d+`)
	diffReTime = regexp.MustCompile(`\d{4}-\d\d-\d\dT[\d:.]+(Z|[+-]\d\d:\d\d)`)
)

func diffNorm(s string) string {
	s = diffReID.ReplaceAllString(s, "$1_ID")
	s = diffRePort.ReplaceAllString(s, "127.0.0.1:PORT")
	return diffReTime.ReplaceAllString(s, "TIME")
}

type diffBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *diffBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *diffBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// diffSetMode sets resilience.mode by reflection so that this file also compiles on a tree without the section (master).
// An empty mode leaves the build's default.
func diffSetMode(cfg *config.Config, mode string) {
	if mode == "" {
		return
	}
	if f := reflect.ValueOf(cfg).Elem().FieldByName("Resilience"); f.IsValid() {
		f.FieldByName("Mode").SetString(mode)
	}
}

// diffRun drives one shape through a gateway built with the given resilience mode ("" = the build's default).
func diffRun(t *testing.T, sh diffShape, mode string) diffOut {
	t.Helper()
	var workers []protocol.WorkerSnapshot
	var servers []*httptest.Server
	for _, w := range sh.workers {
		srv := httptest.NewServer(diffHandler(w))
		servers = append(servers, srv)
		snap := protocol.WorkerSnapshot{
			WorkerID: w.id, Model: w.model, Address: srv.URL, MaxConcurrency: w.conc, QueueSize: 8, State: w.state,
			Health: protocol.HealthHealthy, Eligible: w.eligible, RegisteredAt: time.Unix(1_700_000_000, 0).UTC(),
		}
		snap.Metrics.ActiveRequests = w.active
		if w.state == protocol.StateFailed {
			snap.Health = protocol.HealthUnhealthy
		}
		workers = append(workers, snap)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()
	polled := make(chan struct{}, 1)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case polled <- struct{}{}:
		default:
		}
		if sh.cpDown {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"workers": workers})
	}))
	defer cp.Close()

	cfg := config.Default()
	cfg.Gateway.UpstreamHeaderTimeout = time.Second
	cfg.Gateway.UpstreamIdleTimeout = time.Second
	cfg.Gateway.ShutdownTimeout = time.Second
	cfg.Gateway.RegistryRefresh = time.Hour
	cfg.Gateway.MaxRequestBytes = 1 << 20
	diffSetMode(&cfg, mode)
	logs := &diffBuf{}
	log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var gw *gateway.Server
	if sh.static {
		cfg.Gateway.UpstreamURL = servers[0].URL
		cfg.Gateway.Models = []string{"qwen-7b"}
		gw = gateway.NewFromConfig(cfg, log)
	} else {
		cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
		cfg.Gateway.ControlPlaneURL = cp.URL
		if sh.setup != nil {
			sh.setup(&cfg)
		}
		var err error
		if gw, err = gateway.NewRegistry(cfg, log); err != nil {
			t.Error(err)
			return diffOut{}
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Error(err)
		return diffOut{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	base := "http://" + ln.Addr().String()
	if !sh.static {
		select {
		case <-polled:
		case <-time.After(5 * time.Second):
			cancel()
			t.Error("the gateway never polled the registry")
			return diffOut{}
		}
		time.Sleep(150 * time.Millisecond)
	}

	var results []diffResult
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	for _, rs := range sh.requests {
		body := rs.body
		model := rs.model
		if model == "" {
			model = "qwen-7b"
		}
		if body == "" && rs.method == "POST" {
			body = fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}]`, model)
			if rs.stream {
				body += `,"stream":true`
			}
			body += "}"
		}
		if rs.big > 0 {
			body = `{"model":"qwen-7b","messages":[{"role":"user","content":"` + strings.Repeat("a", rs.big) + `"}]}`
		}
		rctx := context.Background()
		var rcancel context.CancelFunc = func() {}
		if rs.cancelAfter > 0 {
			rctx, rcancel = context.WithTimeout(rctx, rs.cancelAfter)
		}
		req, _ := http.NewRequestWithContext(rctx, rs.method, base+rs.path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		out := diffResult{Request: rs.method + " " + rs.path + fmt.Sprintf(" model=%s stream=%v", model, rs.stream)}
		resp, err := client.Do(req)
		if err != nil {
			out.Error = diffNorm(err.Error())
			if rs.cancelAfter > 0 {
				out.Error = "client gave up"
			}
		} else {
			out.Status = resp.StatusCode
			out.Headers = map[string]string{}
			for k, v := range resp.Header {
				switch k {
				case "Date", "X-Request-Id", "X-Request-ID":
					continue
				}
				out.Headers[k] = diffNorm(strings.Join(v, ","))
			}
			b, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			out.Body = diffNorm(string(b))
			if rerr != nil {
				out.Body += " [read error: " + diffNorm(rerr.Error()) + "]"
			}
		}
		rcancel()
		results = append(results, out)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond) // let the attempt and request lines land

	var series []string
	fams, _ := gw.MetricsGatherer().Gather()
	for _, f := range fams {
		if strings.HasPrefix(f.GetName(), "go_") || strings.HasPrefix(f.GetName(), "process_") || strings.HasPrefix(f.GetName(), "promhttp_") {
			continue
		}
		for _, m := range f.GetMetric() {
			var lb []string
			for _, l := range m.GetLabel() {
				lb = append(lb, l.GetName()+"="+diffNorm(l.GetValue()))
			}
			sort.Strings(lb)
			key := f.GetName() + "{" + strings.Join(lb, ",") + "}"
			switch {
			case m.GetCounter() != nil:
				series = append(series, fmt.Sprintf("%s counter %v", key, m.GetCounter().GetValue()))
			case m.GetGauge() != nil:
				if strings.Contains(f.GetName(), "age") || strings.Contains(f.GetName(), "uptime") {
					series = append(series, key+" gauge ~")
				} else {
					series = append(series, fmt.Sprintf("%s gauge %v", key, m.GetGauge().GetValue()))
				}
			case m.GetHistogram() != nil:
				series = append(series, fmt.Sprintf("%s histogram count=%d", key, m.GetHistogram().GetSampleCount()))
			}
		}
	}
	sort.Strings(series)

	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) != nil {
			lines = append(lines, diffNorm(l))
			continue
		}
		for _, k := range []string{"time", "duration_ms", "ttft_ms", "trace_id", "span_id", "select_ms", "latency_ms", "rate_limit_ms"} {
			delete(m, k)
		}
		b, _ := json.Marshal(m)
		s := diffNorm(string(b))
		if strings.Contains(s, "gateway starting") || strings.Contains(s, "gateway shutting down") || strings.Contains(s, "metrics endpoint") {
			continue
		}
		lines = append(lines, s)
	}
	sort.Strings(lines)

	cancel()
	<-done
	return diffOut{Requests: results, Series: series, Logs: lines}
}

// TestDifferentialRecord writes this build's outputs for every shape into $SF_DIFF_RECORD. It is how the master recordings were made.
func TestDifferentialRecord(t *testing.T) {
	dir := os.Getenv("SF_DIFF_RECORD")
	if dir == "" {
		t.Skip("SF_DIFF_RECORD is not set (this test records the outputs of the build under test)")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, sh := range diffShapes() {
		sh := sh
		t.Run(sh.name, func(t *testing.T) {
			b, _ := json.MarshalIndent(diffRun(t, sh, ""), "", " ")
			if err := os.WriteFile(filepath.Join(dir, sh.name+".json"), append(b, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func diffMaster(t *testing.T, name string) diffOut {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "differential", "master", name+".json"))
	if err != nil {
		t.Fatalf("the recorded master output of %s: %v", name, err)
	}
	var o diffOut
	if err := json.Unmarshal(b, &o); err != nil {
		t.Fatal(err)
	}
	return o
}

// diffWhat lists which parts of two outputs differ.
func diffWhat(a, b diffOut) []string {
	var out []string
	if !reflect.DeepEqual(a.Requests, b.Requests) {
		out = append(out, "requests")
	}
	if !reflect.DeepEqual(a.Series, b.Series) {
		out = append(out, "series")
	}
	if !reflect.DeepEqual(a.Logs, b.Logs) {
		out = append(out, "logs")
	}
	return out
}

// diffRunAll runs every shape (four at a time: each is self-contained) and returns the outputs by name.
func diffRunAll(t *testing.T, mode string) map[string]diffOut {
	t.Helper()
	shapes := diffShapes()
	var mu sync.Mutex
	got := map[string]diffOut{}
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for _, sh := range shapes {
		sh := sh
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			o := diffRun(t, sh, mode)
			mu.Lock()
			got[sh.name] = o
			mu.Unlock()
		}()
	}
	wg.Wait()
	return got
}

func skipDifferentialInShortMode(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("the differential runs 39 shapes per mode (about 30 s each); run without -short")
	}
}

// With resilience.mode off, and with static workers in any mode, the gateway is byte-identical to master (acceptance 4a):
// status, headers, bodies, normalised logs and every metric series with its value.
func TestDifferentialModeOffAndStaticAreIdenticalToMaster(t *testing.T) {
	skipDifferentialInShortMode(t)
	for _, mode := range []string{"off"} { // a literal: this file also compiles on master, which has no such constant
		got := diffRunAll(t, mode)
		for _, sh := range diffShapes() {
			if what := diffWhat(diffMaster(t, sh.name), got[sh.name]); len(what) > 0 {
				t.Errorf("mode %s, shape %s differs from master in %v", mode, sh.name, what)
			}
		}
	}
}

// d25Differences are the shapes in which the shipped default (on, registry workers) may differ from master beyond the
// documented additions, with the D25 items that explain it. Static shapes never differ.
var d25Differences = map[string]string{
	"r13-all-full": "D25 item 1: every eligible worker is full, so the request waits up to queue.max_wait (3 s) and is then shed 503 NO_CAPACITY with Retry-After from the recent waits, and the shed is not a scheduling decision",
}

// diffNormalizeOn removes the documented additions from a default-on output so that it can be compared with master:
// the series the feature registers (D25 item 11, the names of the resilience golden file) and the verdict, probe and
// retry_denied fields of attempt log lines (see "Additional visible details" in docs/operations/resilience.md).
func diffNormalizeOn(t *testing.T, o diffOut, resilienceSeries map[string]bool) diffOut {
	t.Helper()
	var out diffOut
	out.Requests = o.Requests
	for _, s := range o.Series {
		name, _, _ := strings.Cut(s, "{")
		if !resilienceSeries[name] {
			out.Series = append(out.Series, s)
		}
	}
	for _, l := range o.Logs {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) != nil {
			out.Logs = append(out.Logs, l)
			continue
		}
		for _, k := range []string{"verdict", "probe", "retry_denied"} {
			delete(m, k)
		}
		b, _ := json.Marshal(m)
		out.Logs = append(out.Logs, string(b))
	}
	sort.Strings(out.Logs)
	return out
}

func resilienceSeriesNames(t *testing.T) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "metrics_series_resilience.golden"))
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		name, _, _ := strings.Cut(l, " | ")
		names[strings.TrimSpace(name)] = true
	}
	// Series the feature registers that the golden file lists under another name (the breaker series are all in it).
	return names
}

// The shipped default (on, registry workers) differs from master exactly as D25 documents and in nothing else (acceptance 4b, 24):
// a fault-free run with free capacity is identical apart from the new series and the attempt-line fields, and the only shape
// whose responses change is the full-model one (item 1). Static workers are identical whatever the mode.
func TestDifferentialDefaultOnDiffersFromMasterExactlyByTheD25List(t *testing.T) {
	skipDifferentialInShortMode(t)
	got := diffRunAll(t, "") // the build's default
	series := resilienceSeriesNames(t)
	seenDifference := map[string]bool{}
	for _, sh := range diffShapes() {
		master := diffMaster(t, sh.name)
		on := diffNormalizeOn(t, got[sh.name], series)
		what := diffWhat(master, on)
		if sh.static {
			if what2 := diffWhat(master, got[sh.name]); len(what2) > 0 {
				t.Errorf("static shape %s differs from master in %v: the feature must be a no-op with static workers", sh.name, what2)
			}
			continue
		}
		if reason, listed := d25Differences[sh.name]; listed {
			if len(what) == 0 {
				t.Errorf("shape %s is listed as differing (%s) but is identical to master: the list is stale", sh.name, reason)
			}
			seenDifference[sh.name] = true
			continue
		}
		if len(what) > 0 {
			t.Errorf("shape %s differs from master in %v and is not in the D25 list:\nmaster: %+v\non:     %+v", sh.name, what, master, on)
		}
	}
	for name := range d25Differences {
		if !seenDifference[name] {
			t.Errorf("%s is in the D25 table but is not a shape", name)
		}
	}

	// Item 1 in detail: the full model waits for the queue and is refused with 503 NO_CAPACITY, never 429, with Retry-After.
	r13 := got["r13-all-full"]
	for i, r := range r13.Requests {
		if r.Status != http.StatusServiceUnavailable || !strings.Contains(r.Body, `"code":"NO_CAPACITY"`) {
			t.Errorf("r13 request %d: status %d body %s, want 503 NO_CAPACITY", i, r.Status, r.Body)
		}
		if ra := r.Headers["Retry-After"]; ra != "3" && ra != "4" {
			t.Errorf("r13 request %d: Retry-After %q, want the rounded-up recent wait (3 or 4 s for a 3 s queue wait)", i, ra)
		}
	}
	if !strings.Contains(strings.Join(r13.Series, "\n"), `admission_rejections_total{model=qwen-7b,reason=queue_timeout} counter 2`) ||
		!strings.Contains(strings.Join(r13.Series, "\n"), `inference_queue_duration_seconds{model=qwen-7b} histogram count=2`) {
		t.Errorf("r13: the sheds and the waits must be counted:\n%s", strings.Join(r13.Series, "\n"))
	}
	for _, s := range r13.Series {
		if strings.HasPrefix(s, "scheduler_decisions_total") && strings.Contains(s, "no_capacity") {
			t.Errorf("r13: a shed must not be a scheduling decision: %s", s)
		}
	}
}

// d25Evidence names, for each item of the D25 list that the plan asks to be asserted, the tests that pin it. The test below
// fails when one of them disappears or is renamed, so the table in ADR-020 and the operations guide cannot drift from the code.
var d25Evidence = map[string][]struct{ dir, test string }{
	"1 a request that finds every worker full waits up to 3 s": {
		{".", "TestAWaiterPastMaxWaitIsShedWithTheRetryAfterOfRecentWaits"}, {".", "TestAWaiterIsHandedTheFreedSlotEvenWhenTheReportedCountIsStale"},
		{".", "TestDifferentialDefaultOnDiffersFromMasterExactlyByTheD25List"}},
	"2 retries are subject to the budget": {
		{".", "TestADeniedRetryFiresNoAttemptAndRelaysTheFailedResponse"}, {".", "TestConcurrentRetriesNeverOvershootTheBudget"}},
	"3 a worker with sustained faults is ejected and probed": {
		{".", "TestTheBreakerEjectsProbesAndClosesAFailingWorker"}, {".", "TestTheEjectionCapIsPerModel"}, {"../../tests/integration", "TestProcessGatewayEjectsAFlakyWorkerByDefault"}},
	"4 admission.max_global_requests is live": {
		{".", "TestTheGlobalCapRefusesNewArrivalsAtOnce"}, {"../../tests/chaos", "TestTheGlobalCapBoundsTheRequestsInsideTheGateway"}},
	"7 a retried 503 pays a bounded body peek": {
		{".", "TestAStalledPeekCostsAtMostThePeekBound"}, {".", "TestATerminal503IsRelayedWithoutAPeek"}},
	"8 SIGTERM fails readyz and serves for the grace": {
		{"../../tests/integration", "TestProcessGatewayDrainSequence"}, {"../../tests/integration", "TestProcessGatewayDrainSecondSignalExitsAtOnce"}},
	"11 new series, dashboard, alerts; a probe can cost a 504": {
		{".", "TestMetricsSeriesGoldenRegistryResilienceAdditions"}, {".", "TestAProbeUsesItsOwnHeaderTimeout"}, {"../observability", "TestFiveDashboardsWithTheSpecPanels"}},
	"default on, effective in registry mode only": {
		{"../config", "TestResilienceDefaults"}, {"../config", "TestResilienceDefaultOnAndTheKillSwitch"}, {".", "TestDifferentialModeOffAndStaticAreIdenticalToMaster"}},
}

func TestEveryD25ItemNamesATestThatExists(t *testing.T) {
	for item, tests := range d25Evidence {
		for _, tc := range tests {
			if !diffTestExists(t, tc.dir, tc.test) {
				t.Errorf("D25 item %q: the test %s does not exist in %s", item, tc.test, tc.dir)
			}
		}
	}
}

func diffTestExists(t *testing.T, dir, name string) bool {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool { return strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkgs {
		for _, f := range p.Files {
			for _, d := range f.Decls {
				if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
					return true
				}
			}
		}
	}
	return false
}

// The D25 list is repeated verbatim in the plan, ADR-020, the operations guide and the README (acceptance 24): the plan's items are
// the reference, and each of the other documents must contain every item word for word.
func TestD25ListIsRepeatedVerbatimInTheDocuments(t *testing.T) {
	plan, err := os.ReadFile("../../docs/plans/completed/phase-15-fault-tolerance.md")
	if err != nil {
		t.Fatal(err)
	}
	var items []string
	inD25 := false
	for _, l := range strings.Split(string(plan), "\n") {
		if strings.HasPrefix(l, "- **D25 ") {
			inD25 = true
			continue
		}
		if !inD25 {
			continue
		}
		if m := regexp.MustCompile(`^  (\d+)\. (.+)$`).FindStringSubmatch(l); m != nil {
			items = append(items, m[1]+". "+m[2])
			continue
		}
		if strings.HasPrefix(l, "- **D26") {
			break
		}
	}
	if len(items) != 11 {
		t.Fatalf("found %d D25 items in the plan, want 11", len(items))
	}
	for _, doc := range []string{"docs/decisions/ADR-020-fault-tolerance.md", "docs/operations/resilience.md", "README.md"} {
		b, err := os.ReadFile(filepath.Join("../..", doc))
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		for _, it := range items {
			if !strings.Contains(text, it) {
				t.Errorf("%s lacks D25 item %q verbatim", doc, it)
			}
		}
	}
}
