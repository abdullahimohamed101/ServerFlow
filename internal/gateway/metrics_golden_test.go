package gateway

import (
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/ratelimit"
	"serverflow/pkg/protocol"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/metrics_series*.golden from the current code")

// seriesCatalogue describes the gateway's own Prometheus families: name, type, help, label names and (for
// histograms) bucket bounds. Go runtime and process collectors are left out because they vary by host and
// Go version. It is the contract the observer refactor must keep (prep plan, acceptance criterion 2).
func seriesCatalogue(t *testing.T, s *Server) string {
	t.Helper()
	fams, err := s.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, f := range fams {
		name := f.GetName()
		if strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_") {
			continue
		}
		labels := map[string]bool{}
		buckets := ""
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = true
			}
			if h := m.GetHistogram(); h != nil && buckets == "" {
				var bs []string
				for _, b := range h.GetBucket() {
					bs = append(bs, strconv.FormatFloat(b.GetUpperBound(), 'g', -1, 64))
				}
				buckets = strings.Join(bs, ",")
			}
		}
		var names []string
		for l := range labels {
			names = append(names, l)
		}
		sort.Strings(names)
		lines = append(lines, strings.Join([]string{name, f.GetType().String(), f.GetHelp(), "labels=" + strings.Join(names, ","), "buckets=" + buckets}, " | "))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

func checkGolden(t *testing.T, file, got string) {
	t.Helper()
	if *updateGolden {
		if err := os.WriteFile(file, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("the /metrics series changed (name | type | help | labels | buckets).\nwant:\n%s\ngot:\n%s", want, got)
	}
}

// limiterScript lets one fake limiter produce every outcome the gateway counts.
func limiterScript(mode *atomic.Value, lim *fakeLimiter) {
	lim.decide = func(ratelimit.Request) (ratelimit.Decision, error) {
		switch mode.Load().(string) {
		case "unavailable":
			return ratelimit.Decision{}, errors.New("redis down")
		case "refuse":
			return ratelimit.Decision{Allowed: false, Limit: ratelimit.LimitRequests, RetryAfter: time.Second}, nil
		case "bypass":
			return lim.admit(true), nil
		}
		return lim.admit(false), nil
	}
}

func sseOrJSON(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	if strings.Contains(string(b), `"stream":true`) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: {\"i\":0}\n\ndata: [DONE]\n\n")
		return
	}
	okUpstream(w, r)
}

func driveChats(t *testing.T, do func(key, body string) int, key string, mode *atomic.Value) {
	t.Helper()
	plain := `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`
	stream := `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	do(key, plain)
	do(key, stream)
	do(key, `{"model":"nope","messages":[{"role":"user","content":"hi"}]}`) // unknown model
	do("", plain)                                                           // no key: auth refusal
	for _, m := range []string{"refuse", "bypass", "unavailable"} {
		mode.Store(m)
		do(key, plain)
	}
	mode.Store("allow")
}

// TestMetricsSeriesGoldenStatic pins the series a static-mode gateway with auth and rate limiting exports.
func TestMetricsSeriesGoldenStatic(t *testing.T) {
	lim := &fakeLimiter{}
	var mode atomic.Value
	mode.Store("allow")
	limiterScript(&mode, lim)
	env := newLimitEnv(t, sseOrJSON, lim)
	key := env.store.add("acme", nil, quotas(100, 100000, 10))
	driveChats(t, func(k, body string) int {
		hdr := []string{}
		if k != "" {
			hdr = []string{"Authorization", "Bearer " + k}
		}
		resp, _ := env.do(t, http.MethodPost, chatCompletionsPath, body, hdr...)
		return resp.StatusCode
	}, key, &mode)
	if resp, _ := env.do(t, http.MethodGet, "/metrics", ""); resp.StatusCode != 200 {
		t.Fatalf("/metrics status %d", resp.StatusCode)
	}
	checkGolden(t, "testdata/metrics_series_static.golden", seriesCatalogue(t, env.gw))
}

// TestMetricsSeriesGoldenRegistry pins the series a registry-mode gateway with auth and rate limiting
// exports, including attempts and retries (one worker refuses connections, the other answers).
func TestMetricsSeriesGoldenRegistry(t *testing.T) {
	good := newScriptWorker(t, "w-good", sseOrJSON)
	bad := refusing(t, "w-bad")
	env := newRegEnv(t, "round-robin", []protocol.WorkerSnapshot{bad, good.snapshot("qwen-7b")})
	logs := &lockedBuffer{}
	store := newAuthStore()
	env.gw.SetAuthenticator(newAuthenticator(store, logs, &testClock{t: time.Now()}))
	lim := &fakeLimiter{}
	var mode atomic.Value
	mode.Store("allow")
	limiterScript(&mode, lim)
	env.gw.SetLimiter(lim)
	key := store.add("acme", []string{"qwen-7b"}, quotas(100, 100000, 10))
	driveChats(t, func(k, body string) int {
		req, _ := http.NewRequest(http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if k != "" {
			req.Header.Set("Authorization", "Bearer "+k)
		}
		resp, err := env.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}, key, &mode)
	for i := 0; i < 2; i++ { // round-robin: make sure a request starts on the refusing worker
		req, _ := http.NewRequest(http.MethodPost, env.url+chatCompletionsPath, strings.NewReader(chatBody("qwen-7b")))
		req.Header.Set("Authorization", "Bearer "+key)
		if resp, err := env.client.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}
	got := seriesCatalogue(t, env.gw)
	for _, want := range []string{"inference_retries_total", "inference_attempts_total", "rate_limit_bypassed_total", "auth_rejections_total"} {
		if !*updateGolden && !strings.Contains(got, want) {
			t.Fatalf("the scenario did not exercise %s:\n%s", want, got)
		}
	}
	checkGolden(t, "testdata/metrics_series_registry.golden", got)
}
