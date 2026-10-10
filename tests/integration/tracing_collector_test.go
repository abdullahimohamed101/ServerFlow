package integration

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/gwtrace"
)

// The Phase 11 collector failure matrix: whatever the collector does, requests keep their status and their
// latency, the export queue stays bounded, the counters move, and the log does not flood.

type lockedLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

type collector struct {
	srv     *httptest.Server
	mode    atomic.Value
	hits    atomic.Int64
	release chan struct{}
}

func newCollector(t *testing.T, mode string) *collector {
	c := &collector{release: make(chan struct{})}
	c.mode.Store(mode)
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		switch c.mode.Load().(string) {
		case "500":
			http.Error(w, "no", http.StatusInternalServerError)
		case "hang":
			select {
			case <-c.release:
			case <-r.Context().Done():
			}
		case "slow":
			time.Sleep(800 * time.Millisecond)
		}
	}))
	t.Cleanup(func() { close(c.release); c.srv.Close() })
	return c
}

func staticGateway(t *testing.T, obs ...gateway.Option) string {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(up.Close)
	cfg := config.Default().Gateway
	cfg.UpstreamURL, cfg.Models = up.URL, []string{model}
	gw := gateway.New(cfg, quiet(), obs...)
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

// measure sends n sequential requests and returns their sorted latencies; every one must be a 200.
func measure(t *testing.T, url string, n int) []time.Duration {
	t.Helper()
	body := `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`
	c := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}
	out := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		resp, err := c.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status %d", i, resp.StatusCode)
		}
		out = append(out, time.Since(start))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func p(q float64, d []time.Duration) time.Duration { return d[int(float64(len(d)-1)*q)] }

func TestADeadCollectorNeverTouchesRequests(t *testing.T) {
	const n = 400
	base := measure(t, staticGateway(t), n) // tracing off

	closed, _ := net.Listen("tcp", "127.0.0.1:0")
	closedURL := "http://" + closed.Addr().String()
	_ = closed.Close()

	cases := map[string]string{"500": "", "hang": "", "slow": "", "closed port": closedURL}
	for name, fixed := range cases {
		t.Run(name, func(t *testing.T) {
			endpoint := fixed
			if endpoint == "" {
				endpoint = newCollector(t, name).srv.URL
			}
			logs := &lockedLog{}
			prov, err := tracing.Setup(tracing.Config{Endpoint: endpoint, Sampler: tracing.SamplerRatio, SampleRatio: 1, QueueSize: 32, MaxExportBatch: 8,
				BatchTimeout: 20 * time.Millisecond, ExportTimeout: 300 * time.Millisecond}, tracing.Service{Name: "serverflow-gateway"},
				slog.New(slog.NewTextHandler(logs, nil)))
			if err != nil {
				t.Fatal(err)
			}
			url := staticGateway(t, gateway.WithObserver(gwtrace.New(prov, gwtrace.Options{})))
			got := measure(t, url, n) // every request must still be a 200

			if on, off := p(.95, got), p(.95, base); on > 4*off+3*time.Millisecond {
				t.Errorf("p95 with a %s collector is %v against %v with tracing off", name, on, off)
			}
			st := prov.Stats()
			if st.Queued > 32 {
				t.Errorf("queue exceeded its bound: %+v", st)
			}
			if st.Dropped == 0 && st.Failed == 0 {
				t.Errorf("the failure left no trace in the counters: %+v", st)
			}
			if st.Exported != 0 {
				t.Errorf("nothing can be exported to a %s collector: %+v", name, st)
			}
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = prov.Shutdown(ctx)
			if d := time.Since(start); d > 2500*time.Millisecond {
				t.Errorf("shutdown took %v", d)
			}
			if lines := strings.Count(logs.String(), "trace export"); lines > 2 {
				t.Errorf("%d export log lines in one run, want at most 2:\n%s", lines, logs.String())
			}
			if strings.Contains(logs.String(), "127.0.0.1") {
				t.Errorf("the log names the collector address:\n%s", logs.String())
			}
		})
	}
}

func TestSpansReachACollectorThatRecovers(t *testing.T) {
	col := newCollector(t, "500")
	prov, err := tracing.Setup(tracing.Config{Endpoint: col.srv.URL, Sampler: tracing.SamplerRatio, SampleRatio: 1, QueueSize: 64, MaxExportBatch: 8,
		BatchTimeout: 20 * time.Millisecond, ExportTimeout: 300 * time.Millisecond}, tracing.Service{Name: "serverflow-gateway"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	url := staticGateway(t, gateway.WithObserver(gwtrace.New(prov, gwtrace.Options{})))
	measure(t, url, 20)
	waitFor(t, 5*time.Second, "failures to be counted", func() bool { return prov.Stats().Failures > 0 })
	col.mode.Store("ok")
	measure(t, url, 20)
	waitFor(t, 5*time.Second, "spans to be exported after recovery", func() bool { return prov.Stats().Exported > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = prov.Shutdown(ctx)
}
