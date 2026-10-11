package integration

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/tracing"
	"serverflow/internal/tracing/gwtrace"
)

// TestTracingOverhead measures what tracing adds to a streaming request through a real gateway over real
// HTTP, against a zero-latency upstream: tracing off, on but unsampled, and on at 100% sampling with the
// real export pipeline sending to a local receiver. It is opt-in, like the Phase 8 benchmark:
//
//	SERVERFLOW_TRACING_OVERHEAD=1 go test -run TestTracingOverhead -v -count=1 -timeout 600s ./tests/integration
//
// Modes are interleaved round by round so drift on the machine hits all of them alike. The numbers are
// recorded in docs/benchmarks/phase-11-tracing.md.
func TestTracingOverhead(t *testing.T) {
	if os.Getenv("SERVERFLOW_TRACING_OVERHEAD") != "1" {
		t.Skip("opt-in: set SERVERFLOW_TRACING_OVERHEAD=1")
	}
	const (
		rounds      = 5
		warmup      = 300
		requests    = 3000
		concurrency = 16
	)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer up.Close()
	col := newCollector(t, "ok")

	start := func(mode string) (url string, stop func()) {
		cfg := config.Default().Gateway
		cfg.UpstreamURL, cfg.Models = up.URL, []string{model}
		var opts []gateway.Option
		var prov *tracing.Provider
		if mode != "off" {
			ratio := 1.0
			if mode == "unsampled" {
				ratio = 0
			}
			var err error
			prov, err = tracing.Setup(tracing.Config{Endpoint: col.srv.URL, Sampler: tracing.SamplerRatio, SampleRatio: ratio, QueueSize: 2048,
				MaxExportBatch: 512, BatchTimeout: 5 * time.Second, ExportTimeout: 5 * time.Second}, tracing.Service{Name: "serverflow-gateway"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			opts = append(opts, gateway.WithObserver(gwtrace.New(prov, gwtrace.Options{IncludeTenantID: true})))
		}
		srv := httptest.NewServer(gateway.New(cfg, quiet(), opts...).Handler())
		return srv.URL, func() {
			srv.Close()
			if prov != nil {
				_ = prov.Shutdown(t.Context())
			}
		}
	}

	body := `{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	run := func(url string, n int) []time.Duration {
		c := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true}}
		out := make([]time.Duration, n)
		next := make(chan int, n)
		for i := 0; i < n; i++ {
			next <- i
		}
		close(next)
		var wg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range next {
					t0 := time.Now()
					resp, err := c.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
					if err != nil {
						t.Errorf("request failed: %v", err)
						return
					}
					br := bufio.NewReader(resp.Body)
					_, _ = br.ReadString('\n') // the first chunk
					out[i] = time.Since(t0)
					_, _ = io.Copy(io.Discard, br)
					_ = resp.Body.Close()
				}
			}()
		}
		wg.Wait()
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	at := func(d []time.Duration, q float64) time.Duration { return d[int(float64(len(d)-1)*q)] }

	modes := []string{"off", "unsampled", "sampled"}
	p50, p95, p99 := map[string][]time.Duration{}, map[string][]time.Duration{}, map[string][]time.Duration{}
	for r := 0; r < rounds; r++ {
		for i := range modes {
			mode := modes[(i+r)%len(modes)]
			url, stop := start(mode)
			run(url, warmup)
			d := run(url, requests)
			stop()
			p50[mode], p95[mode], p99[mode] = append(p50[mode], at(d, .5)), append(p95[mode], at(d, .95)), append(p99[mode], at(d, .99))
		}
	}
	med := func(v []time.Duration) time.Duration {
		c := append([]time.Duration(nil), v...)
		sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
		return c[len(c)/2]
	}
	span := func(v []time.Duration) string {
		c := append([]time.Duration(nil), v...)
		sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
		return fmt.Sprintf("%v..%v", c[0], c[len(c)-1])
	}
	for _, m := range modes {
		t.Logf("%-9s TTFT p50 %v  p95 %v (rounds %s)  p99 %v", m, med(p50[m]), med(p95[m]), span(p95[m]), med(p99[m]))
	}
	for _, m := range modes[1:] {
		t.Logf("%-9s added p50 %v  added p95 %v (median of %d rounds; the per-round p95 spread of tracing off is %s)",
			m, med(p50[m])-med(p50["off"]), med(p95[m])-med(p95["off"]), rounds, span(p95["off"]))
	}
	if added := med(p95["sampled"]) - med(p95["off"]); added > time.Millisecond {
		t.Errorf("100%% sampling adds %v at p95, over the 1ms target (see the note for why this may be noise)", added)
	}
}
