package gateway

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Spec section 31: gateway overhead (excluding inference) < 25 ms p95.
const overheadBudgetP95 = 25 * time.Millisecond

const (
	overheadWarmup      = 200
	overheadRequests    = 3000
	overheadConcurrency = 16
)

type latencyStats struct{ p50, p95, p99 time.Duration }

func summarize(d []time.Duration) latencyStats {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(p float64) time.Duration { return d[int(float64(len(d)-1)*p)] }
	return latencyStats{at(.50), at(.95), at(.99)}
}

// measure runs total requests at the given concurrency, calling do for each,
// and returns the per-request latency it reports.
func measure(t *testing.T, total, concurrency int, do func() time.Duration) []time.Duration {
	t.Helper()
	out := make([]time.Duration, total)
	var wg sync.WaitGroup
	next := make(chan int, total)
	for i := 0; i < total; i++ {
		next <- i
	}
	close(next)
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				out[i] = do()
			}
		}()
	}
	wg.Wait()
	return out
}

func newBenchClient() *http.Client {
	return &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true}}
}

// TestGatewayOverhead measures the latency the gateway adds on top of a
// zero-latency upstream, for non-streaming requests (total latency) and
// streaming requests (time to first chunk). Run with -v to see the numbers;
// results are recorded in docs/benchmarks/phase-2-gateway-overhead.md.
func TestGatewayOverhead(t *testing.T) {
	if testing.Short() {
		t.Skip("overhead measurement skipped in -short mode")
	}

	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"stream":true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: {\"n\":1}\n\n")
			w.(http.Flusher).Flush()
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","object":"chat.completion","choices":[]}`)
	})
	fake := httptest.NewServer(upstream)
	defer fake.Close()
	env := newEnvForURL(t, fake.URL, fake)

	nonStream := `{"model":"qwen-7b","messages":[{"role":"user","content":"hello"}]}`
	stream := `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hello"}]}`

	timeNonStream := func(c *http.Client, url string) func() time.Duration {
		return func() time.Duration {
			start := time.Now()
			resp, err := c.Post(url+chatCompletionsPath, "application/json", strings.NewReader(nonStream))
			if err != nil {
				t.Errorf("request failed: %v", err)
				return 0
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return time.Since(start)
		}
	}
	timeFirstChunk := func(c *http.Client, url string) func() time.Duration {
		return func() time.Duration {
			start := time.Now()
			resp, err := c.Post(url+chatCompletionsPath, "application/json", strings.NewReader(stream))
			if err != nil {
				t.Errorf("request failed: %v", err)
				return 0
			}
			br := bufio.NewReader(resp.Body)
			_, _ = br.ReadString('\n') // first chunk
			d := time.Since(start)
			_, _ = io.Copy(io.Discard, br)
			_ = resp.Body.Close()
			return d
		}
	}

	direct, via := newBenchClient(), newBenchClient()
	run := func(name string, mk func(*http.Client, string) func() time.Duration) {
		measure(t, overheadWarmup, overheadConcurrency, mk(direct, fake.URL))
		measure(t, overheadWarmup, overheadConcurrency, mk(via, env.url))
		d := summarize(measure(t, overheadRequests, overheadConcurrency, mk(direct, fake.URL)))
		g := summarize(measure(t, overheadRequests, overheadConcurrency, mk(via, env.url)))
		overhead := g.p95 - d.p95
		t.Logf("%-10s direct p50/p95/p99 = %v / %v / %v", name, d.p50, d.p95, d.p99)
		t.Logf("%-10s gateway p50/p95/p99 = %v / %v / %v", name, g.p50, g.p95, g.p99)
		t.Logf("%-10s p95 overhead = %v (budget %v)", name, overhead, overheadBudgetP95)
		if overhead > overheadBudgetP95 {
			t.Errorf("%s: gateway p95 overhead %v exceeds the %v budget", name, overhead, overheadBudgetP95)
		}
	}
	run("non-stream", timeNonStream)
	run("stream-ttft", timeFirstChunk)
}
