package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/events"
	"serverflow/internal/gateway"
	"serverflow/internal/kafka"
	"serverflow/internal/kafka/kafkatest"
)

// TestEventsOverhead measures what publishing lifecycle events costs the gateway (D20, acceptance 14): the same load
// through a gateway with events off, on with the broker up, on with the broker down, and on with the broker frozen
// (the extreme of slow). It is a measurement, not a gate, and runs only with SERVERFLOW_EVENTS_BENCH=1 (it needs
// docker and the Kafka test environment). Results: docs/benchmarks/phase-12-kafka-events.md.
func TestEventsOverhead(t *testing.T) {
	if os.Getenv("SERVERFLOW_EVENTS_BENCH") == "" {
		t.Skip("set SERVERFLOW_EVENTS_BENCH=1 to measure the events overhead")
	}
	kcfg := kafkatest.Config(t)
	topic := kafkatest.NewTopic(t, kcfg, 6)
	priv := kafkatest.StartPrivateBroker(t)
	pcfg := priv.Config()
	ptopic := kafkatest.NewTopic(t, pcfg, 3)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"stream":true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n")
			w.(http.Flusher).Flush()
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","choices":[{"message":{"content":"a"}}],"usage":{"prompt_tokens":3,"completion_tokens":4}}`)
	}))
	defer upstream.Close()

	type scenario struct {
		name  string
		build func(t *testing.T) (obs *events.Observer, closeFn func())
		setup func()
		after func()
	}
	mkProducer := func(cfg kafka.Config, topic string) func(t *testing.T) (*events.Observer, func()) {
		return func(t *testing.T) (*events.Observer, func()) {
			cfg := cfg
			cfg.Topic, cfg.DeliveryTimeout = topic, 30*time.Second
			sink, err := kafka.NewProducer(cfg)
			if err != nil {
				t.Fatal(err)
			}
			o := events.NewObserver(sink, events.Config{BufferSize: 10_000, Source: "bench"}, slog.New(slog.DiscardHandler))
			return o, func() {
				c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = o.Close(c)
				_ = sink.Close()
			}
		}
	}
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	_ = dead.Close()
	downCfg := kcfg
	downCfg.Brokers = []string{deadAddr}
	scenarios := []scenario{
		{name: "off"},
		{name: "on, broker up", build: mkProducer(kcfg, topic)},
		{name: "on, broker down", build: mkProducer(downCfg, topic)},
		{name: "on, broker frozen", build: mkProducer(pcfg, ptopic), setup: priv.Pause, after: priv.Unpause},
	}

	nonStream := `{"model":"qwen-7b","messages":[{"role":"user","content":"hello"}]}`
	stream := `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	const total, conc, warm, rounds = 3000, 16, 200, 5

	type result struct{ ns, st overheadStats }
	results := map[string][]result{}
	for r := 0; r < rounds; r++ {
		for _, sc := range scenarios {
			var obs *events.Observer
			closeFn := func() {}
			if sc.build != nil {
				obs, closeFn = sc.build(t)
			}
			cfg := config.Default().Gateway
			cfg.UpstreamURL, cfg.Models, cfg.UpstreamHeaderTimeout = upstream.URL, []string{model}, 5*time.Second
			var opts []gateway.Option
			if obs != nil {
				opts = append(opts, gateway.WithObserver(obs))
			}
			ts := httptest.NewServer(gateway.New(cfg, slog.New(slog.DiscardHandler), opts...).Handler())
			c := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true}}
			if sc.setup != nil {
				sc.setup()
			}
			timeNon := func() time.Duration {
				s := time.Now()
				resp, err := c.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(nonStream))
				if err != nil {
					t.Error(err)
					return 0
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				return time.Since(s)
			}
			timeFirst := func() time.Duration {
				s := time.Now()
				resp, err := c.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(stream))
				if err != nil {
					t.Error(err)
					return 0
				}
				br := bufio.NewReader(resp.Body)
				_, _ = br.ReadString('\n')
				d := time.Since(s)
				_, _ = io.Copy(io.Discard, br)
				_ = resp.Body.Close()
				return d
			}
			runtime.GC()
			var m0 runtime.MemStats
			runtime.ReadMemStats(&m0)
			g0 := runtime.NumGoroutine()
			overheadRun(warm, conc, timeNon)
			overheadRun(warm, conc, timeFirst)
			res := result{ns: overheadSummary(overheadRun(total, conc, timeNon)), st: overheadSummary(overheadRun(total, conc, timeFirst))}
			results[sc.name] = append(results[sc.name], res)
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)
			dropped := ""
			if obs != nil {
				p := obs.Publisher()
				dropped = fmt.Sprintf(" published=%d buffer_full=%d producer_full=%d delivery_failed=%d depth=%d", p.Published(), p.Dropped(events.ReasonBufferFull),
					p.Dropped(events.ReasonProducerFull), p.Dropped(events.ReasonDeliveryFailed), p.Depth())
			}
			t.Logf("round %d %-18s non-stream p50/p95/p99 %v / %v / %v   stream-ttft %v / %v / %v   goroutines +%d heap +%d KiB%s", r+1, sc.name,
				res.ns.p50, res.ns.p95, res.ns.p99, res.st.p50, res.st.p95, res.st.p99, runtime.NumGoroutine()-g0, (int64(m1.HeapInuse)-int64(m0.HeapInuse))/1024, dropped)
			c.CloseIdleConnections()
			ts.Close()
			if sc.after != nil {
				sc.after()
			}
			closeFn()
		}
	}
	med := func(name string, f func(result) time.Duration) (lo, mid, hi time.Duration) {
		var v []time.Duration
		for _, r := range results[name] {
			v = append(v, f(r))
		}
		sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
		return v[0], v[len(v)/2], v[len(v)-1]
	}
	for _, sc := range scenarios {
		a, b, c := med(sc.name, func(r result) time.Duration { return r.ns.p95 })
		d, e, f := med(sc.name, func(r result) time.Duration { return r.st.p95 })
		t.Logf("SUMMARY %-18s non-stream p95 min/median/max %v / %v / %v    stream-ttft p95 %v / %v / %v", sc.name, a, b, c, d, e, f)
	}
}

func overheadRun(total, conc int, do func() time.Duration) []time.Duration {
	out := make([]time.Duration, total)
	next := make(chan int, total)
	for i := 0; i < total; i++ {
		next <- i
	}
	close(next)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
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

type overheadStats struct{ p50, p95, p99 time.Duration }

func overheadSummary(d []time.Duration) overheadStats {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(p float64) time.Duration { return d[int(float64(len(d)-1)*p)] }
	return overheadStats{at(.50), at(.95), at(.99)}
}
