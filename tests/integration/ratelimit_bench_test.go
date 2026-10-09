package integration

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/bench/workload"
	"serverflow/internal/ratelimit"
	"serverflow/internal/redis/redistest"
)

// The measurements in docs/benchmarks/phase-8-rate-limits.md. They are opt-in because they take about a
// minute and print numbers rather than assert tight bounds:
//
//	SERVERFLOW_BENCH_RATELIMIT=1 go test -count=1 -v -run TestBenchmarkRateLimits ./tests/integration
//
// The load is the Phase 7 harness's own multi-tenant plan (internal/bench/workload: one aggressive tenant
// sending ten times what each of four normal tenants send), offered open-loop to three in-process gateways
// that share one Redis. The harness binary itself cannot be used for this: it sends fake "sk-bench-*" keys,
// which the authenticator rightly refuses, and the harness is not changed in this phase.

func pctl(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[int(float64(len(d)-1)*p)]
}

// offer sends the workload open-loop at rate requests per second for dur, round-robin over the gateways, and
// reports per-tenant outcomes.
type outcome struct{ ok, limited, unavailable, other atomic.Int64 }

func offer(t *testing.T, c *cluster, keys map[string]string, wl *workload.Workload, rate float64, dur time.Duration, out map[string]*outcome, mid func(elapsed time.Duration)) {
	t.Helper()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 512)
	start := time.Now()
	interval := time.Duration(float64(time.Second) / rate)
	next := start
	for i := 0; ; i++ {
		if time.Since(start) >= dur {
			break
		}
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
		next = next.Add(interval)
		if mid != nil {
			mid(time.Since(start))
		}
		r := wl.Request(i)
		g := c.gws[i%len(c.gws)]
		o := out[r.Tenant]
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			switch code, _, _ := g.chat(keys[r.Tenant], model); code {
			case 200:
				o.ok.Add(1)
			case http.StatusTooManyRequests:
				o.limited.Add(1)
			case http.StatusServiceUnavailable:
				o.unavailable.Add(1)
			default:
				o.other.Add(1)
			}
		}()
	}
	wg.Wait()
}

func TestBenchmarkRateLimits(t *testing.T) {
	if os.Getenv("SERVERFLOW_BENCH_RATELIMIT") != "1" {
		t.Skip("set SERVERFLOW_BENCH_RATELIMIT=1 to run the rate limit measurements")
	}
	redistest.Addr(t)
	wl, err := workload.New(workload.Spec{Name: workload.MultiTenant, Seed: 1, Models: workload.Models{Primary: model}})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("shared quota", func(t *testing.T) {
		const (
			aggressiveRPM = 600
			normalRPM     = 6000
			rate          = 300.0 // offered requests per second in total
		)
		dur := 20 * time.Second
		c := newCluster(t, clusterOpts{n: 3})
		keys := map[string]string{}
		out := map[string]*outcome{}
		for _, tn := range wl.Tenants() {
			rpm := normalRPM
			if tn == "tenant-aggressive" {
				rpm = aggressiveRPM
			}
			keys[tn] = c.keys.add(redistest.Unique(tn), nil, rpm, 0, 0)
			out[tn] = &outcome{}
		}
		start := time.Now()
		offer(t, c, keys, wl, rate, dur, out, nil)
		el := time.Since(start)
		burst := float64(aggressiveRPM)
		want := burst + float64(aggressiveRPM)/60*el.Seconds()
		agg := out["tenant-aggressive"]
		t.Logf("offered %.0f req/s for %v over 3 gateways, one Redis", rate, el.Round(time.Millisecond))
		t.Logf("aggressive tenant quota %d/min: accepted %d, expected at most burst+refill = %.0f (%.1f%% of it), rejected(429) %d, other %d",
			aggressiveRPM, agg.ok.Load(), want, 100*float64(agg.ok.Load())/want, agg.limited.Load(), agg.other.Load())
		for _, tn := range wl.Tenants() {
			if tn == "tenant-aggressive" {
				continue
			}
			o := out[tn]
			t.Logf("%s quota %d/min: accepted %d, rejected %d, unavailable %d, other %d", tn, normalRPM, o.ok.Load(), o.limited.Load(), o.unavailable.Load(), o.other.Load())
			if o.limited.Load() != 0 {
				t.Errorf("%s was rate limited", tn)
			}
		}
		if float64(agg.ok.Load()) > want+3 || float64(agg.ok.Load()) < want*0.98 {
			t.Errorf("aggressive tenant admitted %d, expected within 2%% of %.0f", agg.ok.Load(), want)
		}
	})

	t.Run("overhead", func(t *testing.T) {
		base := newCluster(t, clusterOpts{n: 1, noAuth: true}) // limiter present but nothing to enforce: no Redis round trip
		lim := newCluster(t, clusterOpts{n: 1})                // a tenant with all three quotas: one script call per request
		unl := newCluster(t, clusterOpts{n: 1})                // a tenant with no quotas: no round trip
		kLim := lim.keys.add(redistest.Unique("lim"), nil, 1_000_000, 100_000_000, 1000)
		kUnl := unl.keys.add(redistest.Unique("unl"), nil, 0, 0, 0)
		measure := func(g *gw, key string) []time.Duration {
			const total, conc = 4000, 16
			out := make([]time.Duration, total)
			var idx atomic.Int64
			var wg sync.WaitGroup
			for w := 0; w < conc; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(idx.Add(1)) - 1
						if i >= total {
							return
						}
						s := time.Now()
						g.chat(key, model)
						out[i] = time.Since(s)
					}
				}()
			}
			wg.Wait()
			return out[200:]
		}
		measure(base.gws[0], "")
		b := measure(base.gws[0], "")
		l := measure(lim.gws[0], kLim)
		u := measure(unl.gws[0], kUnl)
		row := func(name string, d []time.Duration) string {
			return fmt.Sprintf("%-34s p50 %-9v p95 %-9v p99 %v", name, pctl(d, .5).Round(time.Microsecond), pctl(d, .95).Round(time.Microsecond), pctl(d, .99).Round(time.Microsecond))
		}
		t.Log(row("no tenant, nothing to enforce", b))
		t.Log(row("tenant with no quotas", u))
		t.Log(row("tenant with 3 quotas (1 round trip)", l))
		t.Logf("added by one Redis round trip: p50 %v, p95 %v", (pctl(l, .5) - pctl(u, .5)).Round(time.Microsecond), (pctl(l, .95) - pctl(u, .95)).Round(time.Microsecond))
	})

	for _, mode := range []ratelimit.FailureMode{ratelimit.FailClosed, ratelimit.FailOpen} {
		t.Run("outage "+string(mode), func(t *testing.T) {
			base := redistest.Config(t)
			proxy := redistest.NewProxy(t, base.Address)
			c := newCluster(t, clusterOpts{n: 3, redisAddr: proxy.Addr(), timeout: 50 * time.Millisecond, backoff: time.Second, cfg: ratelimit.Config{OnFailure: mode}})
			keys := map[string]string{}
			out := map[string]*outcome{}
			for _, tn := range wl.Tenants() {
				keys[tn] = c.keys.add(redistest.Unique(tn), nil, 600000, 0, 0)
				out[tn] = &outcome{}
			}
			cut, restored := false, false
			offer(t, c, keys, wl, 200, 15*time.Second, out, func(el time.Duration) {
				if !cut && el > 5*time.Second {
					cut = true
					proxy.SetMode(redistest.Blackhole)
				}
				if !restored && el > 10*time.Second {
					restored = true
					proxy.SetMode(redistest.Pass)
				}
			})
			var ok, unav, other int64
			for _, o := range out {
				ok += o.ok.Load()
				unav += o.unavailable.Load()
				other += o.other.Load()
			}
			t.Logf("%s: Redis black-holed from 5 s to 10 s of 15 s at 200 req/s: %d succeeded, %d answered 503 RATE_LIMIT_UNAVAILABLE, %d other",
				mode, ok, unav, other)
		})
	}
}
