package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"serverflow/internal/redis"
	"serverflow/internal/redis/redistest"
	"serverflow/internal/telemetry"
)

func sampleOf(t *testing.T, c *redis.Client, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c.Collector())
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if bad := telemetry.LabelNamesOutsideAllowlist(fams); len(bad) != 0 {
		t.Fatalf("labels outside the allowlist: %v", bad)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
	next:
		for _, m := range f.GetMetric() {
			for k, v := range labels {
				found := false
				for _, l := range m.GetLabel() {
					if l.GetName() == k && l.GetValue() == v {
						found = true
					}
				}
				if !found {
					continue next
				}
			}
			if m.Gauge != nil {
				return m.GetGauge().GetValue(), true
			}
			return m.GetCounter().GetValue(), true
		}
	}
	return 0, false
}

// An unreachable Redis needs no server: the failure is the point.
func TestCollectorReportsAnOutage(t *testing.T) {
	c := redistest.NewClientWith(t, redis.Config{Address: "127.0.0.1:1", Password: "unused-test-password", Timeout: 200 * time.Millisecond, Backoff: time.Hour})
	if v, ok := sampleOf(t, c, "redis_up", nil); !ok || v != 1 {
		t.Fatalf("redis_up before any call: %v %v", v, ok)
	}
	for _, k := range []string{"timeout", "connection", "script", "other"} {
		if v, ok := sampleOf(t, c, "redis_errors_total", map[string]string{"kind": k}); !ok || v != 0 {
			t.Fatalf("redis_errors_total{kind=%s} must exist at 0: %v %v", k, v, ok)
		}
	}
	_, err := c.Run(context.Background(), echo, []string{"k"}, 1)
	if err == nil {
		t.Fatal("nothing listens on port 1")
	}

	if v, _ := sampleOf(t, c, "redis_up", nil); v != 0 {
		t.Fatalf("redis_up during backoff: %v", v)
	}
	var total float64
	for _, k := range []string{"timeout", "connection", "script", "other"} {
		v, _ := sampleOf(t, c, "redis_errors_total", map[string]string{"kind": k})
		total += v
	}
	if total != 1 {
		t.Fatalf("%v errors counted for one failed call", total)
	}
	// The driver retries a refused dial until the call's deadline, so the kind is timeout or connection.
	tm, _ := sampleOf(t, c, "redis_errors_total", map[string]string{"kind": "timeout"})
	cn, _ := sampleOf(t, c, "redis_errors_total", map[string]string{"kind": "connection"})
	if tm+cn != 1 {
		t.Fatalf("an unreachable server must count as timeout or connection: %v %v", tm, cn)
	}
	// A call turned away by the backoff never reached Redis and is not an error of Redis.
	_, _ = c.Run(context.Background(), echo, []string{"k"}, 1)
	var total2 float64
	for _, k := range []string{"timeout", "connection", "script", "other"} {
		v, _ := sampleOf(t, c, "redis_errors_total", map[string]string{"kind": k})
		total2 += v
	}
	if total2 != 1 {
		t.Fatalf("a backoff refusal was counted: %v", total2)
	}
	if v, ok := sampleOf(t, c, "redis_pool_connections", map[string]string{"state": "max"}); !ok || v != float64(redis.DefaultPoolSize) {
		t.Fatalf("pool max %v %v", v, ok)
	}
}

func TestCollectorCountsCommandsAgainstARealServer(t *testing.T) {
	c := redistest.NewClient(t)
	if _, err := c.Run(context.Background(), echo, []string{"warm"}, 1); err != nil {
		t.Fatal(err)
	}
	before, _ := sampleOf(t, c, "redis_commands_total", nil)
	for i := 0; i < 3; i++ {
		if _, err := c.Run(context.Background(), echo, []string{redistest.Unique("k")}, 1); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := sampleOf(t, c, "redis_commands_total", nil)
	if after-before != 3 {
		t.Fatalf("commands %v -> %v", before, after)
	}
	if v, _ := sampleOf(t, c, "redis_up", nil); v != 1 {
		t.Fatal("redis_up")
	}
	if n := testutil.CollectAndCount(c.Collector()); n != 1+1+4+3 {
		t.Fatalf("%d series", n)
	}
}

func errorKinds(t *testing.T, c *redis.Client) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, k := range []string{"timeout", "connection", "script", "other"} {
		out[k], _ = sampleOf(t, c, "redis_errors_total", map[string]string{"kind": k})
	}
	return out
}

// A real Redis that stops: the proxy in Cut mode closes every connection and refuses new ones, which is what the
// driver sees when the server goes away. With a call timeout long enough for the dial to fail the outage must be
// counted as kind="connection" (never "other"); with a very short timeout the driver gives up on the deadline first
// and it is "timeout". Either way exactly one error per failed call, and redis_up drops to 0.
func TestRealStoppedRedisCountsAsConnectionOrTimeoutNeverOther(t *testing.T) {
	for name, tc := range map[string]struct {
		timeout time.Duration
		kinds   []string
	}{
		"long timeout":  {2 * time.Second, []string{"connection"}},
		"short timeout": {30 * time.Millisecond, []string{"connection", "timeout"}},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := redistest.Config(t)
			px := redistest.NewProxy(t, cfg.Address)
			cfg.Address, cfg.Timeout, cfg.Backoff = px.Addr(), tc.timeout, time.Hour
			c := redistest.NewClientWith(t, cfg)
			if _, err := c.Run(context.Background(), echo, []string{redistest.Unique("k")}, 1); err != nil {
				t.Fatal(err)
			}
			if v, _ := sampleOf(t, c, "redis_up", nil); v != 1 {
				t.Fatalf("redis_up before the outage: %v", v)
			}
			px.SetMode(redistest.Cut)
			if _, err := c.Run(context.Background(), echo, []string{redistest.Unique("k")}, 1); err == nil {
				t.Fatal("the stopped Redis answered")
			}
			got := errorKinds(t, c)
			var total float64
			for _, v := range got {
				total += v
			}
			if total != 1 || got["other"] != 0 || got["script"] != 0 {
				t.Fatalf("error kinds after one failed call: %v", got)
			}
			ok := false
			for _, k := range tc.kinds {
				ok = ok || got[k] == 1
			}
			if !ok {
				t.Fatalf("want one of %v, got %v", tc.kinds, got)
			}
			if v, _ := sampleOf(t, c, "redis_up", nil); v != 0 {
				t.Fatalf("redis_up during the outage: %v", v)
			}
		})
	}
}

// Scraping never talks to Redis: against a dependency that accepts connections and then hangs, with a command stuck
// in flight, the collector still answers at once. (A collector that issued a PING would hang with it.)
func TestCollectorReturnsAtOnceWhileRedisHangs(t *testing.T) {
	cfg := redistest.Config(t)
	px := redistest.NewProxy(t, cfg.Address)
	cfg.Address, cfg.Timeout = px.Addr(), 10*time.Second
	c := redistest.NewClientWith(t, cfg)
	if _, err := c.Run(context.Background(), echo, []string{redistest.Unique("k")}, 1); err != nil {
		t.Fatal(err)
	}
	px.SetMode(redistest.Blackhole)
	stuck := make(chan struct{})
	go func() {
		_, _ = c.Run(context.Background(), echo, []string{redistest.Unique("k")}, 1)
		close(stuck)
	}()
	time.Sleep(100 * time.Millisecond) // the command is in flight and will not be answered for 10 s
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collector())
	start := time.Now()
	done := make(chan struct{})
	go func() { _, _ = reg.Gather(); close(done) }()
	select {
	case <-done:
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("a scrape took %v while Redis hung", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a scrape hangs while Redis hangs: the collector must read local state only")
	}
	px.SetMode(redistest.Cut) // release the stuck call
	select {
	case <-stuck:
	case <-time.After(5 * time.Second):
	}
}
