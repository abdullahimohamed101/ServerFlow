package redis_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/redis"
	"serverflow/internal/redis/redistest"
)

// echo returns {1, ARGV[1]} so a test can tell the script ran.
var echo = redis.NewScript(`return {1, tonumber(ARGV[1])}`, 2)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func TestRunAndPingWithPassword(t *testing.T) {
	c := redistest.NewClient(t)
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	got, err := c.Run(context.Background(), echo, []string{redistest.Unique("k")}, 42)
	if err != nil || len(got) != 2 || got[0] != 1 || got[1] != 42 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestWrongOrMissingPasswordFailsAndNeverEchoesIt(t *testing.T) {
	cfg := redistest.Config(t)
	wrong := "definitely-wrong-password-xyz"
	for name, pw := range map[string]string{"wrong": wrong, "none": ""} {
		c, err := redis.New(redis.Config{Address: cfg.Address, Password: pw, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		err = c.Ping(context.Background())
		if err == nil {
			t.Fatalf("%s: the server accepted a client without the right password; the test Redis must require one", name)
		}
		if strings.Contains(err.Error(), wrong) || strings.Contains(err.Error(), cfg.Password) {
			t.Fatalf("%s: error leaks a password: %v", name, err)
		}
		_, err = c.Run(context.Background(), echo, []string{"k"}, 1)
		if err == nil || strings.Contains(err.Error(), wrong) || strings.Contains(err.Error(), cfg.Password) {
			t.Fatalf("%s: run error %v", name, err)
		}
		_ = c.Close()
	}
}

func TestCommandsAreCounted(t *testing.T) {
	c := redistest.NewClient(t)
	if _, err := c.Run(context.Background(), echo, []string{"warm"}, 1); err != nil { // handshake and script load
		t.Fatal(err)
	}
	before := c.Commands()
	for i := 0; i < 5; i++ {
		if _, err := c.Run(context.Background(), echo, []string{redistest.Unique("k")}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if n := c.Commands() - before; n != 5 {
		t.Fatalf("counted %d commands for 5 runs, want 5", n)
	}
}

func TestSetWithTTL(t *testing.T) {
	c := redistest.NewClient(t)
	key := redistest.Unique("request")
	if err := c.Set(context.Background(), key, "v", time.Hour); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(context.Background(), key); err != nil || v != "v" {
		t.Fatalf("%q %v", v, err)
	}
	if ttl, err := c.TTL(context.Background(), key); err != nil || ttl <= 0 || ttl > time.Hour {
		t.Fatalf("ttl %v %v", ttl, err)
	}
}

func TestOutageBackoffProbeAndSingleLogLines(t *testing.T) {
	cfg := redistest.Config(t)
	proxy := redistest.NewProxy(t, cfg.Address)
	clk := newClock()
	logs := &lockedBuf{}
	cfg.Address, cfg.Timeout, cfg.Backoff, cfg.Now = proxy.Addr(), 100*time.Millisecond, time.Second, clk.Now
	cfg.Logger = slog.New(slog.NewTextHandler(logs, nil))
	c := redistest.NewClientWith(t, cfg)
	ctx := context.Background()

	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err != nil {
		t.Fatalf("healthy: %v", err)
	}
	proxy.SetMode(redistest.Blackhole)
	// A hung Redis costs one timeout...
	start := time.Now()
	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err == nil || errors.Is(err, redis.ErrBackoff) {
		t.Fatalf("want a timeout error, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the call hung for %v despite a 100ms timeout", d)
	}
	// ...and then calls fail at once for the backoff, without touching the network.
	before := c.Commands()
	for i := 0; i < 50; i++ {
		start := time.Now()
		if _, err := c.Run(ctx, echo, []string{"k"}, 1); !errors.Is(err, redis.ErrBackoff) {
			t.Fatalf("want ErrBackoff during the backoff, got %v", err)
		}
		if time.Since(start) > 50*time.Millisecond {
			t.Fatal("a call during the backoff was slow")
		}
	}
	if c.Commands() != before {
		t.Fatal("commands were sent during the backoff")
	}
	if down, left := c.Down(); !down || left <= 0 || left > time.Second {
		t.Fatalf("Down() = %v, %v", down, left)
	}

	// After the backoff, exactly one probe goes out; concurrent callers keep failing fast.
	clk.Advance(1100 * time.Millisecond)
	var backoffs, others atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Run(ctx, echo, []string{"k"}, 1); errors.Is(err, redis.ErrBackoff) {
				backoffs.Add(1)
			} else {
				others.Add(1)
			}
		}()
	}
	wg.Wait()
	if others.Load() != 1 || backoffs.Load() != 19 {
		t.Fatalf("expected one probe and 19 fast failures, got %d and %d", others.Load(), backoffs.Load())
	}

	// Recovery: the next probe after the backoff succeeds and everything resumes.
	proxy.SetMode(redistest.Pass)
	clk.Advance(1100 * time.Millisecond)
	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err != nil {
		t.Fatalf("probe after recovery failed: %v", err)
	}
	if down, _ := c.Down(); down {
		t.Fatal("still down after a successful probe")
	}
	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	if n := strings.Count(out, "redis unavailable"); n != 1 {
		t.Fatalf("%d outage lines, want 1:\n%s", n, out)
	}
	if n := strings.Count(out, "redis recovered"); n != 1 {
		t.Fatalf("%d recovery lines, want 1:\n%s", n, out)
	}
	if strings.Contains(out, cfg.Password) {
		t.Fatal("the log contains the password")
	}
}

func TestCutProxyConnectionRefusedAndRecovery(t *testing.T) {
	cfg := redistest.Config(t)
	proxy := redistest.NewProxy(t, cfg.Address)
	clk := newClock()
	cfg.Address, cfg.Timeout, cfg.Backoff, cfg.Now = proxy.Addr(), 200*time.Millisecond, time.Second, clk.Now
	c := redistest.NewClientWith(t, cfg)
	ctx := context.Background()
	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err != nil {
		t.Fatal(err)
	}
	proxy.SetMode(redistest.Cut)
	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err == nil {
		t.Fatal("run succeeded through a cut proxy")
	}
	proxy.SetMode(redistest.Pass)
	clk.Advance(2 * time.Second)
	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err != nil {
		t.Fatalf("no recovery after the proxy came back: %v", err)
	}
}

func TestSlowRedisIsTreatedAsFailed(t *testing.T) {
	cfg := redistest.Config(t)
	proxy := redistest.NewProxy(t, cfg.Address)
	cfg.Address, cfg.Timeout = proxy.Addr(), 100*time.Millisecond
	c := redistest.NewClientWith(t, cfg)
	if _, err := c.Run(context.Background(), echo, []string{"k"}, 1); err != nil {
		t.Fatal(err)
	}
	proxy.SetDelay(400 * time.Millisecond)
	proxy.SetMode(redistest.Slow)
	start := time.Now()
	if _, err := c.Run(context.Background(), echo, []string{"k"}, 1); err == nil {
		t.Fatal("a reply slower than the timeout was accepted")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v with a 100ms timeout", d)
	}
}

func TestErrorRepliesCountAsFailure(t *testing.T) {
	cfg := redistest.Config(t)
	fake := redistest.NewFakeServer(t, "-OOM command not allowed when used memory > 'maxmemory'")
	cfg.Address = fake.Addr()
	c := redistest.NewClientWith(t, cfg)
	_, err := c.Run(context.Background(), echo, []string{"k"}, 1)
	if err == nil || !strings.Contains(err.Error(), "OOM") {
		t.Fatalf("got %v", err)
	}
	if down, _ := c.Down(); !down {
		t.Fatal("an erroring Redis should start a backoff")
	}
}

func TestCancelledClientContextIsNotARedisFailure(t *testing.T) {
	c := redistest.NewClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Run(ctx, echo, []string{"k"}, 1); err != nil {
		t.Fatalf("a client that hung up must not fail the call: %v", err)
	}
	if down, _ := c.Down(); down {
		t.Fatal("a cancelled context started a backoff")
	}
}

func TestBestEffortSetNeverStartsABackoff(t *testing.T) {
	cfg := redistest.Config(t)
	fake := redistest.NewFakeServer(t, "-READONLY You can't write against a read only replica.")
	cfg.Address = fake.Addr()
	c := redistest.NewClientWith(t, cfg)
	if err := c.Set(context.Background(), "k", "v", time.Minute); err == nil {
		t.Fatal("expected an error")
	}
	if down, _ := c.Down(); down {
		t.Fatal("a failed best-effort write must not mark Redis down")
	}
}

func TestNoLeaksAfterAnOutage(t *testing.T) {
	cfg := redistest.Config(t)
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()
	func() {
		proxy := redistest.NewProxy(t, cfg.Address)
		defer proxy.Close()
		cfg.Address, cfg.Timeout, cfg.Backoff = proxy.Addr(), 50*time.Millisecond, 10*time.Millisecond
		c, err := redis.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		for round := 0; round < 3; round++ {
			proxy.SetMode(redistest.Blackhole)
			for i := 0; i < 20; i++ {
				_, _ = c.Run(context.Background(), echo, []string{"k"}, 1)
				time.Sleep(12 * time.Millisecond)
			}
			proxy.SetMode(redistest.Pass)
			time.Sleep(20 * time.Millisecond)
			_, _ = c.Run(context.Background(), echo, []string{"k"}, 1)
		}
		if total, _ := c.PoolStats(); total > 12 {
			t.Errorf("the pool holds %d connections", total)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base+3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base+3 {
		t.Fatalf("goroutines grew from %d to %d after closing everything", base, n)
	}
}

// Every Redis client in this repository's tests must go through redistest, which refuses to build one without a
// password. A client built directly would pass against a passwordless local server and then fail in CI, whose
// server requires one.
func TestNoClientIsBuiltOutsideTheAllowedPlaces(t *testing.T) {
	root := filepath.Join("..", "..")
	allowed := map[string]bool{
		filepath.Join("internal", "redis", "client.go"):                 true, // the constructor itself
		filepath.Join("internal", "redis", "client_test.go"):            true, // wrong-password probes, which are the point
		filepath.Join("internal", "redis", "redistest", "redistest.go"): true,
		filepath.Join("cmd", "gateway", "main.go"):                      true, // production wiring
	}
	var bad []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && (info.Name() == ".git" || info.Name() == ".data" || info.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if allowed[rel] || strings.HasSuffix(rel, "config_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		s := string(b)
		for _, pat := range []string{"redis.New(", "goredis.NewClient(", "redis.NewClient("} {
			if strings.Contains(s, pat) {
				bad = append(bad, rel+": "+pat)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) > 0 {
		t.Fatalf("Redis clients must be built with redistest.NewClient in tests (it refuses an empty password):\n%s", strings.Join(bad, "\n"))
	}
}

func TestRedistestRefusesAClientWithoutAPassword(t *testing.T) {
	addr, _ := redistest.Addr(t)
	rec := &recorder{TB: t}
	func() {
		defer func() { _ = recover() }()
		redistest.NewClientWith(rec, redis.Config{Address: addr})
	}()
	if !rec.failed {
		t.Fatal("redistest.NewClientWith accepted a configuration without a password")
	}
}

// recorder captures Fatal instead of ending the real test.
type recorder struct {
	testing.TB
	failed bool
}

func (r *recorder) Fatal(...any)          { r.failed = true; panic("fatal") }
func (r *recorder) Fatalf(string, ...any) { r.failed = true; panic("fatal") }
func (r *recorder) Helper()               {}
func (r *recorder) Cleanup(func())        {}
