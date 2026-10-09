package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/ratelimit"
	"serverflow/internal/redis"
	"serverflow/internal/redis/redistest"
	"serverflow/pkg/protocol"
)

// Rate limiting across gateways: several in-process gateways, each with its own Redis client and limiter,
// sharing one real Redis (password-protected, like CI's) and, where a key matters, one authenticator store.

type memKeys struct {
	mu   sync.Mutex
	recs map[string]auth.KeyRecord
}

func (m *memKeys) LookupKey(_ context.Context, prefix string) (auth.KeyRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.recs[prefix]; ok {
		return r, nil
	}
	return auth.KeyRecord{}, auth.ErrNotFound
}

func (m *memKeys) add(tenant string, models []string, rpm, tpm, conc int) string {
	key, prefix, hash := auth.GenerateKey()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recs == nil {
		m.recs = map[string]auth.KeyRecord{}
	}
	m.recs[prefix] = auth.KeyRecord{KeyID: "key_" + tenant, TenantID: tenant, Prefix: prefix, SecretHash: hash, KeyStatus: auth.KeyActive,
		Policy: auth.TenantPolicy{Status: auth.TenantActive, AllowedModels: models, RequestsPerMinute: rpm, TokensPerMinute: tpm, MaxConcurrent: conc}}
	return key
}

type gw struct {
	url string
	rc  *redis.Client
	lim *ratelimit.RedisLimiter
	srv *gateway.Server
	log *syncLog
}

type syncLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncLog) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncLog) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type cluster struct {
	gws  []*gw
	keys *memKeys
}

type clusterOpts struct {
	n         int
	upstream  http.HandlerFunc
	cfg       ratelimit.Config
	redisAddr string // overrides the test server (a proxy)
	timeout   time.Duration
	backoff   time.Duration
	noAuth    bool
	metadata  bool
	run       bool
	models    []string // served models; default model and other-model
}

func okUpstream(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"ok":true}`)
}

func newCluster(t *testing.T, o clusterOpts) *cluster {
	t.Helper()
	if o.upstream == nil {
		o.upstream = okUpstream
	}
	up := httptest.NewServer(o.upstream)
	t.Cleanup(up.Close)
	c := &cluster{keys: &memKeys{}}
	for i := 0; i < o.n; i++ {
		rcfg := redistest.Config(t)
		if o.redisAddr != "" {
			rcfg.Address = o.redisAddr
		}
		if o.timeout > 0 {
			rcfg.Timeout = o.timeout
		}
		if o.backoff > 0 {
			rcfg.Backoff = o.backoff
		}
		g := &gw{log: &syncLog{}}
		rcfg.Logger = slog.New(slog.NewJSONHandler(g.log, nil))
		g.rc = redistest.NewClientWith(t, rcfg)
		lcfg := o.cfg
		lcfg.Logger = rcfg.Logger
		lim, err := ratelimit.NewRedis(g.rc, lcfg)
		if err != nil {
			t.Fatal(err)
		}
		g.lim = lim
		gcfg := config.Default().Gateway
		gcfg.UpstreamURL, gcfg.Models = up.URL, append([]string{model, "other-model"}, o.models...)
		opts := []gateway.Option{gateway.WithLimiter(lim)}
		if !o.noAuth {
			opts = append(opts, gateway.WithAuthenticator(auth.New(c.keys, auth.Config{CacheTTL: time.Minute, NegativeTTL: time.Minute, CacheSize: 100, StaleGrace: time.Minute})))
		}
		if o.metadata {
			rec := redis.NewRecorder(g.rc, time.Minute)
			opts = append(opts, gateway.WithRequestRecorder(rec))
			ctx, cancel := context.WithCancel(context.Background())
			go rec.Run(ctx)
			t.Cleanup(cancel)
		}
		g.srv = gateway.New(gcfg, slog.New(slog.NewJSONHandler(g.log, nil)), opts...)
		ts := httptest.NewServer(g.srv.Handler())
		t.Cleanup(ts.Close)
		g.url = ts.URL
		if o.run {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { lim.Run(ctx); close(done) }()
			t.Cleanup(func() { cancel(); <-done })
		}
		c.gws = append(c.gws, g)
	}
	return c
}

var httpc = &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}

func (g *gw) chat(key, mdl string) (int, http.Header, string) {
	req, _ := http.NewRequest(http.MethodPost, g.url+"/v1/chat/completions", strings.NewReader(`{"model":"`+mdl+`","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return 0, nil, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b)
}

func TestThreeGatewaysShareOneRequestQuota(t *testing.T) {
	c := newCluster(t, clusterOpts{n: 3})
	tenant := redistest.Unique("ten")
	const quota = 120 // per minute: the bucket starts with 120 and refills 2 per second
	key := c.keys.add(tenant, nil, quota, 0, 0)
	start := time.Now()
	var ok, limited, other atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for i := 0; i < 600; i++ {
		wg.Add(1)
		sem <- struct{}{}
		g := c.gws[i%3]
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			switch code, _, _ := g.chat(key, model); code {
			case 200:
				ok.Add(1)
			case 429:
				limited.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	maxOK := int64(quota) + int64(elapsed.Seconds()*2) + 2 // the burst plus what refilled while we ran
	if ok.Load() < quota || ok.Load() > maxOK || other.Load() != 0 || ok.Load()+limited.Load() != 600 {
		t.Fatalf("admitted %d (limited %d, other %d) in %v; want between %d and %d", ok.Load(), limited.Load(), other.Load(), elapsed, quota, maxOK)
	}
	t.Logf("quota %d, admitted %d, limited %d in %v", quota, ok.Load(), limited.Load(), elapsed)
}

func TestThreeGatewaysShareOneTokenQuota(t *testing.T) {
	c := newCluster(t, clusterOpts{n: 3})
	tenant := redistest.Unique("ten")
	// Each request costs about 10 (max_tokens) + a few input tokens; a 1000 token budget admits a bounded number.
	key := c.keys.add(tenant, nil, 0, 1000, 0)
	var ok atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 150; i++ {
		wg.Add(1)
		g := c.gws[i%3]
		go func() {
			defer wg.Done()
			if code, _, _ := g.chat(key, model); code == 200 {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	// The gateway charges the whole body (framing included), as chat() builds it.
	body := `{"model":"` + model + `","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`
	cost := ratelimit.EstimateRequestCost([]protocol.Message{{Role: "user", Content: "hi"}}, "", len(body), 10, 4096)
	want := int64(1000 / cost)
	// Real Redis time keeps refilling while the load runs (1000 tokens a minute), so the upper bound follows the
	// measured duration instead of a fixed count that depends on how fast the machine is.
	refilled := int64(elapsed.Seconds()*1000/60/float64(cost)) + 2
	if ok.Load() < want || ok.Load() > want+refilled {
		t.Fatalf("admitted %d requests of cost %d against 1000 tokens in %v (want %d to %d)", ok.Load(), cost, elapsed, want, want+refilled)
	}
}

func TestAggressiveTenantIsLimitedAndNormalTenantsAreNot(t *testing.T) {
	c := newCluster(t, clusterOpts{n: 3})
	aggressive := c.keys.add(redistest.Unique("loud"), nil, 60, 0, 0)
	var normal []string
	for i := 0; i < 5; i++ {
		normal = append(normal, c.keys.add(redistest.Unique("quiet"), nil, 600, 0, 0))
	}
	var aggOK, aggLimited, normOK, normRejected atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 800; i++ {
		wg.Add(1)
		g := c.gws[i%3]
		go func() {
			defer wg.Done()
			switch code, _, _ := g.chat(aggressive, model); code {
			case 200:
				aggOK.Add(1)
			case 429:
				aggLimited.Add(1)
			}
		}()
	}
	for i, k := range normal {
		for j := 0; j < 30; j++ {
			wg.Add(1)
			g := c.gws[(i+j)%3]
			go func() {
				defer wg.Done()
				if code, _, _ := g.chat(k, model); code == 200 {
					normOK.Add(1)
				} else {
					normRejected.Add(1)
				}
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(start)
	maxAgg := int64(60) + int64(elapsed.Seconds()) + 2 // the bucket plus one token a second refilled during the run
	if aggOK.Load() < 60 || aggOK.Load() > maxAgg || aggLimited.Load() < 800-maxAgg-150 {
		t.Fatalf("aggressive tenant: %d admitted, %d limited in %v (quota 60, at most %d expected)", aggOK.Load(), aggLimited.Load(), elapsed, maxAgg)
	}
	if normRejected.Load() != 0 || normOK.Load() != 150 {
		t.Fatalf("normal tenants: %d ok, %d rejected", normOK.Load(), normRejected.Load())
	}
}

func TestRetryAfterIsHonestAcrossGateways(t *testing.T) {
	// A frozen clock (the limiter's test clock replaces Redis TIME), so how fast the machine runs the 60 requests cannot matter.
	var now atomic.Int64
	now.Store(time.Now().UnixMilli())
	clock := func() time.Time { return time.UnixMilli(now.Load()) }
	c := newCluster(t, clusterOpts{n: 2, cfg: ratelimit.Config{Clock: clock}})
	key := c.keys.add(redistest.Unique("ten"), nil, 60, 0, 0) // a bucket of 60 that refills one per second
	for i := 0; i < 60; i++ {
		if code, _, _ := c.gws[i%2].chat(key, model); code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	code, h, body := c.gws[0].chat(key, model)
	if code != 429 || h.Get("Retry-After") != "1" {
		t.Fatalf("%d retry-after %q", code, h.Get("Retry-After"))
	}
	if !strings.Contains(body, "RATE_LIMITED") || strings.Contains(body, "ten-") {
		t.Fatalf("body %s", body)
	}
	now.Add(999) // just short of the advertised second: still refused
	if code, _, _ := c.gws[1].chat(key, model); code != 429 {
		t.Fatalf("999 ms after: %d", code)
	}
	now.Add(1) // a client that waits the advertised second succeeds
	if code, _, _ := c.gws[1].chat(key, model); code != 200 {
		t.Fatalf("after waiting Retry-After: %d", code)
	}
}

func TestConcurrencyIsEnforcedAcrossGatewaysAndReleased(t *testing.T) {
	var inflight atomic.Int64
	release := make(chan struct{})
	started := make(chan struct{}, 32)
	c := newCluster(t, clusterOpts{n: 3, run: true, upstream: func(w http.ResponseWriter, r *http.Request) {
		inflight.Add(1)
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		inflight.Add(-1)
		okUpstream(w, r)
	}})
	key := c.keys.add(redistest.Unique("ten"), nil, 0, 0, 6)
	var wg sync.WaitGroup
	var ok atomic.Int64
	for i := 0; i < 6; i++ {
		wg.Add(1)
		g := c.gws[i%3]
		go func() {
			defer wg.Done()
			if code, _, _ := g.chat(key, model); code == 200 {
				ok.Add(1)
			}
		}()
	}
	for i := 0; i < 6; i++ {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("streams did not start")
		}
	}
	for i, g := range c.gws {
		code, h, body := g.chat(key, model)
		if code != 429 || !strings.Contains(body, "concurrent") || h.Get("Retry-After") == "" {
			t.Fatalf("gateway %d, request 7: %d %s", i, code, body)
		}
	}
	close(release)
	wg.Wait()
	if ok.Load() != 6 {
		t.Fatalf("%d of 6 admitted requests completed", ok.Load())
	}
	// Completion released every slot (asynchronously).
	eventually(t, 10*time.Second, func() bool {
		code, _, _ := c.gws[1].chat(key, model)
		return code == 200
	}, "slots to be released after completion")
}

func TestKilledGatewaysLeasesExpireAndLongRequestsKeepTheirs(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	var calls atomic.Int64
	c := newCluster(t, clusterOpts{n: 2, cfg: ratelimit.Config{LeaseTTL: 900 * time.Millisecond}, upstream: func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 2 { // only the first two requests hold their slots
			okUpstream(w, r)
			return
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		okUpstream(w, r)
	}})
	key := c.keys.add(redistest.Unique("ten"), nil, 0, 0, 2)
	// Gateway 0 never runs its renewer: it behaves like a gateway that died holding two slots.
	for i := 0; i < 2; i++ {
		go c.gws[0].chat(key, model)
	}
	<-started
	<-started
	if code, _, _ := c.gws[1].chat(key, model); code != 429 {
		t.Fatalf("both slots are held: %d", code)
	}
	eventually(t, 10*time.Second, func() bool {
		code, _, _ := c.gws[1].chat(key, model)
		return code == 200
	}, "the dead gateway's leases to expire")
	close(release)
}

func TestLongStreamKeepsItsSlotThroughRenewal(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	c := newCluster(t, clusterOpts{n: 2, run: true, cfg: ratelimit.Config{LeaseTTL: 1500 * time.Millisecond}, upstream: func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		okUpstream(w, r)
	}})
	key := c.keys.add(redistest.Unique("ten"), nil, 0, 0, 1)
	done := make(chan int, 1)
	go func() { code, _, _ := c.gws[0].chat(key, model); done <- code }()
	<-started
	end := time.Now().Add(3300 * time.Millisecond) // more than two lease TTLs
	for time.Now().Before(end) {
		if code, _, _ := c.gws[1].chat(key, model); code != 429 {
			t.Fatalf("the slot was lost during a long request: %d", code)
		}
		time.Sleep(60 * time.Millisecond)
	}
	close(release)
	if code := <-done; code != 200 {
		t.Fatalf("long request: %d", code)
	}
}

func TestQuotaZeroMeansUnlimitedWithNoRedisRoundTrip(t *testing.T) {
	c := newCluster(t, clusterOpts{n: 1})
	key := c.keys.add(redistest.Unique("free"), nil, 0, 0, 0)
	g := c.gws[0]
	before := g.rc.Commands()
	for i := 0; i < 200; i++ {
		if code, _, _ := g.chat(key, model); code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	if n := g.rc.Commands() - before; n != 0 {
		t.Fatalf("%d Redis commands for a tenant with no quotas", n)
	}
}

func TestModelCapAppliesWithAuthOffAcrossGateways(t *testing.T) {
	capped := redistest.Unique("capped") // the cap is global per model name, so each run uses its own
	c := newCluster(t, clusterOpts{n: 3, noAuth: true, models: []string{capped}, cfg: ratelimit.Config{ModelRequestsPerMinute: map[string]int{capped: 9}}})
	var ok, limited atomic.Int64
	for i := 0; i < 30; i++ {
		switch code, _, _ := c.gws[i%3].chat("", capped); code {
		case 200:
			ok.Add(1)
		case 429:
			limited.Add(1)
		}
	}
	if ok.Load() != 9 || limited.Load() != 21 {
		t.Fatalf("model cap 9: %d admitted, %d limited", ok.Load(), limited.Load())
	}
	// An uncapped model is untouched.
	if code, _, _ := c.gws[0].chat("", "other-model"); code != 200 {
		t.Fatalf("uncapped model: %d", code)
	}
}

func TestRedisFailureMatrixThroughTheGateway(t *testing.T) {
	for _, mode := range []ratelimit.FailureMode{ratelimit.FailClosed, ratelimit.FailOpen} {
		for _, fault := range []string{"cut", "blackhole", "slow"} {
			t.Run(string(mode)+"/"+fault, func(t *testing.T) {
				base := redistest.Config(t)
				proxy := redistest.NewProxy(t, base.Address)
				c := newCluster(t, clusterOpts{n: 1, redisAddr: proxy.Addr(), timeout: 150 * time.Millisecond, backoff: 300 * time.Millisecond, cfg: ratelimit.Config{OnFailure: mode}})
				g := c.gws[0]
				key := c.keys.add(redistest.Unique("ten"), nil, 100000, 0, 0)
				if code, _, _ := g.chat(key, model); code != 200 {
					t.Fatalf("healthy: %d", code)
				}
				switch fault {
				case "cut":
					proxy.SetMode(redistest.Cut)
				case "blackhole":
					proxy.SetMode(redistest.Blackhole)
				case "slow":
					proxy.SetDelay(time.Second)
					proxy.SetMode(redistest.Slow)
				}
				start := time.Now()
				code, h, body := g.chat(key, model)
				first := time.Since(start)
				if first > 2*time.Second {
					t.Fatalf("the first request in an outage took %v", first)
				}
				if mode == ratelimit.FailClosed {
					if code != 503 || h.Get("Retry-After") == "" || !strings.Contains(body, "RATE_LIMIT_UNAVAILABLE") {
						t.Fatalf("closed: %d %v %s", code, h, body)
					}
				} else if code != 200 {
					t.Fatalf("open: %d %s", code, body)
				}
				// During the backoff every request is answered at once.
				for i := 0; i < 20; i++ {
					start := time.Now()
					code, _, _ := g.chat(key, model)
					if d := time.Since(start); d > 200*time.Millisecond {
						t.Fatalf("a request in the backoff took %v", d)
					}
					if (mode == ratelimit.FailClosed) == (code == 200) {
						t.Fatalf("%s mode answered %d", mode, code)
					}
				}
				// Recovery needs no restart.
				proxy.SetMode(redistest.Pass)
				eventually(t, 10*time.Second, func() bool {
					g.chat(key, model) // the request after the backoff is the probe
					down, _ := g.rc.Down()
					return !down
				}, "recovery")
				if code, _, _ := g.chat(key, model); code != 200 {
					t.Fatalf("after recovery: %d", code)
				}
				logs := g.log.String()
				if strings.Count(logs, "redis unavailable") != 1 || strings.Count(logs, "redis recovered") != 1 {
					t.Fatalf("outage and recovery must each be logged once:\n%s", logs)
				}
				if mode == ratelimit.FailOpen && !strings.Contains(logs, `"rate_limit_bypassed":true`) {
					t.Fatal("bypassed requests must be visible in the log")
				}
			})
		}
	}
}

func TestRequestMetadataIsWrittenWithATTLAndNeverBlocks(t *testing.T) {
	base := redistest.Config(t)
	proxy := redistest.NewProxy(t, base.Address)
	c := newCluster(t, clusterOpts{n: 1, noAuth: true, metadata: true, redisAddr: proxy.Addr(), timeout: 150 * time.Millisecond,
		cfg: ratelimit.Config{ModelRequestsPerMinute: map[string]int{redistest.Unique("m"): 5}}})
	g := c.gws[0]
	req, _ := http.NewRequest(http.MethodPost, g.url+"/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","messages":[{"role":"user","content":"hi"}]}`))
	resp, err := httpc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	id := resp.Header.Get("X-Request-Id")
	if id == "" {
		t.Fatal("no request id")
	}
	eventually(t, 5*time.Second, func() bool {
		v, err := g.rc.Get(context.Background(), "request:{"+id+"}")
		return err == nil && strings.Contains(v, `"worker":"static"`) && strings.Contains(v, model)
	}, "request metadata")
	if ttl, err := g.rc.TTL(context.Background(), "request:{"+id+"}"); err != nil || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("ttl %v %v", ttl, err)
	}
	// With Redis black-holed, requests are not delayed or failed by the recorder.
	proxy.SetMode(redistest.Blackhole)
	for i := 0; i < 20; i++ {
		start := time.Now()
		code, _, _ := g.chat("", model)
		if code != 200 || time.Since(start) > time.Second {
			t.Fatalf("request %d: %d in %v", i, code, time.Since(start))
		}
	}
}

func TestRequestMetadataIsAbsentWhenDisabled(t *testing.T) {
	c := newCluster(t, clusterOpts{n: 1, noAuth: true, cfg: ratelimit.Config{ModelRequestsPerMinute: map[string]int{redistest.Unique("m"): 5}}})
	g := c.gws[0]
	req, _ := http.NewRequest(http.MethodPost, g.url+"/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","messages":[{"role":"user","content":"hi"}]}`))
	resp, err := httpc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	time.Sleep(300 * time.Millisecond)
	if _, err := g.rc.Get(context.Background(), "request:{"+resp.Header.Get("X-Request-Id")+"}"); err == nil {
		t.Fatal("request metadata was written although it is disabled")
	}
}

func TestKeysAndPasswordsNeverReachTheLogs(t *testing.T) {
	c := newCluster(t, clusterOpts{n: 1})
	key := c.keys.add(redistest.Unique("ten"), nil, 1, 0, 0)
	g := c.gws[0]
	g.chat(key, model)
	g.chat(key, model) // 429
	pw := redistest.Config(t).Password
	logs := g.log.String()
	if strings.Contains(logs, key) || strings.Contains(logs, key[3:]) || strings.Contains(logs, pw) {
		t.Fatalf("a secret reached the log:\n%s", logs)
	}
	if !strings.Contains(logs, `"limit":"requests"`) || !strings.Contains(logs, `"tenant_id"`) {
		t.Fatalf("a rate limited request must log tenant and limit:\n%s", logs)
	}
}

// A Redis that is up but echoes the password in its errors: through the whole gateway the password must reach
// no response body, no log line and no metric.
func TestPasswordEchoedByRedisReachesNoResponseLogOrMetric(t *testing.T) {
	pw := redistest.Config(t).Password
	fake := redistest.NewFakeServer(t, "-ERR WRONGPASS invalid username-password pair for "+pw)
	c := newCluster(t, clusterOpts{n: 1, redisAddr: fake.Addr(), cfg: ratelimit.Config{OnFailure: ratelimit.FailClosed}})
	g := c.gws[0]
	key := c.keys.add(redistest.Unique("ten"), nil, 10, 0, 0)
	code, h, body := g.chat(key, model)
	if code != 503 || h.Get("Retry-After") == "" {
		t.Fatalf("%d %v %s", code, h, body)
	}
	resp, err := httpc.Get(g.url + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metrics, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	for what, text := range map[string]string{"response": body, "log": g.log.String(), "metrics": string(metrics)} {
		if strings.Contains(text, pw) {
			t.Fatalf("the Redis password reached the %s:\n%s", what, text)
		}
	}
	if !strings.Contains(g.log.String(), "redis unavailable") {
		t.Fatal("the outage should be logged")
	}
}
