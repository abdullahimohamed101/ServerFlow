// Package embedded boots a whole simulated ServerFlow cluster inside the benchmark process
// on loopback: a control plane, mock workers behind real worker agents, and a gateway in
// registry mode running the scheduler under test. It is a non-test copy of the helpers in
// tests/integration, which a binary cannot import; consolidating the two is a later
// refactor. Load generator, gateway and workers share one machine, so numbers from it
// compare schedulers with each other, not absolute capacity.
package embedded

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
	"serverflow/internal/mockworker"
	"serverflow/internal/registry"
	"serverflow/internal/registry/client"
	"serverflow/internal/registry/server"
	"serverflow/internal/worker"
)

// Worker profiles.
const (
	// Identical gives every worker the same speed.
	Identical = "identical"
	// Heterogeneous cycles workers through fast, medium and slow (speeds 1, 0.6 and 0.2 of the
	// base, first-token delays 1, 1.5 and 3 times), like `make dev-cluster`. Identical workers
	// cannot show a scheduling difference.
	Heterogeneous = "heterogeneous"
)

// Limits and defaults.
const (
	MaxWorkers = 64

	// The registry and gateway timings are short so a run is not dominated by waiting; they
	// are recorded in every result, because least-active routing follows load reported up to
	// one heartbeat ago.
	HeartbeatInterval = time.Second
	RegistryRefresh   = 100 * time.Millisecond

	// MockOutputTokens is the mock workers' natural output length: above the largest max_tokens
	// any workload asks for, so max_tokens decides how much each request generates.
	MockOutputTokens = 2000

	// settle is how long Start waits, once the control plane lists every worker, for the gateway
	// to have taken several snapshots of it.
	settle = 5 * RegistryRefresh
)

// Defaults for the mock workers' base speed.
const (
	DefaultTokensPerSecond = 1000.0
	DefaultTTFT            = 20 * time.Millisecond
	DefaultMaxConcurrency  = 8
	DefaultQueueSize       = 128
)

var speedFactors = []float64{1, 0.6, 0.2}
var ttftFactors = []float64{1, 1.5, 3}

// Config configures a cluster.
type Config struct {
	Scheduler string
	Workers   int
	Profile   string
	// Models are the models the cluster serves; each gets at least one worker. With more than
	// one, the first is the primary and the rest share a quarter of the workers (at least
	// one each).
	Models []string

	TokensPerSecond float64
	TTFT            time.Duration
	MaxConcurrency  int
	QueueSize       int

	// LogWriter, if set, receives the JSON logs of the control plane, workers and gateway; by
	// default they are discarded.
	LogWriter io.Writer
}

// Slots is the number of requests the workers can serve at once (the sum of their
// concurrency), the most closed-loop clients that cannot overload them.
func Slots(specs []WorkerSpec) int {
	n := 0
	for _, w := range specs {
		n += w.MaxConcurrency
	}
	return n
}

// WorkerSpec is one mock worker.
type WorkerSpec struct {
	ID              string
	Model           string
	TokensPerSecond float64
	TTFT            time.Duration
	MaxConcurrency  int
	QueueSize       int
	// URL is where the worker listens; set once the cluster is running.
	URL string
}

func (c *Config) withDefaults() {
	if c.Profile == "" {
		c.Profile = Identical
	}
	if c.TokensPerSecond == 0 {
		c.TokensPerSecond = DefaultTokensPerSecond
	}
	if c.TTFT == 0 {
		c.TTFT = DefaultTTFT
	}
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = DefaultMaxConcurrency
	}
	if c.QueueSize == 0 {
		c.QueueSize = DefaultQueueSize
	}
}

// Plan validates cfg and returns the workers it describes. It is pure.
func Plan(cfg Config) ([]WorkerSpec, error) {
	cfg.withDefaults()
	switch {
	case cfg.Workers < 1 || cfg.Workers > MaxWorkers:
		return nil, fmt.Errorf("workers must be between 1 and %d, got %d", MaxWorkers, cfg.Workers)
	case len(cfg.Models) == 0:
		return nil, errors.New("at least one model is required")
	case cfg.Workers < len(cfg.Models):
		return nil, fmt.Errorf("%d models need at least %d workers, got %d", len(cfg.Models), len(cfg.Models), cfg.Workers)
	case cfg.Profile != Identical && cfg.Profile != Heterogeneous:
		return nil, fmt.Errorf("unknown worker profile %q (choose %s or %s)", cfg.Profile, Identical, Heterogeneous)
	case cfg.TokensPerSecond < 1 || cfg.TokensPerSecond > 1e6:
		return nil, fmt.Errorf("mock tokens per second must be in 1-1000000, got %v", cfg.TokensPerSecond)
	case cfg.TTFT < 0 || cfg.TTFT > time.Minute:
		return nil, fmt.Errorf("mock first-token delay must be in 0-1m, got %v", cfg.TTFT)
	case cfg.MaxConcurrency < 1 || cfg.MaxConcurrency > 1024:
		return nil, fmt.Errorf("mock concurrency must be in 1-1024, got %d", cfg.MaxConcurrency)
	case cfg.QueueSize < 0 || cfg.QueueSize > 100000:
		return nil, fmt.Errorf("mock queue size must be in 0-100000, got %d", cfg.QueueSize)
	}
	seen := map[string]bool{}
	for _, m := range cfg.Models {
		if m == "" || seen[m] {
			return nil, fmt.Errorf("model names must be non-empty and distinct, got %q", m)
		}
		seen[m] = true
	}
	// Workers per model: secondary models share a quarter of the workers, one each at least.
	counts := make([]int, len(cfg.Models))
	secondaries := 0
	if len(cfg.Models) > 1 {
		secondaries = max(cfg.Workers/4, len(cfg.Models)-1)
		for i := 1; i < len(counts); i++ {
			counts[i] = secondaries / (len(counts) - 1)
		}
		for i := 1; i <= secondaries%(len(counts)-1); i++ {
			counts[i]++
		}
	}
	counts[0] = cfg.Workers - secondaries
	if counts[0] < 1 {
		return nil, fmt.Errorf("%d workers cannot serve %d models", cfg.Workers, len(cfg.Models))
	}
	var out []WorkerSpec
	for mi, m := range cfg.Models {
		for range counts[mi] {
			i := len(out)
			w := WorkerSpec{ID: fmt.Sprintf("worker-%02d", i+1), Model: m, TokensPerSecond: cfg.TokensPerSecond,
				TTFT: cfg.TTFT, MaxConcurrency: cfg.MaxConcurrency, QueueSize: cfg.QueueSize}
			if cfg.Profile == Heterogeneous {
				w.TokensPerSecond = cfg.TokensPerSecond * speedFactors[i%len(speedFactors)]
				w.TTFT = time.Duration(float64(cfg.TTFT) * ttftFactors[i%len(ttftFactors)])
			}
			out = append(out, w)
		}
	}
	return out, nil
}

// Cluster is a running simulated cluster.
type Cluster struct {
	GatewayURL      string
	ControlPlaneURL string
	// Token is the control plane's shared secret, generated per cluster. It is never logged.
	Token   string
	Workers []WorkerSpec

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	cp      *client.Client
	settled time.Duration
}

// Settled is how long Start waited, after the control plane listed every worker, for the gateway
// to take several snapshots of it.
func (c *Cluster) Settled() time.Duration { return c.settled }

// ControlPlane returns a client for the cluster's control plane.
func (c *Cluster) ControlPlane() *client.Client { return c.cp }

// Start boots the cluster and returns when every worker is eligible and the gateway
// serves every model. Call Close to stop it. ctx bounds the startup only.
func Start(ctx context.Context, cfg Config) (*Cluster, error) {
	cfg.withDefaults()
	specs, err := Plan(cfg)
	if err != nil {
		return nil, err
	}
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	logW := cfg.LogWriter
	if logW == nil {
		logW = io.Discard
	}
	log := slog.New(slog.NewJSONHandler(logW, nil))
	runCtx, cancel := context.WithCancel(context.Background())
	c := &Cluster{Token: token, Workers: specs, cancel: cancel}
	fail := func(err error) (*Cluster, error) { c.Close(); return nil, err }

	// Control plane. The failure thresholds are long: a loaded machine (the load generator shares
	// the CPU) must not make a healthy worker look suspect: under CPU starvation a 2s threshold
	// made the gateway answer 503 for workers that were alive.
	reg, err := registry.New(registry.Config{Suspect: 10 * time.Second, Unhealthy: 20 * time.Second, Lost: 60 * time.Second,
		Retention: time.Minute, MaxWorkers: 1000, HeartbeatInterval: HeartbeatInterval}, log)
	if err != nil {
		return fail(err)
	}
	cpURL, err := c.serve(runCtx, func(ctx context.Context, ln net.Listener) error {
		return server.New(reg, server.Config{Token: token, ShutdownTimeout: 2 * time.Second}, log).Serve(ctx, ln)
	})
	if err != nil {
		return fail(err)
	}
	c.ControlPlaneURL = cpURL
	c.cp = client.New(cpURL, token, nil)

	// Mock workers, each behind its own agent.
	for i := range c.Workers {
		w := &c.Workers[i]
		mcfg := mockworker.DefaultConfig()
		mcfg.Model, mcfg.WorkerID, mcfg.Seed = w.Model, w.ID, 1
		mcfg.TTFT, mcfg.TokensPerSecond, mcfg.OutputTokens = w.TTFT, w.TokensPerSecond, MockOutputTokens
		mcfg.MaxConcurrency, mcfg.QueueSize = w.MaxConcurrency, w.QueueSize
		mcfg.DrainTimeout = time.Second
		mock := mockworker.New(mcfg, log)
		if w.URL, err = c.serve(runCtx, mock.Serve); err != nil {
			return fail(err)
		}
		agent := worker.New(worker.Config{WorkerID: w.ID, Model: w.Model, AdvertiseURL: w.URL, Interval: HeartbeatInterval},
			worker.NewMockBackend(w.URL), c.cp, log)
		c.wg.Add(1)
		go func() { defer c.wg.Done(); _ = agent.Run(runCtx) }()
	}

	// Gateway in registry mode.
	gcfg := config.Default()
	gcfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	gcfg.Gateway.ControlPlaneURL = cpURL
	gcfg.Gateway.RegistryRefresh = RegistryRefresh
	gcfg.Gateway.RegistryMaxStaleness = 10 * time.Second
	gcfg.Gateway.UpstreamHeaderTimeout = 30 * time.Second // a hung worker must not cost minutes per repeat
	gcfg.Gateway.ShutdownTimeout = 2 * time.Second
	gcfg.Worker.SuspectTimeout = 10 * time.Second
	gcfg.ControlPlane.Token = token
	gcfg.Scheduler.Strategy = cfg.Scheduler
	gw, err := gateway.NewRegistry(gcfg, log)
	if err != nil {
		return fail(err)
	}
	if c.GatewayURL, err = c.serve(runCtx, gw.Serve); err != nil {
		return fail(err)
	}

	if err := c.waitReady(ctx, cfg.Models); err != nil {
		return fail(err)
	}
	return c, nil
}

// serve listens on a free loopback port, runs serve in the background, and returns its URL.
func (c *Cluster) serve(ctx context.Context, serve func(context.Context, net.Listener) error) (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	c.wg.Add(1)
	go func() { defer c.wg.Done(); _ = serve(ctx, ln) }()
	return "http://" + ln.Addr().String(), nil
}

func (c *Cluster) waitReady(ctx context.Context, models []string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		if c.ready(ctx, models) {
			// The control plane lists every worker, but the gateway reads it on its own refresh
			// timer and /v1/models is true as soon as it sees one. Without this pause the first
			// requests found a single worker, which was at capacity, and got 503 NO_CAPACITY
			// (hundreds of them in the first 250ms of a run). Let the gateway take several
			// snapshots first.
			began := time.Now()
			select {
			case <-ctx.Done():
				return fmt.Errorf("cluster not ready: %w", ctx.Err())
			case <-time.After(settle):
			}
			c.settled = time.Since(began)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster not ready: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (c *Cluster) ready(ctx context.Context, models []string) bool {
	ws, err := c.cp.Workers(ctx, client.Query{EligibleOnly: true})
	if err != nil || len(ws) < len(c.Workers) {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.GatewayURL+"/v1/models", nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return false
	}
	for _, m := range models {
		if !strings.Contains(string(b), `"`+m+`"`) {
			return false
		}
	}
	return true
}

// Close stops the cluster and waits for it to finish.
func (c *Cluster) Close() {
	c.cancel()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
}

func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "bench-" + hex.EncodeToString(b), nil
}
