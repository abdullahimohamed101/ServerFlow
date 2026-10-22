package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// Process-level tests of graceful drain (Phase 15 PR F, D15 and D16): real gateway, worker agent, mock worker and
// control plane binaries, real SIGTERM. They need the drain feature on (resilience.mode on, registry workers);
// the same signals with the feature off are covered by signals_process_test.go and by the "off" cases below.

// drainGatewayEnv is the gateway's environment for a drain test: resilience on, a readiness grace and a shutdown
// timeout that is long enough to see the sequence.
func drainGatewayEnv(cpAddr, gwAddr string, grace, shutdown time.Duration, extra ...string) []string {
	env := append(gatewayEnv(cpAddr, clusterToken, gwAddr, "least-active"),
		"SERVERFLOW_RESILIENCE_MODE=on",
		"SERVERFLOW_RESILIENCE_DRAIN_READINESS_GRACE="+grace.String(),
		"SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT="+shutdown.String())
	return append(env, extra...)
}

func startDrainGateway(t *testing.T, cpAddr, gwAddr string, grace, shutdown time.Duration, extra ...string) *proc {
	t.Helper()
	return startProc(t, "gateway", "gateway starting", drainGatewayEnv(cpAddr, gwAddr, grace, shutdown, extra...))
}

// startDrainCluster starts a control plane and workers (one mock and one agent each, ids w1..wn) and waits until
// they are eligible. mockArgs apply to every mock.
func startDrainCluster(t *testing.T, workers int, mockArgs ...string) (cpAddr string, mocks []*proc, mockAddrs []string) {
	t.Helper()
	cpAddr = freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	for i := 1; i <= workers; i++ {
		addr := freePort(t)
		mocks = append(mocks, startMockProc(t, addr, mockArgs...))
		mockAddrs = append(mockAddrs, addr)
		startAgentProc(t, cpAddr, clusterToken, fmt.Sprintf("w%d", i), addr)
	}
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "every worker eligible", func(ws map[string]protocol.WorkerSnapshot) bool {
		n := 0
		for _, w := range ws {
			if w.Eligible {
				n++
			}
		}
		return n == workers
	})
	return cpAddr, mocks, mockAddrs
}

func waitGatewayServing(t *testing.T, gwAddr string) {
	t.Helper()
	waitFor(t, 10*time.Second, "the gateway to serve", func() bool {
		code, _ := gatewayChat(gwAddr)
		return code == 200
	})
}

// streamResult is what a client reading a stream saw.
type streamResult struct {
	status int
	chunks int
	done   bool
	err    error
	ended  time.Time
}

// startStream opens a streaming chat request and reads it in the background.
func startStream(gwAddr string) <-chan streamResult {
	out := make(chan streamResult, 1)
	go func() {
		var r streamResult
		defer func() { r.ended = time.Now(); out <- r }()
		resp, err := (&http.Client{Timeout: 60 * time.Second}).Post("http://"+gwAddr+"/v1/chat/completions", "application/json", strings.NewReader(chat(true, "drain")))
		if err != nil {
			r.err = err
			return
		}
		defer func() { _ = resp.Body.Close() }()
		r.status = resp.StatusCode
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "data: [DONE]":
				r.done = true
			case strings.HasPrefix(line, "data: "):
				r.chunks++
			}
		}
		r.err = sc.Err()
	}()
	return out
}

func metricLine(t *testing.T, mAddr, prefix string) string {
	t.Helper()
	_, body := authGet(t, mAddr, "/metrics", "")
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, prefix) {
			return l
		}
	}
	return ""
}

func sigterm(t *testing.T, p *proc) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
}

// F1 (acceptance 17): the sequence of a gateway drain, observed from outside. SIGTERM with a stream in flight:
// /readyz fails within 200 ms (the probe cache is bypassed), requests during the grace succeed and end their
// connection, the metrics listener says gateway_draining 1, the listener closes after the grace, the stream
// completes in full, and the process exits 0 after the stream and after the grace.
func TestProcessGatewayDrainSequence(t *testing.T) {
	const grace = 1500 * time.Millisecond
	cpAddr, _, _ := startDrainCluster(t, 1, "--tokens-per-second=2") // an 8-token stream takes about 4 s
	gwAddr := freePort(t)
	gw := startDrainGateway(t, cpAddr, gwAddr, grace, 30*time.Second)
	waitGatewayServing(t, gwAddr)
	mAddr := strings.TrimPrefix(gw.metricsURL(t), "http://")

	stream := startStream(gwAddr)
	waitFor(t, 5*time.Second, "the stream to be in flight", func() bool {
		return strings.HasSuffix(metricLine(t, mAddr, "inference_requests_active"), " 1")
	})
	// Fill the readiness cache with a "ready" answer, so that bypassing it is what the test measures.
	if r, _ := authGet(t, gwAddr, "/readyz", ""); r.StatusCode != 200 {
		t.Fatalf("not ready before the signal: %d", r.StatusCode)
	}
	if l := metricLine(t, mAddr, "gateway_draining"); l != "gateway_draining 0" {
		t.Fatalf("before the signal: %q", l)
	}

	signalled := time.Now()
	sigterm(t, gw)
	var readyzAt time.Duration
	for {
		r, _ := authGet(t, gwAddr, "/readyz", "")
		if r.StatusCode == 503 {
			readyzAt = time.Since(signalled)
			if r.Header.Get("Retry-After") == "" {
				t.Fatal("a draining /readyz carries Retry-After")
			}
			break
		}
		if time.Since(signalled) > 2*time.Second {
			t.Fatal("/readyz never failed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if readyzAt > 200*time.Millisecond {
		t.Fatalf("/readyz failed %v after the signal, want within 200 ms", readyzAt)
	}
	if l := metricLine(t, mAddr, "gateway_draining"); l != "gateway_draining 1" {
		t.Fatalf("during the drain: %q", l)
	}

	// A request in the grace is served, and the connection ends with it.
	req, _ := http.NewRequest(http.MethodPost, "http://"+gwAddr+"/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","max_tokens":1,"messages":[{"role":"user","content":"grace"}]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("a request during the grace failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !resp.Close {
		t.Fatalf("a request during the grace: %d, connection closing %v", resp.StatusCode, resp.Close)
	}

	// After the grace the listener is gone while the stream is still running.
	time.Sleep(time.Until(signalled.Add(grace + 300*time.Millisecond)))
	if c, err := net.DialTimeout("tcp", gwAddr, 300*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatal("the listener is still open after the readiness grace")
	}
	select {
	case r := <-stream:
		t.Fatalf("the stream ended during the drain, too early: %+v", r)
	default:
	}
	// The metrics listener outlives the drain.
	if l := metricLine(t, mAddr, "gateway_draining"); l != "gateway_draining 1" {
		t.Fatalf("metrics after the grace: %q", l)
	}

	r := <-stream
	if r.err != nil || r.status != 200 || !r.done || r.chunks < 8 {
		t.Fatalf("the stream in flight at SIGTERM did not complete: %+v", r)
	}
	if err := gw.wait(t, 10*time.Second); err != nil {
		t.Fatalf("the drained gateway must exit 0: %v\n%s", err, gw.stderr.String())
	}
	if exited := time.Now(); exited.Sub(signalled) < grace {
		t.Fatalf("the process left %v after the signal, before the %v grace", exited.Sub(signalled), grace)
	}
	for _, want := range []string{"gateway draining: not ready, still serving", "gateway readiness grace over", "gateway stopped"} {
		if !strings.Contains(gw.stderr.String(), want) {
			t.Fatalf("log line %q missing:\n%s", want, gw.stderr.String())
		}
	}
}

// F2 (acceptance 17): a second signal ends the gateway at once, in the grace and in the drain after it.
func TestProcessGatewayDrainSecondSignalExitsAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay time.Duration // from the first signal to the second
	}{{"in the readiness grace", 300 * time.Millisecond}, {"in the drain after the grace", 1800 * time.Millisecond}} {
		t.Run(tc.name, func(t *testing.T) {
			cpAddr, _, _ := startDrainCluster(t, 1, "--tokens-per-second=0.5") // a stream takes about 16 s
			gwAddr := freePort(t)
			gw := startDrainGateway(t, cpAddr, gwAddr, 1200*time.Millisecond, 60*time.Second)
			waitGatewayServing(t, gwAddr)
			_ = startStream(gwAddr)
			mAddr := strings.TrimPrefix(gw.metricsURL(t), "http://")
			waitFor(t, 5*time.Second, "the stream to be in flight", func() bool {
				return strings.HasSuffix(metricLine(t, mAddr, "inference_requests_active"), " 1")
			})
			sigterm(t, gw)
			time.Sleep(tc.delay)
			if gw.dead.Load() {
				t.Fatalf("the gateway exited on the first signal:\n%s", gw.stderr.String())
			}
			second := time.Now()
			sigterm(t, gw)
			if err := gw.wait(t, 3*time.Second); err == nil {
				t.Fatal("a forced exit must be non-zero")
			}
			if d := time.Since(second); d > 2*time.Second {
				t.Fatalf("the second signal took %v to end the gateway", d)
			}
		})
	}
}

// F3 (acceptance 17): requests waiting in the admission queue when the signal comes are not flushed; they are
// served when a slot frees (the grace covers them) and the process then exits cleanly.
func TestProcessGatewayDrainLetsQueuedWaitersFinish(t *testing.T) {
	cpAddr, _, _ := startDrainCluster(t, 1, "--max-concurrency=1", "--tokens-per-second=8") // 8 tokens: about 1 s each
	gwAddr := freePort(t)
	gw := startDrainGateway(t, cpAddr, gwAddr, 1500*time.Millisecond, 30*time.Second, "SERVERFLOW_RESILIENCE_QUEUE_MAX_WAIT=10s")
	waitGatewayServing(t, gwAddr)
	mAddr := strings.TrimPrefix(gw.metricsURL(t), "http://")

	held := startStream(gwAddr)
	waitFor(t, 5*time.Second, "the slot to be taken", func() bool {
		return strings.HasSuffix(metricLine(t, mAddr, "inference_requests_active"), " 1")
	})
	waiter := startStream(gwAddr)
	waitFor(t, 5*time.Second, "a request to wait in the queue", func() bool {
		return metricLine(t, mAddr, `admission_queue_depth{model="`+model+`"}`) == `admission_queue_depth{model="`+model+`"} 1`
	})
	sigterm(t, gw)
	for name, c := range map[string]<-chan streamResult{"held": held, "waiter": waiter} {
		r := <-c
		if r.err != nil || r.status != 200 || !r.done {
			t.Fatalf("%s did not complete across the signal: %+v\n%s", name, r, gw.stderr.String())
		}
	}
	if err := gw.wait(t, 10*time.Second); err != nil {
		t.Fatalf("exit: %v\n%s", err, gw.stderr.String())
	}
	if strings.Contains(gw.stderr.String(), `"msg":"request shed"`) {
		t.Fatalf("no request should have been shed:\n%s", gw.stderr.String())
	}
}

// F4: with the feature off (the default) the same signal behaves as it always did: no grace, the listener closes
// at once, readiness is not touched, the stream in flight completes, and the process exits 0.
func TestProcessGatewayDrainOffKeepsTheOldSequence(t *testing.T) {
	cpAddr, _, _ := startDrainCluster(t, 1, "--tokens-per-second=2")
	gwAddr := freePort(t)
	gw := startProc(t, "gateway", "gateway starting", append(gatewayEnv(cpAddr, clusterToken, gwAddr, "least-active"),
		"SERVERFLOW_RESILIENCE_MODE=off", "SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT=30s"))
	waitGatewayServing(t, gwAddr)
	mAddr := strings.TrimPrefix(gw.metricsURL(t), "http://")
	stream := startStream(gwAddr)
	waitFor(t, 5*time.Second, "the stream to be in flight", func() bool {
		return strings.HasSuffix(metricLine(t, mAddr, "inference_requests_active"), " 1")
	})
	sigterm(t, gw)
	waitFor(t, 500*time.Millisecond, "the listener to close at once", func() bool {
		c, err := net.DialTimeout("tcp", gwAddr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return false
		}
		return true
	})
	if l := metricLine(t, mAddr, "gateway_draining"); l != "" {
		t.Fatalf("gateway_draining exists with the feature off: %q", l)
	}
	if r := <-stream; r.err != nil || !r.done {
		t.Fatalf("stream: %+v", r)
	}
	if err := gw.wait(t, 10*time.Second); err != nil {
		t.Fatalf("exit: %v", err)
	}
	if strings.Contains(gw.stderr.String(), "readiness grace") {
		t.Fatal("no readiness grace with the feature off")
	}
}

// countRetries reads the gateway's retry counter (all models).
func countRetries(t *testing.T, mAddr string) float64 {
	t.Helper()
	_, body := authGet(t, mAddr, "/metrics", "")
	var total float64
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "inference_retries_total") {
			var v float64
			if _, err := fmt.Sscanf(l[strings.LastIndex(l, " ")+1:], "%g", &v); err == nil {
				total += v
			}
		}
	}
	return total
}

// drainLoad is what a closed-loop load generator saw.
type drainLoad struct {
	ok, failed atomic.Int64
	mu         sync.Mutex
	failures   []string
}

func (l *drainLoad) fail(s string) {
	l.failed.Add(1)
	l.mu.Lock()
	if len(l.failures) < 10 {
		l.failures = append(l.failures, s)
	}
	l.mu.Unlock()
}

// runLoad drives clients closed-loop non-stream requests at addr until the returned stop function is called.
func runDrainLoad(addr string, clients int) (*drainLoad, func()) {
	res := &drainLoad{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hc := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{}}
			defer hc.CloseIdleConnections()
			for ctx.Err() == nil {
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/v1/chat/completions", strings.NewReader(chat(false, "load")))
				req.Header.Set("Content-Type", "application/json")
				resp, err := hc.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						res.fail(err.Error())
					}
					continue
				}
				b, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != 200 {
					res.fail(fmt.Sprintf("%d %.80s", resp.StatusCode, b))
				} else {
					res.ok.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	return res, func() { cancel(); wg.Wait() }
}

// F5 (acceptance 18, process level): replace both workers one after the other under load. Each old mock gets
// SIGTERM with a drain announce above the heartbeat plus the registry refresh; the agent reports the worker
// draining and the gateway stops routing to it before it refuses. No client request fails and no request is
// retried because of the drain. With announce 0 the same roll fails no client either (retry covers the
// window) but the retries show; they are reported, not failed.
func TestProcessWorkerRollUnderLoad(t *testing.T) {
	for _, tc := range []struct {
		name     string
		announce string
		strict   bool
	}{{"announce above heartbeat plus refresh", "1s", true}, {"announce 0", "0s", false}} {
		t.Run(tc.name, func(t *testing.T) {
			cpAddr := freePort(t)
			startControlPlaneProc(t, cpAddr, clusterToken)
					var mocks [2]*proc
			var addrs [2]string
			start := func(i int, id string) {
				addrs[i] = freePort(t)
				mocks[i] = startMockProc(t, addrs[i], "--drain-announce="+tc.announce, "--drain-timeout=10s", "--tokens-per-second=200")
				startAgentProc(t, cpAddr, clusterToken, id, addrs[i])
			}
			start(0, "a1")
			start(1, "b1")
			gwAddr := freePort(t)
			gw := startProc(t, "gateway", "gateway starting", append(gatewayEnv(cpAddr, clusterToken, gwAddr, "least-active"), "SERVERFLOW_RESILIENCE_MODE=off"))
			waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "both eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["a1"].Eligible && ws["b1"].Eligible })
			waitGatewayServing(t, gwAddr)
			mAddr := strings.TrimPrefix(gw.metricsURL(t), "http://")
			retries0 := countRetries(t, mAddr)

			// Two clients on the four slots of the one worker left serving: within the safe share (0.6), so a stale
			// reported load cannot turn the roll into NO_CAPACITY (a known scheduler property, Phase 14).
			res, stop := runDrainLoad(gwAddr, 2)
			time.Sleep(500 * time.Millisecond)
			for i, next := range []string{"a2", "b2"} {
				old := mocks[i]
				began := time.Now()
				sigterm(t, old) // the agent notices the draining backend and reports it
				if err := old.wait(t, 15*time.Second); err != nil {
					t.Fatalf("the drained mock must exit 0: %v", err)
				}
				drained := time.Since(began)
				start(i, next)
				t.Logf("replacing worker %d: the old mock drained in %v, the replacement started in %v", i, drained, time.Since(began)-drained)
				waitWorkers(t, cpAddr, clusterToken, 10*time.Second, next+" eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws[next].Eligible })
				time.Sleep(300 * time.Millisecond)
			}
			stop()
			retries := countRetries(t, mAddr) - retries0
			t.Logf("roll with announce %s: %d requests ok, %d failed, %v retries", tc.announce, res.ok.Load(), res.failed.Load(), retries)
			if res.ok.Load() < 20 {
				t.Fatalf("only %d requests completed: the load did not run", res.ok.Load())
			}
			if f := res.failed.Load(); f != 0 {
				t.Fatalf("%d client failures during the roll: %v", f, res.failures)
			}
			if tc.strict && retries != 0 {
				t.Fatalf("%v retries during a roll with an announce above heartbeat plus refresh; the drain must not cost a retry", retries)
			}
		})
	}
}

// F6: SIGTERM to an agent reports the worker draining and takes it out of the registry at once, the backend keeps
// serving what is in flight (the agent is not in the data path), and a second signal forces an agent that cannot
// reach its control plane to quit. (The second-signal case is covered in cluster_process_test.go.)
func TestProcessAgentSigtermDeregistersWhileTheBackendFinishes(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	mAddr := freePort(t)
	startMockProc(t, mAddr, "--tokens-per-second=2")
	agent := startAgentProc(t, cpAddr, clusterToken, "w1", mAddr)
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })
	// A request straight to the backend, in flight when the agent is told to stop.
	stream := startStream(mAddr)
	time.Sleep(300 * time.Millisecond)
	sigterm(t, agent)
	if err := agent.wait(t, 8*time.Second); err != nil {
		t.Fatalf("the agent must exit 0 on SIGTERM: %v\n%s", err, agent.stderr.String())
	}
	if ws, err := fetchWorkers(cpAddr, clusterToken); err != nil || ws["w1"].Eligible {
		t.Fatalf("the worker must not be routable once its agent has drained: %+v %v", ws["w1"], err)
	}
	if r := <-stream; r.err != nil || !r.done {
		t.Fatalf("the backend's stream did not complete: %+v", r)
	}
}

// F7 (acceptance 17, gateway roll): two gateways behind a load balancer that polls /readyz every 50 ms and sends
// each request to a ready gateway. Rolling them one at a time under load, with a readiness grace well above the
// poll interval, fails no client request: the draining gateway leaves rotation within a poll while it still serves,
// and its in-flight requests finish. With grace 0 the same roll is run and reported, not asserted: the requests the
// balancer routes in its detection gap hit a closed listener.
func TestProcessGatewayRollBehindALoadBalancer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		grace  time.Duration
		strict bool
	}{{"grace 1s", time.Second, true}, {"grace 0 (reported)", 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			cpAddr, _, _ := startDrainCluster(t, 2, "--tokens-per-second=200")
			lb := newTestBalancer(t)
			var gws [2]*proc
			var addrs [2]string
			start := func(i int) {
				addrs[i] = freePort(t)
				gws[i] = startDrainGateway(t, cpAddr, addrs[i], tc.grace, 20*time.Second)
				lb.set(i, addrs[i])
			}
			start(0)
			start(1)
			waitFor(t, 10*time.Second, "both gateways ready in the balancer", func() bool { return lb.readyCount() == 2 })

			res, stop := runDrainLoad(lb.addr, 2)
			time.Sleep(500 * time.Millisecond)
			inflight := startStream(addrs[0]) // straight to the gateway about to drain
			time.Sleep(200 * time.Millisecond)
			for i := 0; i < 2; i++ {
				old := gws[i]
				sigterm(t, old)
				if err := old.wait(t, 20*time.Second); err != nil {
					t.Fatalf("gateway %d must exit 0 after draining: %v\n%s", i, err, old.stderr.String())
				}
				start(i)
				waitFor(t, 10*time.Second, "the replacement to be ready in the balancer", func() bool { return lb.readyCount() == 2 })
				time.Sleep(300 * time.Millisecond)
			}
			stop()
			if r := <-inflight; r.err != nil || !r.done {
				t.Fatalf("a stream in flight at the first gateway's SIGTERM did not complete: %+v", r)
			}
			t.Logf("gateway roll, grace %v: %d requests ok, %d failed %v", tc.grace, res.ok.Load(), res.failed.Load(), res.failures)
			if res.ok.Load() < 20 {
				t.Fatalf("only %d requests completed: the load did not run", res.ok.Load())
			}
			if tc.strict && res.failed.Load() != 0 {
				t.Fatalf("%d client failures during the gateway roll: %v", res.failed.Load(), res.failures)
			}
		})
	}
}

// testBalancer is a minimal layer-4/7 balancer in the test process: it polls each backend's /readyz every 50 ms,
// sends each request to a ready backend (round robin), and does not retry: a connection to a closed listener is a
// failed request, as with a plain load balancer.
type testBalancer struct {
	addr string
	mu   sync.Mutex
	back [2]string
	up   [2]bool
	next atomic.Uint64
	stop chan struct{}
}

func newTestBalancer(t *testing.T) *testBalancer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &testBalancer{addr: ln.Addr().String(), stop: make(chan struct{})}
	rp := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.URL.Host = b.pick()
		},
		Transport:     &http.Transport{DisableKeepAlives: true},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "bad gateway: "+err.Error(), http.StatusBadGateway)
		},
	}
	srv := &http.Server{Handler: rp}
	go func() { _ = srv.Serve(ln) }()
	go b.poll()
	t.Cleanup(func() { close(b.stop); _ = srv.Close() })
	return b
}

func (b *testBalancer) set(i int, addr string) { b.mu.Lock(); b.back[i], b.up[i] = addr, false; b.mu.Unlock() }

func (b *testBalancer) readyCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for i := range b.up {
		if b.up[i] {
			n++
		}
	}
	return n
}

// pick returns a ready backend; with none ready it returns a backend anyway (an unreachable address fails the
// request, which the test counts).
func (b *testBalancer) pick() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	start := int(b.next.Add(1))
	for k := 0; k < 2; k++ {
		i := (start + k) % 2
		if b.up[i] && b.back[i] != "" {
			return b.back[i]
		}
	}
	return "127.0.0.1:1"
}

func (b *testBalancer) poll() {
	hc := &http.Client{Timeout: 500 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-tick.C:
		}
		for i := 0; i < 2; i++ {
			b.mu.Lock()
			addr := b.back[i]
			b.mu.Unlock()
			if addr == "" {
				continue
			}
			ok := false
			if resp, err := hc.Get((&url.URL{Scheme: "http", Host: addr, Path: "/readyz"}).String()); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				ok = resp.StatusCode == 200
			}
			b.mu.Lock()
			if b.back[i] == addr {
				b.up[i] = ok
			}
			b.mu.Unlock()
		}
	}
}
