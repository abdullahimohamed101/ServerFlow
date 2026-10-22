package chaos

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/bench/embedded"
	"serverflow/internal/config"
)

// Phase 15 PR E (acceptance 12, 13): more clients than the cluster has slots. The gateway queues what it can for a
// bounded time and sheds the rest with 503 NO_CAPACITY and Retry-After; every bound is read from the gateway's own
// series while the load runs. This is the deterministic version of the `overload` scenario of `benchmark chaos`.

const (
	overloadDepth  = 6
	overloadWait   = 400 * time.Millisecond
	overloadTol    = 600 * time.Millisecond // scheduling jitter on top of max_wait under -race and load
	overloadSlots  = 4
	overloadClient = 16
)

type overloadRig struct {
	t      *testing.T
	c      *embedded.Cluster
	client *http.Client
	stop   chan struct{}
	wg     sync.WaitGroup

	mu       sync.Mutex
	status   map[int]int
	bad      []string // anything that is not a 200 or a well-formed 503 NO_CAPACITY
	ok, shed atomic.Int64
}

func newOverloadRig(t *testing.T, mutate func(*config.Config)) *overloadRig {
	t.Helper()
	if testing.Short() {
		t.Skip("chaos tests start a cluster; skipped with -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := embedded.Start(ctx, embedded.Config{
		Scheduler: "least-active", Workers: 2, Models: []string{"mock-model"}, MaxConcurrency: overloadSlots / 2,
		TTFT: 40 * time.Millisecond, TokensPerSecond: 200, Resilience: "on",
		GatewayMutate: func(c *config.Config) {
			c.Resilience.Queue.MaxModelDepth, c.Resilience.Queue.MaxWait = overloadDepth, overloadWait
			if mutate != nil {
				mutate(c)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	r := &overloadRig{t: t, c: cl, stop: make(chan struct{}), status: map[int]int{},
		client: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}}
	t.Cleanup(r.client.CloseIdleConnections)
	return r
}

func (r *overloadRig) start(clients int) {
	for i := 0; i < clients; i++ {
		r.wg.Add(1)
		go func(i int) {
			defer r.wg.Done()
			for {
				select {
				case <-r.stop:
					return
				default:
				}
				r.one(i%2 == 0)
			}
		}(i)
	}
}

func (r *overloadRig) one(stream bool) {
	body := `{"model":"mock-model","max_tokens":8,"messages":[{"role":"user","content":"hello"}]`
	if stream {
		body += `,"stream":true`
	}
	req, _ := http.NewRequest(http.MethodPost, r.c.GatewayURL+"/v1/chat/completions", strings.NewReader(body+"}"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		r.record(0, "transport error: "+err.Error())
		return
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		r.record(200, "")
		r.ok.Add(1)
	case resp.StatusCode == 503 && strings.Contains(string(b), `"NO_CAPACITY"`) && resp.Header.Get("Retry-After") != "":
		r.record(503, "")
		r.shed.Add(1)
		time.Sleep(20 * time.Millisecond) // a closed-loop client told to retry later does not spin
	default:
		r.record(resp.StatusCode, "unexpected answer: "+resp.Status+" "+string(b))
	}
}

func (r *overloadRig) record(code int, bad string) {
	r.mu.Lock()
	r.status[code]++
	if bad != "" && len(r.bad) < 5 {
		r.bad = append(r.bad, bad)
	}
	r.mu.Unlock()
}

func (r *overloadRig) finish() {
	close(r.stop)
	r.wg.Wait()
}

func (r *overloadRig) gauge(name string) float64 {
	r.t.Helper()
	fams, err := r.c.Metrics().Gather()
	if err != nil {
		r.t.Fatal(err)
	}
	total := 0.0
	for _, f := range fams {
		if f.GetName() == name {
			for _, m := range f.GetMetric() {
				total += m.GetGauge().GetValue() + m.GetCounter().GetValue()
			}
		}
	}
	return total
}

// waits returns the count of waits and how many of them exceeded the bound.
func (r *overloadRig) waits(bound float64) (n, over uint64) {
	r.t.Helper()
	fams, err := r.c.Metrics().Gather()
	if err != nil {
		r.t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != "inference_queue_duration_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			h := m.GetHistogram()
			n += h.GetSampleCount()
			within := uint64(0)
			for _, b := range h.GetBucket() {
				if b.GetUpperBound() >= bound {
					within = b.GetCumulativeCount()
					break
				}
			}
			over += h.GetSampleCount() - within
		}
	}
	return n, over
}

func TestOverloadIsQueuedAndShedWithinTheBounds(t *testing.T) {
	r := newOverloadRig(t, nil)
	g0 := runtime.NumGoroutine()
	r.start(overloadClient)

	deepest := 0.0
	stopSample := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stopSample:
				return
			case <-time.After(15 * time.Millisecond):
				deepest = max(deepest, r.gauge("admission_queue_depth"))
			}
		}
	}()
	time.Sleep(4 * time.Second)
	r.finish()
	close(stopSample)
	<-sampled

	r.mu.Lock()
	status, bad := r.status, r.bad
	r.mu.Unlock()
	t.Logf("statuses %v, deepest queue %v", status, deepest)
	if len(bad) != 0 {
		t.Fatalf("answers that are neither 200 nor a 503 NO_CAPACITY with Retry-After: %v", bad)
	}
	if status[429] != 0 {
		t.Fatalf("overload is never a 429: %v", status)
	}
	if r.ok.Load() < 20 {
		t.Fatalf("only %d requests were served", r.ok.Load())
	}
	if deepest > overloadDepth {
		t.Fatalf("the queue reached depth %v, the bound is %d", deepest, overloadDepth)
	}
	if deepest == 0 {
		t.Fatal("nothing ever waited: 16 closed-loop clients on 4 slots must queue")
	}
	n, over := r.waits((overloadWait + overloadTol).Seconds())
	if n == 0 {
		t.Fatal("no wait was recorded")
	}
	if over != 0 {
		t.Fatalf("%d of %d queue waits exceeded max_wait (%v) plus the %v tolerance", over, n, overloadWait, overloadTol)
	}
	if r.shed.Load() == 0 || r.gauge("admission_rejections_total") == 0 {
		t.Fatalf("16 clients on 4 slots with a queue of %d must shed some (client saw %d)", overloadDepth, r.shed.Load())
	}
	if got := r.gauge("admission_rejections_total"); got != float64(r.shed.Load()) {
		t.Fatalf("the gateway counted %v sheds, the clients saw %d", got, r.shed.Load())
	}
	if v := r.gauge("scheduler_no_capacity_total"); v != 0 {
		t.Fatalf("a shed is admission, never a scheduling decision: scheduler_no_capacity_total %v", v)
	}
	if v := r.gauge("scheduler_ineligible_selections_total"); v != 0 {
		t.Fatalf("scheduler_ineligible_selections_total %v", v)
	}

	// Everything is given back: no waiter, no request in flight, goroutines near where they started.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && (r.gauge("admission_queue_depth") != 0 || r.gauge("inference_requests_active") != 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if d, a := r.gauge("admission_queue_depth"), r.gauge("inference_requests_active"); d != 0 || a != 0 {
		t.Fatalf("after the load: queue depth %v, requests active %v", d, a)
	}
	r.client.CloseIdleConnections()
	for time.Now().Before(deadline) && runtime.NumGoroutine() > g0+25 {
		time.Sleep(50 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > g0+25 {
		t.Fatalf("goroutines %d, %d before the load", g, g0)
	}
}

// With a global cap below the clients, the cap refuses at once (reason global_limit) and the cluster keeps serving.
func TestTheGlobalCapBoundsTheRequestsInsideTheGateway(t *testing.T) {
	r := newOverloadRig(t, func(c *config.Config) { c.Admission.MaxGlobalRequests = 8 })
	r.start(overloadClient)
	time.Sleep(3 * time.Second)
	r.finish()
	r.mu.Lock()
	bad := r.bad
	r.mu.Unlock()
	if len(bad) != 0 {
		t.Fatalf("unexpected answers: %v", bad)
	}
	fams, _ := r.c.Metrics().Gather()
	global := 0.0
	for _, f := range fams {
		if f.GetName() == "admission_rejections_total" {
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "reason" && l.GetValue() == "global_limit" {
						global += m.GetCounter().GetValue()
					}
				}
			}
		}
	}
	if global == 0 {
		t.Fatal("16 clients against a cap of 8 must hit the cap")
	}
	if r.ok.Load() < 10 {
		t.Fatalf("only %d requests were served", r.ok.Load())
	}
}
