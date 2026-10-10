package integration

import (
	"bufio"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/mockworker"
)

// The Phase 6 acceptance run: 1000 synthetic requests through a real gateway in registry mode against mock
// workers behind real agents. The numbers it logs are recorded in docs/benchmarks/phase-6-distribution.md.
// They describe what happened in this run; they are not a claim that one strategy beats another.

const (
	llama      = "llama-8b"
	totalReqs  = 1000
	clientConc = 16
)

type loadResult struct {
	byModel      map[string]*modelResult
	failures     int64
	retried      int64 // requests that needed a second attempt
	attemptsSeen int64
}

type modelResult struct{ sent, ok int64 }

func sendOne(gw, modelName string, stream bool) (status int, attempts int) {
	body := `{"model":"` + modelName + `","messages":[{"role":"user","content":"hello"}]`
	if stream {
		body += `,"stream":true`
	}
	resp, err := http.Post(gw+"/v1/chat/completions", "application/json", strings.NewReader(body+"}"))
	if err != nil {
		return -1, 0
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, bufio.NewReader(resp.Body))
	attempts = 1
	if v := resp.Header.Get("X-ServerFlow-Attempts"); v != "" {
		attempts, _ = strconv.Atoi(v)
	}
	return resp.StatusCode, attempts
}

// runLoad sends n requests with clientConc concurrent clients. modelFor picks each request's model; every
// fourth request streams.
func runLoad(gw string, from, n int, modelFor func(i int) string, onProgress func(i int)) *loadResult {
	res := &loadResult{byModel: map[string]*modelResult{}}
	var mu sync.Mutex
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < clientConc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				m := modelFor(i)
				status, attempts := sendOne(gw, m, i%4 == 0)
				mu.Lock()
				mr := res.byModel[m]
				if mr == nil {
					mr = &modelResult{}
					res.byModel[m] = mr
				}
				mr.sent++
				if status == 200 {
					mr.ok++
				} else {
					res.failures++
				}
				res.attemptsSeen += int64(attempts)
				if attempts > 1 {
					res.retried++
				}
				mu.Unlock()
			}
		}()
	}
	for i := from; i < from+n; i++ {
		if onProgress != nil {
			onProgress(i)
		}
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return res
}

func mix3to1(i int) string {
	if i%4 == 3 {
		return llama
	}
	return model
}

func chatsOf(nodes []*countedNode) (counts []int64, sum int64) {
	for _, n := range nodes {
		c := n.chats.Load()
		counts = append(counts, c)
		sum += c
	}
	return
}

func resetChats(nodes ...[]*countedNode) {
	for _, group := range nodes {
		for _, n := range group {
			n.chats.Store(0)
		}
	}
}

func spread(counts []int64) (min, max int64) {
	min, max = counts[0], counts[0]
	for _, c := range counts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	return
}

// startRelaxedGateway is a registry-mode gateway whose view of the workers survives a loaded machine.
func startRelaxedGateway(t *testing.T, c *controlPlane, strategy string) string {
	return startRegistryGateway(t, c, strategy, func(cfg *config.Config) {
		cfg.Worker.SuspectTimeout = c.suspect
		cfg.Gateway.RegistryMaxStaleness = 3 * time.Second
	})
}

// warm sends a few requests of each model so every worker has been seen by the gateway.
func warm(t *testing.T, c *controlPlane, gw string, ids ...string) {
	t.Helper()
	waitFor(t, 10*time.Second, "all workers eligible", func() bool {
		for _, id := range ids {
			if !c.eligible(id) {
				return false
			}
		}
		return true
	})
	waitFor(t, 10*time.Second, "the gateway lists both models", func() bool {
		resp, err := http.Get(gw + "/v1/models")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return strings.Contains(string(b), model) && strings.Contains(string(b), llama)
	})
	time.Sleep(150 * time.Millisecond) // a few refreshes, so the gateway holds every worker
}

func TestThousandRequestsAreDistributedByStrategyAndReachOnlyWorkersOfTheirModel(t *testing.T) {
	for _, strategy := range []string{"round-robin", "random", "least-active", "least-queue"} {
		t.Run(strategy, func(t *testing.T) {
			c := startRelaxedControlPlane(t)
			qwen := []*countedNode{c.startModelNode(t, "q1", model, nil), c.startModelNode(t, "q2", model, nil),
				c.startModelNode(t, "q3", model, nil), c.startModelNode(t, "q4", model, nil)}
			llamas := []*countedNode{c.startModelNode(t, "l1", llama, nil), c.startModelNode(t, "l2", llama, nil)}
			gw := startRelaxedGateway(t, c, strategy)
			warm(t, c, gw, "q1", "q2", "q3", "q4", "l1", "l2")
			resetChats(qwen, llamas)

			res := runLoad(gw, 0, totalReqs, mix3to1, nil)
			qc, qsum := chatsOf(qwen)
			lc, lsum := chatsOf(llamas)
			qmin, qmax := spread(qc)
			lmin, lmax := spread(lc)
			t.Logf("%s: qwen %v (min %d max %d)  llama %v (min %d max %d)  failures %d retried %d", strategy, qc, qmin, qmax, lc, lmin, lmax, res.failures, res.retried)

			if res.failures != 0 || res.retried != 0 {
				t.Fatalf("with every worker healthy nothing fails and nothing is retried: %d failures, %d retried", res.failures, res.retried)
			}
			if qsum != 750 || lsum != 250 || res.byModel[model].sent != 750 || res.byModel[llama].sent != 250 {
				t.Fatalf("every request must reach a worker of its own model: qwen workers served %d of 750, llama workers %d of 250", qsum, lsum)
			}
			switch strategy {
			case "round-robin":
				if qmax-qmin > 1 || lmax-lmin > 1 {
					t.Fatalf("round-robin splits evenly (within one request): qwen %v llama %v", qc, lc)
				}
			case "random":
				// 750 over 4 workers: mean 187.5, sd about 12; 6 sd is about 72. Llama: 250 over 2, sd about 8.
				if qmin < 115 || qmax > 260 || lmin < 75 || lmax > 175 {
					t.Fatalf("random strayed far from uniform: qwen %v llama %v", qc, lc)
				}
			default:
				// Reported load is at most one heartbeat old, so only a loose bound is promised: nobody starves.
				if qmin < 100 || qmax > 300 || lmin < 60 || lmax > 190 {
					t.Fatalf("%s left a worker starved or swamped: qwen %v llama %v", strategy, qc, lc)
				}
			}
		})
	}
}

func TestAFlakyWorkerCostsNoClientFailuresWhileHealthyWorkersExist(t *testing.T) {
	for _, strategy := range []string{"round-robin", "random", "least-active", "least-queue"} {
		t.Run(strategy, func(t *testing.T) {
			c := startRelaxedControlPlane(t)
			flaky := c.startModelNode(t, "q1", model, func(m *mockworker.Config) { m.FailureRate, m.FailureMode = 1, mockworker.ModeUnavailable })
			good := []*countedNode{c.startModelNode(t, "q2", model, nil), c.startModelNode(t, "q3", model, nil), c.startModelNode(t, "q4", model, nil)}
			llamas := []*countedNode{c.startModelNode(t, "l1", llama, nil), c.startModelNode(t, "l2", llama, nil)}
			gw := startRelaxedGateway(t, c, strategy)
			warm(t, c, gw, "q1", "q2", "q3", "q4", "l1", "l2")
			resetChats(good, llamas)
			flaky.chats.Store(0)

			res := runLoad(gw, 0, totalReqs, mix3to1, nil)
			gc, gsum := chatsOf(good)
			t.Logf("%s: flaky received %d first attempts; healthy qwen %v; failures %d; retried %d of %d", strategy, flaky.chats.Load(), gc, res.failures, res.retried, totalReqs)

			if res.failures != 0 {
				t.Fatalf("%d client-visible failures although three healthy workers existed", res.failures)
			}
			if flaky.chats.Load() == 0 {
				t.Fatal("the flaky worker was never tried, so this proved nothing")
			}
			if res.retried != flaky.chats.Load() {
				t.Fatalf("every request that reached the flaky worker is retried exactly once: retried %d, flaky hits %d", res.retried, flaky.chats.Load())
			}
			if gsum != 750 {
				t.Fatalf("the healthy workers served the 750 qwen requests between them (%d)", gsum)
			}
			if got := res.attemptsSeen; got != totalReqs+res.retried {
				t.Fatalf("attempts %d, want %d requests plus %d retries", got, totalReqs, res.retried)
			}
		})
	}
}

// retriesWithReason sums inference_retries_total for a reason from the gateway's /metrics.
func retriesWithReason(t *testing.T, gw, reason string) float64 {
	t.Helper()
	resp, err := http.Get(metricsOf(gw) + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	var sum float64
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "inference_retries_total{") && strings.Contains(line, `reason="`+reason+`"`) {
			f := strings.Fields(line)
			v, _ := strconv.ParseFloat(f[len(f)-1], 64)
			sum += v
		}
	}
	return sum
}

func TestAWorkerKilledMidRunCostsNothingAndIsNeverUsedAfterItIsNoticed(t *testing.T) {
	c := startRelaxedControlPlane(t)
	nodes := []*countedNode{c.startModelNode(t, "q1", model, nil), c.startModelNode(t, "q2", model, nil), c.startModelNode(t, "q3", model, nil)}
	llamas := []*countedNode{c.startModelNode(t, "l1", llama, nil)}
	gw := startRelaxedGateway(t, c, "round-robin")
	warm(t, c, gw, "q1", "q2", "q3", "l1")
	resetChats(nodes, llamas)

	var killedAt time.Time
	var before float64

	// Phase one: 300 requests with all workers up, then the backend of q2 dies.
	r1 := runLoad(gw, 0, 300, mix3to1, nil)
	nodes[1].mock.CloseClientConnections()
	nodes[1].mock.Close()
	killedAt = time.Now()

	// Phase two: keep sending while the registry notices. Requests that land on the dead worker are retried.
	r2 := runLoad(gw, 300, 200, mix3to1, nil)
	waitFor(t, 5*time.Second, "the registry marks q2 not eligible", func() bool { return !c.eligible("q2") })
	noticed := time.Since(killedAt)
	time.Sleep(150 * time.Millisecond) // the gateway's next refreshes
	before = retriesWithReason(t, gw, "connect")

	// Phase three: from here on nothing may be sent to the dead worker, so no connect retries happen.
	r3 := runLoad(gw, 500, 500, mix3to1, nil)
	after := retriesWithReason(t, gw, "connect")
	t.Logf("killed q2 after 300 requests; registry noticed after %v; failures %d/%d/%d; retried %d/%d/%d; connect retries before=%v after=%v",
		noticed.Round(time.Millisecond), r1.failures, r2.failures, r3.failures, r1.retried, r2.retried, r3.retried, before, after)

	if r1.failures+r2.failures+r3.failures != 0 {
		t.Fatalf("a dead worker with healthy alternatives must cost no client failures: %d/%d/%d", r1.failures, r2.failures, r3.failures)
	}
	if r2.retried == 0 || before == 0 {
		t.Fatalf("the window before the death is noticed must have exercised the retry path (retried %d, connect retries %v)", r2.retried, before)
	}
	if r1.retried != 0 || r3.retried != 0 {
		t.Fatalf("retries only while the death is undetected: before %d, after %d", r1.retried, r3.retried)
	}
	if after != before {
		t.Fatalf("connect retries grew from %v to %v after the worker was noticed: traffic still reached it", before, after)
	}
	_, qsum := chatsOf([]*countedNode{nodes[0], nodes[2]})
	if qsum+nodes[1].chats.Load() < 750-5 {
		t.Fatalf("lost requests? qwen workers served %d", qsum+nodes[1].chats.Load())
	}
}
