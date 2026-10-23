package embedded

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/config"
)

// The H1 guard of Phase 14 (plan): "over-capacity refusals under the ledger overlay rise by less than 0.5 percentage
// points against max". Closed-loop clients against a 4 x 8-slot embedded cluster with least-active routing, with the
// resilience feature off (a refusal is then a 503 NO_CAPACITY the client sees) or on (a refusal is a queue wait,
// visible as throughput and latency). Alongside the refusals the test samples every worker's own active-request count,
// so an overlay that under-counts would show as a worker running more than its slots.
//
// Simulator results from an idle machine (embedded mock workers), not hardware results. The default test runs the
// worst cell the independent verifier found for the first boundary rule; SERVERFLOW_LEDGER_LOAD=full runs the whole
// matrix and logs it.

type loadResult struct {
	ok, refused int64
	p50, p95    time.Duration
	maxActive   int // highest active-request count any worker reported while the load ran
}

func runLoad(t *testing.T, overlay, resilience string, maxTokens, clients int, dur time.Duration) loadResult {
	t.Helper()
	c := startCluster(t, Config{Workers: 4, MaxConcurrency: 8, Scheduler: "least-active", Resilience: resilience,
		GatewayMutate: func(g *config.Config) { g.Scheduler.LoadOverlay = overlay }})
	defer c.Close()
	var ok, bad atomic.Int64
	var mu sync.Mutex
	var lats []time.Duration
	stop := time.Now().Add(dur)
	var maxActive atomic.Int64
	sampDone := make(chan struct{})
	go func() {
		defer close(sampDone)
		for time.Now().Before(stop) {
			for _, a := range sampleActive(c.Specs()) {
				for {
					cur := maxActive.Load()
					if int64(a) <= cur || maxActive.CompareAndSwap(cur, int64(a)) {
						break
					}
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	// One keep-alive connection per client: with the default transport (two idle connections per host) a refusal
	// storm churns thousands of connections and exhausts the machine's ephemeral ports, which then dominates the result.
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: clients}}
	defer hc.CloseIdleConnections()
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				t0 := time.Now()
				resp, err := hc.Post(c.GatewayURL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":`+strconv.Itoa(maxTokens)+`}`))
				if err != nil {
					bad.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode == 200 {
					ok.Add(1)
					mu.Lock()
					lats = append(lats, time.Since(t0))
					mu.Unlock()
				} else {
					bad.Add(1)
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	<-sampDone
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	r := loadResult{ok: ok.Load(), refused: bad.Load(), maxActive: int(maxActive.Load())}
	if len(lats) > 0 {
		r.p50, r.p95 = lats[len(lats)/2], lats[len(lats)*95/100]
	}
	return r
}

func (r loadResult) refusedPct() float64 { return 100 * float64(r.refused) / float64(r.ok+r.refused+1) }

func TestLedgerRefusalsAndOversubscriptionUnderLoad(t *testing.T) {
	type cell struct {
		res        string
		maxTokens  int
		clients    int
		dur        time.Duration
		assertCell bool
		// refusalGuard says the demand is below capacity, so every refusal is a false one and the H1 guard applies;
		// at saturation refusals are mostly real and vary by tens of points from run to run
		refusalGuard bool
	}
	cells := []cell{{"off", 20, 24, 4 * time.Second, true, true}, {"off", 20, 40, 4 * time.Second, true, false}}
	if os.Getenv("SERVERFLOW_LEDGER_LOAD") == "full" {
		cells = nil
		for _, res := range []string{"off", ""} {
			for _, mt := range []int{20, 100, 500} {
				for _, cl := range []int{8, 16, 24, 40} {
					cells = append(cells, cell{res, mt, cl, 6 * time.Second, false, false})
				}
			}
		}
	}
	for _, cl := range cells {
		mx := runLoad(t, config.LoadOverlayMax, cl.res, cl.maxTokens, cl.clients, cl.dur)
		lg := runLoad(t, config.LoadOverlayLedger, cl.res, cl.maxTokens, cl.clients, cl.dur)
		t.Logf("resilience=%-3q max_tokens=%3d clients=%2d | max: ok=%5d refused=%5.2f%% p50=%v p95=%v maxActive=%d | ledger: ok=%5d refused=%5.2f%% p50=%v p95=%v maxActive=%d",
			cl.res, cl.maxTokens, cl.clients, mx.ok, mx.refusedPct(), mx.p50.Round(time.Millisecond), mx.p95.Round(time.Millisecond), mx.maxActive,
			lg.ok, lg.refusedPct(), lg.p50.Round(time.Millisecond), lg.p95.Round(time.Millisecond), lg.maxActive)
		if !cl.assertCell {
			continue
		}
		if d := lg.refusedPct() - mx.refusedPct(); cl.refusalGuard && d >= 0.5 {
			t.Errorf("ledger refuses %.2f%% against max %.2f%%: more than 0.5 percentage points worse (the H1 guard)", lg.refusedPct(), mx.refusedPct())
		}
		if lg.maxActive > 8+1 {
			t.Errorf("a worker ran %d requests at once with 8 slots: the overlay under-counted", lg.maxActive)
		}
	}
}

// sampleActive reads each worker's active-request count, ignoring a worker that cannot be reached right now (a
// refusal storm can use up the machine's ephemeral ports; a sample is optional, the load is not).
func sampleActive(specs []WorkerSpec) []int {
	out := make([]int, 0, len(specs))
	for _, s := range specs {
		resp, err := sampleClient.Get(s.URL + "/stats")
		if err != nil {
			continue
		}
		var v struct {
			Active int `json:"active_requests"`
		}
		if json.NewDecoder(resp.Body).Decode(&v) == nil {
			out = append(out, v.Active)
		}
		_ = resp.Body.Close()
	}
	return out
}

var sampleClient = &http.Client{Timeout: time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 8}}
