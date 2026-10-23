package embedded

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/registry/client"
)

// E0, the recovered-worker finding (Phase 14, ADR-011 amendment, plan H1). The router combines a worker's reported
// load with its own in-flight count as max(reported, local). A report is up to one heartbeat old, so after this
// gateway abandons a worker's requests the worker still looks full until its next heartbeat, and a worker that has
// just recovered (here: its stuck requests were abandoned) can sit unpicked for about a heartbeat even though the
// gateway itself knows it is empty. scheduler.load_overlay: ledger corrects the report for what the gateway started
// and finished since the report was made.
//
// The scenario is scripted so that its timing does not depend on luck: two workers with two slots each; four
// streams that stay in flight (two per worker); the test waits until a heartbeat that includes all four has reached
// the registry and the gateway has read it; it then abandons the two streams on worker-01 (the client goes away)
// and sends one probe request. Worker-02 is genuinely full, so the probe can only be served by worker-01: the time
// from the abandonment to the probe's answer is the time worker-01 stayed unpicked. With the embedded cluster's
// constants (heartbeat 1 s, registry refresh 100 ms) the max rule has to wait for the next heartbeat: the pre-
// registered H1 range for it is 0.3 to 1.1 s. The ledger rule needs nothing but the gateway's own release.

const (
	e0Slots = 2
	// e0MaxMin is the lowest the max rule can legitimately take: the test waits 300 ms after the heartbeat that
	// showed the load before it abandons anything, and the next heartbeat is a full second after that one.
	e0MaxMin = 300 * time.Millisecond
	// e0LedgerMax is the pre-registered bound for the ledger rule (H1: under 0.2 s).
	e0LedgerMax = 200 * time.Millisecond
)

// The mechanism itself is pinned deterministically, with a fake clock, in internal/gateway
// (TestStaleHighReportNoLongerHidesAnEmptiedWorker); this test is the real-time measurement. It is sensitive to a
// starved machine (a test goroutine that is not scheduled for a second misses the heartbeat window it is built
// around), so: a repetition whose abandonment came more than e0MaxGap after the heartbeat it relies on is not valid
// by construction and is repeated (the rule does not exclude a heartbeat that lands just after the abandonment,
// which makes a `max` repetition answer early; the median assertion absorbs it) (at most e0Attempts times in all); the assertions are on the medians of the valid
// repetitions. Numbers from it are simulator results from an idle machine, not hardware results.
const (
	e0Reps     = 5
	e0Attempts = 3 * e0Reps
	// e0MaxGap is the longest the test may take between seeing the heartbeat and abandoning: the next heartbeat is a
	// second after the one it saw, and the gateway reads the registry only every 100 ms.
	e0MaxGap = 600 * time.Millisecond
)

func TestE0RecoveredWorkerIsPickedAgain(t *testing.T) {
	reps := e0Reps
	if v, err := strconv.Atoi(os.Getenv("SERVERFLOW_E0_REPS")); err == nil && v > 0 {
		reps = v
	}
	results := map[string][]time.Duration{}
	for _, overlay := range []string{config.LoadOverlayMax, config.LoadOverlayLedger} {
		invalid := 0
		for len(results[overlay]) < reps && len(results[overlay])+invalid < 3*reps {
			d, gap := runRecoveredWorker(t, overlay)
			if gap > e0MaxGap {
				invalid++
				t.Logf("E0 load_overlay=%s: repetition discarded, the abandonment came %v after the heartbeat (machine too busy)", overlay, gap.Round(time.Millisecond))
				continue
			}
			results[overlay] = append(results[overlay], d)
			t.Logf("E0 load_overlay=%-6s rep %d: first answer %v after the abandonment", overlay, len(results[overlay]), d.Round(time.Millisecond))
		}
		if len(results[overlay]) < (reps+1)/2 {
			t.Fatalf("%s: only %d valid repetitions of %d (%d discarded): the machine is too busy for this measurement", overlay, len(results[overlay]), reps, invalid)
		}
	}
	median := map[string]time.Duration{}
	for _, overlay := range []string{config.LoadOverlayMax, config.LoadOverlayLedger} {
		ds := slices.Clone(results[overlay])
		slices.Sort(ds)
		median[overlay] = ds[len(ds)/2]
		t.Logf("E0 summary load_overlay=%s n=%d min=%v median=%v max=%v", overlay, len(ds), ds[0].Round(time.Millisecond), median[overlay].Round(time.Millisecond), ds[len(ds)-1].Round(time.Millisecond))
	}
	if m := median[config.LoadOverlayMax]; m < e0MaxMin {
		t.Errorf("max: median %v < %v: the stale report did not hide worker-01 (the finding is not reproduced)", m, e0MaxMin)
	}
	if m := median[config.LoadOverlayLedger]; m > e0LedgerMax {
		t.Errorf("ledger: median %v > %v", m, e0LedgerMax)
	}
}

func runRecoveredWorker(t *testing.T, overlay string) (answer, gap time.Duration) {
	t.Helper()
	c := startCluster(t, Config{
		Workers: 2, MaxConcurrency: e0Slots, TokensPerSecond: 100, Scheduler: "least-active",
		GatewayMutate: func(g *config.Config) { g.Scheduler.LoadOverlay = overlay },
	})
	specs := c.Specs()
	type stream struct {
		cancel context.CancelFunc
		worker int
	}
	var streams []stream
	for i := 0; i < 2*e0Slots; i++ {
		before := activeNow(t, specs)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.GatewayURL+"/v1/chat/completions",
			strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":2000,"stream":true}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		after := activeNow(t, specs)
		w := -1
		for j := range specs {
			if after[j] == before[j]+1 {
				w = j
			}
		}
		if w < 0 {
			t.Fatalf("stream %d: no worker's active count rose (before %v after %v)", i, before, after)
		}
		streams = append(streams, stream{cancel: cancel, worker: w})
	}
	per := make([]int, len(specs))
	for _, s := range streams {
		per[s.worker]++
	}
	if per[0] != e0Slots || per[1] != e0Slots {
		t.Fatalf("streams per worker %v: least-active should have filled both workers evenly", per)
	}

	// A heartbeat that shows all four streams must reach the registry, and the gateway must read it.
	var heartbeatSeen time.Time
	deadline := time.Now().Add(5 * time.Second)
	for {
		ws, err := c.ControlPlane().Workers(context.Background(), client.Query{})
		if err != nil {
			t.Fatal(err)
		}
		full := 0
		for _, w := range ws {
			if w.Metrics.ActiveRequests == e0Slots {
				full++
			}
		}
		if full == len(specs) {
			heartbeatSeen = time.Now()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no heartbeat reported the four streams")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(3 * RegistryRefresh)

	// Abandon the streams on worker-01 and ask for one answer.
	began := time.Now()
	gap = began.Sub(heartbeatSeen)
	for _, s := range streams {
		if s.worker == 0 {
			s.cancel()
		}
	}
	probeDone := make(chan time.Duration, 1)
	go func() {
		resp, err := chat(t, c.GatewayURL, 2, false)
		if err != nil {
			probeDone <- -1
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			probeDone <- -2
			return
		}
		probeDone <- time.Since(began)
	}()
	select {
	case d := <-probeDone:
		if d < 0 {
			t.Fatalf("probe failed (%d)", d)
		}
		return d, gap
	case <-time.After(10 * time.Second):
		t.Fatal("the probe was never answered")
		return 0, gap
	}
}

// activeNow reads each worker's own active-request count straight from the worker.
func activeNow(t *testing.T, specs []WorkerSpec) []int {
	t.Helper()
	out := make([]int, len(specs))
	for i, s := range specs {
		resp, err := http.Get(s.URL + "/stats")
		if err != nil {
			t.Fatal(err)
		}
		var v struct {
			Active int `json:"active_requests"`
		}
		err = json.NewDecoder(resp.Body).Decode(&v)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(fmt.Errorf("stats of %s: %w", s.ID, err))
		}
		out[i] = v.Active
	}
	return out
}
