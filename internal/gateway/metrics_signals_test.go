package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"serverflow/internal/cost"
	"serverflow/internal/scheduler"
	"serverflow/internal/scheduler/signals"
	"serverflow/pkg/protocol"
)

// The work-aware series (ADR-022 D24, the minimum assigned to PR C1): present only with least-tokens or least-work.

// waEnv is a registry gateway with a work-aware strategy, the ledger on and a fake clock, in front of fake workers.
func waEnv(t *testing.T, strategy string, ws []protocol.WorkerSnapshot) (*regEnv, *recObserver) {
	t.Helper()
	cfg := testConfig("http://127.0.0.1:1")
	lister := &fakeLister{workers: ws}
	gw, err := newRegistryServer(cfg, strategy, 5*time.Second, lister, slog.New(slog.NewJSONHandler(io.Discard, nil)), scheduler.WithCostWeights(cost.DefaultWeights()))
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{t: t0}
	gw.router.cache.now = clock.Now
	gw.router.enableLedger(signals.Config{Window: 12 * time.Second})
	gw.router.enableWorkAware(cost.DefaultWeights())
	rec := &recObserver{}
	WithObserver(rec)(gw)
	srv := httptest.NewServer(gw.Handler())
	tr := &http.Transport{}
	t.Cleanup(func() { tr.CloseIdleConnections(); srv.Close() })
	e := &regEnv{gw: gw, url: srv.URL, lister: lister, clock: clock, client: &http.Client{Transport: tr, Timeout: 30 * time.Second}}
	e.refresh(t)
	return e, rec
}

func TestWorkAwareSeriesExistOnlyWithAWorkAwareStrategy(t *testing.T) {
	for _, st := range scheduler.Strategies() {
		a := newFakeWorker(t, "a", nil)
		var e *regEnv
		if scheduler.WorkAware(st) {
			e, _ = waEnv(t, st, []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
		} else {
			e = newRegEnv(t, st, []protocol.WorkerSnapshot{a.snapshot("qwen-7b", 4)})
		}
		if resp, body := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
			t.Fatalf("%s: %d %s", st, resp.StatusCode, body)
		}
		text := scrape(t, e.gw)
		has := strings.Contains(text, "scheduler_predicted_wait_seconds") || strings.Contains(text, "scheduler_signal_fallbacks_total")
		if has != scheduler.WorkAware(st) {
			t.Errorf("%s: the work-aware series present = %v", st, has)
		}
	}
}

func TestWorkAwareSeriesCatalogue(t *testing.T) {
	s := benchServer(false)
	s.metrics.setStrategy(scheduler.LeastWork)
	s.metrics.enableSignals()
	s.metrics.AttemptStarted(context.Background(), AttemptStart{Model: "qwen-7b", WorkerID: "w", Number: 1, WorkerState: "READY", WorkerEligible: true,
		Signals: SchedulerSignals{Predicted: true, PredictedWait: 1.5, ColdStart: true, Untracked: true, Invalid: true}})
	var sig []string
	for _, l := range strings.Split(seriesCatalogue(t, s), "\n") {
		if strings.HasPrefix(l, "scheduler_predicted_wait_seconds") || strings.HasPrefix(l, "scheduler_signal_fallbacks_total") {
			sig = append(sig, l)
		}
	}
	if len(sig) != 2 {
		t.Fatalf("want the two Phase 14 families, got %v", sig)
	}
	checkGolden(t, "testdata/metrics_series_workaware.golden", strings.Join(sig, "\n")+"\n")
	text := scrape(t, s)
	for _, reason := range []string{"cold_start", "untracked", "invalid"} {
		if v, ok := seriesSample(text, `scheduler_signal_fallbacks_total{model="qwen-7b",reason="`+reason+`"}`); !ok || v != 1 {
			t.Errorf("reason %s: %v %v", reason, v, ok)
		}
	}
}


var sampleLine = regexp.MustCompile(`(?m)^([a-z_]+(?:\{[^}]*\})?) ([0-9.e+-]+)$`)

func seriesSample(text, series string) (float64, bool) {
	for _, m := range sampleLine.FindAllStringSubmatch(text, -1) {
		if m[1] == series {
			v, _ := strconv.ParseFloat(m[2], 64)
			return v, true
		}
	}
	return 0, false
}

// Hand-checked: worker a reports a rate (100 tokens/s with an active request), b has never reported one, so b's
// capacity is the prior (100) and is flagged. The estimate of a request is the default 4096-token output budget
// plus about 10 input tokens, so every decision has a prediction; the ones that chose a worker whose capacity is a
// prior count as cold_start fallbacks.
func TestPredictedWaitAndFallbacksAreObservedPerDecision(t *testing.T) {
	a, b := newFakeWorker(t, "a", nil), newFakeWorker(t, "b", nil)
	sa, sb := a.snapshot("qwen-7b", 4), b.snapshot("qwen-7b", 4)
	sa.LastHeartbeat, sb.LastHeartbeat = t0, t0
	sa.Metrics = protocol.Metrics{ActiveRequests: 1, RecentTokensPerSecond: 100}
	e, rec := waEnv(t, scheduler.LeastWork, []protocol.WorkerSnapshot{sa, sb})
	// let the tracker see the report as a sample: refresh once more (a new snapshot, same heartbeat = one sample)
	e.clock.Advance(time.Second)
	e.refresh(t)
	const n = 6
	for i := 0; i < n; i++ {
		if resp, body := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
			t.Fatalf("%d %s", resp.StatusCode, body)
		}
	}
	rec.mu.Lock()
	var predicted, cold int
	for _, at := range rec.att {
		if at.Signals.Predicted {
			predicted++
			if at.Signals.PredictedWait <= 0 {
				t.Errorf("non-positive predicted wait: %+v", at.Signals)
			}
		}
		if at.Signals.ColdStart {
			cold++
		}
		if at.Signals.Untracked || at.Signals.Invalid {
			t.Errorf("unexpected flags: %+v", at.Signals)
		}
	}
	rec.mu.Unlock()
	if predicted != n {
		t.Fatalf("%d of %d decisions carried a prediction", predicted, n)
	}
	text := scrape(t, e.gw)
	if v, ok := seriesSample(text, `scheduler_predicted_wait_seconds_count{model="qwen-7b"}`); !ok || int(v) != n {
		t.Fatalf("predicted_wait count %v %v, want %d\n%s", v, ok, n, text)
	}
	got, _ := seriesSample(text, `scheduler_signal_fallbacks_total{model="qwen-7b",reason="cold_start"}`)
	if int(got) != cold || cold == 0 {
		t.Fatalf("cold_start counter %v, observed %d decisions on a prior-capacity worker", got, cold)
	}
}

func TestSignalsOfOtherStrategiesAndHandOffsAreZero(t *testing.T) {
	plain := &routed{worker: protocol.WorkerSnapshot{Observed: protocol.ObservedLoad{EffectiveActive: 3, WorkAheadTokens: 10}}}
	if got := schedulerSignals(plain); !got.IsZero() {
		t.Fatalf("a worker the work-aware path did not fill: %+v", got)
	}
	handoff := &routed{worker: protocol.WorkerSnapshot{}}
	if got := schedulerSignals(handoff); !got.IsZero() {
		t.Fatalf("a hand-off: %+v", got)
	}
	filled := &routed{worker: protocol.WorkerSnapshot{Observed: protocol.ObservedLoad{Flags: protocol.ObservedLedger | protocol.ObservedUntracked | protocol.ObservedInvalid}}, predicted: 2.5, hasPrediction: true}
	want := SchedulerSignals{Predicted: true, PredictedWait: 2.5, Untracked: true, Invalid: true}
	if got := schedulerSignals(filled); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	// a degraded decision (no ledger observation at all) is a cold start
	deg := &routed{worker: protocol.WorkerSnapshot{Observed: protocol.ObservedLoad{Flags: protocol.ObservedDegraded}}}
	if got := schedulerSignals(deg); !got.ColdStart {
		t.Fatalf("degraded: %+v", got)
	}
}

// The new observation costs the request path nothing in allocations once the model's children exist.
func TestSignalsObservationDoesNotAllocate(t *testing.T) {
	m := newMetrics()
	m.setStrategy(scheduler.LeastWork)
	m.enableSignals()
	ev := AttemptStart{Model: "qwen-7b", WorkerID: "w", Number: 2, WorkerState: "READY", WorkerEligible: true,
		Signals: SchedulerSignals{Predicted: true, PredictedWait: 0.5, ColdStart: true, Untracked: true, Invalid: true}}
	ctx := context.Background()
	m.AttemptStarted(ctx, ev) // creates the children
	if n := testing.AllocsPerRun(200, func() { m.AttemptStarted(ctx, ev) }); n != 0 {
		t.Fatalf("%v allocations per AttemptStarted with signals", n)
	}
}
