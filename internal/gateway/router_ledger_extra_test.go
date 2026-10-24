package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"serverflow/internal/config"
	"strings"
	"testing"
	"time"

	"serverflow/internal/scheduler/signals"
	"serverflow/pkg/protocol"
)

func TestLedgerCapWarningIsLoggedOnce(t *testing.T) {
	var snaps []protocol.WorkerSnapshot
	for i := 0; i < 4; i++ {
		g := newGateWorker(t, fmt.Sprintf("w%d", i))
		snaps = append(snaps, g.snap(2))
	}
	e := newResEnv(t, snaps, noBreaker)
	e.gw.router.enableLedger(signals.Config{Window: 12 * time.Second, MaxWorkers: 1})
	for i := 0; i < 8; i++ {
		e.clock.Advance(100 * time.Millisecond)
		e.refresh(t)
	}
	n := strings.Count(e.logs.String(), "tracks a limited number of workers")
	if n != 1 {
		t.Fatalf("cap log lines: %d, want 1\n%s", n, e.logs.String())
	}
}

// capacity seen by reserve == freeSlots, on random states, with a different generator than the implementer's.
func TestReserveCapacityEqualsFreeSlotsOnRandomStates(t *testing.T) {
	for seed := int64(0); seed < 150; seed++ {
		rng := rand.New(rand.NewSource(seed*31 + 5))
		n := 1 + rng.Intn(5)
		var snaps []protocol.WorkerSnapshot
		reg := time.Unix(1_699_000_000, 0)
		for i := 0; i < n; i++ {
			g := newGateWorker(t, fmt.Sprintf("w%d", i))
			s := g.snap(1 + rng.Intn(6))
			s.RegisteredAt = reg
			s.Metrics.ActiveRequests = rng.Intn(9)
			s.HeartbeatAgeSeconds = []float64{0, 0.1, 0.5, 1.5, 3}[rng.Intn(5)]
			s.LastHeartbeat = reg.Add(time.Hour)
			snaps = append(snaps, s)
		}
		e := newResEnv(t, snaps, noBreaker)
		r := e.gw.router
		r.enableLedger(signals.Config{Window: 12 * time.Second})
		e.refresh(t)
		var held []*routed
		for step := 0; step < 30; step++ {
			e.clock.Advance(time.Duration(rng.Intn(150)) * time.Millisecond)
			switch rng.Intn(5) {
			case 0, 1:
				if rt, rerr := r.Route(context.Background(), ledgerReq(1+rng.Intn(50), 1+rng.Intn(50))); rerr == nil {
					held = append(held, rt)
				}
			case 2, 3:
				if len(held) > 0 {
					i := rng.Intn(len(held))
					held[i].release()
					held = append(held[:i], held[i+1:]...)
				}
			case 4:
				ss := append([]protocol.WorkerSnapshot(nil), snaps...)
				for i := range ss {
					ss[i].Metrics.ActiveRequests = rng.Intn(9)
					ss[i].HeartbeatAgeSeconds = []float64{0, 0.1, 0.5, 1.5, 3}[rng.Intn(5)]
					ss[i].LastHeartbeat = ss[i].LastHeartbeat.Add(time.Duration(rng.Intn(3)) * time.Second)
				}
				e.lister.set(ss, nil)
				e.refresh(t)
			}
			free := r.freeSlots("qwen-7b")
			spy := &observedSpy{inner: r.sched}
			saved := r.sched
			r.sched = spy
			rt, rerr := r.Route(context.Background(), ledgerReq(1, 1))
			r.sched = saved
			if rt != nil {
				rt.release()
			}
			_ = rerr
			spy.mu.Lock()
			want := 0
			for _, w := range spy.last {
				if schedulable(&w) {
					want += max(0, w.MaxConcurrency-w.Metrics.ActiveRequests)
				}
			}
			saw := len(spy.last)
			spy.mu.Unlock()
			if saw > 0 && want != free {
				t.Fatalf("seed %d step %d: freeSlots=%d but the scheduler's view has %d free", seed, step, free, want)
			}
		}
		for _, h := range held {
			h.release()
		}
		if n, w := r.sig.Totals(); n != 0 || w != 0 {
			t.Fatalf("seed %d: ledger not zero: %d %d", seed, n, w)
		}
	}
}

func TestLedgerSurvivesHostileSnapshotValues(t *testing.T) {
	g := newGateWorker(t, "w0")
	s := g.snap(4)
	e := newResEnv(t, []protocol.WorkerSnapshot{s}, noBreaker)
	r := e.gw.router
	r.enableLedger(signals.Config{Window: 12 * time.Second})
	vals := []float64{math.NaN(), math.Inf(1), math.Inf(-1), -5, 1e300, 1e12, 0}
	acts := []int{0, 1, math.MaxInt32, math.MaxInt64, math.MinInt64, -1}
	for _, a := range vals {
		for _, act := range acts {
			ss := s
			ss.HeartbeatAgeSeconds = a
			ss.Metrics.ActiveRequests = act
			ss.LastHeartbeat = time.Time{}
			e.lister.set([]protocol.WorkerSnapshot{ss}, nil)
			e.clock.Advance(50 * time.Millisecond)
			e.refresh(t)
			rt, rerr := r.Route(context.Background(), ledgerReq(math.MaxInt32, math.MaxInt32))
			if rerr == nil {
				rt.release()
			}
			_ = r.freeSlots("qwen-7b")
		}
	}
	if n, w := r.sig.Totals(); n != 0 || w != 0 {
		t.Fatalf("ledger %d %d", n, w)
	}
}

// Other gateways' load (reported, not local) must reach the scheduler under the ledger overlay, and a fallback must
// never go below the local count.
func TestLedgerRouterUsesReportedAndLocal(t *testing.T) {
	g := newGateWorker(t, "w0")
	s := g.snap(8)
	s.Metrics.ActiveRequests = 3 // somebody else's requests
	s.HeartbeatAgeSeconds = 0
	s.LastHeartbeat = time.Unix(1_699_000_000, 0).Add(time.Hour)
	e := newResEnv(t, []protocol.WorkerSnapshot{s}, noBreaker)
	r := e.gw.router
	r.enableLedger(signals.Config{Window: 12 * time.Second})
	e.clock.Advance(time.Second)
	e.refresh(t)
	spy := &observedSpy{inner: r.sched}
	r.sched = spy
	rt, rerr := r.Route(context.Background(), ledgerReq(1, 1))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt.release()
	spy.mu.Lock()
	got := spy.last[0].Metrics.ActiveRequests
	spy.mu.Unlock()
	if got != 3 {
		t.Fatalf("scheduler saw active %d, want the reported 3 (nothing local yet)", got)
	}
	// now local 1 on top of reported 3 (report predates the start): 4
	rt2, rerr := r.Route(context.Background(), ledgerReq(1, 1))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt2.release()
	spy.mu.Lock()
	got = spy.last[0].Metrics.ActiveRequests
	spy.mu.Unlock()
	if got != 4 {
		t.Fatalf("scheduler saw active %d, want reported 3 + 1 started since = 4", got)
	}
	// fallback (a new, untracked-from-before-the-report state): reported 0, local 2 must still show 2
	s2 := s
	s2.Metrics.ActiveRequests = 0
	s2.RegisteredAt = s.RegisteredAt.Add(time.Second) // re-registered: history unusable until a later report
	e.lister.set([]protocol.WorkerSnapshot{s2}, nil)
	e.refresh(t)
	rt3, rerr := r.Route(context.Background(), ledgerReq(1, 1))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt3.release()
	rt4, rerr := r.Route(context.Background(), ledgerReq(1, 1))
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer rt4.release()
	spy.mu.Lock()
	got = spy.last[0].Metrics.ActiveRequests
	spy.mu.Unlock()
	if got < 2 {
		t.Fatalf("fallback went below the local count: %d", got)
	}
}

func TestWiredLedgerWindowCoversStalenessPlusAHeartbeat(t *testing.T) {
	cfg := config.Default()
	cfg.Gateway = testConfig("http://127.0.0.1:1")
	cfg.Gateway.WorkerSource = config.WorkerSourceRegistry
	cfg.Scheduler.LoadOverlay = config.LoadOverlayLedger
	gw, err := NewRegistry(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	tr := gw.router.sig
	t0 := time.Unix(1_700_000_000, 0)
	k := signals.Key{WorkerID: "w", Registered: 1}
	tr.Sync(t0, []signals.Observation{{Key: k}})
	span := cfg.Gateway.RegistryMaxStaleness + cfg.Worker.HeartbeatInterval - 500*time.Millisecond
	res := tr.Overlay(t0.Add(span), signals.Input{Key: k, Local: 0, Reported: 1, At: t0})
	if res.Fallback != signals.FallbackNone {
		t.Fatalf("a report %v old (inside staleness+heartbeat = %v) fell back: the wired window is too small", span, cfg.Gateway.RegistryMaxStaleness+cfg.Worker.HeartbeatInterval)
	}
}
