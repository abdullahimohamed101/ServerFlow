package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/registry/client"
	"serverflow/pkg/protocol"
)

type fakeLister struct {
	mu      sync.Mutex
	workers []protocol.WorkerSnapshot
	err     error
	calls   int
}

func (f *fakeLister) Workers(_ context.Context, _ client.Query) ([]protocol.WorkerSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return append([]protocol.WorkerSnapshot(nil), f.workers...), nil
}

func (f *fakeLister) set(ws []protocol.WorkerSnapshot, err error) {
	f.mu.Lock()
	f.workers, f.err = ws, err
	f.mu.Unlock()
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func snapWorker(id, model string, age float64) protocol.WorkerSnapshot {
	return protocol.WorkerSnapshot{
		WorkerID: id, Model: model, Address: "http://127.0.0.1:1", MaxConcurrency: 4, State: protocol.StateReady,
		Health: protocol.HealthHealthy, Eligible: true, HeartbeatAgeSeconds: age,
	}
}

func newCache(t *testing.T, src workerLister) (*snapshotCache, *testClock, *lockedLog) {
	t.Helper()
	logs := &lockedLog{}
	c := newSnapshotCache(src, time.Second, 10*time.Second, 5*time.Second, slog.New(slog.NewJSONHandler(logs, nil)))
	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	c.now = clock.Now
	return c, clock, logs
}

type lockedLog struct {
	mu sync.Mutex
	sb strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sb.Write(p)
}
func (l *lockedLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.sb.String() }

var _ io.Writer = (*lockedLog)(nil)

func TestNoSnapshotMeansNotUsable(t *testing.T) {
	c, _, _ := newCache(t, &fakeLister{})
	if _, ok := c.View("m"); ok {
		t.Fatal("nothing has been fetched yet")
	}
	if _, ok := c.Fresh(); ok {
		t.Fatal("Fresh must be false before the first fetch")
	}
	if _, ok := c.Models(); ok {
		t.Fatal("Models must be unusable before the first fetch")
	}
}

func TestViewReturnsOnlyTheModelsWorkersAndDistinguishesUnknownModels(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("a", "qwen", 0), snapWorker("b", "llama", 0), snapWorker("c", "qwen", 1)}}
	c, _, _ := newCache(t, src)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	ws, ok := c.View("qwen")
	if !ok || len(ws) != 2 {
		t.Fatalf("qwen: %v %v", ws, ok)
	}
	ws, ok = c.View("nobody")
	if !ok || len(ws) != 0 {
		t.Fatalf("a model nobody serves is a usable, empty answer: %v %v", ws, ok)
	}
}

func TestViewDoesNotLetCallersCorruptTheSnapshot(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("a", "qwen", 0)}}
	c, _, _ := newCache(t, src)
	_ = c.Refresh(context.Background())
	ws, _ := c.View("qwen")
	ws[0].Metrics.ActiveRequests = 99
	ws[0].Eligible = false
	again, _ := c.View("qwen")
	if again[0].Metrics.ActiveRequests != 0 || !again[0].Eligible {
		t.Fatalf("the snapshot was modified through a returned copy: %+v", again[0])
	}
}

func TestHeartbeatAgesAdvanceAndEligibilityExpiresAtTheSuspectThreshold(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("fresh", "m", 0), snapWorker("old", "m", 3)}}
	c, clock, _ := newCache(t, src)
	_ = c.Refresh(context.Background())

	elig := func() map[string]bool {
		ws, ok := c.View("m")
		if !ok {
			t.Fatal("snapshot should be usable")
		}
		out := map[string]bool{}
		for _, w := range ws {
			out[w.WorkerID] = w.Eligible
		}
		return out
	}
	if got := elig(); !got["fresh"] || !got["old"] {
		t.Fatalf("both eligible at fetch time: %v", got)
	}
	clock.Advance(1999 * time.Millisecond) // old: 3 + 1.999 = 4.999s < 5s
	if got := elig(); !got["old"] {
		t.Fatalf("old is still under the suspect threshold: %v", got)
	}
	clock.Advance(1 * time.Millisecond) // old: exactly 5s
	if got := elig(); got["old"] || !got["fresh"] {
		t.Fatalf("old reaches the threshold and must stop being eligible: %v", got)
	}
	ws, _ := c.View("m")
	for _, w := range ws {
		if w.WorkerID == "old" && (w.Health != protocol.HealthSuspect || w.HeartbeatAgeSeconds < 4.99) {
			t.Fatalf("the view must show the advanced age and health: %+v", w)
		}
	}
	clock.Advance(4999 * time.Millisecond) // snapshot age 9.999s; fresh is now 9.999s old: suspect-or-worse
	if got := elig(); got["fresh"] {
		t.Fatalf("every worker has aged past the threshold: %v", got)
	}
}

func TestAWorkerAlreadyIneligibleStaysIneligible(t *testing.T) {
	w := snapWorker("d", "m", 0)
	w.State, w.Eligible = protocol.StateDraining, false
	c, _, _ := newCache(t, &fakeLister{workers: []protocol.WorkerSnapshot{w}})
	_ = c.Refresh(context.Background())
	ws, _ := c.View("m")
	if ws[0].Eligible || ws[0].Health != protocol.HealthHealthy {
		t.Fatalf("aging must never make a worker eligible or rewrite the health of an ineligible one: %+v", ws[0])
	}
}

func TestSnapshotsExpireAtTheStalenessBoundAndFailClosed(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("a", "m", 0)}}
	c, clock, _ := newCache(t, src)
	_ = c.Refresh(context.Background())
	clock.Advance(10 * time.Second) // exactly the bound: still usable
	if _, ok := c.Fresh(); !ok {
		t.Fatal("a snapshot exactly at the bound is usable")
	}
	clock.Advance(time.Nanosecond)
	if _, ok := c.View("m"); ok {
		t.Fatal("a snapshot past the bound must not be used")
	}
	if _, ok := c.Models(); ok {
		t.Fatal("Models must fail closed too")
	}
}

func TestAFailedRefreshKeepsTheLastSnapshotAndLogsOncePerOutage(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("a", "m", 0)}}
	c, clock, logs := newCache(t, src)
	_ = c.Refresh(context.Background())

	src.set(nil, errors.New("connection refused"))
	for i := 0; i < 5; i++ {
		clock.Advance(time.Second)
		if err := c.Refresh(context.Background()); err == nil {
			t.Fatal("the failure must be reported to the caller")
		}
	}
	if ws, ok := c.View("m"); !ok || len(ws) != 1 {
		t.Fatalf("the last good snapshot must still serve within the bound: %v %v", ws, ok)
	}
	if n := strings.Count(logs.String(), "could not refresh"); n != 1 {
		t.Fatalf("one warning per outage, got %d", n)
	}
	src.set([]protocol.WorkerSnapshot{snapWorker("a", "m", 0), snapWorker("b", "m", 0)}, nil)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "reachable again") {
		t.Fatal("recovery must be logged")
	}
	if ws, _ := c.View("m"); len(ws) != 2 {
		t.Fatalf("recovery must replace the snapshot: %v", ws)
	}
	// A second outage warns again.
	src.set(nil, errors.New("down"))
	_ = c.Refresh(context.Background())
	if n := strings.Count(logs.String(), "could not refresh"); n != 2 {
		t.Fatalf("a new outage must warn again, got %d", n)
	}
}

func TestFreshnessIsMeasuredFromWhenTheFetchStarted(t *testing.T) {
	src := &slowLister{}
	c, clock, _ := newCache(t, src)
	src.onCall = func() { clock.Advance(3 * time.Second) } // the fetch itself takes 3s
	_ = c.Refresh(context.Background())
	age, ok := c.Fresh()
	if !ok || age != 3*time.Second {
		t.Fatalf("age must include the time the fetch took (conservative): %v %v", age, ok)
	}
}

type slowLister struct{ onCall func() }

func (s *slowLister) Workers(context.Context, client.Query) ([]protocol.WorkerSnapshot, error) {
	if s.onCall != nil {
		s.onCall()
	}
	return nil, nil
}

func TestModelsListsOnlyModelsWithAnEligibleWorker(t *testing.T) {
	dr := snapWorker("d", "drained-model", 0)
	dr.State, dr.Eligible = protocol.StateDraining, false
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("a", "qwen", 0), snapWorker("b", "llama", 0), dr, snapWorker("c", "qwen", 0)}}
	c, clock, _ := newCache(t, src)
	_ = c.Refresh(context.Background())
	if got, ok := c.Models(); !ok || !reflect.DeepEqual(got, []string{"llama", "qwen"}) {
		t.Fatalf("got %v %v", got, ok)
	}
	clock.Advance(6 * time.Second) // every worker is past the suspect threshold
	if got, ok := c.Models(); !ok || len(got) != 0 {
		t.Fatalf("no model has an eligible worker any more: %v %v", got, ok)
	}
}

func TestRunRefreshesRepeatedlyAndStops(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("a", "m", 0)}}
	c := newSnapshotCache(src, 10*time.Millisecond, time.Second, time.Second, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		src.mu.Lock()
		n := src.calls
		src.mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if time.Since(deadline.Add(-5*time.Second)) > time.Second {
		t.Errorf("three refreshes at a 10ms interval took %v", time.Since(deadline.Add(-5*time.Second)))
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	if _, ok := c.View("m"); !ok {
		t.Fatal("Run must have populated the snapshot")
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.calls < 3 {
		t.Fatalf("only %d refreshes", src.calls)
	}
}

func TestAnAlreadyIneligibleWorkerKeepsItsHealthEvenWhenItsAgeIsHigh(t *testing.T) {
	w := snapWorker("d", "m", 30) // 30s since its last heartbeat, but ineligible for another reason
	w.State, w.Eligible = protocol.StateDraining, false
	c, clock, _ := newCache(t, &fakeLister{workers: []protocol.WorkerSnapshot{w}})
	_ = c.Refresh(context.Background())
	clock.Advance(time.Second)
	ws, _ := c.View("m")
	if ws[0].Health != protocol.HealthHealthy || ws[0].Eligible {
		t.Fatalf("only an eligible worker is re-judged by age: %+v", ws[0])
	}
}

func TestTheSnapshotKeepsEachModelsWorkersInIDOrder(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("c", "m", 0), snapWorker("a", "m", 0), snapWorker("b", "m", 0)}}
	c, _, _ := newCache(t, src)
	_ = c.Refresh(context.Background())
	ws, _ := c.View("m")
	if ws[0].WorkerID != "a" || ws[1].WorkerID != "b" || ws[2].WorkerID != "c" {
		t.Fatalf("got %v %v %v", ws[0].WorkerID, ws[1].WorkerID, ws[2].WorkerID)
	}
}

func TestBelievableRejectsNumbersAndAddressesTheRegistryWouldNeverProduce(t *testing.T) {
	ok := protocol.WorkerSnapshot{HeartbeatAgeSeconds: 1, MaxConcurrency: 1, Address: "http://127.0.0.1:1"}
	if !believable(&ok) {
		t.Fatal("a normal worker is believable")
	}
	for name, mutate := range map[string]func(*protocol.WorkerSnapshot){
		"negative age": func(w *protocol.WorkerSnapshot) { w.HeartbeatAgeSeconds = -0.001 },
		"inf age":      func(w *protocol.WorkerSnapshot) { w.HeartbeatAgeSeconds = math.Inf(1) },
		"nan age":      func(w *protocol.WorkerSnapshot) { w.HeartbeatAgeSeconds = math.NaN() },
		"zero conc":    func(w *protocol.WorkerSnapshot) { w.MaxConcurrency = 0 },
		"neg active":   func(w *protocol.WorkerSnapshot) { w.Metrics.ActiveRequests = -1 },
		"neg queue":    func(w *protocol.WorkerSnapshot) { w.Metrics.QueueDepth = -1 },
		"bad address":  func(w *protocol.WorkerSnapshot) { w.Address = "http://169.254.169.254" },
		"no address":   func(w *protocol.WorkerSnapshot) { w.Address = "" },
	} {
		w := ok
		mutate(&w)
		if believable(&w) {
			t.Errorf("%s must not be believable", name)
		}
	}
	zero := ok
	zero.HeartbeatAgeSeconds, zero.Metrics.ActiveRequests = 0, 0
	if !believable(&zero) {
		t.Error("zero age and zero load are valid")
	}
}

func TestAHugeAgeNeverMakesAWorkerEligible(t *testing.T) {
	// Converting a huge float to a Duration wraps on some platforms; ages are compared as seconds.
	w := snapWorker("a", "m", 1e19)
	c, _, _ := newCache(t, &fakeLister{workers: []protocol.WorkerSnapshot{w}})
	_ = c.Refresh(context.Background())
	if ws, _ := c.View("m"); ws[0].Eligible {
		t.Fatal("an absurd heartbeat age must not be eligible")
	}
}

func TestDegradedMeansMoreThanOneMissedRefresh(t *testing.T) {
	src := &fakeLister{workers: []protocol.WorkerSnapshot{snapWorker("a", "m", 0)}}
	c, clock, _ := newCache(t, src) // refresh 1s, staleness 10s
	_ = c.Refresh(context.Background())
	if c.Degraded() {
		t.Fatal("a healthy cache is not degraded")
	}
	src.set(nil, errors.New("blip"))
	clock.Advance(time.Second)
	_ = c.Refresh(context.Background())
	if c.Degraded() {
		t.Fatal("one failed refresh on a fresh snapshot is a blip, not an outage")
	}
	clock.Advance(1100 * time.Millisecond) // the view is now older than two refresh intervals
	_ = c.Refresh(context.Background())
	if !c.Degraded() {
		t.Fatal("refreshes failing for more than two intervals is an outage")
	}
	src.set([]protocol.WorkerSnapshot{snapWorker("a", "m", 0)}, nil)
	_ = c.Refresh(context.Background())
	if c.Degraded() {
		t.Fatal("recovery clears it")
	}
}
