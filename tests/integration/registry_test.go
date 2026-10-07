package integration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"serverflow/internal/mockworker"
	"serverflow/internal/registry"
	"serverflow/internal/registry/client"
	"serverflow/internal/registry/server"
	"serverflow/internal/worker"
	"serverflow/pkg/protocol"
)

// The thresholds are short so a worker's death can be watched in real time.
// Heartbeat 50ms; suspect after 150ms, unhealthy after 300ms, lost after 600ms.
const (
	hbInterval     = 50 * time.Millisecond
	suspectAfter   = 150 * time.Millisecond
	unhealthyAfter = 300 * time.Millisecond
	lostAfter      = 600 * time.Millisecond
	testToken      = "an-integration-test-shared-secret"
)

type controlPlane struct {
	reg atomic.Pointer[registry.Registry]
	h   atomic.Pointer[http.Handler]
	cp  *client.Client
}

func newRegistry(t *testing.T) (*registry.Registry, http.Handler) {
	t.Helper()
	reg, err := registry.New(registry.Config{Suspect: suspectAfter, Unhealthy: unhealthyAfter, Lost: lostAfter,
		Retention: time.Minute, MaxWorkers: 50, HeartbeatInterval: hbInterval}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	return reg, server.New(reg, server.Config{Token: testToken}, quiet()).Handler()
}

func startControlPlane(t *testing.T) *controlPlane {
	t.Helper()
	c := &controlPlane{}
	reg, h := newRegistry(t)
	c.reg.Store(reg)
	c.h.Store(&h)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*c.h.Load()).ServeHTTP(w, r) }))
	t.Cleanup(ts.Close)
	c.cp = client.New(ts.URL, testToken, nil)
	return c
}

// restart replaces the registry with an empty one behind the same URL, which is
// what a control plane restart looks like to its workers.
func (c *controlPlane) restart(t *testing.T) {
	t.Helper()
	reg, h := newRegistry(t)
	c.reg.Store(reg)
	c.h.Store(&h)
}

func (c *controlPlane) registry() *registry.Registry { return c.reg.Load() }

type node struct {
	id     string
	mock   *httptest.Server
	stop   context.CancelFunc // stops the agent without deregistering: a crash
	agent  *worker.Agent
	exited chan struct{}
	err    error // what Run returned; valid once exited is closed
}

func (c *controlPlane) startNode(t *testing.T, id string) *node {
	t.Helper()
	mcfg := mockworker.DefaultConfig()
	mcfg.Model, mcfg.TTFT, mcfg.Seed = model, 5*time.Millisecond, 1
	mock := httptest.NewServer(mockworker.New(mcfg, quiet()).Handler())
	t.Cleanup(mock.Close)

	agent := worker.New(worker.Config{WorkerID: id, Model: model, AdvertiseURL: mock.URL, Interval: hbInterval},
		worker.NewMockBackend(mock.URL), c.cp, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{id: id, mock: mock, stop: cancel, agent: agent, exited: make(chan struct{})}
	go func() { n.err = agent.Run(ctx); close(n.exited) }()
	t.Cleanup(cancel)
	return n
}

func (c *controlPlane) get(id string) (protocol.WorkerSnapshot, bool) { return c.registry().Get(id) }

func (c *controlPlane) eligible(id string) bool {
	s, ok := c.get(id)
	return ok && s.Eligible
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < d {
		if cond() {
			return time.Since(start)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", d, what)
	return 0
}

func TestAWorkerRegistersAndIsFoundByModel(t *testing.T) {
	c := startControlPlane(t)
	c.startNode(t, "w1")
	c.startNode(t, "w2")
	waitFor(t, 5*time.Second, "both workers eligible", func() bool { return c.eligible("w1") && c.eligible("w2") })

	got, err := c.cp.Workers(context.Background(), client.Query{Model: model, EligibleOnly: true})
	if err != nil || len(got) != 2 {
		t.Fatalf("lookup by model: %v %v", got, err)
	}
	w := got[0]
	if w.WorkerID != "w1" || w.MaxConcurrency != 4 || w.QueueSize != 32 || !strings.HasPrefix(w.Address, "http://127.0.0.1:") {
		t.Fatalf("the registration must carry the worker's address and real capacity: %+v", w)
	}
	if other, _ := c.cp.Workers(context.Background(), client.Query{Model: "another-model"}); len(other) != 0 {
		t.Fatalf("no workers serve another model: %v", other)
	}
	models, _ := c.cp.Models(context.Background())
	if len(models) != 1 || models[0] != (protocol.ModelInfo{Model: model, Workers: 2, Eligible: 2}) {
		t.Fatalf("model inventory: %+v", models)
	}
}

func TestAWorkerStartingUpIsNotOfferedUntilItIsReady(t *testing.T) {
	c := startControlPlane(t)
	mcfg := mockworker.DefaultConfig()
	mcfg.Model, mcfg.StartupDelay, mcfg.Seed = model, 600*time.Millisecond, 1
	mock := httptest.NewServer(mockworker.New(mcfg, quiet()).Handler())
	t.Cleanup(mock.Close)
	agent := worker.New(worker.Config{WorkerID: "slow", Model: model, AdvertiseURL: mock.URL, Interval: hbInterval}, worker.NewMockBackend(mock.URL), c.cp, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = agent.Run(ctx) }()

	waitFor(t, 3*time.Second, "the worker to register while loading", func() bool {
		s, ok := c.get("slow")
		return ok && s.State == protocol.StateLoadingModel
	})
	if c.eligible("slow") || len(c.registry().Eligible(model)) != 0 {
		t.Fatal("a worker that is still loading its model must never be offered")
	}
	waitFor(t, 5*time.Second, "the worker to become ready", func() bool { return c.eligible("slow") })
}

// observe polls a worker until done() and returns the distinct state/health
// pairs it showed, in order, and whether it became eligible again after first
// becoming ineligible.
func (c *controlPlane) observe(id string, timeout time.Duration, done func(protocol.WorkerSnapshot) bool) (seq []string, eligibleAgain bool) {
	deadline := time.Now().Add(timeout)
	wasIneligible := false
	for time.Now().Before(deadline) {
		s, ok := c.get(id)
		if !ok {
			return
		}
		cur := fmt.Sprintf("%s/%s", s.State, s.Health)
		if len(seq) == 0 || seq[len(seq)-1] != cur {
			seq = append(seq, cur)
		}
		if !s.Eligible {
			wasIneligible = true
		} else if wasIneligible {
			eligibleAgain = true
		}
		if done(s) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	return
}

// The headline acceptance test: worker death.
func TestWhenAnAgentDiesItsWorkerGoesSuspectUnhealthyLostAndIsNeverOfferedAgain(t *testing.T) {
	c := startControlPlane(t)
	n := c.startNode(t, "w1")
	c.startNode(t, "w2") // a bystander that must be unaffected
	waitFor(t, 5*time.Second, "both eligible", func() bool { return c.eligible("w1") && c.eligible("w2") })

	killed := time.Now()
	n.stop() // the agent dies: heartbeats stop, nothing is deregistered
	<-n.exited

	seq, eligibleAgain := c.observe("w1", 5*time.Second, func(s protocol.WorkerSnapshot) bool { return s.State == protocol.StateLost })
	took := time.Since(killed)

	want := []string{"READY/healthy", "READY/suspect", "UNHEALTHY/unhealthy", "LOST/lost"}
	if strings.Join(seq, " ") != strings.Join(want, " ") {
		t.Fatalf("a dead worker must walk %v, observed %v", want, seq)
	}
	if eligibleAgain {
		t.Fatal("once a worker stopped being eligible it must not become eligible again without reporting in")
	}
	if took < lostAfter-hbInterval { // the last heartbeat can be up to one interval before the kill
		t.Fatalf("the worker was declared lost after %v, before the %v threshold", took, lostAfter)
	}
	if took > lostAfter+3*time.Second {
		t.Fatalf("a dead worker took %v to be declared lost", took)
	}
	if !c.eligible("w2") {
		t.Fatal("a healthy bystander must be unaffected")
	}
	if got := c.registry().Eligible(model); len(got) != 1 || got[0].WorkerID != "w2" {
		t.Fatalf("only the live worker may be offered: %+v", got)
	}
}

func TestADeadBackendIsReportedFailedAtOnceAndRecoversWhenItReturns(t *testing.T) {
	c := startControlPlane(t)
	n := c.startNode(t, "w1")
	waitFor(t, 5*time.Second, "eligible", func() bool { return c.eligible("w1") })

	n.mock.Close() // the inference backend dies; the agent survives
	took := waitFor(t, 3*time.Second, "FAILED", func() bool {
		s, _ := c.get("w1")
		return s.State == protocol.StateFailed
	})
	if took > unhealthyAfter {
		t.Fatalf("a dead backend must be reported within a heartbeat or two, not wait out the %v timeout (took %v)", unhealthyAfter, took)
	}
	s, _ := c.get("w1")
	if s.Eligible || s.Health != protocol.HealthHealthy || s.Reason != "backend unreachable" {
		t.Fatalf("the agent is alive, so health stays healthy while the state says FAILED: %+v", s)
	}
}

func TestARestartedWorkerComesBackAsANewIncarnation(t *testing.T) {
	c := startControlPlane(t)
	n := c.startNode(t, "w1")
	waitFor(t, 5*time.Second, "eligible", func() bool { return c.eligible("w1") })
	first, _ := c.get("w1")

	n.stop()
	<-n.exited
	waitFor(t, 3*time.Second, "unhealthy", func() bool { s, _ := c.get("w1"); return s.State == protocol.StateUnhealthy })

	c.startNode(t, "w1") // the same worker ID starts again, on a new port
	waitFor(t, 5*time.Second, "eligible again", func() bool { return c.eligible("w1") })
	second, _ := c.get("w1")
	if second.Address == first.Address || c.registry().Len() != 1 {
		t.Fatalf("the restart must replace the old record: %+v vs %+v (len %d)", first, second, c.registry().Len())
	}
}

func TestWorkersReRegisterThemselvesAfterAControlPlaneRestart(t *testing.T) {
	c := startControlPlane(t)
	c.startNode(t, "w1")
	c.startNode(t, "w2")
	waitFor(t, 5*time.Second, "eligible", func() bool { return c.eligible("w1") && c.eligible("w2") })

	c.restart(t) // an empty registry: the control plane lost everything
	if c.registry().Len() != 0 {
		t.Fatal("setup: the new registry should be empty")
	}
	took := waitFor(t, 3*time.Second, "both workers to re-register", func() bool { return c.eligible("w1") && c.eligible("w2") })
	if took > 10*hbInterval {
		t.Fatalf("workers should recover within about a heartbeat interval, took %v", took)
	}
}

func TestGracefulShutdownRemovesTheWorkerImmediately(t *testing.T) {
	c := startControlPlane(t)
	n := c.startNode(t, "w1")
	waitFor(t, 5*time.Second, "eligible", func() bool { return c.eligible("w1") })

	n.stop()
	<-n.exited
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	n.agent.Shutdown(ctx) // what the binary does after a SIGTERM
	if _, ok := c.get("w1"); ok || len(c.registry().Eligible(model)) != 0 {
		t.Fatal("a gracefully stopped worker must disappear at once, without waiting for timeouts")
	}
}

func TestADrainingWorkerStopsBeingOfferedAndLeavesWhenItExits(t *testing.T) {
	c := startControlPlane(t)
	mcfg := mockworker.DefaultConfig()
	mcfg.Model, mcfg.TTFT, mcfg.TokensPerSecond, mcfg.OutputTokens, mcfg.Seed = model, 5*time.Millisecond, 25, 50, 1 // a ~2s stream
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + ln.Addr().String()
	ctx, stopMock := context.WithCancel(context.Background())
	t.Cleanup(stopMock)
	mockDone := make(chan error, 1)
	go func() { mockDone <- mockworker.New(mcfg, quiet()).Serve(ctx, ln) }()

	agent := worker.New(worker.Config{WorkerID: "w1", Model: model, AdvertiseURL: url, Interval: hbInterval}, worker.NewMockBackend(url), c.cp, quiet())
	actx, stopAgent := context.WithCancel(context.Background())
	t.Cleanup(stopAgent)
	go func() { _ = agent.Run(actx) }()
	waitFor(t, 5*time.Second, "eligible", func() bool { return c.eligible("w1") })

	// A request is running, then the worker is told to stop (SIGTERM).
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(chat(true, "hi")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = bufio.NewReader(resp.Body).ReadString('\n')
	stopMock()

	waitFor(t, 3*time.Second, "the registry to see the drain", func() bool {
		s, _ := c.get("w1")
		return s.State == protocol.StateDraining
	})
	if c.eligible("w1") || len(c.registry().Eligible(model)) != 0 {
		t.Fatal("a draining worker must not be offered for new work")
	}
	// When the drain finishes and the backend exits, the worker leaves cleanly.
	select {
	case err := <-mockDone:
		if err != nil {
			t.Fatalf("the drain should have been clean: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the mock worker did not finish draining")
	}
	waitFor(t, 3*time.Second, "the worker to be deregistered after its backend exited gracefully", func() bool {
		_, ok := c.get("w1")
		return !ok
	})
}

// countingCP counts the registrations an agent makes.
type countingCP struct {
	worker.ControlPlane
	registers atomic.Int32
}

func (c *countingCP) Register(ctx context.Context, info protocol.WorkerInfo) (protocol.RegisterResponse, error) {
	c.registers.Add(1)
	return c.ControlPlane.Register(ctx, info)
}

// Two live agents with one worker ID used to flip the registration back and
// forth forever, each stealing the identity from the other.
func TestTwoAgentsWithTheSameWorkerIDDoNotFight(t *testing.T) {
	c := startControlPlane(t)
	start := func(id string) (*countingCP, *node) {
		mcfg := mockworker.DefaultConfig()
		mcfg.Model, mcfg.TTFT, mcfg.Seed = model, 5*time.Millisecond, 1
		mock := httptest.NewServer(mockworker.New(mcfg, quiet()).Handler())
		t.Cleanup(mock.Close)
		cp := &countingCP{ControlPlane: c.cp}
		agent := worker.New(worker.Config{WorkerID: id, Model: model, AdvertiseURL: mock.URL, Interval: hbInterval}, worker.NewMockBackend(mock.URL), cp, quiet())
		ctx, cancel := context.WithCancel(context.Background())
		n := &node{id: id, mock: mock, stop: cancel, agent: agent, exited: make(chan struct{})}
		go func() { n.err = agent.Run(ctx); close(n.exited) }()
		t.Cleanup(cancel)
		return cp, n
	}
	cpA, a := start("dup")
	waitFor(t, 5*time.Second, "the first agent to be eligible", func() bool { return c.eligible("dup") })

	cpB, b := start("dup") // a duplicate: a zombie, a second container, a misconfiguration
	select {
	case <-a.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the superseded agent must stop, not keep fighting")
	}
	if !errors.Is(a.err, worker.ErrSuperseded) {
		t.Fatalf("the superseded agent must say why it stopped, got %v", a.err)
	}
	if cpA.registers.Load() != 1 || cpB.registers.Load() != 1 {
		t.Fatalf("each agent registers exactly once: A %d, B %d (more means they are stealing the identity back and forth)", cpA.registers.Load(), cpB.registers.Load())
	}
	waitFor(t, 5*time.Second, "the surviving agent to be eligible", func() bool { return c.eligible("dup") })
	want := b.mock.URL
	for i := 0; i < 20; i++ { // and the registration stays put
		s, ok := c.get("dup")
		if !ok || s.Address != want || !s.Eligible {
			t.Fatalf("the registration must stay with the newer agent: %+v ok=%v", s, ok)
		}
		time.Sleep(25 * time.Millisecond)
	}
	select {
	case <-b.exited:
		t.Fatal("the newer agent must keep running")
	default:
	}
}
