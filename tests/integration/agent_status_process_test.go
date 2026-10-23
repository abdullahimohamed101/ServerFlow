package integration

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// Process-level tests of the worker agent's status endpoint (Phase 17 PR 1, ADR-022 D6) with the real
// binaries: control plane, mock worker and agent.

func startStatusAgent(t *testing.T, cpAddr, token, id, mockAddr, statusAddr string, extraEnv ...string) *proc {
	t.Helper()
	env := append(clusterEnv(cpAddr, token),
		"SERVERFLOW_WORKER_ID="+id, "SERVERFLOW_WORKER_MODEL="+model,
		"SERVERFLOW_WORKER_BACKEND_URL=http://"+mockAddr, "SERVERFLOW_WORKER_ADVERTISE_URL=http://"+mockAddr)
	if statusAddr != "" {
		env = append(env, "SERVERFLOW_WORKER_STATUS_LISTEN="+statusAddr)
	}
	return startProc(t, "worker-agent", "worker-agent starting", append(env, extraEnv...))
}

// probe asks the status endpoint once; err is set when nothing answers.
func probe(addr, path string) (int, string, error) {
	resp, err := (&http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}).Get("http://" + addr + path)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
	return resp.StatusCode, strings.TrimSpace(string(b)), nil
}

func waitProbe(t *testing.T, addr, path string, wantCode int, d time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(d)
	var code int
	var body string
	var err error
	for time.Now().Before(deadline) {
		if code, body, err = probe(addr, path); err == nil && code == wantCode {
			return body
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s%s: wanted %d within %v, last: %d %q %v", addr, path, wantCode, d, code, body, err)
	return ""
}

// The endpoint tells the truth through the life of an agent: alive but not ready while the control plane is
// absent, ready once registered, not ready when the backend dies, not ready the moment the agent is told to
// stop, and still answering until the agent is done.
func TestProcessAgentStatusFollowsRegistrationAndDrain(t *testing.T) {
	cpAddr, mock, status := freePort(t), freePort(t), freePort(t)
	startMockProc(t, mock)
	// worker.unhealthy_timeout is 2.5 s for the agent here: readiness must outlive a lost control plane that long.
	agent := startStatusAgent(t, cpAddr, clusterToken, "w1", mock, status, "SERVERFLOW_WORKER_UNHEALTHY_TIMEOUT=2500ms") // no control plane yet

	waitProbe(t, status, "/healthz", 200, 5*time.Second)
	if body := waitProbe(t, status, "/readyz", 503, 5*time.Second); body != "not registered" {
		t.Fatalf("before any registration: %q", body)
	}
	if code, _, _ := probe(status, "/healthz"); code != 200 {
		t.Fatal("an agent that cannot reach its control plane is alive")
	}

	cp := startControlPlaneProc(t, cpAddr, clusterToken)
	waitProbe(t, status, "/readyz", 200, 15*time.Second)
	waitWorkers(t, cpAddr, clusterToken, 5*time.Second, "w1 eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })

	// The control plane dies: the agent stays alive, and is ready until the registry would stop routing to it.
	cp.kill9()
	killed := time.Now()
	waitProbe(t, status, "/readyz", 503, 6*time.Second)
	if lapse := time.Since(killed); lapse < 1500*time.Millisecond {
		t.Fatalf("readiness lapsed %v after the control plane died; it must follow worker.unhealthy_timeout (2.5s)", lapse)
	}
	if code, _, _ := probe(status, "/healthz"); code != 200 {
		t.Fatal("a control plane outage must not fail liveness")
	}
	cp = startControlPlaneProc(t, cpAddr, clusterToken)
	waitProbe(t, status, "/readyz", 200, 15*time.Second)

	// Drain with a frozen control plane so the shutdown lasts long enough to watch.
	freeze(t, cp.cmd.Process) // returns once the process is really stopped (kill returns when the signal is queued)
	t.Cleanup(func() { _ = cp.cmd.Process.Signal(syscall.SIGCONT) })
	signalled := time.Now()
	sigterm(t, agent)
	var firstDraining, lastDraining time.Time
	sawDraining, left := false, false // left: the first non-200 answer; after it a 200 would be a regression
	var lastReady time.Duration
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !agent.dead.Load() {
		code, body, err := probe(status, "/readyz")
		if err == nil {
			if code == 200 {
				if left {
					t.Fatal("/readyz went back to 200 during the shutdown")
				}
				lastReady = time.Since(signalled) // the signal is still on its way to the process
			} else {
				left = true
			}
			if body == "draining" {
				if !sawDraining {
					firstDraining = time.Now()
				}
				sawDraining, lastDraining = true, time.Now()
			}
		}
		if hc, _, err := probe(status, "/healthz"); err == nil && hc != 200 {
			t.Fatalf("/healthz must stay 200 during the shutdown, got %d", hc)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if span := lastDraining.Sub(firstDraining); span < 150*time.Millisecond {
		t.Fatalf("the endpoint answered draining for only %v: it must stay up while the agent deregisters", span)
	}
	if lastReady > 250*time.Millisecond {
		t.Fatalf("/readyz was still 200 %v after SIGTERM", lastReady)
	}
	t.Logf("last 200 on /readyz %v after SIGTERM was sent", lastReady)
	if err := agent.wait(t, 5*time.Second); err != nil {
		t.Fatalf("the agent must exit 0 on SIGTERM: %v\n%s", err, agent.stderr.String())
	}
	if !sawDraining {
		t.Fatalf("never observed 503 draining during the shutdown\n%s", agent.stderr.String())
	}
	if _, _, err := probe(status, "/readyz"); err == nil {
		t.Fatal("the endpoint must be closed once the agent has exited")
	}
}

func TestProcessAgentStatusReflectsTheBackend(t *testing.T) {
	cpAddr, mock, status := freePort(t), freePort(t), freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	m := startMockProc(t, mock)
	startStatusAgent(t, cpAddr, clusterToken, "w1", mock, status)
	waitProbe(t, status, "/readyz", 200, 15*time.Second)
	m.kill9()
	if body := waitProbe(t, status, "/readyz", 503, 5*time.Second); body != "backend unreachable" {
		t.Fatalf("a dead backend: %q", body)
	}
	if code, _, _ := probe(status, "/healthz"); code != 200 {
		t.Fatal("a dead backend does not make the agent itself unhealthy")
	}
	startMockProc(t, mock)
	waitProbe(t, status, "/readyz", 200, 15*time.Second)
}

// A second signal still forces the agent out while its shutdown is stuck, with the endpoint enabled.
func TestProcessASecondSignalForcesTheAgentWithTheStatusEndpointOn(t *testing.T) {
	cpAddr, mock, status := freePort(t), freePort(t), freePort(t)
	cp := startControlPlaneProc(t, cpAddr, clusterToken)
	startMockProc(t, mock)
	agent := startStatusAgent(t, cpAddr, clusterToken, "w1", mock, status)
	waitProbe(t, status, "/readyz", 200, 15*time.Second)

	freeze(t, cp.cmd.Process) // returns once the process is really stopped (kill returns when the signal is queued)
	t.Cleanup(func() { _ = cp.cmd.Process.Signal(syscall.SIGCONT) })
	sigterm(t, agent)
	// The second signal must find the first one already being handled: the agent says so by answering /readyz with 503.
	waitProbe(t, status, "/readyz", 503, 5*time.Second)
	start := time.Now()
	sigterm(t, agent)
	if err := agent.wait(t, 3*time.Second); err == nil {
		t.Fatal("a forced exit is not a clean one")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the second signal took %v", took)
	}
}

// A graceful stop with the endpoint on takes no longer than without it.
func TestProcessAgentShutdownWithTheEndpointOnIsNotSlower(t *testing.T) {
	cpAddr, mock, status := freePort(t), freePort(t), freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	startMockProc(t, mock)
	agent := startStatusAgent(t, cpAddr, clusterToken, "w1", mock, status)
	waitProbe(t, status, "/readyz", 200, 15*time.Second)
	start := time.Now()
	sigterm(t, agent)
	if err := agent.wait(t, 5*time.Second); err != nil {
		t.Fatalf("exit: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a graceful stop against a healthy control plane took %v", took)
	}
	if !strings.Contains(agent.stderr.String(), `"msg":"worker-agent stopped"`) {
		t.Fatalf("no clean stop line: %s", agent.stderr.String())
	}
}

// With worker.status_listen unset the agent opens no listener and logs no status line (the default is unchanged).
func TestProcessAgentOpensNoListenerByDefault(t *testing.T) {
	cpAddr, mock := freePort(t), freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	startMockProc(t, mock)
	agent := startStatusAgent(t, cpAddr, clusterToken, "w1", mock, "")
	waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "w1 eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })
	if strings.Contains(agent.stderr.String(), "status endpoint") {
		t.Fatalf("a status line without the endpoint configured: %s", agent.stderr.String())
	}
	if lsof, err := exec.LookPath("lsof"); err == nil {
		out, _ := exec.Command(lsof, "-nP", "-a", "-p", fmt.Sprint(agent.cmd.Process.Pid), "-iTCP", "-sTCP:LISTEN").CombinedOutput()
		if strings.Contains(string(out), "LISTEN") {
			t.Fatalf("the agent listens on a port by default:\n%s", out)
		}
	}
}

func TestProcessAgentRefusesAStatusAddressThatIsNotLoopbackOrIsTaken(t *testing.T) {
	base := []string{"SERVERFLOW_WORKER_ID=w", "SERVERFLOW_WORKER_MODEL=m", "SERVERFLOW_WORKER_BACKEND_URL=http://127.0.0.1:1", "SERVERFLOW_WORKER_ADVERTISE_URL=http://127.0.0.1:1"}
	for _, bad := range []string{"0.0.0.0:9102", ":9102", "10.0.0.5:9102", "9102"} {
		code, out := runBin(t, "worker-agent", append(append([]string{}, base...), "SERVERFLOW_WORKER_STATUS_LISTEN="+bad))
		if code != 1 || !strings.Contains(out, "worker.status_listen") {
			t.Errorf("%q: exit %d, output %q", bad, code, out)
		}
	}
	taken := freePort(t)
	startMockProc(t, taken) // something else owns the port
	code, out := runBin(t, "worker-agent", append(append([]string{}, base...), "SERVERFLOW_WORKER_STATUS_LISTEN="+taken))
	if code != 1 || !strings.Contains(out, "status endpoint") {
		t.Errorf("a taken port must stop the agent: exit %d, output %q", code, out)
	}
}

// A probe connection that stalls must not hold the agent's exit back: the endpoint is cut within a quarter of a
// second of the end of the agent's own shutdown.
func TestProcessAStalledStatusClientDoesNotDelayTheAgentsExit(t *testing.T) {
	cpAddr, mock, status := freePort(t), freePort(t), freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	startMockProc(t, mock)
	agent := startStatusAgent(t, cpAddr, clusterToken, "w1", mock, status)
	waitProbe(t, status, "/readyz", 200, 15*time.Second)
	conn, err := net.Dial("tcp", status)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = conn.Write([]byte("GET /readyz HTTP/1.1\r\nHost: x\r\n")) // never finished
	// The listener takes connections in order, so once a later connection has been served the stalled one has been accepted
	// and is being read: that is what the agent's exit must not wait for.
	waitProbe(t, status, "/readyz", 200, 5*time.Second)
	start := time.Now()
	sigterm(t, agent)
	if err := agent.wait(t, 5*time.Second); err != nil {
		t.Fatalf("exit: %v\n%s", err, agent.stderr.String())
	}
	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Fatalf("a stalled status client delayed the exit to %v", took)
	}
}
