package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

// Process-level tests of the control plane and worker agent binaries, with real
// kill -9 and signals. They share the process helpers of mockworker_process_test.go
// in spirit but are generic over the binary.

var (
	binMu    sync.Mutex
	binPaths = map[string]string{}
	binErrs  = map[string]error{}
)

func binary(t *testing.T, name string) string {
	t.Helper()
	if testing.Short() {
		t.Skip("process tests skipped in -short mode")
	}
	binMu.Lock()
	defer binMu.Unlock()
	if p, ok := binPaths[name]; ok {
		return p
	}
	if err, ok := binErrs[name]; ok {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", name+"-bin")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	goBin := "go"
	if p, err := exec.LookPath("go"); err == nil {
		goBin = p
	}
	cmd := exec.Command(goBin, "build", "-o", path, "./cmd/"+name)
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		binErrs[name] = fmt.Errorf("go build %s: %v\n%s", name, err, out)
		t.Fatal(binErrs[name])
	}
	binPaths[name] = path
	return path
}

type proc struct {
	name   string
	cmd    *exec.Cmd
	addr   string // from the startup log line, if it has one
	stderr *lockedBuf
	exited chan error
	dead   atomic.Bool
}

// startProc launches a binary and waits for its startup log line (startMsg).
func startProc(t *testing.T, name, startMsg string, env []string, args ...string) *proc {
	t.Helper()
	cmd := exec.Command(binary(t, name), args...)
	cmd.Env = append(os.Environ(), env...)
	pipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{name: name, cmd: cmd, stderr: &lockedBuf{}, exited: make(chan error, 1)}
	started := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(pipe)
		for sc.Scan() {
			line := sc.Text()
			_, _ = p.stderr.Write([]byte(line + "\n"))
			var rec struct{ Msg, Addr string }
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == startMsg {
				select {
				case started <- rec.Addr:
				default:
				}
			}
		}
		err := cmd.Wait()
		p.dead.Store(true)
		p.exited <- err
	}()
	t.Cleanup(func() {
		if !p.dead.Load() {
			_ = cmd.Process.Kill()
		}
	})
	select {
	case p.addr = <-started:
	case err := <-p.exited:
		t.Fatalf("%s exited before starting: %v: %s", name, err, p.stderr.String())
	case <-time.After(15 * time.Second):
		t.Fatalf("%s did not start: %s", name, p.stderr.String())
	}
	return p
}

func (p *proc) kill9() { _ = p.cmd.Process.Signal(syscall.SIGKILL); <-p.exited; p.exited <- nil }

func (p *proc) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case err := <-p.exited:
		return err
	case <-time.After(d):
		t.Fatalf("%s did not exit within %v; stderr: %s", p.name, d, p.stderr.String())
		return nil
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// clusterEnv is the environment shared by the control plane and agents, with
// thresholds short enough to watch a death in a few seconds.
func clusterEnv(cpAddr, token string) []string {
	return []string{
		"SERVERFLOW_WORKER_HEARTBEAT_INTERVAL=200ms", "SERVERFLOW_WORKER_SUSPECT_TIMEOUT=500ms",
		"SERVERFLOW_WORKER_UNHEALTHY_TIMEOUT=1s", "SERVERFLOW_WORKER_LOST_TIMEOUT=3s", "SERVERFLOW_WORKER_RETENTION=30s",
		"SERVERFLOW_CONTROL_PLANE_ADDR=" + cpAddr, "SERVERFLOW_CONTROL_PLANE_TOKEN=" + token,
		"SERVERFLOW_WORKER_CONTROL_PLANE_URL=http://" + cpAddr,
	}
}

func startControlPlaneProc(t *testing.T, cpAddr, token string) *proc {
	return startProc(t, "control-plane", "control-plane starting", clusterEnv(cpAddr, token))
}

func startMockProc(t *testing.T, addr string, extra ...string) *proc {
	args := append([]string{"--addr=" + addr, "--model=" + model, "--ttft=10ms", "--tokens-per-second=1000", "--output-tokens=8", "--seed=1"}, extra...)
	return startProc(t, "mock-worker", "mock-worker starting", nil, args...)
}

func startAgentProc(t *testing.T, cpAddr, token, id, mockAddr string) *proc {
	env := append(clusterEnv(cpAddr, token),
		"SERVERFLOW_WORKER_ID="+id, "SERVERFLOW_WORKER_MODEL="+model,
		"SERVERFLOW_WORKER_BACKEND_URL=http://"+mockAddr, "SERVERFLOW_WORKER_ADVERTISE_URL=http://"+mockAddr)
	return startProc(t, "worker-agent", "worker-agent starting", env)
}

func fetchWorkers(cpAddr, token string) (map[string]protocol.WorkerSnapshot, error) {
	req, _ := http.NewRequest("GET", "http://"+cpAddr+"/v1/workers", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var out struct{ Workers []protocol.WorkerSnapshot }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	m := map[string]protocol.WorkerSnapshot{}
	for _, w := range out.Workers {
		m[w.WorkerID] = w
	}
	return m, nil
}

func waitWorkers(t *testing.T, cpAddr, token string, d time.Duration, what string, pred func(map[string]protocol.WorkerSnapshot) bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < d {
		if ws, err := fetchWorkers(cpAddr, token); err == nil && pred(ws) {
			return time.Since(start)
		}
		time.Sleep(25 * time.Millisecond)
	}
	ws, err := fetchWorkers(cpAddr, token)
	t.Fatalf("timed out after %v waiting for: %s (registry: %+v, err %v)", d, what, ws, err)
	return 0
}

const clusterToken = "a-process-test-shared-secret-token"

func TestProcessWorkerDeathAndRecovery(t *testing.T) {
	// Processes restarted inside a subtest must be owned by this test, not the
	// subtest, or the subtest's cleanup would kill them when it ends.
	top := t
	cpAddr := freePort(t)
	cp := startControlPlaneProc(t, cpAddr, clusterToken)
	m1, m2 := freePort(t), freePort(t)
	mock1 := startMockProc(t, m1)
	startMockProc(t, m2)
	agent1 := startAgentProc(t, cpAddr, clusterToken, "w1", m1)
	agent2 := startAgentProc(t, cpAddr, clusterToken, "w2", m2)
	elig := func(ids ...string) func(map[string]protocol.WorkerSnapshot) bool {
		return func(ws map[string]protocol.WorkerSnapshot) bool {
			for _, id := range ids {
				if !ws[id].Eligible {
					return false
				}
			}
			return true
		}
	}
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "both workers eligible", elig("w1", "w2"))

	t.Run("a killed backend is FAILED at once and recovers on restart", func(t *testing.T) {
		mock1.kill9()
		took := waitWorkers(t, cpAddr, clusterToken, 5*time.Second, "w1 FAILED", func(ws map[string]protocol.WorkerSnapshot) bool {
			return ws["w1"].State == protocol.StateFailed
		})
		if took > time.Second {
			t.Fatalf("FAILED took %v; it must not wait for the unhealthy timeout", took)
		}
		ws, _ := fetchWorkers(cpAddr, clusterToken)
		if ws["w1"].Eligible || !ws["w2"].Eligible {
			t.Fatalf("only the failed worker is withdrawn: %+v", ws)
		}
		mock1 = startMockProc(top, m1)
		waitWorkers(t, cpAddr, clusterToken, 5*time.Second, "w1 eligible again", elig("w1", "w2"))
	})

	t.Run("a killed agent walks suspect, unhealthy, lost and is never offered", func(t *testing.T) {
		agent2.kill9()
		var seq []string
		wasIneligible, eligibleAgain := false, false
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			ws, err := fetchWorkers(cpAddr, clusterToken)
			if err != nil {
				t.Fatal(err)
			}
			w := ws["w2"]
			cur := fmt.Sprintf("%s/%s", w.State, w.Health)
			if len(seq) == 0 || seq[len(seq)-1] != cur {
				seq = append(seq, cur)
			}
			if !w.Eligible {
				wasIneligible = true
			} else if wasIneligible {
				eligibleAgain = true
			}
			if w.State == protocol.StateLost {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if got, want := strings.Join(seq, " "), "READY/healthy READY/suspect UNHEALTHY/unhealthy LOST/lost"; got != want {
			t.Fatalf("a dead agent must walk %q, observed %q", want, got)
		}
		if eligibleAgain {
			t.Fatal("a dead worker became eligible again")
		}
		ws, _ := fetchWorkers(cpAddr, clusterToken)
		if !ws["w1"].Eligible {
			t.Fatal("the bystander must be unaffected")
		}
	})

	t.Run("restarting the agent brings the worker back", func(t *testing.T) {
		agent2 = startAgentProc(top, cpAddr, clusterToken, "w2", m2)
		waitWorkers(t, cpAddr, clusterToken, 5*time.Second, "w2 eligible again", elig("w1", "w2"))
	})

	t.Run("a restarted control plane is repopulated by the workers themselves", func(t *testing.T) {
		cp.kill9()
		cp = startControlPlaneProc(top, cpAddr, clusterToken)
		took := waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "both workers to re-register", elig("w1", "w2"))
		if took > 3*time.Second {
			t.Fatalf("workers took %v to re-register", took)
		}
	})

	t.Run("SIGTERM on an agent deregisters its worker and exits 0", func(t *testing.T) {
		start := time.Now()
		_ = agent1.cmd.Process.Signal(syscall.SIGTERM)
		if err := agent1.wait(t, 5*time.Second); err != nil {
			t.Fatalf("a graceful agent exit is 0, got %v: %s", err, agent1.stderr.String())
		}
		ws, _ := fetchWorkers(cpAddr, clusterToken)
		if _, still := ws["w1"]; still {
			t.Fatalf("a gracefully stopped worker must be removed, not left to expire: %+v", ws["w1"])
		}
		if time.Since(start) > 3*time.Second {
			t.Fatalf("graceful shutdown took %v", time.Since(start))
		}
	})

	t.Run("SIGTERM on the control plane exits 0 and never logs the token", func(t *testing.T) {
		_ = cp.cmd.Process.Signal(syscall.SIGTERM)
		if err := cp.wait(t, 5*time.Second); err != nil {
			t.Fatalf("got %v: %s", err, cp.stderr.String())
		}
		for _, p := range []*proc{cp, agent2} {
			if strings.Contains(p.stderr.String(), clusterToken) {
				t.Fatalf("%s logged the shared secret", p.name)
			}
		}
	})
	_ = agent2.cmd.Process.Signal(syscall.SIGTERM)
}

func TestProcessAuthentication(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	mock := freePort(t)
	startMockProc(t, mock)

	if _, err := fetchWorkers(cpAddr, ""); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("an unauthenticated read must be refused: %v", err)
	}
	if _, err := fetchWorkers(cpAddr, "wrong-token-wrong-token"); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a wrong token must be refused: %v", err)
	}

	bad := startAgentProc(t, cpAddr, "wrong-token-wrong-token", "w-bad", mock)
	time.Sleep(1500 * time.Millisecond)
	ws, err := fetchWorkers(cpAddr, clusterToken)
	if err != nil || len(ws) != 0 {
		t.Fatalf("an agent with the wrong token must never register: %v %v", ws, err)
	}
	if !strings.Contains(bad.stderr.String(), "control_plane.token") {
		t.Fatalf("the agent should say the token is wrong: %s", bad.stderr.String())
	}
	if strings.Contains(bad.stderr.String(), "wrong-token-wrong-token") {
		t.Fatal("the agent logged its token")
	}

	startAgentProc(t, cpAddr, clusterToken, "w-good", mock)
	waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "the authenticated agent to register", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w-good"].Eligible })
}

func runBin(t *testing.T, name string, env []string, args ...string) (int, string) {
	t.Helper()
	// A binary that should have refused to start but is serving instead must fail the
	// test quickly, not hang it.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary(t, name), args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("%s did not exit within 10s (it should have failed fast); output: %s", name, out)
	}
	code := 0
	if err != nil {
		code = -2
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	return code, string(out)
}

func TestProcessConfigurationErrorsFailFast(t *testing.T) {
	secret := "http://user:topsecretpassword@127.0.0.1:1"
	tests := []struct {
		name string
		bin  string
		env  []string
		want string
	}{
		{"control plane off loopback without a token", "control-plane", []string{"SERVERFLOW_CONTROL_PLANE_ADDR=0.0.0.0:0"}, "control_plane.token is required"},
		{"control plane with a short token", "control-plane", []string{"SERVERFLOW_CONTROL_PLANE_TOKEN=short"}, "at least 16 characters"},
		{"control plane with a bad address", "control-plane", []string{"SERVERFLOW_CONTROL_PLANE_ADDR=nonsense"}, "host:port"},
		{"control plane with bad thresholds", "control-plane", []string{"SERVERFLOW_WORKER_SUSPECT_TIMEOUT=20s"}, "suspect_timeout < unhealthy_timeout"},
		{"agent without an id", "worker-agent", nil, "worker.id is required"},
		{"agent sending the token over cleartext http to another host", "worker-agent", []string{"SERVERFLOW_WORKER_ID=w", "SERVERFLOW_WORKER_MODEL=m", "SERVERFLOW_WORKER_BACKEND_URL=http://127.0.0.1:1", "SERVERFLOW_WORKER_ADVERTISE_URL=http://127.0.0.1:1", "SERVERFLOW_WORKER_CONTROL_PLANE_URL=http://10.0.0.5:9090", "SERVERFLOW_CONTROL_PLANE_TOKEN=" + clusterToken}, "https"},
		{"control plane with a 1ms heartbeat interval", "control-plane", []string{"SERVERFLOW_WORKER_HEARTBEAT_INTERVAL=1ms"}, "heartbeat_interval"},
		{"agent with credentials in a url", "worker-agent", []string{"SERVERFLOW_WORKER_ID=w", "SERVERFLOW_WORKER_MODEL=m", "SERVERFLOW_WORKER_BACKEND_URL=" + secret, "SERVERFLOW_WORKER_ADVERTISE_URL=http://127.0.0.1:1"}, "must not contain credentials"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := runBin(t, tt.bin, tt.env)
			if code != 1 || !strings.Contains(out, tt.want) {
				t.Fatalf("exit %d, output %q; want exit 1 mentioning %q", code, out, tt.want)
			}
			if strings.Contains(out, "topsecretpassword") || strings.Contains(out, "short") && strings.Contains(tt.name, "short token") && strings.Contains(out, "=short") {
				t.Fatalf("the error leaks a secret: %s", out)
			}
		})
	}
}

func TestProcessControlPlaneOffLoopbackIsAllowedWithAToken(t *testing.T) {
	p := startProc(t, "control-plane", "control-plane starting",
		[]string{"SERVERFLOW_CONTROL_PLANE_ADDR=127.0.0.1:0", "SERVERFLOW_CONTROL_PLANE_TOKEN=" + clusterToken})
	if !strings.Contains(p.stderr.String(), `"auth_required":true`) {
		t.Fatalf("the startup line must say whether auth is required: %s", p.stderr.String())
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if err := p.wait(t, 5*time.Second); err != nil {
		t.Fatalf("got %v", err)
	}
}

// --- independent review and verification findings, end to end ---------------------------------------

func TestProcessTwoAgentsWithTheSameWorkerIDDoNotFight(t *testing.T) {
	cpAddr := freePort(t)
	cp := startControlPlaneProc(t, cpAddr, clusterToken)
	m1, m2 := freePort(t), freePort(t)
	startMockProc(t, m1)
	startMockProc(t, m2)
	a := startAgentProc(t, cpAddr, clusterToken, "dup", m1)
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "the first agent eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["dup"].Eligible })

	b := startAgentProc(t, cpAddr, clusterToken, "dup", m2) // a duplicate
	err := a.wait(t, 5*time.Second)
	if exitCodeOf(err) != 1 {
		t.Fatalf("the superseded agent must exit with an error, got %v: %s", err, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "another process") {
		t.Fatalf("it must say why: %s", a.stderr.String())
	}
	for i := 0; i < 12; i++ { // the registration stays with the newer agent
		ws, err := fetchWorkers(cpAddr, clusterToken)
		if err != nil {
			t.Fatal(err)
		}
		if got := ws["dup"].Address; got != "http://"+m2 {
			t.Fatalf("the registration flipped back to %q", got)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := strings.Count(cp.stderr.String(), "superseding the previous incarnation"); n != 1 {
		t.Fatalf("exactly one supersede is expected, the control plane logged %d", n)
	}
	if b.dead.Load() {
		t.Fatal("the newer agent must keep running")
	}
}

func exitCodeOf(err error) int {
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return -2
}

func TestProcessTheAdvertisedAddressIsWhatGetsRegistered(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	mock := freePort(t)
	startMockProc(t, mock)
	env := append(clusterEnv(cpAddr, clusterToken), "SERVERFLOW_WORKER_ID=w1", "SERVERFLOW_WORKER_MODEL="+model,
		"SERVERFLOW_WORKER_BACKEND_URL=http://"+mock, "SERVERFLOW_WORKER_ADVERTISE_URL=http://gateway-facing.internal:7777")
	startProc(t, "worker-agent", "worker-agent starting", env)
	waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "w1 registered", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })
	ws, _ := fetchWorkers(cpAddr, clusterToken)
	if got := ws["w1"].Address; got != "http://gateway-facing.internal:7777" {
		t.Fatalf("the registry must hold the advertised address (what the gateway will use), not the backend's: %q", got)
	}
}

func TestProcessMaxWorkersIsEnforced(t *testing.T) {
	cpAddr := freePort(t)
	env := append(clusterEnv(cpAddr, clusterToken), "SERVERFLOW_CONTROL_PLANE_MAX_WORKERS=1")
	startProc(t, "control-plane", "control-plane starting", env)
	m1, m2 := freePort(t), freePort(t)
	startMockProc(t, m1)
	startMockProc(t, m2)
	startAgentProc(t, cpAddr, clusterToken, "w1", m1)
	waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "w1 registered", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })
	second := startAgentProc(t, cpAddr, clusterToken, "w2", m2)
	time.Sleep(1500 * time.Millisecond)
	ws, _ := fetchWorkers(cpAddr, clusterToken)
	if len(ws) != 1 {
		t.Fatalf("the cap of 1 must hold, registry has %d workers: %+v", len(ws), ws)
	}
	if second.dead.Load() {
		t.Fatal("an agent refused by a full registry keeps retrying; it must not exit")
	}
}

func TestProcessRetentionEvictsDeadWorkers(t *testing.T) {
	cpAddr := freePort(t)
	env := append(clusterEnv(cpAddr, clusterToken), "SERVERFLOW_WORKER_LOST_TIMEOUT=1500ms", "SERVERFLOW_WORKER_RETENTION=500ms")
	startProc(t, "control-plane", "control-plane starting", env)
	m := freePort(t)
	startMockProc(t, m)
	agentEnv := append(env, "SERVERFLOW_WORKER_ID=w1", "SERVERFLOW_WORKER_MODEL="+model,
		"SERVERFLOW_WORKER_BACKEND_URL=http://"+m, "SERVERFLOW_WORKER_ADVERTISE_URL=http://"+m)
	agent := startProc(t, "worker-agent", "worker-agent starting", agentEnv)
	waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "w1 eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })

	agent.kill9()
	start := time.Now()
	waitWorkers(t, cpAddr, clusterToken, 5*time.Second, "w1 LOST", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].State == protocol.StateLost })
	waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "w1 evicted", func(ws map[string]protocol.WorkerSnapshot) bool { _, ok := ws["w1"]; return !ok })
	if took := time.Since(start); took < 1900*time.Millisecond {
		t.Fatalf("a worker lost at 1.5s and kept 0.5s must not vanish after %v", took)
	}
}

func TestProcessBrowserStyleAttacksAreRefused(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, "") // tokenless loopback: the case a web page could reach
	body := `{"worker_id":"mock-1","model":"m","address":"http://attacker.example:80","max_concurrency":1,"queue_size":0}`
	post := func(hdr map[string]string) int {
		req, _ := http.NewRequest("POST", "http://"+cpAddr+"/v1/workers/register", strings.NewReader(body))
		for k, v := range hdr {
			if k == "Host" {
				req.Host = v
			} else {
				req.Header.Set(k, v)
			}
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}
	if got := post(map[string]string{"Content-Type": "text/plain"}); got != 415 {
		t.Fatalf("a text/plain registration must be refused, got %d", got)
	}
	if got := post(map[string]string{"Content-Type": "application/json", "Origin": "http://evil.example"}); got != 403 {
		t.Fatalf("a cross-origin registration must be refused, got %d", got)
	}
	if got := post(map[string]string{"Content-Type": "application/json", "Host": "evil.example"}); got != 421 {
		t.Fatalf("a registration through a foreign Host must be refused, got %d", got)
	}
	ws, err := fetchWorkers(cpAddr, "")
	if err != nil || len(ws) != 0 {
		t.Fatalf("nothing may have been registered: %v %v", ws, err)
	}
	if got := post(map[string]string{"Content-Type": "application/json"}); got != 201 {
		t.Fatalf("a normal local registration must still work, got %d", got)
	}
}

func TestProcessASecondSignalForcesTheControlPlaneToQuitEvenWithAStalledRequest(t *testing.T) {
	cpAddr := freePort(t)
	cp := startControlPlaneProc(t, cpAddr, clusterToken)
	conn, err := net.Dial("tcp", cpAddr) // an authenticated client that stalls mid-body
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = fmt.Fprintf(conn, "POST /v1/workers/register HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{", cpAddr, clusterToken)
	time.Sleep(200 * time.Millisecond)

	_ = cp.cmd.Process.Signal(syscall.SIGTERM)
	time.Sleep(300 * time.Millisecond)
	if cp.dead.Load() {
		t.Fatal("the control plane should still be draining the stalled request")
	}
	start := time.Now()
	_ = cp.cmd.Process.Signal(syscall.SIGTERM) // an impatient operator
	if err := cp.wait(t, 3*time.Second); err == nil {
		t.Fatal("a forced exit is not a clean one")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the second signal took %v", took)
	}
}

func TestProcessASecondSignalForcesTheAgentToQuitWhileTheControlPlaneIsFrozen(t *testing.T) {
	cpAddr := freePort(t)
	cp := startControlPlaneProc(t, cpAddr, clusterToken)
	m := freePort(t)
	startMockProc(t, m)
	agent := startAgentProc(t, cpAddr, clusterToken, "w1", m)
	waitWorkers(t, cpAddr, clusterToken, 8*time.Second, "w1 eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })

	_ = cp.cmd.Process.Signal(syscall.SIGSTOP) // the control plane hangs
	t.Cleanup(func() { _ = cp.cmd.Process.Signal(syscall.SIGCONT) })
	_ = agent.cmd.Process.Signal(syscall.SIGTERM) // the agent tries to deregister and waits
	time.Sleep(500 * time.Millisecond)
	if agent.dead.Load() {
		t.Fatal("the agent should be waiting for the frozen control plane")
	}
	start := time.Now()
	_ = agent.cmd.Process.Signal(syscall.SIGTERM)
	if err := agent.wait(t, 3*time.Second); err == nil {
		t.Fatal("a forced exit is not a clean one")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the second signal took %v", took)
	}
}

func TestProcessTheConfiguredHeartbeatIntervalReachesTheAgent(t *testing.T) {
	// The control plane suggests 5s; the agent is configured for 250ms, so it may adopt at most
	// 10x that (2.5s). Seeing exactly 2.5s proves the configured value, not a default, bounds it.
	cpAddr := freePort(t)
	cpEnv := append(clusterEnv(cpAddr, clusterToken),
		"SERVERFLOW_WORKER_HEARTBEAT_INTERVAL=5s", "SERVERFLOW_WORKER_SUSPECT_TIMEOUT=10s",
		"SERVERFLOW_WORKER_UNHEALTHY_TIMEOUT=20s", "SERVERFLOW_WORKER_LOST_TIMEOUT=60s")
	startProc(t, "control-plane", "control-plane starting", cpEnv)
	mock := freePort(t)
	startMockProc(t, mock)
	env := append(clusterEnv(cpAddr, clusterToken), "SERVERFLOW_WORKER_HEARTBEAT_INTERVAL=250ms",
		"SERVERFLOW_WORKER_ID=w1", "SERVERFLOW_WORKER_MODEL="+model,
		"SERVERFLOW_WORKER_BACKEND_URL=http://"+mock, "SERVERFLOW_WORKER_ADVERTISE_URL=http://"+mock)
	agent := startProc(t, "worker-agent", "worker-agent starting", env)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, line := range strings.Split(agent.stderr.String(), "\n") {
			if strings.Contains(line, "registered with the control plane") {
				if !strings.Contains(line, `"heartbeat_interval":"2.5s"`) {
					t.Fatalf("the agent must bound the suggested interval by its configured one (250ms x 10 = 2.5s): %s", line)
				}
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the agent never registered: %s", agent.stderr.String())
}
