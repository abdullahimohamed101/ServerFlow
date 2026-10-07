package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
)

// These tests run the real mock-worker binary as a process, covering what the
// in-process tests cannot: flag handling, exit codes, the listen address, and
// signal handling in cmd/mock-worker/main.go.

var (
	buildOnce sync.Once
	builtPath string
	buildErr  error
)

func mockWorkerBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("process tests skipped in -short mode")
	}
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mock-worker-bin")
		if err != nil {
			buildErr = err
			return
		}
		builtPath = filepath.Join(dir, "mock-worker")
		goBin := "go"
		if p, err := exec.LookPath("go"); err == nil {
			goBin = p
		}
		cmd := exec.Command(goBin, "build", "-o", builtPath, "./cmd/mock-worker")
		cmd.Dir = filepath.Join("..", "..")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtPath
}

type worker struct {
	cmd    *exec.Cmd
	addr   string // the address it actually listens on
	stderr *lockedBuf
	exited chan error
	dead   atomic.Bool // set once the process has been reaped; safe to read from any goroutine
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// startWorker launches the binary on a free loopback port and waits for its
// startup log line, which reports the real address.
func startWorker(t *testing.T, args ...string) *worker {
	t.Helper()
	bin := mockWorkerBinary(t)
	cmd := exec.Command(bin, append([]string{"--addr=127.0.0.1:0", "--seed=1"}, args...)...)
	pipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	w := &worker{cmd: cmd, stderr: &lockedBuf{}, exited: make(chan error, 1)}
	addrc := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(pipe)
		for sc.Scan() {
			line := sc.Text()
			_, _ = w.stderr.Write([]byte(line + "\n"))
			var rec struct {
				Msg  string
				Addr string
			}
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "mock-worker starting" {
				select {
				case addrc <- rec.Addr:
				default:
				}
			}
		}
		err := cmd.Wait()
		w.dead.Store(true)
		w.exited <- err
	}()
	t.Cleanup(func() {
		if !w.dead.Load() {
			_ = cmd.Process.Kill()
		}
	})
	select {
	case a := <-addrc:
		w.addr = a
	case <-time.After(10 * time.Second):
		t.Fatalf("worker did not start: %s", w.stderr.String())
	}
	return w
}

func (w *worker) url() string {
	_, port, _ := strings.Cut(w.addr, ":")
	if i := strings.LastIndex(w.addr, ":"); i >= 0 {
		port = w.addr[i+1:]
	}
	return "http://127.0.0.1:" + port
}

func (w *worker) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case err := <-w.exited:
		return err
	case <-time.After(d):
		t.Fatalf("worker did not exit within %v; stderr: %s", d, w.stderr.String())
		return nil
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return -2
}

func runOnce(t *testing.T, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(mockWorkerBinary(t), args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return exitCode(err), out.String()
}

func TestProcessHelpAndBadFlags(t *testing.T) {
	code, out := runOnce(t, "--help")
	if code != 0 || strings.Count(out, "Usage:") != 1 || !strings.Contains(out, "-tokens-per-second") {
		t.Fatalf("--help: exit %d, output %q", code, out)
	}
	for _, args := range [][]string{{"--tokens-per-second=0"}, {"--failure-rate=2"}, {"--ttft=soon"}, {"--bogus"}, {"stray"}} {
		code, out := runOnce(t, args...)
		if code != 1 || strings.Count(strings.TrimSpace(out), "\n") != 0 || !strings.HasPrefix(out, "mock-worker: ") {
			t.Fatalf("%v: want exit 1 and a single message, got exit %d: %q", args, code, out)
		}
	}
}

func TestProcessPortInUseExitsNonZero(t *testing.T) {
	a := startWorker(t)
	code, out := runOnce(t, "--addr="+strings.TrimPrefix(a.url(), "http://"))
	if code != 1 || !strings.Contains(out, "address already in use") {
		t.Fatalf("exit %d: %q", code, out)
	}
}

func TestProcessWorkerIDComesFromTheRealPort(t *testing.T) {
	a, b := startWorker(t), startWorker(t) // both asked for port 0
	id := func(w *worker) string {
		resp, err := http.Get(w.url() + "/stats")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var s struct {
			WorkerID string `json:"worker_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&s)
		return s.WorkerID
	}
	ia, ib := id(a), id(b)
	_, pa, _ := strings.Cut(strings.TrimPrefix(a.url(), "http://127.0.0.1"), ":")
	if ia == ib || ia != "mock-"+pa || ia == "mock-0" {
		t.Fatalf("worker IDs must be unique and derived from the real port, got %q and %q", ia, ib)
	}
	_ = a.cmd.Process.Signal(syscall.SIGTERM)
	_ = b.cmd.Process.Signal(syscall.SIGTERM)
	_ = a.wait(t, 5*time.Second)
	_ = b.wait(t, 5*time.Second)
}

func TestProcessListensOnLoopbackByDefault(t *testing.T) {
	bin := mockWorkerBinary(t)
	cmd := exec.Command(bin) // default address 127.0.0.1:9000
	var out lockedBuf
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(out.String(), "mock-worker starting") && !strings.Contains(out.String(), "address already in use") {
		time.Sleep(20 * time.Millisecond)
	}
	if strings.Contains(out.String(), "address already in use") {
		t.Skip("port 9000 is in use on this machine")
	}
	if !strings.Contains(out.String(), `"addr":"127.0.0.1:9000"`) {
		t.Fatalf("default listen address is not loopback: %s", out.String())
	}
}

func TestProcessSIGTERMWhenIdleExitsZero(t *testing.T) {
	w := startWorker(t)
	_ = w.cmd.Process.Signal(syscall.SIGTERM)
	if code := exitCode(w.wait(t, 5*time.Second)); code != 0 {
		t.Fatalf("exit %d: %s", code, w.stderr.String())
	}
	if !strings.Contains(w.stderr.String(), "mock-worker stopped") {
		t.Fatalf("missing the stopped log line: %s", w.stderr.String())
	}
}

func TestProcessSIGTERMLetsAStreamFinishThenExitsZero(t *testing.T) {
	w := startWorker(t, "--model=qwen-7b", "--ttft=0s", "--tokens-per-second=20", "--output-tokens=15") // ~0.75s
	resp, err := http.Post(w.url()+"/v1/chat/completions", "application/json", strings.NewReader(chat(true, "hi")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)
	_, _ = br.ReadString('\n')

	_ = w.cmd.Process.Signal(syscall.SIGTERM)
	rest, _ := io.ReadAll(br)
	if !strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("the in-flight stream was cut off: %q", rest)
	}
	if code := exitCode(w.wait(t, 5*time.Second)); code != 0 {
		t.Fatalf("exit %d: %s", code, w.stderr.String())
	}
}

func TestProcessSecondSignalForcesExit(t *testing.T) {
	w := startWorker(t, "--model=qwen-7b", "--ttft=0s", "--tokens-per-second=2", "--output-tokens=200") // ~100s if left alone
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, w.url()+"/v1/chat/completions", strings.NewReader(chat(true, "hi")))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = bufio.NewReader(resp.Body).ReadString('\n')

	_ = w.cmd.Process.Signal(syscall.SIGTERM) // starts a long drain
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-w.exited:
		t.Fatalf("the worker exited before the second signal: %v", err)
	default:
	}
	start := time.Now()
	_ = w.cmd.Process.Signal(syscall.SIGTERM) // an impatient operator
	err = w.wait(t, 3*time.Second)
	if err == nil {
		t.Fatal("expected the second signal to kill the process, not a clean exit")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("second signal took %v to take effect", took)
	}
}

func TestProcessDrainTimeoutExitsOne(t *testing.T) {
	w := startWorker(t, "--model=qwen-7b", "--ttft=0s", "--tokens-per-second=2", "--output-tokens=200", "--drain-timeout=300ms")
	resp, err := http.Post(w.url()+"/v1/chat/completions", "application/json", strings.NewReader(chat(true, "hi")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = bufio.NewReader(resp.Body).ReadString('\n')

	start := time.Now()
	_ = w.cmd.Process.Signal(syscall.SIGTERM)
	err = w.wait(t, 5*time.Second)
	if code := exitCode(err); code != 1 {
		t.Fatalf("a drain that times out must exit 1, got %d (%v)", code, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a 300ms drain timeout took %v", took)
	}
	if !strings.Contains(w.stderr.String(), "drain timed out") {
		t.Fatalf("the reason must be logged: %s", w.stderr.String())
	}
}
