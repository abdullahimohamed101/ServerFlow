package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestDifferentialAgainstBase proves the default mock worker did not change: it runs a binary built from
// this tree and a reference binary (built from the base commit by the caller) with identical flags and
// an identical sequential request script, and compares every response (status, headers without Date,
// body without the random id and the clock), /stats without the wall-clock rate, and the normalised log
// lines, including the startup and drain lines. It is skipped unless MOCKWORKER_DIFF_BASE names the
// reference binary:
//
//	git worktree add /tmp/base <base-commit> && (cd /tmp/base && go build -o /tmp/base-mock-worker ./cmd/mock-worker)
//	MOCKWORKER_DIFF_BASE=/tmp/base-mock-worker go test ./cmd/mock-worker -run Differential -v
func TestDifferentialAgainstBase(t *testing.T) {
	base := os.Getenv("MOCKWORKER_DIFF_BASE")
	if base == "" {
		t.Skip("MOCKWORKER_DIFF_BASE is not set")
	}
	cur := buildCurrent(t)
	flagSets := [][]string{
		{"--model=qwen-7b", "--ttft=5ms", "--tokens-per-second=2000", "--output-tokens=12", "--max-concurrency=2", "--queue-size=1", "--seed=7"},
		{"--model=qwen-7b", "--ttft=1ms", "--tokens-per-second=5000", "--output-tokens=6", "--failure-rate=0.5", "--failure-mode=error", "--seed=3"},
		{"--model=qwen-7b", "--ttft=1ms", "--tokens-per-second=5000", "--output-tokens=6", "--failure-rate=0.5", "--failure-mode=midstream", "--seed=11"},
		{"--model=qwen-7b", "--ttft=1ms", "--tokens-per-second=5000", "--output-tokens=6", "--failure-rate=0.4", "--failure-mode=unavailable", "--seed=5", "--log-level=debug"},
	}
	for i, flags := range flagSets {
		a := runScript(t, base, flags)
		b := runScript(t, cur, flags)
		if len(a) != len(b) {
			t.Fatalf("flag set %d: %d lines from base, %d from this tree", i, len(a), len(b))
		}
		for j := range a {
			if a[j] != b[j] {
				t.Errorf("flag set %d, line %d differs:\n base: %.600s\n here: %.600s", i, j, a[j], b[j])
			}
		}
		if len(a) < 30 {
			t.Errorf("flag set %d: the script produced only %d lines; the comparison is too thin to mean anything", i, len(a))
		}
	}
}

var (
	idRe      = regexp.MustCompile(`chatcmpl-[0-9a-f]+`)
	createdRe = regexp.MustCompile(`"created":\d+`)
)

func buildCurrent(t *testing.T) string {
	t.Helper()
	cur := filepath.Join(t.TempDir(), "mock-worker")
	if out, err := exec.Command("go", "build", "-o", cur, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return cur
}

func norm(s string) string {
	return createdRe.ReplaceAllString(idRe.ReplaceAllString(s, "chatcmpl-X"), `"created":0`)
}

// runScript starts the binary, drives it with a fixed script and returns a normalised transcript.
func runScript(t *testing.T, bin string, flags []string) []string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	var logs bytes.Buffer
	cmd := exec.Command(bin, append([]string{"--addr=" + addr, "--worker-id=w1"}, flags...)...)
	cmd.Stderr, cmd.Stdout = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	waitUp(t, addr)

	var out []string
	rec := func(format string, args ...any) { out = append(out, norm(fmt.Sprintf(format, args...))) }
	cl := &http.Client{Timeout: 10 * time.Second}
	do := func(method, path, body string) {
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, "http://"+addr+path, rd)
		req.Header.Set("X-Request-ID", "req_fixed")
		resp, err := cl.Do(req)
		if err != nil {
			rec("%s %s -> error", method, path)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, rerr := io.ReadAll(resp.Body)
		var hs []string
		for k, v := range resp.Header {
			if k != "Date" {
				hs = append(hs, k+"="+strings.Join(v, ","))
			}
		}
		sort.Strings(hs)
		rec("%s %s %q -> %d %v readerr=%v body=%q", method, path, body, resp.StatusCode, hs, rerr != nil, norm(string(b)))
	}
	chat := func(stream bool, prompt, extra string) string {
		return fmt.Sprintf(`{"model":"qwen-7b","messages":[{"role":"user","content":%q}],"stream":%v%s}`, prompt, stream, extra)
	}
	for _, p := range []string{"hello there", "one", strings.Repeat("word ", 40)} {
		for _, stream := range []bool{false, true} {
			for _, extra := range []string{``, `,"max_tokens":3`, `,"max_tokens":12`, `,"max_tokens":50`} {
				do("POST", "/v1/chat/completions", chat(stream, p, extra))
			}
		}
	}
	do("POST", "/v1/chat/completions", `{"model":"nope","messages":[{"role":"user","content":"x"}]}`)
	do("POST", "/v1/chat/completions", `{"model":"qwen-7b"}`)
	do("POST", "/v1/chat/completions", `not json`)
	do("GET", "/v1/models", "")
	do("GET", "/health", "")
	do("GET", "/readyz", "")
	do("GET", "/nothing", "")
	do("PUT", "/stats", "")
	time.Sleep(50 * time.Millisecond)
	resp, err := cl.Get("http://" + addr + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&st)
	_ = resp.Body.Close()
	delete(st, "recent_tokens_per_second") // depends on the wall clock
	keys := make([]string, 0, len(st))
	for k := range st {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		rec("stats %s=%v", k, st[k])
	}
	// The raw field order of /stats must match too.
	raw, _ := cl.Get("http://" + addr + "/stats")
	rb, _ := io.ReadAll(raw.Body)
	_ = raw.Body.Close()
	var order []string
	for _, m := range regexp.MustCompile(`"([a-z_]+)":`).FindAllStringSubmatch(string(rb), -1) {
		order = append(order, m[1])
	}
	rec("stats order %v", order)

	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		rec("exit %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not stop")
	}
	sc := bufio.NewScanner(&logs)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			rec("log (unparsed) %s", sc.Text())
			continue
		}
		delete(m, "time")
		for _, k := range []string{"duration_ms", "queue_ms", "ttft_ms"} {
			if _, ok := m[k]; ok {
				m[k] = "<present>" // the value depends on the clock; its presence does not
			}
		}
		line := norm(fmt.Sprint(m))
		line = strings.ReplaceAll(line, addr, "ADDR")
		out = append(out, "log "+line)
	}
	return out
}

func waitUp(t *testing.T, addr string) {
	t.Helper()
	for range 200 {
		if c, err := net.Dial("tcp", addr); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("worker did not listen on %s", addr)
}

// TestDifferentialConcurrentAndHelp covers what the sequential script cannot: a queue that overflows
// (the exact 503 body), a client that cancels while queued, real queue and first-token times, and the
// flag help.
func TestDifferentialConcurrentAndHelp(t *testing.T) {
	base := os.Getenv("MOCKWORKER_DIFF_BASE")
	if base == "" {
		t.Skip("MOCKWORKER_DIFF_BASE is not set")
	}
	cur := buildCurrent(t)
	a, b := concurrentScenario(t, base), concurrentScenario(t, cur)
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Errorf("concurrent scenario differs:\n--- base ---\n%.2000s\n--- here ---\n%.2000s", strings.Join(a, "\n"), strings.Join(b, "\n"))
	}
	if len(a) < 8 {
		t.Errorf("the concurrent scenario produced only %d lines", len(a))
	}
	ha, hb := helpText(t, base), helpText(t, cur)
	if ha != hb {
		t.Errorf("-h differs beyond the three opt-in flags:\n--- base ---\n%s\n--- here ---\n%s", ha, hb)
	}
	if !strings.Contains(ha, "-failure-mode") {
		t.Errorf("help text looks empty: %q", ha)
	}
}

// helpText runs -h and drops the entries of the flags this phase added (each is two lines).
func helpText(t *testing.T, bin string) string {
	t.Helper()
	cmd := exec.Command(bin, "-h")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s -h: %v\n%s", bin, err, out)
	}
	var keep []string
	lines := strings.Split(string(out), "\n")
	for i := 0; i < len(lines); i++ {
		for _, f := range []string{"  -model-engine ", "  -profile ", "  -sim-params "} {
			if strings.HasPrefix(lines[i], f) {
				i++ // the description line
				goto next
			}
		}
		keep = append(keep, lines[i])
	next:
	}
	return strings.Join(keep, "\n")
}

// concurrentScenario: one slot, a queue of one. Request 1 runs, 2 waits and is cancelled by its client,
// 3 overflows the queue.
func concurrentScenario(t *testing.T, bin string) []string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	var logs bytes.Buffer
	cmd := exec.Command(bin, "--addr="+addr, "--worker-id=w1", "--model=qwen-7b", "--ttft=300ms", "--tokens-per-second=1000",
		"--output-tokens=8", "--max-concurrency=1", "--queue-size=1", "--seed=1")
	cmd.Stderr, cmd.Stdout = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	waitUp(t, addr)

	post := func(ctx context.Context, prompt string) string {
		body := fmt.Sprintf(`{"model":"qwen-7b","messages":[{"role":"user","content":%q}],"stream":true}`, prompt)
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://"+addr+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("X-Request-ID", "req_"+prompt)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "client error"
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return fmt.Sprintf("%d %s %q", resp.StatusCode, resp.Header.Get("Content-Type"), norm(string(b)))
	}
	var out []string
	var mu sync.Mutex
	rec := func(s string) { mu.Lock(); out = append(out, s); mu.Unlock() }
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); rec("r1 " + post(context.Background(), "one")) }()
	time.Sleep(60 * time.Millisecond)
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { defer wg.Done(); rec("r2 " + post(ctx2, "two")) }()
	time.Sleep(30 * time.Millisecond)
	rec("r3 " + post(context.Background(), "three"))
	time.Sleep(10 * time.Millisecond)
	cancel2()
	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://" + addr + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&st)
	_ = resp.Body.Close()
	for _, k := range []string{"completed", "failed", "rejected", "cancelled", "tokens_generated", "active_requests", "queue_depth", "queued_input_tokens"} {
		rec(fmt.Sprintf("stats %s=%v", k, st[k]))
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	_ = cmd.Wait()

	var lines []string
	sc := bufio.NewScanner(&logs)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			lines = append(lines, "log (unparsed) "+sc.Text())
			continue
		}
		// Real timings must be real: the running request waited 300ms for its first token, the cancelled
		// one sat in the queue for a while. Check them here, then compare only that they are present.
		switch m["request_id"] {
		case "req_one":
			if v, _ := m["ttft_ms"].(float64); v < 250 {
				t.Errorf("%s: request one ttft_ms = %v, want about 300", bin, m["ttft_ms"])
			}
		case "req_two":
			if v, _ := m["queue_ms"].(float64); v < 20 {
				t.Errorf("%s: cancelled request queue_ms = %v, want >= 20", bin, m["queue_ms"])
			}
		}
		delete(m, "time")
		for _, k := range []string{"duration_ms", "queue_ms", "ttft_ms"} {
			if _, ok := m[k]; ok {
				m[k] = "<present>"
			}
		}
		lines = append(lines, "log "+strings.ReplaceAll(norm(fmt.Sprint(m)), addr, "ADDR"))
	}
	sort.Strings(lines) // completion order of concurrent requests is not part of the contract
	sort.Strings(out)
	return append(out, lines...)
}

// TestSimStartupLineOmitsTheConstantSpeedSettings: with the simulator engine ttft and tokens_per_second
// have no meaning and are not logged; the default line (compared above) keeps them.
func TestSimStartupLineOmitsTheConstantSpeedSettings(t *testing.T) {
	cur := buildCurrent(t)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close()
	var logs bytes.Buffer
	cmd := exec.Command(cur, "--addr="+addr, "--model-engine=sim", "--profile=slow")
	cmd.Stderr, cmd.Stdout = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	waitUp(t, addr)
	_ = cmd.Process.Signal(syscall.SIGTERM)
	_ = cmd.Wait()
	text := logs.String()
	first := strings.SplitN(text, "\n", 2)[0]
	if !strings.Contains(first, `"mock-worker starting"`) || strings.Contains(first, "ttft") || strings.Contains(first, "tokens_per_second") {
		t.Errorf("startup line = %s", first)
	}
	if !strings.Contains(text, `"model_engine":"sim"`) || !strings.Contains(text, `"profile":"slow"`) || !strings.Contains(text, "params_sha256") {
		t.Errorf("the simulator notice is missing:\n%s", text)
	}
}
