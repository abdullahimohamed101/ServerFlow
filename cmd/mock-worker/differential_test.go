package main

import (
	"bufio"
	"bytes"
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
	cur := filepath.Join(t.TempDir(), "mock-worker")
	if out, err := exec.Command("go", "build", "-o", cur, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
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
		for _, k := range []string{"time", "duration_ms", "queue_ms", "ttft_ms"} {
			delete(m, k)
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
