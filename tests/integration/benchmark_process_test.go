package integration

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Process-level test of the built benchmark binary: run, run, compare, list, and bad input.

func bench(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	cmd := exec.Command(binary(t, "benchmark"), args...)
	cmd.Dir = filepath.Join("..", "..") // the repository, so git metadata is real
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		code = 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Minute):
		_ = cmd.Process.Kill()
		t.Fatalf("benchmark %v did not finish: %s", args, e.String())
	}
	return code, o.String(), e.String()
}

func TestTheBenchmarkBinaryRunsComparesAndRefusesBadInput(t *testing.T) {
	dir := t.TempDir()
	common := []string{"--out", dir, "--workers", "2", "--workload", "uniform-short", "--concurrency", "4", "--duration", "1s",
		"--warmup", "0s", "--mock-tps", "20000", "--mock-ttft", "1ms", "--seed", "3"}

	for _, sched := range []string{"round-robin", "least-active"} {
		code, out, errOut := bench(t, append([]string{"run", "--scheduler", sched}, common...)...)
		if code != 0 || !strings.Contains(out, "wrote") {
			t.Fatalf("%s: exit %d\n%s\n%s", sched, code, out, errOut)
		}
		if strings.Contains(out+errOut, "sk-bench") {
			t.Fatal("an API key reached the output")
		}
	}
	for _, f := range []string{"result.json", "report.md"} {
		for _, id := range []string{"run_001", "run_002"} {
			if _, err := os.Stat(filepath.Join(dir, id, f)); err != nil {
				t.Errorf("%s/%s: %v", id, f, err)
			}
		}
	}

	code, out, errOut := bench(t, "compare", "--dir", dir, "run_001", "run_002")
	if code != 0 || errOut != "" {
		t.Fatalf("compare: exit %d %s", code, errOut)
	}
	for _, want := range []string{"throughput", "TTFT p95", "latency p95", "worker balance", "scheduler: round-robin -> least-active", "not better"} {
		if !strings.Contains(out, want) {
			t.Errorf("compare output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "WARNING") {
		t.Errorf("only the scheduler differs, so the comparison is fair:\n%s", out)
	}
	if code, out, _ := bench(t, "compare", "--dir", dir, "run_001", "run_001"); code != 0 || strings.Contains(out, "+10") {
		t.Errorf("self-comparison: %d\n%s", code, out)
	}
	if code, out, _ := bench(t, "list", "--dir", dir); code != 0 || !strings.Contains(out, "run_002") || !strings.Contains(out, "least-active") {
		t.Errorf("list: %d\n%s", code, out)
	}

	// Bad input gives a clear error and a non-zero exit, and writes nothing.
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"unknown flag":    {[]string{"run", "--bogus"}, "bogus"},
		"unknown sched":   {[]string{"run", "--out", dir, "--scheduler", "magic"}, "known strategy"},
		"both loads":      {[]string{"run", "--out", dir, "--concurrency", "2", "--rate", "2"}, "mutually exclusive"},
		"remote refused":  {[]string{"run", "--out", dir, "--target", "http://example.com:8080"}, "--allow-remote"},
		"cap":             {[]string{"run", "--out", dir, "--concurrency", "99999"}, "cap"},
		"missing compare": {[]string{"compare", "--dir", dir, "run_001", "run_099"}, "not found"},
	} {
		code, _, errOut := bench(t, tc.args...)
		if code == 0 || !strings.Contains(errOut, tc.want) {
			t.Errorf("%s: exit %d, stderr %q (want %q)", name, code, errOut, tc.want)
		}
	}
	ids, _ := os.ReadDir(dir)
	n := 0
	for _, e := range ids {
		if e.IsDir() {
			n++
		}
	}
	if n != 2 {
		t.Errorf("bad input must not leave runs behind: %d directories", n)
	}
}
