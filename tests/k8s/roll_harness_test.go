package k8s

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The roll-test command run end to end against a stub kubectl and a fake gateway on loopback. It cannot say anything about a
// cluster; it pins what the script does around the measurement: the order of the output, the restore, the exit status, and that
// a rollout which replaced nothing cannot pass.

const stubKubectl = `#!/bin/sh
D="$FAKE_DIR"
echo "$*" >> "$D/calls.log"
case "$*" in
  "version --client") echo "Client Version: $FAKE_VERSION" ;;
  kustomize*) cat "$D/render.yaml" ;;
  *"config current-context"*) echo kind-serverflow ;;
  *"config view --minify"*) echo https://127.0.0.1:1 ;;
  *"containerStatuses"*) echo "pod-a Running true/0 " ;;
  *"deletionGracePeriodSeconds"*) printf 'g%s-1 2026-01-01T00:00:45Z 45\n' "$(cat "$D/gen" 2>/dev/null || echo 0)" ;;
  *"deletionTimestamp}"*) : ;;
  *"get pods"*"{.metadata.name}"*) n=$(cat "$D/gen" 2>/dev/null || echo 0); printf 'g%s-1\ng%s-2\ng%s-3\n' "$n" "$n" "$n" ;;
  *"get deployment gateway"*) echo 3 ;;
  *"get configmaps"*) printf 'gateway-config-rollmain1\n' ;;
  *"rollout status"*)
    # A rollout lasts as long as it takes the fake gateway to serve FAKE_ROLL_REQUESTS more requests, not a number of seconds:
    # the verdicts then never depend on how fast the machine is. With no load running (the restore) the count stands still and
    # the wait ends after 2 s.
    c0=$(curl -s "$FAKE_GW/__count" || echo 0); last=$c0; idle=0; t=0
    while [ $((c0 + ${FAKE_ROLL_REQUESTS:-1600})) -gt "$last" ] && [ "$idle" -lt 4 ] && [ "$t" -lt 240 ]; do
      sleep 0.5; t=$((t + 1)); c=$(curl -s "$FAKE_GW/__count" || echo "$last")
      if [ "$c" = "$last" ]; then idle=$((idle + 1)); else idle=0; fi
      last=$c
    done
    ;;
  *"apply -f -"*)
    cat > "$D/applied.yaml"
    grep -o 'gateway-config-[a-z0-9]*' "$D/applied.yaml" | head -1 >> "$D/applied.log"
    if [ -f "$D/failrestore" ] && ! grep -q 'gateway-config-roll' "$D/applied.yaml"; then exit 1; fi
    if [ ! -f "$D/static" ]; then n=$(cat "$D/gen" 2>/dev/null || echo 0); echo $((n + 1)) > "$D/gen"; fi
    # The flag is replaced atomically (write a temporary file, rename it): the fake gateway reads it on every request, and a plain
    # "echo > outage" truncates the file first, so a request that arrives between the truncation and the write reads an empty flag.
    if grep -q 'READINESS_GRACE: 0s' "$D/applied.yaml"; then echo "${FAKE_OUTAGE:-abort}" > "$D/outage.tmp" && mv "$D/outage.tmp" "$D/outage"; else rm -f "$D/outage"; fi
    ;;
esac
exit 0
`

type rollRun struct {
	out   string
	lines []string
	err   error
	dir   string
}

func runRollTest(t *testing.T, bash string, files ...string) rollRun {
	return runRollTestEnv(t, bash, nil, files...)
}

func runRollTestEnv(t *testing.T, bash string, extra []string, files ...string) rollRun {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("bash scripts")
	}
	rendered := render(t, overlay)
	fake := t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(fake, "kubectl"), []byte(stubKubectl), 0o755))
	must(os.WriteFile(filepath.Join(fake, "render.yaml"), rendered, 0o600))
	for _, f := range files {
		must(os.WriteFile(filepath.Join(fake, f), nil, 0o600))
	}
	state := stateDir(t)
	must(os.WriteFile(filepath.Join(state, "kubeconfig"), []byte("# not read\n"), 0o600))
	const key = "sf_live_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE"
	must(os.WriteFile(filepath.Join(state, "api_key"), []byte(key+"\n"), 0o600))

	// FAKE_SLOW=1 caps the fake gateway at about 50 requests a second (24 clients, 450 ms each). A machine can only be slower than
	// that, never faster, so a run that needs more than 3000 requests during a few rollouts cannot get there on any machine.
	slow := false
	for _, e := range extra {
		slow = slow || e == "FAKE_SLOW=1"
	}
	var n atomic.Int64
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
		case "/__count": // read by the stub kubectl to time a rollout by requests served
			_, _ = fmt.Fprint(w, n.Load())
		case "/v1/models":
			_, _ = io.WriteString(w, `{"data":[{"id":"mock-model"}]}`)
		default:
			n.Add(1)
			if slow {
				time.Sleep(450 * time.Millisecond)
			}
			time.Sleep(20 * time.Millisecond) // slow enough not to exhaust local ports; rollouts are timed by requests served (the stub), not by seconds
			// The request body is always read first: a server that answers and closes with request bytes still unread makes the
			// kernel send a reset, which the client reports as a reset connection, one of the failures the control counts.
			b, _ := io.ReadAll(r.Body)
			switch outageKind(fake) {
			case "capacity": // load, not the drain
				w.WriteHeader(503)
				_, _ = io.WriteString(w, `{"error":{"code":"no_capacity"}}`)
				return
			case "abort": // the drain's absence: connections cut
				panic(http.ErrAbortHandler)
			}
			if strings.Contains(string(b), `"stream":true`) {
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n")
				return
			}
			_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"hi"}}]}`)
		}
	}))
	t.Cleanup(gw.Close)
	u, _ := url.Parse(gw.URL)

	cmd := exec.Command(bash, "-u", filepath.Join(root(t), "scripts/k8s-up.sh"), "roll-test")
	cmd.Dir = root(t)
	cmd.Env = append(os.Environ(), "PATH="+fake+string(os.PathListSeparator)+os.Getenv("PATH"), "K8S_UP_DIR="+state, "FAKE_DIR="+fake,
		"FAKE_VERSION="+pins(t)["KUBERNETES_VERSION"], "K8S_UP_TEST_GATEWAY_PORT="+u.Port(), "FAKE_GW="+gw.URL, "ROLL_PHASE_SECONDS=1", "K8S_UP_WAIT=5")
	cmd.Env = append(cmd.Env, extra...)
	b, err := cmd.CombinedOutput()
	out := string(b)
	if strings.Contains(out, key) {
		t.Error("the API key appeared in the output")
	}
	return rollRun{out: out, lines: strings.Split(strings.TrimRight(out, "\n"), "\n"), err: err, dir: fake}
}

func lastLine(r rollRun) string { return r.lines[len(r.lines)-1] }

func readFake(t *testing.T, r rollRun, name string) string {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join(r.dir, name))
	return string(b)
}

// TestRollTestVerdictIsTheLastLineAfterTheRestore: a passing run ends with the PASS line, nothing is printed after it, the original
// gateway objects were applied last (the roll variants are gone) and the restore's rollout was awaited.
func TestRollTestVerdictIsTheLastLineAfterTheRestore(t *testing.T) {
	for _, bash := range bashes(t) {
		r := runRollTest(t, bash)
		if r.err != nil {
			t.Fatalf("%s: roll-test failed: %v\n%s", bash, r.err, diag(r))
		}
		if !strings.HasPrefix(lastLine(r), "roll-test: PASS") {
			t.Errorf("%s: the last line is %q, not the verdict\n%s", bash, lastLine(r), diag(r))
		}
		if strings.Contains(r.out, "restoring") {
			t.Errorf("%s: a successful restore must be silent", bash)
		}
		applied := strings.Fields(readFake(t, r, "applied.log"))
		if len(applied) < 5 || strings.HasPrefix(applied[len(applied)-1], "gateway-config-roll") {
			t.Errorf("%s: the last applied gateway ConfigMap is not the original: %v", bash, applied)
		}
		calls := strings.Split(strings.TrimSpace(readFake(t, r, "calls.log")), "\n")
		if !strings.Contains(calls[len(calls)-1], "rollout status") {
			t.Errorf("%s: the restore's rollout was not the last thing awaited: %q", bash, calls[len(calls)-1])
		}
		if !strings.Contains(r.out, "replaced 3 gateway pods") {
			t.Errorf("%s: no report of the replaced pods\n%s", bash, diag(r))
		}
	}
}

// TestRollTestFailedRestoreFailsEvenAfterAPass: the measurement passes but the original objects cannot be applied: the exit status
// is non-zero and the last line says so.
func TestRollTestFailedRestoreFailsEvenAfterAPass(t *testing.T) {
	r := runRollTest(t, "bash", "failrestore")
	if r.err == nil {
		t.Fatalf("a failed restore exited 0\n%s", diag(r))
	}
	if !strings.HasPrefix(lastLine(r), "roll-test: FAIL (the gateway configuration could not be restored") {
		t.Errorf("last line %q\n%s", lastLine(r), diag(r))
	}
}

// TestRollTestRolloutThatReplacesNothingCannotPass: pods keep their names across the apply, so the rollout replaced nothing; the
// command stops, fails, still restores, and its last line is a FAIL.
func TestRollTestRolloutThatReplacesNothingCannotPass(t *testing.T) {
	r := runRollTest(t, "bash", "static")
	if r.err == nil || strings.Contains(r.out, "roll-test: PASS") {
		t.Fatalf("a rollout that replaced no pod passed\n%s", diag(r))
	}
	if !strings.Contains(r.out, "did not replace every gateway pod") || !strings.HasPrefix(lastLine(r), "roll-test: FAIL") {
		t.Errorf("unexpected output\n%s", diag(r))
	}
	calls := strings.Split(strings.TrimSpace(readFake(t, r, "calls.log")), "\n")
	if !strings.Contains(calls[len(calls)-1], "rollout status") {
		t.Errorf("the gateway was not restored after the abort: %q", calls[len(calls)-1])
	}
}

// TestRollTestSignalsAreHandledAndTheRestoreIsProtected pins the signal handling in the script text: interrupts exit 130 and 143
// through the exit trap, and the cleanup ignores signals before it restores.
func TestRollTestSignalsAreHandledAndTheRestoreIsProtected(t *testing.T) {
	r := scriptRegion(t)
	for _, want := range []string{"trap roll_cleanup EXIT", "trap 'exit 130' INT", "trap 'exit 143' TERM", "trap '' INT TERM HUP"} {
		if !strings.Contains(r, want) {
			t.Errorf("the roll-test section lacks %q", want)
		}
	}
	c := r[strings.Index(r, "roll_cleanup() {"):]
	if strings.Index(c, "trap '' INT TERM HUP") > strings.Index(c, "roll_restore ||") || strings.Index(c, "roll_restore ||") > strings.Index(c, `printf '%s\n' "$ROLL_VERDICT"`) {
		t.Error("the cleanup must ignore signals, then restore, then print the verdict")
	}
	if strings.Contains(r[:strings.Index(r, "\nroll_cleanup() {")], "roll-test: PASS") {
		t.Error("only roll_cleanup may print the verdict: nothing before it prints PASS")
	}
}

// TestRollTestControlIsNotFooledByCapacityRejections: with the grace removed the fake gateway answers only capacity 503s, which
// are load and not the drain. The control must not pass: the command fails, says the test cannot see the outage, still restores
// the gateway, and the real run was not affected.
func TestRollTestControlIsNotFooledByCapacityRejections(t *testing.T) {
	r := runRollTestEnv(t, "bash", []string{"FAKE_OUTAGE=capacity", "ROLL_MAX_ROLLOUTS=3"})
	if r.err == nil || strings.Contains(r.out, "roll-test: PASS") {
		t.Fatalf("capacity rejections made the control pass\n%s", diag(r))
	}
	if !strings.HasPrefix(lastLine(r), "roll-test: FAIL (the negative control produced no drain-attributable failure") {
		t.Errorf("last line %q", lastLine(r))
	}
	if !strings.Contains(r.out, "capacity rejections (503 no_capacity)") || !strings.Contains(r.out, "the cap of 3 rollouts was reached") {
		t.Errorf("the capacity rejections or the cap are not reported\n%s", diag(r))
	}
	if !strings.Contains(r.out, "result: PASS") {
		t.Errorf("the real run should have passed before the control failed\n%s", diag(r))
	}
}

// TestRollTestKeepsRollingUntilEnoughRequests: the rollouts continue past two until 3000 requests were made during them, and the
// cap stops a run that cannot get there with a clear failure. The fake gateway is capped at about 50 requests a
// second and a rollout of the stub kubectl lasts for 100 served requests, so three rollouts make some hundreds of requests on
// any machine (a slower one makes fewer).
func TestRollTestKeepsRollingUntilEnoughRequests(t *testing.T) {
	r := runRollTestEnv(t, "bash", []string{"FAKE_SLOW=1", "FAKE_ROLL_REQUESTS=100", "ROLL_MAX_ROLLOUTS=3"})
	if r.err == nil {
		t.Fatalf("a run that could not reach 3000 requests passed\n%s", diag(r))
	}
	for _, want := range []string{"main: roll3 replaced 3 gateway pods", "the cap of 3 rollouts was reached", "at least 3000 are needed"} {
		if !strings.Contains(r.out, want) {
			t.Errorf("the output lacks %q\n%s", want, diag(r))
		}
	}
	if strings.Contains(r.out, "main: roll4") {
		t.Error("the cap was exceeded")
	}
	if !strings.HasPrefix(lastLine(r), "roll-test: FAIL") {
		t.Errorf("last line %q", lastLine(r))
	}
}

// outageKind reads the fake gateway's outage flag: "none" without the file, "abort" when it says so, and "capacity" for anything
// else, an empty or half-written flag included. An unreadable flag must never turn into a cut connection, which the control
// would count as the drain's absence: that was the flake of this test (a shell's `echo > file` truncates the file before it
// writes it, and the fake read the empty file on a request that arrived in between).
func outageKind(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, "outage"))
	if err != nil {
		return "none"
	}
	if strings.TrimSpace(string(b)) == "abort" {
		return "abort"
	}
	return "capacity"
}

func TestOutageFlagIsNeverReadAsAnAbortWhileItIsRewritten(t *testing.T) {
	dir := t.TempDir()
	if outageKind(dir) != "none" {
		t.Fatal("no file must mean no outage")
	}
	flag := filepath.Join(dir, "outage")
	for content, want := range map[string]string{"": "capacity", "cap": "capacity", "capacity\n": "capacity", "abort\n": "abort"} {
		if err := os.WriteFile(flag, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := outageKind(dir); got != want {
			t.Errorf("flag %q read as %q, want %q", content, got, want)
		}
	}
	// Rewritten the way the stub does it (temporary file, rename) while it is read all the time, a capacity flag is never read as
	// anything but capacity.
	if err := os.WriteFile(flag, []byte("capacity\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wrong atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if k := outageKind(dir); k != "capacity" {
				wrong.Add(1)
			}
		}
	}()
	for i := 0; i < 300; i++ {
		tmp := flag + ".tmp"
		if err := os.WriteFile(tmp, []byte("capacity\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, flag); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	<-done
	if wrong.Load() != 0 {
		t.Errorf("%d reads saw something other than capacity while the flag was replaced", wrong.Load())
	}
	// The stub replaces the flag atomically; pin the text so nobody turns it back into "echo > outage".
	if !strings.Contains(stubKubectl, `> "$D/outage.tmp" && mv "$D/outage.tmp" "$D/outage"`) || strings.Contains(stubKubectl, `> "$D/outage";`) {
		t.Error("the stub must replace the outage flag atomically")
	}
}

// tailOf returns the last n lines of s, so a failure message shows the end of a long run (the report of the control and the
// verdict) and not its beginning.
func tailOf(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return strings.Join(lines, "\n")
	}
	return fmt.Sprintf("[%d earlier lines left out]\n%s", len(lines)-n, strings.Join(lines[len(lines)-n:], "\n"))
}

// diag is what a failed assertion shows of a run: the exit status, the last 60 lines and, when ROLL_HARNESS_FULL is set, all of it.
func diag(r rollRun) string {
	if os.Getenv("ROLL_HARNESS_FULL") != "" {
		return fmt.Sprintf("exit: %v\n%s", r.err, r.out)
	}
	return fmt.Sprintf("exit: %v\n%s", r.err, tailOf(r.out, 60))
}
