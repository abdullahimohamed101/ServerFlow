package k8s

import (
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
  *"rollout status"*) sleep "${FAKE_ROLL_SLEEP:-3}" ;;
  *"apply -f -"*)
    cat > "$D/applied.yaml"
    grep -o 'gateway-config-[a-z0-9]*' "$D/applied.yaml" | head -1 >> "$D/applied.log"
    if [ -f "$D/failrestore" ] && ! grep -q 'gateway-config-roll' "$D/applied.yaml"; then exit 1; fi
    if [ ! -f "$D/static" ]; then n=$(cat "$D/gen" 2>/dev/null || echo 0); echo $((n + 1)) > "$D/gen"; fi
    if grep -q 'RESILIENCE_MODE: "off"' "$D/applied.yaml"; then : > "$D/outage"; else rm -f "$D/outage"; fi
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

	var n atomic.Int64
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
		case "/v1/models":
			_, _ = io.WriteString(w, `{"data":[{"id":"mock-model"}]}`)
		default:
			n.Add(1)
			time.Sleep(20 * time.Millisecond) // about 1000 requests a second: fast enough for 3000 in a rollout, slow enough not to exhaust ports
			if _, err := os.Stat(filepath.Join(fake, "outage")); err == nil {
				w.WriteHeader(503)
				_, _ = io.WriteString(w, `{"error":{"code":"no_available_workers"}}`)
				return
			}
			b, _ := io.ReadAll(r.Body)
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
		"FAKE_VERSION="+pins(t)["KUBERNETES_VERSION"], "K8S_UP_TEST_GATEWAY_PORT="+u.Port(), "ROLL_PHASE_SECONDS=1", "K8S_UP_WAIT=5")
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
			t.Fatalf("%s: roll-test failed: %v\n%s", bash, r.err, r.out)
		}
		if !strings.HasPrefix(lastLine(r), "roll-test: PASS") {
			t.Errorf("%s: the last line is %q, not the verdict\n%s", bash, lastLine(r), r.out)
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
			t.Errorf("%s: no report of the replaced pods\n%s", bash, r.out)
		}
	}
}

// TestRollTestFailedRestoreFailsEvenAfterAPass: the measurement passes but the original objects cannot be applied: the exit status
// is non-zero and the last line says so.
func TestRollTestFailedRestoreFailsEvenAfterAPass(t *testing.T) {
	r := runRollTest(t, "bash", "failrestore")
	if r.err == nil {
		t.Fatalf("a failed restore exited 0\n%s", r.out)
	}
	if !strings.HasPrefix(lastLine(r), "roll-test: FAIL (the gateway configuration could not be restored") {
		t.Errorf("last line %q\n%s", lastLine(r), r.out)
	}
}

// TestRollTestRolloutThatReplacesNothingCannotPass: pods keep their names across the apply, so the rollout replaced nothing; the
// command stops, fails, still restores, and its last line is a FAIL.
func TestRollTestRolloutThatReplacesNothingCannotPass(t *testing.T) {
	r := runRollTest(t, "bash", "static")
	if r.err == nil || strings.Contains(r.out, "roll-test: PASS") {
		t.Fatalf("a rollout that replaced no pod passed\n%s", r.out)
	}
	if !strings.Contains(r.out, "did not replace every gateway pod") || !strings.HasPrefix(lastLine(r), "roll-test: FAIL") {
		t.Errorf("unexpected output\n%s", r.out)
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
