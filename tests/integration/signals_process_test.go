package integration

import (
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// startSlowGateway starts a gateway in front of a mock worker whose responses take about 8*1/tps seconds, with n
// streaming requests in flight, and returns the process and the gateway's metrics address.
func startSlowGateway(t *testing.T, tps string, shutdown string, n int) *proc {
	t.Helper()
	mockAddr := freePort(t)
	startMockProc(t, mockAddr, "--tokens-per-second="+tps, "--output-tokens=8")
	gwAddr := freePort(t)
	_, port, _ := strings.Cut(gwAddr, ":")
	gw := startProc(t, "gateway", "gateway starting", []string{
		"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + model, "SERVERFLOW_GATEWAY_UPSTREAM_URL=http://" + mockAddr,
		"SERVERFLOW_GATEWAY_SHUTDOWN_TIMEOUT=" + shutdown,
	})
	waitFor(t, 10*time.Second, "the gateway to be ready", func() bool {
		r, _ := authGet(t, gwAddr, "/readyz", "")
		return r.StatusCode == 200
	})
	for i := 0; i < n; i++ {
		go func() {
			req, _ := http.NewRequest(http.MethodPost, "http://"+gwAddr+"/v1/chat/completions", strings.NewReader(chat(true, "hi")))
			req.Header.Set("Content-Type", "application/json")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				buf := make([]byte, 4096)
				for {
					if _, err := resp.Body.Read(buf); err != nil {
						break
					}
				}
				_ = resp.Body.Close()
			}
		}()
	}
	mAddr := strings.TrimPrefix(gw.metricsURL(t), "http://")
	waitFor(t, 5*time.Second, "the requests to be in flight", func() bool {
		_, body := authGet(t, mAddr, "/metrics", "")
		return strings.Contains(body, "inference_requests_active 3\n")
	})
	return gw
}

// A second signal ends the gateway at once, as it does for the other binaries, so an operator is never stuck behind
// a drain that will not finish.
func TestProcessGatewaySecondSignalExitsPromptly(t *testing.T) {
	gw := startSlowGateway(t, "1", "60s", 3) // each response takes about 8 s; the drain would wait up to 60 s
	if err := gw.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	select {
	case err := <-gw.exited:
		t.Fatalf("the gateway exited during a drain it should have waited for: %v", err)
	default:
	}
	start := time.Now()
	if err := gw.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := gw.wait(t, 4*time.Second)
	if err == nil {
		t.Fatal("a forced exit must be non-zero")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("the second signal took %v to end the gateway", d)
	}
}

// A drain that never finishes still ends at the configured shutdown timeout.
func TestProcessGatewayHungDrainEndsAtTheShutdownTimeout(t *testing.T) {
	gw := startSlowGateway(t, "0.5", "2s", 3) // each response would take about 16 s
	start := time.Now()
	if err := gw.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := gw.wait(t, 8*time.Second)
	d := time.Since(start)
	if err == nil {
		t.Fatalf("a drain that timed out must exit non-zero\n%s", gw.stderr.String())
	}
	if d < 1500*time.Millisecond || d > 6*time.Second {
		t.Fatalf("the gateway left after %v, want about the 2 s shutdown timeout", d)
	}
}
