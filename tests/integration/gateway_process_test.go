package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"serverflow/pkg/protocol"
)

func gatewayEnv(cpAddr, token, gwAddr, strategy string) []string {
	_, port, _ := strings.Cut(gwAddr, ":")
	return append(clusterEnv(cpAddr, token),
		"SERVERFLOW_GATEWAY_PORT="+port, "SERVERFLOW_GATEWAY_WORKER_SOURCE=registry",
		"SERVERFLOW_GATEWAY_CONTROL_PLANE_URL=http://"+cpAddr,
		"SERVERFLOW_GATEWAY_REGISTRY_REFRESH=100ms", "SERVERFLOW_GATEWAY_REGISTRY_MAX_STALENESS=1s",
		"SERVERFLOW_SCHEDULER_STRATEGY="+strategy)
}

func startGatewayProc(t *testing.T, cpAddr, token, gwAddr, strategy string) *proc {
	t.Helper()
	return startProc(t, "gateway", "gateway starting", gatewayEnv(cpAddr, token, gwAddr, strategy))
}

// completed reads how many requests a mock worker has finished.
func completed(t *testing.T, mockAddr string) int64 {
	t.Helper()
	resp, err := http.Get("http://" + mockAddr + "/stats")
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	var s struct{ Completed int64 }
	b, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(b, &s)
	return s.Completed
}

func gatewayChat(gwAddr string) (int, string) {
	resp, err := http.Post("http://"+gwAddr+"/v1/chat/completions", "application/json", strings.NewReader(chat(false, "hello")))
	if err != nil {
		return -1, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestProcessGatewaySchedulesAcrossWorkersAndSurvivesDeathAndOutage(t *testing.T) {
	cpAddr := freePort(t)
	cp := startControlPlaneProc(t, cpAddr, clusterToken)
	m1, m2 := freePort(t), freePort(t)
	mock1 := startMockProc(t, m1)
	startMockProc(t, m2)
	startAgentProc(t, cpAddr, clusterToken, "w1", m1)
	startAgentProc(t, cpAddr, clusterToken, "w2", m2)
	gwAddr := freePort(t)
	startGatewayProc(t, cpAddr, clusterToken, gwAddr, "round-robin")
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "both eligible", func(ws map[string]protocol.WorkerSnapshot) bool {
		return ws["w1"].Eligible && ws["w2"].Eligible
	})
	waitFor(t, 10*time.Second, "the gateway routes to both workers", func() bool {
		a0, b0 := completed(t, m1), completed(t, m2)
		for i := 0; i < 4; i++ {
			gatewayChat(gwAddr)
		}
		return completed(t, m1) > a0 && completed(t, m2) > b0
	})

	// Distribution: round-robin over two workers splits an even number of requests evenly.
	a0, b0 := completed(t, m1), completed(t, m2)
	for i := 0; i < 20; i++ {
		if code, body := gatewayChat(gwAddr); code != 200 {
			t.Fatalf("request %d: %d %s", i, code, body)
		}
	}
	if da, db := completed(t, m1)-a0, completed(t, m2)-b0; da != 10 || db != 10 {
		t.Fatalf("round-robin should split 20 requests 10/10, got %d/%d", da, db)
	}

	// A killed backend stops receiving traffic.
	mock1.kill9()
	waitWorkers(t, cpAddr, clusterToken, 5*time.Second, "w1 not eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return !ws["w1"].Eligible })
	time.Sleep(400 * time.Millisecond) // the gateway's next refreshes
	for i := 0; i < 20; i++ {
		if code, body := gatewayChat(gwAddr); code != 200 {
			t.Fatalf("w2 is healthy but request %d got %d %s", i, code, body)
		}
	}

	// The control plane dies. The gateway keeps trusting its view only briefly, then fails closed.
	b1 := completed(t, m2)
	cp.kill9()
	waitFor(t, 5*time.Second, "the gateway fails closed", func() bool {
		code, _ := gatewayChat(gwAddr)
		return code == 503
	})
	frozen := completed(t, m2)
	for i := 0; i < 5; i++ {
		if code, body := gatewayChat(gwAddr); code != 503 {
			t.Fatalf("closed means closed, got %d %s", code, body)
		}
	}
	if completed(t, m2) != frozen || frozen < b1 {
		t.Fatal("a request reached a worker while failing closed")
	}

	// And it recovers when the control plane returns and the agents re-register.
	startControlPlaneProc(t, cpAddr, clusterToken)
	waitFor(t, 15*time.Second, "traffic resumes", func() bool {
		code, _ := gatewayChat(gwAddr)
		return code == 200
	})
}

func TestProcessGatewayConfigurationErrorsFailFast(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{"least-work is not implemented", []string{"SERVERFLOW_GATEWAY_WORKER_SOURCE=registry", "SERVERFLOW_SCHEDULER_STRATEGY=least-work"}, "not implemented yet"},
		{"unknown source", []string{"SERVERFLOW_GATEWAY_WORKER_SOURCE=magic"}, "worker_source"},
		{"cleartext token to another host", []string{"SERVERFLOW_GATEWAY_WORKER_SOURCE=registry", "SERVERFLOW_GATEWAY_CONTROL_PLANE_URL=http://10.0.0.5:9090", "SERVERFLOW_CONTROL_PLANE_TOKEN=" + clusterToken}, "cleartext"},
		{"bad worker network", []string{"SERVERFLOW_GATEWAY_WORKER_SOURCE=registry", "SERVERFLOW_GATEWAY_WORKER_NETWORKS=nonsense"}, "not a CIDR"},
		{"staleness too small", []string{"SERVERFLOW_GATEWAY_WORKER_SOURCE=registry", "SERVERFLOW_GATEWAY_REGISTRY_REFRESH=1s", "SERVERFLOW_GATEWAY_REGISTRY_MAX_STALENESS=1s"}, "at least twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := runBin(t, "gateway", tt.env)
			if code != 1 || !strings.Contains(out, tt.want) {
				t.Fatalf("exit %d, output %q; want exit 1 mentioning %q", code, out, tt.want)
			}
			if strings.Contains(out, clusterToken) {
				t.Fatalf("the token leaked: %s", out)
			}
		})
	}
}

func TestProcessGatewayStaticModeIsUnchanged(t *testing.T) {
	m := freePort(t)
	startMockProc(t, m)
	gwAddr := freePort(t)
	_, port, _ := strings.Cut(gwAddr, ":")
	startProc(t, "gateway", "gateway starting", []string{"SERVERFLOW_GATEWAY_PORT=" + port,
		"SERVERFLOW_GATEWAY_UPSTREAM_URL=http://" + m, "SERVERFLOW_GATEWAY_MODELS=" + model})
	waitFor(t, 10*time.Second, "static gateway answers", func() bool { code, _ := gatewayChat(gwAddr); return code == 200 })
}

func TestProcessGatewayWithTheWrongTokenStaysUpAndFailsClosed(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	m := freePort(t)
	startMockProc(t, m)
	startAgentProc(t, cpAddr, clusterToken, "w1", m)
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "w1 eligible", func(ws map[string]protocol.WorkerSnapshot) bool { return ws["w1"].Eligible })

	gwAddr := freePort(t)
	gw := startGatewayProc(t, cpAddr, "a-different-but-long-enough-token", gwAddr, "round-robin")
	waitFor(t, 5*time.Second, "the gateway reports the rejected token", func() bool {
		return strings.Contains(gw.stderr.String(), "could not refresh the worker registry")
	})
	for i := 0; i < 5; i++ {
		code, body := gatewayChat(gwAddr)
		if code != 503 || !strings.Contains(body, "WORKER_UNAVAILABLE") {
			t.Fatalf("with no usable registry the gateway must fail closed: %d %s", code, body)
		}
	}
	if completed(t, m) != 0 {
		t.Fatal("a request reached a worker without a trusted registry view")
	}
	if strings.Contains(gw.stderr.String(), clusterToken) || strings.Contains(gw.stderr.String(), "a-different-but-long-enough-token") {
		t.Fatal("tokens must never be logged")
	}
}

func TestProcessGatewayStopsUsingAWorkerWhoseAgentIsKilled(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	m1, m2 := freePort(t), freePort(t)
	startMockProc(t, m1)
	startMockProc(t, m2)
	startAgentProc(t, cpAddr, clusterToken, "w1", m1)
	agent2 := startAgentProc(t, cpAddr, clusterToken, "w2", m2)
	gwAddr := freePort(t)
	startGatewayProc(t, cpAddr, clusterToken, gwAddr, "round-robin")
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "both eligible", func(ws map[string]protocol.WorkerSnapshot) bool {
		return ws["w1"].Eligible && ws["w2"].Eligible
	})
	waitFor(t, 10*time.Second, "both receive traffic", func() bool {
		a, b := completed(t, m1), completed(t, m2)
		for i := 0; i < 4; i++ {
			gatewayChat(gwAddr)
		}
		return completed(t, m1) > a && completed(t, m2) > b
	})

	agent2.kill9() // the worker is fine, but nobody vouches for it any more
	waitFor(t, 5*time.Second, "w2 stops receiving traffic", func() bool {
		before := completed(t, m2)
		for i := 0; i < 4; i++ {
			gatewayChat(gwAddr)
		}
		return completed(t, m2) == before
	})
	for i := 0; i < 10; i++ {
		if code, body := gatewayChat(gwAddr); code != 200 {
			t.Fatalf("w1 is healthy but request %d got %d %s", i, code, body)
		}
	}
}

func gatewayPost(t *testing.T, gwAddr string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post("http://"+gwAddr+"/v1/chat/completions", "application/json", strings.NewReader(chat(false, "hello")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// With two attempts, a request only has a guarantee while at most one worker is bad, so each scenario
// below has exactly one.

func TestProcessGatewayRetriesAroundAFlakyWorker(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	flakyAddr, good1, good2 := freePort(t), freePort(t), freePort(t)
	startMockProc(t, flakyAddr, "--failure-rate=1", "--failure-mode=unavailable")
	startMockProc(t, good1)
	startMockProc(t, good2)
	startAgentProc(t, cpAddr, clusterToken, "w1", flakyAddr)
	startAgentProc(t, cpAddr, clusterToken, "w2", good1)
	startAgentProc(t, cpAddr, clusterToken, "w3", good2)
	gwAddr := freePort(t)
	gw := startGatewayProc(t, cpAddr, clusterToken, gwAddr, "round-robin")
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "all eligible", func(ws map[string]protocol.WorkerSnapshot) bool {
		return ws["w1"].Eligible && ws["w2"].Eligible && ws["w3"].Eligible
	})
	waitFor(t, 10*time.Second, "the gateway sees all three", func() bool {
		a, b := completed(t, good1), completed(t, good2)
		for i := 0; i < 6; i++ {
			gatewayChat(gwAddr)
		}
		return completed(t, good1) > a && completed(t, good2) > b
	})

	var retried int
	for i := 0; i < 90; i++ {
		resp, body := gatewayPost(t, gwAddr)
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: %d %s", i, resp.StatusCode, body)
		}
		if resp.Header.Get("X-ServerFlow-Attempts") == "2" {
			retried++
		}
	}
	// A retry also takes a turn in the rotation, so the worker after the flaky one is picked twice as often and
	// the flaky one is first choice for up to half of the requests (docs/decisions/ADR-012).
	if retried < 25 || retried > 55 {
		t.Fatalf("expected roughly a third to a half of the 90 requests to hit the flaky worker first: %d were retried", retried)
	}
	if !strings.Contains(gw.stderr.String(), `"outcome":"retried"`) || !strings.Contains(gw.stderr.String(), `"class":"status_503"`) {
		t.Fatal("the retried attempts must be on record in the gateway log")
	}
	if strings.Contains(gw.stderr.String(), clusterToken) {
		t.Fatal("the token leaked into the log")
	}
}

func TestProcessGatewayRetriesAroundABackendKilledMidRun(t *testing.T) {
	cpAddr := freePort(t)
	startControlPlaneProc(t, cpAddr, clusterToken)
	a1, a2, a3 := freePort(t), freePort(t), freePort(t)
	startMockProc(t, a1)
	victim := startMockProc(t, a2)
	startMockProc(t, a3)
	startAgentProc(t, cpAddr, clusterToken, "w1", a1)
	startAgentProc(t, cpAddr, clusterToken, "w2", a2)
	startAgentProc(t, cpAddr, clusterToken, "w3", a3)
	gwAddr := freePort(t)
	gw := startGatewayProc(t, cpAddr, clusterToken, gwAddr, "round-robin")
	waitWorkers(t, cpAddr, clusterToken, 10*time.Second, "all eligible", func(ws map[string]protocol.WorkerSnapshot) bool {
		return ws["w1"].Eligible && ws["w2"].Eligible && ws["w3"].Eligible
	})
	waitFor(t, 10*time.Second, "all three receive traffic", func() bool {
		c1, c2, c3 := completed(t, a1), completed(t, a2), completed(t, a3)
		for i := 0; i < 6; i++ {
			gatewayChat(gwAddr)
		}
		return completed(t, a1) > c1 && completed(t, a2) > c2 && completed(t, a3) > c3
	})

	victim.kill9()
	for i := 0; i < 90; i++ {
		if resp, body := gatewayPost(t, gwAddr); resp.StatusCode != 200 {
			t.Fatalf("request %d after the kill: %d %s", i, resp.StatusCode, body)
		}
	}
	if !strings.Contains(gw.stderr.String(), `"class":"connect"`) {
		t.Fatal("the connection failures that were retried must be on record")
	}
}
