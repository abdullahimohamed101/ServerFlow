package integration

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"serverflow/internal/redis/redistest"
)

// Process-level tests of rate limiting: the real gateway binary against the real (password-protected) test Redis.

func TestProcessRateLimitingEndToEnd(t *testing.T) {
	addr, password := redistest.Addr(t)
	capped := redistest.Unique("model") // the model cap is global per name; a fresh name keeps runs independent
	mockAddr := freePort(t)
	startMockProc(t, mockAddr, "--model="+capped)
	gwAddr := freePort(t)
	_, port, _ := strings.Cut(gwAddr, ":")
	gw := startProc(t, "gateway", "gateway starting", []string{
		"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + capped, "SERVERFLOW_GATEWAY_UPSTREAM_URL=http://" + mockAddr,
		"SERVERFLOW_RATE_LIMIT_MODE=required", "SERVERFLOW_RATE_LIMIT_MODEL_REQUESTS_PER_MINUTE=" + capped + "=3",
		"SERVERFLOW_REDIS_ADDRESS=" + addr, "SERVERFLOW_REDIS_PASSWORD=" + password, "SERVERFLOW_REDIS_REQUEST_METADATA=true",
	})
	waitFor(t, 10*time.Second, "mock worker ready", func() bool {
		r, _ := authGet(t, gwAddr, "/readyz", "")
		return r.StatusCode == 200
	})
	body := `{"model":"` + capped + `","messages":[{"role":"user","content":"hi"}]}`
	var codes []int
	var last *http.Response
	var lastBody string
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest(http.MethodPost, "http://"+gwAddr+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		last, lastBody = doReq(t, req)
		codes = append(codes, last.StatusCode)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 200 || codes[3] != 429 || codes[4] != 429 {
		t.Fatalf("a model cap of 3: %v", codes)
	}
	if last.Header.Get("Retry-After") == "" || !strings.Contains(lastBody, "RATE_LIMITED") {
		t.Fatalf("%v %s", last.Header, lastBody)
	}
	_, metrics := authGet(t, gwAddr, "/metrics", "")
	if !strings.Contains(metrics, `rate_limit_rejections_total{limit="model"} 2`) || !strings.Contains(metrics, "rate_limit_decision_seconds_count") {
		t.Fatalf("metrics:\n%s", grepLines(metrics, "rate_limit"))
	}
	logs := gw.stderr.String()
	if strings.Contains(logs, password) {
		t.Fatal("the Redis password reached the gateway log")
	}
	if !strings.Contains(logs, "rate limiting required") || !strings.Contains(logs, `"limit":"model"`) {
		t.Fatalf("log:\n%s", logs)
	}
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func TestProcessRateLimitingOffNeverTouchesRedis(t *testing.T) {
	mockAddr := freePort(t)
	startMockProc(t, mockAddr)
	gwAddr := freePort(t)
	_, port, _ := strings.Cut(gwAddr, ":")
	// The address points nowhere and carries a password; with rate limiting off it must not matter.
	startProc(t, "gateway", "gateway starting", []string{"SERVERFLOW_GATEWAY_PORT=" + port, "SERVERFLOW_GATEWAY_MODELS=" + model,
		"SERVERFLOW_GATEWAY_UPSTREAM_URL=http://" + mockAddr, "SERVERFLOW_REDIS_ADDRESS=127.0.0.1:1", "SERVERFLOW_REDIS_PASSWORD=never-used-pw"})
	waitFor(t, 10*time.Second, "mock worker ready", func() bool {
		r, _ := authGet(t, gwAddr, "/readyz", "")
		return r.StatusCode == 200
	})
	for i := 0; i < 10; i++ {
		if r, b := authChat(t, gwAddr, ""); r.StatusCode != 200 {
			t.Fatalf("%d %s", r.StatusCode, b)
		}
	}
}

func TestProcessRateLimitingRequiredFailsFast(t *testing.T) {
	addr, _ := redistest.Addr(t)
	const secret = "a-very-secret-redis-password"
	base := func(extra ...string) []string {
		return append([]string{"SERVERFLOW_GATEWAY_PORT=" + freePortNumber(t), "SERVERFLOW_RATE_LIMIT_MODE=required",
			"SERVERFLOW_RATE_LIMIT_MODEL_REQUESTS_PER_MINUTE=qwen-7b=5"}, extra...)
	}
	for name, tc := range map[string]struct {
		env  []string
		want string
	}{
		"redis unreachable":  {base("SERVERFLOW_REDIS_ADDRESS=127.0.0.1:1", "SERVERFLOW_REDIS_PASSWORD="+secret), "Redis is not usable"},
		"wrong password":     {base("SERVERFLOW_REDIS_ADDRESS="+addr, "SERVERFLOW_REDIS_PASSWORD="+secret), "Redis is not usable"},
		"no password":        {base("SERVERFLOW_REDIS_ADDRESS=" + addr), "Redis is not usable"},
		"insecure remote":    {base("SERVERFLOW_REDIS_ADDRESS=redis.example.com:6379", "SERVERFLOW_REDIS_PASSWORD="+secret), "TLS"},
		"remote without pw":  {base("SERVERFLOW_REDIS_ADDRESS=redis.example.com:6379", "SERVERFLOW_REDIS_TLS=true"), "password"},
		"nothing to enforce": {[]string{"SERVERFLOW_GATEWAY_PORT=" + freePortNumber(t), "SERVERFLOW_RATE_LIMIT_MODE=required"}, "nothing to enforce"},
		"typo in mode":       {[]string{"SERVERFLOW_GATEWAY_PORT=" + freePortNumber(t), "SERVERFLOW_RATE_LIMIT_MODE=requierd"}, "rate_limit.mode"},
		"typo in on_failure": {base("SERVERFLOW_REDIS_ON_FAILURE=ajar"), "redis.on_failure"},
		"bad address":        {base("SERVERFLOW_REDIS_ADDRESS=http://u:" + secret + "@h:1"), "redis.address"},
	} {
		t.Run(name, func(t *testing.T) {
			code, out := runBin(t, "gateway", tc.env)
			if code != 1 || !strings.Contains(out, tc.want) {
				t.Fatalf("exit %d, want 1 mentioning %q: %s", code, tc.want, out)
			}
			if strings.Contains(out, secret) || strings.Contains(out, "example.com") {
				t.Fatalf("output leaks configuration: %s", out)
			}
		})
	}
	_ = io.Discard
}
