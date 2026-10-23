package gateway

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Wider differential coverage (Phase 14 PR A fix round): request bodies at and over the size cap, max_tokens
// extremes, a smaller gateway.max_tokens_limit, and API-key authentication on and off. Its fixtures were recorded
// from UNMODIFIED master production code (a git archive of 32b20d4 plus this test), in their own commit.

func wideScript() []diffRequest {
	const cap = 1 << 20
	head := `{"model":"qwen-7b","messages":[{"role":"user","content":"`
	tail := `"}]}`
	atCap := head + strings.Repeat("a", cap-len(head)-len(tail)) + tail
	over := head + strings.Repeat("a", cap-len(head)-len(tail)+1) + tail
	mt := func(v string) diffRequest {
		return chatReq("max-tokens-"+v, `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}],"max_tokens":`+v+`}`)
	}
	huge, _ := json.Marshal(math.MaxInt64)
	return []diffRequest{
		chatReq("body-at-cap", atCap),
		chatReq("body-over-cap", over),
		mt("0"), mt("-1"), mt("1"), mt("256"), mt("257"), mt("4096"), mt("4097"),
		mt(string(huge)), mt(strconv.FormatInt(1<<32, 10)), mt("1.5"), mt(`"12"`), mt("null"),
		chatReq("max-completion-tokens-0", `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":0}`),
		chatReq("both-token-fields", `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"max_completion_tokens":300}`),
		chatReq("plain", `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`),
		{name: "noauth-plain", method: http.MethodPost, path: chatCompletionsPath, body: `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`, bare: true},
		{name: "badkey-plain", method: http.MethodPost, path: chatCompletionsPath, body: `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]}`, bare: true,
			hdr: map[string]string{"Authorization": "Bearer sk-not-a-real-key"}},
		{name: "get-models", method: http.MethodGet, path: "/v1/models"},
	}
}

// withLimit changes gateway.max_tokens_limit after construction (the handler reads it per request).
func withLimit(v diffVariant, limit int) diffVariant {
	base := v.build
	v.build = func(t *testing.T, hits *hitLog, lim *fakeLimiter) (*Server, *lockedBuffer) {
		gw, logs := base(t, hits, lim)
		gw.cfg.MaxTokensLimit = limit
		return gw, logs
	}
	return v
}

// withAuth requires an API key; the key is random per run, so it is only ever sent, never recorded.
func withAuth(v diffVariant) diffVariant {
	base := v.build
	key := ""
	v.build = func(t *testing.T, hits *hitLog, lim *fakeLimiter) (*Server, *lockedBuffer) {
		gw, logs := base(t, hits, lim)
		store := newAuthStore()
		gw.SetAuthenticator(newAuthenticator(store, logs, &testClock{t: time.Now()}))
		key = store.add("acme", nil, quotas(100, 1_000_000, 10))
		return gw, logs
	}
	v.headers = func() map[string]string { return map[string]string{"Authorization": "Bearer " + key} }
	return v
}

func wideVariants() []diffVariant {
	rename := func(v diffVariant, name string) diffVariant { v.name = name; v.script = wideScript; return v }
	return []diffVariant{
		rename(staticVariant("x", false), "wide-static"),
		rename(withLimit(staticVariant("x", true), 256), "wide-static-limiter-limit256"),
		rename(withLimit(registryVariant("x", "round-robin", []int{0, 0}, true), 256), "wide-registry-limiter-limit256"),
		rename(withAuth(registryVariant("x", "least-active", []int{1, 0}, true)), "wide-registry-auth-limiter"),
		rename(withAuth(staticVariant("x", false)), "wide-static-auth"),
	}
}
