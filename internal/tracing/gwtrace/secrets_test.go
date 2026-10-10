package gwtrace_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"serverflow/internal/tracing/tracingtest"
)

// Canaries are planted in every place a client or worker controls. None may appear in any span.
const (
	cPrompt  = "canary-prompt-7f3a"
	cAuth    = "sk-canary-apikey-91bc"
	cAgent   = "canary-user-agent-55d1"
	cXFF     = "203.0.113.77-canary"
	cQuery   = "canary-query-3e90"
	cState   = "canary-tracestate-8812"
	cBody    = "canary-upstream-body-6c4e"
	cCookie  = "canary-cookie-12ab"
	cBaggage = "canary-baggage-99ff"
)

func TestNoSecretsInSpansAndOnlyAllowListedKeys(t *testing.T) {
	for _, mode := range []string{"link", "ignore", "trust"} {
		t.Run(mode, func(t *testing.T) {
			e := newEnv(t, envOpts{incoming: mode, upstream: func(s *seen) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					s.add(r.Header)
					if strings.Contains(r.URL.RawQuery, "fail") || strings.Contains(r.Header.Get("X-Fail"), "1") {
						http.Error(w, `{"error":{"message":"`+cBody+` at 10.9.8.7:8000"}}`, http.StatusInternalServerError)
						return
					}
					sseWorker(s).ServeHTTP(w, r)
				})
			}})
			body := `{"model":"qwen-7b","stream":true,"messages":[{"role":"user","content":"` + cPrompt + `"}]}`
			hdr := []string{"Authorization", "Bearer " + cAuth, "User-Agent", cAgent, "X-Forwarded-For", cXFF, "Cookie", "s=" + cCookie,
				"Tracestate", "v=" + cState, "Baggage", "k=" + cBaggage, "Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
			e.post(body, hdr...)
			// a request the worker fails with a body that carries an address and a canary
			req, _ := http.NewRequest(http.MethodPost, e.url+"/v1/chat/completions?token="+cQuery, strings.NewReader(body))
			for i := 0; i+1 < len(hdr); i += 2 {
				req.Header.Set(hdr[i], hdr[i+1])
			}
			req.Header.Set("X-Fail", "1")
			resp, err := e.client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			// and one refused for an unknown model whose name is a canary
			e.post(`{"model":"`+cPrompt+`-model","messages":[{"role":"user","content":"x"}]}`, hdr...)

			spans := e.spans()
			if len(spans) < 6 {
				t.Fatalf("expected several spans, got %v", names(spans))
			}
			tracingtest.RequireNoCanary(t, spans, cPrompt, cAuth, cAgent, cXFF, cQuery, cBody, cCookie, cBaggage, "10.9.8.7", "Bearer", "127.0.0.1")
			if mode != "trust" {
				tracingtest.RequireNoCanary(t, spans, cState)
			}
			tracingtest.RequireAllowListed(t, spans)
		})
	}
}
