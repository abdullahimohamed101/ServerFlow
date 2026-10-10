package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// scrape returns what a Prometheus scrape of the server's metrics handler would see. /metrics is no longer on
// the data listener (ADR-017), so tests read it from MetricsHandler instead of Handler.
func scrape(t *testing.T, s *Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler: %d", rec.Code)
	}
	return rec.Body.String()
}

// TestMetricsAreNotOnTheDataListener pins the shipped-surface change: the data mux must not serve /metrics,
// with or without authentication, in static or registry mode.
func TestMetricsAreNotOnTheDataListener(t *testing.T) {
	static := newEnv(t, http.HandlerFunc(okUpstream))
	reg := newRegistryAuthEnv(t)
	for name, get := range map[string]func() (int, string){
		"static":         func() (int, string) { r, b := static.get(t, "/metrics"); return r.StatusCode, b },
		"registry+auth":  func() (int, string) { r, b := reg.do(t, http.MethodGet, "/metrics", ""); return r.StatusCode, b },
		"static handler": func() (int, string) { return recorded(static.gw.Handler(), "/metrics") },
	} {
		code, body := get()
		if code != http.StatusNotFound || strings.Contains(body, "inference_requests") || strings.Contains(body, "go_goroutines") {
			t.Errorf("%s: GET /metrics on the data listener answered %d", name, code)
		}
	}
	if code, _ := recorded(static.gw.MetricsHandler(), "/metrics"); code != http.StatusOK {
		t.Errorf("MetricsHandler: %d", code)
	}
}

func recorded(h http.Handler, path string) (int, string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}
