package metricsserver

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/internal/config"
)

const tok = "scrape-token-canary-0123456789"

func registry() *prometheus.Registry {
	r := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "things_total", Help: "x"})
	r.MustRegister(c)
	c.Inc()
	return r
}

func get(t *testing.T, h http.Handler, method, path string, hdr ...string) (int, string) {
	t.Helper()
	srv := newTestServer(h)
	defer srv.Close()
	req, _ := http.NewRequest(method, srv.URL+path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHandlerWithoutTokenServesOnlyMetrics(t *testing.T) {
	h := Handler(registry(), "")
	if code, body := get(t, h, http.MethodGet, "/metrics"); code != 200 || !strings.Contains(body, "things_total 1") {
		t.Fatalf("%d %s", code, body)
	}
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/"}, {http.MethodGet, "/healthz"}, {http.MethodGet, "/metrics/"}, {http.MethodGet, "/v1/chat/completions"},
		{http.MethodPost, "/metrics"}, {http.MethodDelete, "/metrics"},
	} {
		if code, _ := get(t, h, c.method, c.path); code == 200 {
			t.Errorf("%s %s answered 200", c.method, c.path)
		}
	}
}

func TestHandlerTokenIsRequiredAndCheckedExactly(t *testing.T) {
	h := Handler(registry(), tok)
	if code, _ := get(t, h, http.MethodGet, "/metrics"); code != 401 {
		t.Fatalf("no token: %d", code)
	}
	for _, v := range []string{"Bearer wrong", "Bearer " + tok + "x", "Bearer " + tok[:len(tok)-1], "Bearer ", "Bearer", tok, "Basic " + tok, "Token " + tok} {
		if code, body := get(t, h, http.MethodGet, "/metrics", "Authorization", v); code != 401 || strings.Contains(body, "things_total") {
			t.Errorf("%q: %d", v, code)
		}
	}
	if code, body := get(t, h, http.MethodGet, "/metrics", "Authorization", "Bearer "+tok); code != 200 || !strings.Contains(body, "things_total 1") {
		t.Fatalf("right token: %d", code)
	}
	if code, _ := get(t, h, http.MethodGet, "/metrics", "Authorization", "bearer "+tok); code != 200 {
		t.Fatalf("the scheme is case-insensitive: %d", code)
	}
	code, body := get(t, h, http.MethodGet, "/metrics", "Authorization", "Bearer nope")
	if code != 401 || strings.Contains(body, tok) {
		t.Fatalf("the refusal must not echo anything: %q", body)
	}
}

func TestListenRefusesExposedAddressWithoutToken(t *testing.T) {
	var cfg config.MetricsConfig
	_, err := Listen("0.0.0.0:0", cfg, registry(), slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "metrics.token") {
		t.Fatalf("want a clear refusal, got %v", err)
	}
	cfg.Token = tok
	s, err := Listen("0.0.0.0:0", cfg, registry(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	_ = s.ln.Close()
	if strings.Contains(func() string {
		_, e := Listen("bad", config.MetricsConfig{Token: tok}, registry(), nil)
		return e.Error()
	}(), tok) {
		t.Fatal("errors must not contain the token")
	}
}

func TestListenOffReturnsNothing(t *testing.T) {
	s, err := Listen("", config.MetricsConfig{}, registry(), nil)
	if s != nil || err != nil {
		t.Fatalf("%v %v", s, err)
	}
}

func TestServeOnLoopbackAndShutdown(t *testing.T) {
	s, err := Listen("127.0.0.1:0", config.MetricsConfig{}, registry(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no clean shutdown")
	}
}
