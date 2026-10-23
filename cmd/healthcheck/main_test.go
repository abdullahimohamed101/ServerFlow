package main

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"serverflow/internal/testutil"
)

func probe(t *testing.T, to time.Duration, args ...string) (int, string) {
	t.Helper()
	var buf bytes.Buffer
	code := run(args, to, &buf)
	return code, buf.String()
}

func TestHealthy2xx(t *testing.T) {
	for _, status := range []int{200, 204} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		code, out := probe(t, time.Second, srv.URL+"/healthz")
		srv.Close()
		if code != 0 {
			t.Errorf("status %d: exit %d (%s)", status, code, out)
		}
	}
}

func TestNon2xxFails(t *testing.T) {
	for _, status := range []int{300, 404, 500, 503} { // 300 is covered by the redirect test too
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		code, _ := probe(t, time.Second, srv.URL)
		srv.Close()
		if code != 1 {
			t.Errorf("status %d: exit %d, want 1", status, code)
		}
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer srv.Close()
	if code, _ := probe(t, time.Second, srv.URL); code != 1 {
		t.Fatalf("a redirect must fail the probe, exit %d", code)
	}
}

func TestTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	start := time.Now()
	code, _ := probe(t, 200*time.Millisecond, srv.URL)
	if code != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("exit %d after %v, want 1 within the timeout", code, time.Since(start))
	}
}

func TestNonHTTPPortAndClosedPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write([]byte("SSH-2.0-not-http\r\n"))
			_ = c.Close()
		}
	}()
	addr := ln.Addr().String()
	if code, _ := probe(t, time.Second, "http://"+addr+"/"); code != 1 {
		t.Errorf("non-HTTP port: exit %d, want 1", code)
	}
	_ = ln.Close()
	// A port that was just closed is no closed port: the next test server on :0 may be given it. Probe an address the
	// kernel never hands out instead.
	if code, _ := probe(t, time.Second, "http://"+testutil.RefusedAddr(t)+"/"); code != 1 {
		t.Errorf("closed port: exit %d, want 1", code)
	}
}

func TestOnlyLoopbackAndWellFormedArguments(t *testing.T) {
	bad := [][]string{
		{}, {"a", "b"}, {"http://example.com/healthz"}, {"http://10.1.2.3:80/"}, {"http://[::2]:80/"},
		{"ftp://127.0.0.1/"}, {"127.0.0.1:80"}, {"http://user:pw@127.0.0.1:80/"}, {"http://:80/"},
	}
	for _, args := range bad {
		code, out := probe(t, time.Second, args...)
		if code != 2 {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
		if strings.Contains(out, "pw@") || strings.Contains(out, "example.com") {
			t.Errorf("%q: the argument is echoed: %s", args, out)
		}
	}
}

func TestLoopbackForms(t *testing.T) {
	for _, h := range []string{"127.0.0.1", "localhost", "::1", "127.9.9.9"} {
		if !loopback(h) {
			t.Errorf("%s should be loopback", h)
		}
	}
	for _, h := range []string{"0.0.0.0", "gateway", "192.168.1.1", ""} {
		if loopback(h) {
			t.Errorf("%s should not be loopback", h)
		}
	}
}

func TestStalledOrBrokenBodyFails(t *testing.T) {
	release := make(chan struct{})
	stall := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer stall.Close()
	defer close(release)
	if code, _ := probe(t, 300*time.Millisecond, stall.URL); code != 1 {
		t.Errorf("a stalled body: exit %d, want 1", code)
	}
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("short"))
	}))
	defer broken.Close()
	if code, _ := probe(t, time.Second, broken.URL); code != 1 {
		t.Errorf("a truncated body: exit %d, want 1", code)
	}
}
