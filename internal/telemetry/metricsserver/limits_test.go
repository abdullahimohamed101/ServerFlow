package metricsserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"serverflow/internal/config"
)

// The bounds on what a scraper can make the metrics endpoint do (ADR-017). Each has a test of the mechanism with a
// shortened value and a test that the shipped value is the documented one, so changing either fails something.

func TestShippedLimits(t *testing.T) {
	want := Limits{MaxInFlight: 4, HandlerTimeout: 10 * time.Second, ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 8 << 10}
	if DefaultLimits() != want {
		t.Fatalf("the shipped limits changed: %+v, want %+v (ADR-017 and the operations guide state them)", DefaultLimits(), want)
	}
	if limits != want {
		t.Fatalf("the limits in force are not the shipped ones: %+v", limits)
	}
}

// slowGatherer blocks every Gather until released.
type slowGatherer struct {
	started chan struct{}
	release chan struct{}
}

func (g *slowGatherer) Gather() ([]*dto.MetricFamily, error) {
	g.started <- struct{}{}
	<-g.release
	return nil, nil
}

func withLimits(t *testing.T, l Limits) {
	t.Helper()
	old := limits
	limits = l
	t.Cleanup(func() { limits = old })
}

func TestScrapeConcurrencyIsCapped(t *testing.T) {
	l := DefaultLimits()
	withLimits(t, l)
	g := &slowGatherer{started: make(chan struct{}, 16), release: make(chan struct{})}
	srv := newTestServer(Handler(g, ""))
	defer srv.Close()
	codes := make(chan int, l.MaxInFlight+1)
	for i := 0; i < l.MaxInFlight; i++ {
		go func() {
			resp, err := http.Get(srv.URL + "/metrics")
			if err != nil {
				codes <- -1
				return
			}
			_ = resp.Body.Close()
			codes <- resp.StatusCode
		}()
	}
	for i := 0; i < l.MaxInFlight; i++ {
		select {
		case <-g.started: // all of them are inside Gather
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d scrapes were admitted", i, l.MaxInFlight)
		}
	}
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("scrape number %d got no answer: %v", l.MaxInFlight+1, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("scrape number %d got %d, want 503", l.MaxInFlight+1, resp.StatusCode)
	}
	close(g.release)
	for i := 0; i < l.MaxInFlight; i++ {
		if c := <-codes; c != 200 {
			t.Fatalf("a scrape within the cap got %d", c)
		}
	}
}

func TestSlowScrapeTimesOut(t *testing.T) {
	l := DefaultLimits()
	l.HandlerTimeout = 200 * time.Millisecond
	withLimits(t, l)
	g := &slowGatherer{started: make(chan struct{}, 4), release: make(chan struct{})}
	srv := newTestServer(Handler(g, ""))
	defer srv.Close()
	defer close(g.release) // runs before srv.Close, which waits for handlers still inside Gather
	start := time.Now()
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("no answer within 3 s from a scrape that outlasts the handler timeout: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || time.Since(start) > 3*time.Second {
		t.Fatalf("a scrape that outlasts the handler timeout: %d after %v", resp.StatusCode, time.Since(start))
	}
}

func listenOnLoopback(t *testing.T) *Server {
	t.Helper()
	s, err := Listen("127.0.0.1:0", config.MetricsConfig{}, registry(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Serve(ctx) }()
	t.Cleanup(cancel)
	return s
}

func TestHeaderReadTimeoutDropsASlowClient(t *testing.T) {
	l := DefaultLimits()
	l.ReadHeaderTimeout = 200 * time.Millisecond
	withLimits(t, l)
	s := listenOnLoopback(t)
	c, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_, _ = c.Write([]byte("GET /metrics HTTP/1.1\r\nHost: x\r\nX-Slow: ")) // the headers never finish
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 512)
	n, err := c.Read(buf)
	// The server answers 408 or just closes; either way the read returns long before our own 3 s deadline.
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("the server held a half-sent request open past the header timeout (read %q)", buf[:n])
	}
}

func TestOversizedHeadersAreRefused(t *testing.T) {
	s := listenOnLoopback(t)
	get := func(size int) int {
		req, _ := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/metrics", nil)
		req.Header.Set("X-Pad", strings.Repeat("a", size))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if c := get(4 << 10); c != 200 {
		t.Fatalf("4 KiB of headers: %d", c)
	}
	if c := get(16 << 10); c != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("16 KiB of headers: %d, want 431", c)
	}
}
