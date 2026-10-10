// Package metricsserver serves a Prometheus registry on a listener of its own (ADR-017). It is shared by the
// gateway and the control plane so the exposure rules are written once: loopback by default, a bearer token for
// anything else, GET /metrics only, bounded in time and size.
package metricsserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"serverflow/internal/config"
)

const (
	metricsPath       = "/metrics"
	shutdownGraceTime = 5 * time.Second
)

// Limits are the bounds on what a scraper can make the endpoint do.
type Limits struct {
	MaxInFlight       int           // scrapes served at once; more get 503
	HandlerTimeout    time.Duration // one scrape's time before it gets 503
	ReadHeaderTimeout time.Duration // time to send the request headers before the connection is dropped
	MaxHeaderBytes    int           // request header size before 431
}

// limits is a variable only so tests can shorten the durations; DefaultLimits are the shipped values.
var limits = DefaultLimits()

// DefaultLimits returns the bounds the shipped binaries run with.
func DefaultLimits() Limits {
	return Limits{MaxInFlight: 4, HandlerTimeout: 10 * time.Second, ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 8 << 10}
}

// Handler returns the /metrics handler for g. With a non-empty token every request must present it as
// "Authorization: Bearer <token>"; the comparison is constant time and the token is never written anywhere.
// Only GET (and HEAD) on exactly /metrics is served.
func Handler(g prometheus.Gatherer, token string) http.Handler {
	scrape := promhttp.HandlerFor(g, promhttp.HandlerOpts{
		MaxRequestsInFlight: limits.MaxInFlight,
		Timeout:             limits.HandlerTimeout,
		// Errors while gathering are visible in the response status; not echoing them keeps internals out.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
	var want [sha256.Size]byte
	if token != "" {
		want = sha256.Sum256([]byte(token))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+metricsPath, func(w http.ResponseWriter, r *http.Request) {
		if token != "" && !bearerMatches(r, want) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="metrics"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		scrape.ServeHTTP(w, r)
	})
	return mux
}

// bearerMatches compares the presented bearer token with the expected digest in constant time.
func bearerMatches(r *http.Request, want [sha256.Size]byte) bool {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return false
	}
	got := sha256.Sum256([]byte(h[len(prefix):]))
	return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}

// Server is a bound metrics listener that has not started serving yet.
type Server struct {
	ln   net.Listener
	http *http.Server
	log  *slog.Logger
}

// Listen applies the exposure guard to cfg and addr, then binds addr (the effective address: the configured one
// or the binary's default). It returns (nil, nil) when addr is empty: the endpoint is switched off. The
// token is never part of an error or a log line.
func Listen(addr string, cfg config.MetricsConfig, g prometheus.Gatherer, log *slog.Logger) (*Server, error) {
	if addr == "" {
		return nil, nil
	}
	if err := cfg.CheckListen(addr); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listener: %w", err)
	}
	return &Server{
		ln: ln,
		http: &http.Server{
			Handler:           Handler(g, cfg.Token),
			ReadHeaderTimeout: limits.ReadHeaderTimeout,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      limits.HandlerTimeout + 5*time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    limits.MaxHeaderBytes,
		},
		log: log,
	}, nil
}

// Addr is the address actually bound (useful when the port was 0).
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Serve serves until ctx ends, then shuts down gracefully. A clean shutdown returns nil.
func (s *Server) Serve(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() { errc <- s.http.Serve(s.ln) }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownGraceTime)
	defer cancel()
	if err := s.http.Shutdown(sctx); err != nil {
		_ = s.http.Close()
	}
	<-errc
	return nil
}
