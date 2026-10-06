package gateway

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Upstream is the inference backend the gateway forwards to. Phase 2 has a
// single static HTTP implementation; the worker registry and scheduler
// replace it in Phases 4-5 behind this interface.
type Upstream interface {
	// Do sends a POST with the given JSON body to path on the backend and
	// returns the response once its headers arrive. The caller closes the
	// response body. ctx cancellation aborts the request.
	Do(ctx context.Context, path string, body []byte, requestID string) (*http.Response, error)
	// Probe reports whether the backend is reachable and healthy.
	Probe(ctx context.Context) error
}

// httpUpstream talks to one OpenAI-compatible server over HTTP.
type httpUpstream struct {
	baseURL       string
	readinessPath string
	client        *http.Client
}

func newHTTPUpstream(baseURL, readinessPath string, headerTimeout time.Duration) *httpUpstream {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil // the upstream is an internal service; ignore proxy env vars
	tr.ResponseHeaderTimeout = headerTimeout
	tr.DisableCompression = true // relay bytes exactly as the upstream sent them
	tr.MaxIdleConnsPerHost = 256
	return &httpUpstream{
		baseURL:       strings.TrimRight(baseURL, "/"),
		readinessPath: readinessPath,
		client: &http.Client{
			Transport: tr,
			// No Client.Timeout: it would cut off long streaming bodies.
			// Never follow redirects: they would replay the prompt to a host
			// the operator did not configure, or turn a POST into a GET.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (u *httpUpstream) Do(ctx context.Context, path string, body []byte, requestID string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	// Only these headers cross the boundary (allowlist), so client
	// credentials such as Authorization never reach the upstream.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("User-Agent", "serverflow-gateway")
	req.Header.Set("X-Request-ID", requestID)
	return u.client.Do(req)
}

func (u *httpUpstream) Probe(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.baseURL+u.readinessPath, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "serverflow-gateway")
	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("upstream readiness probe returned status %d", resp.StatusCode)
	}
	return nil
}
