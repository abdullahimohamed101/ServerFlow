// Package worker is the worker agent: it watches a local inference backend and
// keeps the control plane's registry informed (register, heartbeat, drain). It
// is not in the data path; the gateway talks to the backend directly.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"serverflow/pkg/protocol"
)

// BackendStatus is what the agent learns from the inference backend.
type BackendStatus struct {
	// State is LOADING_MODEL, WARMING, READY, or DRAINING as the backend sees itself.
	State          protocol.WorkerState
	Metrics        protocol.Metrics
	MaxConcurrency int
	QueueSize      int
}

// Backend reports on a local inference server. An error means it is unreachable
// or unintelligible, which the agent reports as FAILED.
type Backend interface {
	Status(ctx context.Context) (BackendStatus, error)
}

const maxBackendResponse = 1 << 20

// MockBackend reads a mock worker's provisional /stats endpoint. A vLLM
// implementation of Backend arrives with Phase 13.
type MockBackend struct {
	base string
	hc   *http.Client
}

// NewMockBackend returns a backend for the mock worker at baseURL. Redirects
// are never followed.
func NewMockBackend(baseURL string) *MockBackend {
	return &MockBackend{
		base: strings.TrimRight(baseURL, "/"),
		hc: &http.Client{
			Timeout:       2 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

type mockStats struct {
	Status                string  `json:"status"`
	ActiveRequests        int     `json:"active_requests"`
	QueueDepth            int     `json:"queue_depth"`
	QueuedInputTokens     int     `json:"queued_input_tokens"`
	RecentTokensPerSecond float64 `json:"recent_tokens_per_second"`
	MaxConcurrency        int     `json:"max_concurrency"`
	QueueSize             int     `json:"queue_size"`
}

// Status implements Backend.
func (m *MockBackend) Status(ctx context.Context) (BackendStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.base+"/stats", nil)
	if err != nil {
		return BackendStatus{}, err
	}
	resp, err := m.hc.Do(req)
	if err != nil {
		return BackendStatus{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return BackendStatus{}, fmt.Errorf("backend /stats returned status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBackendResponse+1))
	if err != nil {
		return BackendStatus{}, err
	}
	if len(raw) > maxBackendResponse {
		return BackendStatus{}, errors.New("backend /stats response is too large")
	}
	var s mockStats
	if err := json.Unmarshal(raw, &s); err != nil {
		return BackendStatus{}, fmt.Errorf("backend /stats is not valid JSON: %w", err)
	}
	var state protocol.WorkerState
	switch s.Status {
	case "starting":
		state = protocol.StateLoadingModel
	case "ready":
		state = protocol.StateReady
	case "draining":
		state = protocol.StateDraining
	default:
		return BackendStatus{}, fmt.Errorf("%w: unknown status %q", errUnusableReport, truncate(s.Status, 32))
	}
	return BackendStatus{
		State: state,
		Metrics: protocol.Metrics{
			ActiveRequests: s.ActiveRequests, QueueDepth: s.QueueDepth,
			QueuedInputTokens: s.QueuedInputTokens, RecentTokensPerSecond: s.RecentTokensPerSecond,
		},
		MaxConcurrency: s.MaxConcurrency,
		QueueSize:      s.QueueSize,
	}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
