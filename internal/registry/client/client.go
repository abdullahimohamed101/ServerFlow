// Package client talks to the control plane's registry API. The agent uses it
// to register and heartbeat; the gateway will use it to read workers.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"serverflow/pkg/protocol"
)

const maxResponseBytes = 4 << 20

// Error is a non-success response from the control plane.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("control plane: %d %s: %s", e.Status, e.Code, e.Message)
}

// IsUnknownWorker reports whether err means the control plane does not know the
// worker (for example after a control plane restart): register again.
func IsUnknownWorker(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound && e.Code == "unknown_worker"
}

// IsConflict reports any 409: a stale registration or an illegal transition.
// Prefer IsStale and IsIllegalTransition, which call for different responses.
func IsConflict(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusConflict
}

// IsStale reports that another process has registered under this worker ID and
// superseded this registration. The right response is NOT to register again
// (that would steal the identity back and start a fight) but to stop.
func IsStale(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusConflict && e.Code == "stale_registration"
}

// IsIllegalTransition reports that the control plane refused a state change,
// for example a backend that restarted in place after draining. Registering
// again starts a fresh incarnation.
func IsIllegalTransition(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusConflict && e.Code == "illegal_transition"
}

// IsInvalid reports a 400: the control plane considered the request malformed
// or out of range. Retrying the same request cannot succeed.
func IsInvalid(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusBadRequest
}

// IsUnauthorized reports a rejected token.
func IsUnauthorized(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusUnauthorized
}

// Client is safe for concurrent use.
type Client struct {
	base  string
	token string
	hc    *http.Client
}

// New returns a client for the control plane at baseURL. A nil hc gets a client
// with a short timeout. Redirects are never followed: they would carry the
// bearer token to whatever host the redirect names.
func New(baseURL, token string, hc *http.Client) *Client {
	if hc == nil {
		// The control plane is internal and the bearer token must not be handed to
		// a proxy named in the environment.
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.Proxy = nil
		hc = &http.Client{Timeout: 5 * time.Second, Transport: tr}
	}
	c := *hc
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: strings.TrimRight(baseURL, "/"), token: token, hc: &c}
}

func (c *Client) do(ctx context.Context, method, path string, header http.Header, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		e := &Error{Status: resp.StatusCode}
		var body struct {
			Error struct{ Message, Code string }
		}
		if json.Unmarshal(raw, &body) == nil {
			e.Code, e.Message = body.Error.Code, body.Error.Message
		}
		return e
	}
	if out != nil {
		// A success with nothing in it is not an answer: treating it as "no workers" would
		// wipe a caller's view instead of failing closed.
		if len(raw) == 0 {
			return errors.New("control plane: empty response")
		}
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("control plane: bad response: %w", err)
		}
	}
	return nil
}

func checkID(id string) error {
	if !protocol.ValidWorkerID(id) {
		return errors.New("client: invalid worker id")
	}
	return nil
}

// Register registers a worker and returns the registration the worker must use.
func (c *Client) Register(ctx context.Context, info protocol.WorkerInfo) (protocol.RegisterResponse, error) {
	var out protocol.RegisterResponse
	err := c.do(ctx, http.MethodPost, "/v1/workers/register", nil, info, &out)
	return out, err
}

// Heartbeat reports a worker's state and load.
func (c *Client) Heartbeat(ctx context.Context, workerID string, hb protocol.Heartbeat) error {
	if err := checkID(workerID); err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, "/v1/workers/"+workerID+"/heartbeat", nil, hb, nil)
}

// Deregister removes a worker that is shutting down gracefully.
func (c *Client) Deregister(ctx context.Context, workerID, registrationID string) error {
	if err := checkID(workerID); err != nil {
		return err
	}
	h := http.Header{"X-Registration-Id": {registrationID}}
	return c.do(ctx, http.MethodDelete, "/v1/workers/"+workerID, h, nil, nil)
}

// Query filters Workers. Zero fields match everything.
type Query struct {
	Model        string
	State        protocol.WorkerState
	EligibleOnly bool
}

// Workers lists workers.
func (c *Client) Workers(ctx context.Context, q Query) ([]protocol.WorkerSnapshot, error) {
	v := url.Values{}
	if q.Model != "" {
		v.Set("model", q.Model)
	}
	if q.State != "" {
		v.Set("state", string(q.State))
	}
	if q.EligibleOnly {
		v.Set("eligible", strconv.FormatBool(true))
	}
	path := "/v1/workers"
	if len(v) > 0 {
		path += "?" + v.Encode()
	}
	var out struct {
		Workers *[]protocol.WorkerSnapshot `json:"workers"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	if out.Workers == nil {
		return nil, errors.New("control plane: bad response: no workers list")
	}
	return *out.Workers, nil
}

// Worker returns one worker.
func (c *Client) Worker(ctx context.Context, id string) (protocol.WorkerSnapshot, error) {
	if err := checkID(id); err != nil {
		return protocol.WorkerSnapshot{}, err
	}
	var out protocol.WorkerSnapshot
	err := c.do(ctx, http.MethodGet, "/v1/workers/"+id, nil, nil, &out)
	return out, err
}

// Models returns the model inventory.
func (c *Client) Models(ctx context.Context) ([]protocol.ModelInfo, error) {
	var out struct {
		Models []protocol.ModelInfo `json:"models"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/models", nil, nil, &out)
	return out.Models, err
}
