// Command e2e is the load generator of the rolling-update test (Phase 17, D20; docs/operations/kubernetes-deployment.md).
//
// It runs a closed loop of concurrent clients against the gateway's host entry point, records the outcome of every request
// with its start and end time, and classifies each failure. It uses only the standard library HTTP client and sends every
// request exactly once: there are no client retries, and the transport's own replay of a request is switched off (see
// newClient), so a failure the gateway causes during a rollout is visible rather than hidden. It never prints the API key.
//
// This file is the part that needs no cluster: how one response is judged. The unit tests drive it with a fake HTTP server.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// Failure classes. A request that is not in one of these succeeded.
const (
	ClassRefused       = "connection_refused"
	ClassReset         = "connection_reset"
	ClassClosed        = "connection_closed" // the peer closed the connection before any response
	ClassTimeout       = "timeout"
	ClassTransport     = "transport_other"
	ClassStreamCut     = "stream_truncated"  // the stream ended or broke before [DONE]
	ClassSSEMalformed  = "sse_malformed"     // a line or event that is not valid server-sent events of the OpenAI shape
	ClassBodyCut       = "body_truncated"    // a non-streaming body that ended early
	ClassBodyMalformed = "body_malformed"    // a 200 whose JSON has no choices
	ClassBadRequest    = "request_not_built" // a bug of the generator, never a server failure
)

// Outcome is what one request did.
type Outcome struct {
	Client    int
	Stream    bool
	KeepAlive bool
	Start     time.Time
	End       time.Time
	Status    int    // 0 when no response arrived
	Class     string // "" on success
}

// OK reports whether the request succeeded.
func (o Outcome) OK() bool { return o.Class == "" }

const (
	maxBody  = 1 << 20
	maxLine  = 1 << 20
	bodyJSON = `{"model":"mock-model","messages":[{"role":"user","content":"say hello"}],"max_tokens":16}`
	bodySSE  = `{"model":"mock-model","messages":[{"role":"user","content":"say hello"}],"max_tokens":16,"stream":true}`
)

// newClient: one transport per client. keepAlive false opens a connection per request (and asks the server to close it).
// The transport does not replay a request on a fresh connection when a reused one turns out to be dead: that replay is
// done by net/http for requests with a replayable body, so the body is a reader without GetBody (see doRequest).
func newClient(keepAlive bool) *http.Client {
	tr := &http.Transport{
		DisableKeepAlives:   !keepAlive,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		Proxy:               nil, // the entry point is on loopback; an environment proxy must never intervene
	}
	return &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// doRequest sends one chat completion and judges the whole response, including the body or the stream.
func doRequest(ctx context.Context, c *http.Client, base, key string, stream bool, timeout time.Duration) (status int, class string) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	payload := bodyJSON
	if stream {
		payload = bodySSE
	}
	// io.NopCloser hides the reader's type, so NewRequest sets no GetBody and net/http cannot replay the request.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/v1/chat/completions", io.NopCloser(strings.NewReader(payload)))
	if err != nil {
		return 0, ClassBadRequest
	}
	req.ContentLength = int64(len(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, classifyTransport(ctx, redact(err, key))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, classifyStatus(resp.StatusCode, resp.Body)
	}
	if stream {
		return resp.StatusCode, judgeStream(ctx, resp.Body)
	}
	return resp.StatusCode, judgeBody(ctx, resp.Body)
}

// redact: an error from net/http never contains the Authorization header, but the key is removed from the text anyway.
func redact(err error, key string) error {
	if key == "" || !strings.Contains(err.Error(), key) {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), key, "[redacted]"))
}

// classifyTransport names why Do (or a body read) failed.
func classifyTransport(ctx context.Context, err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()):
		return ClassTimeout
	case errors.Is(err, syscall.ECONNREFUSED):
		return ClassRefused
	case errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE):
		return ClassReset
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		return ClassClosed
	}
	return ClassTransport
}

var codeChars = regexp.MustCompile(`[^a-z0-9_]+`)

// classifyStatus: any non-2xx status is a failure of this test (the tenant is generous and the cluster healthy). The class
// carries the status and, when the body is the gateway's JSON error, its code: "http_503_no_available_workers".
func classifyStatus(status int, body io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(body, 64<<10))
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	class := fmt.Sprintf("http_%d", status)
	if json.Unmarshal(b, &e) == nil {
		if code := codeChars.ReplaceAllString(strings.ToLower(e.Error.Code), "_"); code != "" {
			class += "_" + code
		}
	}
	return class
}

// judgeBody: a 200 must carry the whole JSON body, with at least one choice.
func judgeBody(ctx context.Context, body io.Reader) string {
	b, err := io.ReadAll(io.LimitReader(body, maxBody))
	if err != nil {
		if c := classifyTransport(ctx, err); c == ClassTimeout {
			return c
		}
		return ClassBodyCut
	}
	var r struct {
		Choices []json.RawMessage `json:"choices"`
	}
	if json.Unmarshal(b, &r) != nil || len(r.Choices) == 0 {
		return ClassBodyMalformed
	}
	return ""
}

// judgeStream reads server-sent events and returns "" only for a well-formed stream that ends with [DONE]: every non-blank
// line is a comment (":"), a field (data:, event:, id:, retry:) or the continuation of none; every data payload is [DONE] or
// a JSON object with a choices member; an error event, anything after [DONE], and a stream that ends without [DONE] fail.
func judgeStream(ctx context.Context, body io.Reader) string {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	done := false
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			return ClassSSEMalformed
		}
		switch field {
		case "event", "id", "retry":
			continue
		case "data":
		default:
			return ClassSSEMalformed
		}
		payload := strings.TrimSpace(value)
		if done {
			return ClassSSEMalformed // an event after [DONE]
		}
		if payload == "[DONE]" {
			done = true
			continue
		}
		var ev struct {
			Choices json.RawMessage `json:"choices"`
			Error   *struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(payload), &ev) != nil {
			return ClassSSEMalformed
		}
		if ev.Error != nil {
			if code := strings.Trim(codeChars.ReplaceAllString(strings.ToLower(ev.Error.Code), "_"), "_"); code != "" {
				return "stream_error_" + code
			}
			return "stream_error"
		}
		if len(bytes.TrimSpace(ev.Choices)) == 0 {
			return ClassSSEMalformed
		}
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return ClassSSEMalformed
		}
		if c := classifyTransport(ctx, err); c == ClassTimeout {
			return c
		}
		return ClassStreamCut
	}
	if !done {
		return ClassStreamCut
	}
	return ""
}
