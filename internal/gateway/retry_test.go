package gateway

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
)

type stubTimeoutErr struct{}

func (stubTimeoutErr) Error() string   { return "i/o timeout" }
func (stubTimeoutErr) Timeout() bool   { return true }
func (stubTimeoutErr) Temporary() bool { return true }

func TestTransportErrorsAreRetryableOnlyWhenNoOutputCanHaveStarted(t *testing.T) {
	p := newRetryPolicy(2, []int{502, 503, 504})
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	cases := []struct {
		name  string
		err   error
		class string
		retry bool
	}{
		{"connection refused", dialErr, classConnect, true},
		{"refused, wrapped like http.Client does", &url.Error{Op: "Post", URL: "http://x", Err: dialErr}, classConnect, true},
		{"raw ECONNREFUSED", syscall.ECONNREFUSED, classConnect, true},
		{"dial timeout", &net.OpError{Op: "dial", Err: stubTimeoutErr{}}, classConnect, true},
		{"the dial guard refusing an address", &net.OpError{Op: "dial", Err: errForbiddenAddress}, classConnect, true},
		{"dns failure", &net.DNSError{Err: "no such host", Name: "x"}, classConnect, true},
		{"connection reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, classReset, true},
		{"broken pipe", &net.OpError{Op: "write", Err: syscall.EPIPE}, classReset, true},
		{"closed before headers (EOF)", &url.Error{Op: "Post", Err: io.EOF}, classReset, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, classReset, true},
		{"timeout waiting for response headers", &url.Error{Op: "Post", Err: stubTimeoutErr{}}, "", false},
		{"read timeout", &net.OpError{Op: "read", Err: stubTimeoutErr{}}, "", false},
		{"deadline exceeded", context.DeadlineExceeded, "", false},
		{"the client went away", context.Canceled, "", false},
		{"wrapped cancellation", &url.Error{Op: "Post", Err: context.Canceled}, "", false},
		{"a TLS failure", &tls.CertificateVerificationError{}, "", false},
		{"something unknown", errors.New("boom"), "", false},
		{"nil", nil, "", false},
		{"a file error", os.ErrNotExist, "", false},
	}
	for _, c := range cases {
		class, retry := p.transportError(c.err)
		if class != c.class || retry != c.retry {
			t.Errorf("%s: got (%q, %v), want (%q, %v)", c.name, class, retry, c.class, c.retry)
		}
	}
}

func TestOnlyConfiguredStatusesAreRetryable(t *testing.T) {
	p := newRetryPolicy(2, []int{502, 503, 504})
	for code := 100; code < 600; code++ {
		class, retry := p.status(code)
		want := code == 502 || code == 503 || code == 504
		if retry != want {
			t.Errorf("%d: retry=%v, want %v", code, retry, want)
		}
		if want && class != fmt.Sprintf("status_%d", code) {
			t.Errorf("%d: class %q", code, class)
		}
		if !want && class != "" {
			t.Errorf("%d: a final status has no class: %q", code, class)
		}
	}
	custom := newRetryPolicy(3, []int{500})
	if _, retry := custom.status(500); !retry {
		t.Error("a configured 500 is retried")
	}
	if _, retry := custom.status(503); retry {
		t.Error("503 is not retried when it is not listed")
	}
	if _, retry := newRetryPolicy(2, nil).status(503); retry {
		t.Error("an empty list retries no statuses")
	}
}

func TestFirstChunkFailuresAreRetryableExceptTimeoutsAndCancellation(t *testing.T) {
	p := newRetryPolicy(2, nil)
	for name, tc := range map[string]struct {
		err   error
		retry bool
	}{
		"EOF with nothing":      {io.EOF, true},
		"unexpected EOF":        {io.ErrUnexpectedEOF, true},
		"reset":                 {&net.OpError{Op: "read", Err: syscall.ECONNRESET}, true},
		"other read error":      {errors.New("garbled"), true},
		"idle timeout":          {&net.OpError{Op: "read", Err: stubTimeoutErr{}}, false},
		"http2-style timeout":   {&url.Error{Op: "Get", Err: stubTimeoutErr{}}, false},
		"client cancelled":      {context.Canceled, false},
		"wrapped cancellation":  {fmt.Errorf("read: %w", context.Canceled), false},
		"nil":                   {nil, false},
		"deadline from our own": {http.ErrHandlerTimeout, true},
	} {
		class, retry := p.firstChunkError(tc.err)
		if retry != tc.retry || (retry && class != classEmptyStream) || (!retry && class != "") {
			t.Errorf("%s: got (%q, %v)", name, class, retry)
		}
	}
}
