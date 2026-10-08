package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"syscall"
)

// Retry policy (spec section 25, ADR-012). A request may be retried on a different worker only while
// nothing has been sent to the client, and only for failures that say the worker never produced output:
//
//   - the connection failed (refused, unreachable, dial timeout, or the dial guard refused the address);
//   - the connection was reset or closed before response headers;
//   - the worker answered with a configured status (502 and 503 by default: unavailable, queue full,
//     draining, starting; a worker-reported 504 means its backend already timed out and is not retried);
//   - a 200 whose body ended or failed before its first byte (stream or not).
//
// Not retried: any 2xx, 3xx or 4xx; a 500 unless configured (it can be a deterministic failure every worker
// would repeat); a timeout waiting for response headers (the worker may still be generating, and a second
// copy of the prompt doubles the work); and anything after the first byte.

// Attempt error classes. They are also Prometheus label values, so the set is fixed (status classes are
// bounded by the configured list).
const (
	classConnect     = "connect"
	classReset       = "reset"
	classEmptyStream = "empty_stream"
)

func statusClass(code int) string { return "status_" + strconv.Itoa(code) }

// retryPolicy decides whether a failed attempt may be followed by another.
type retryPolicy struct {
	max      int
	statuses map[int]bool
}

func newRetryPolicy(max int, statuses []int) retryPolicy {
	p := retryPolicy{max: max, statuses: make(map[int]bool, len(statuses))}
	for _, s := range statuses {
		p.statuses[s] = true
	}
	return p
}

// status reports whether a worker response with this status may be retried elsewhere.
func (p retryPolicy) status(code int) (class string, retryable bool) {
	if p.statuses[code] {
		return statusClass(code), true
	}
	return "", false
}

// transportError classifies an error from sending the request, before any response headers.
func (p retryPolicy) transportError(err error) (class string, retryable bool) {
	if err == nil || errors.Is(err, context.Canceled) {
		return "", false
	}
	var opErr *net.OpError
	var dnsErr *net.DNSError
	switch {
	case errors.Is(err, syscall.ECONNREFUSED), errors.As(err, &dnsErr), errors.As(err, &opErr) && opErr.Op == "dial":
		return classConnect, true // includes a dial timeout and a refused address: nothing was sent
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return classReset, true
	}
	return "", false // header timeouts, TLS errors, anything unknown
}

// firstChunkError classifies a failure to read the first byte of a stream.
func (p retryPolicy) firstChunkError(err error) (class string, retryable bool) {
	if err == nil || errors.Is(err, context.Canceled) {
		return "", false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "", false // a stalled worker is cut by the idle timeout, not retried (ADR-012)
	}
	return classEmptyStream, true
}
