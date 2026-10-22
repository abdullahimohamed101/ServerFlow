// Package api holds the OpenAI-compatible wire types, request validation,
// and the error model for the public gateway API. Wire types stay in this
// package; the rest of the system uses protocol.InferenceRequest.
package api

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Error codes from spec section 51 that the gateway can produce today.
const (
	CodeInvalidRequest    = "INVALID_REQUEST"
	CodeModelNotFound     = "MODEL_NOT_FOUND"
	CodeNoCapacity        = "NO_CAPACITY"
	CodeUpstreamTimeout   = "UPSTREAM_TIMEOUT"
	CodeWorkerUnavailable = "WORKER_UNAVAILABLE"
	CodeInferenceFailed   = "INFERENCE_FAILED"
	CodeInternalError     = "INTERNAL_ERROR"
	CodeUnauthorized      = "UNAUTHORIZED"
	CodeForbidden         = "FORBIDDEN"
	CodeAuthUnavailable   = "AUTH_UNAVAILABLE"
	CodeRateLimited       = "RATE_LIMITED"
	CodeRateLimitUnavail  = "RATE_LIMIT_UNAVAILABLE"
)

// Error is a gateway-originated API error. It implements error.
type Error struct {
	Code       string
	HTTPStatus int
	Message    string

	// Optional detail for NO_CAPACITY (spec section 15).
	Model           string
	EligibleWorkers *int
	// RetryAfter, when positive, is sent as a Retry-After header in seconds.
	RetryAfter int
	// WWWAuthenticate, when set, is sent as a WWW-Authenticate header (RFC 9110 requires one on 401).
	WWWAuthenticate string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// ErrInvalidRequest reports a malformed or invalid client request.
func ErrInvalidRequest(msg string) *Error {
	return &Error{Code: CodeInvalidRequest, HTTPStatus: http.StatusBadRequest, Message: msg}
}

// ErrBodyTooLarge reports a request body over the configured limit.
func ErrBodyTooLarge() *Error {
	return &Error{Code: CodeInvalidRequest, HTTPStatus: http.StatusRequestEntityTooLarge, Message: "request body exceeds the maximum allowed size"}
}

// ErrModelNotFound reports a model the gateway does not serve.
func ErrModelNotFound(model string) *Error {
	return &Error{Code: CodeModelNotFound, HTTPStatus: http.StatusNotFound, Message: "the model `" + truncate(model, 64) + "` does not exist"}
}

// ErrUpstreamTimeout reports that the upstream did not respond in time.
func ErrUpstreamTimeout() *Error {
	return &Error{Code: CodeUpstreamTimeout, HTTPStatus: http.StatusGatewayTimeout, Message: "the upstream did not respond in time"}
}

// ErrWorkerUnavailable reports that the upstream could not be reached.
func ErrWorkerUnavailable() *Error {
	return &Error{Code: CodeWorkerUnavailable, HTTPStatus: http.StatusServiceUnavailable, Message: "no inference upstream is available"}
}

// ErrInferenceFailed reports an upstream transport or protocol failure.
func ErrInferenceFailed() *Error {
	return &Error{Code: CodeInferenceFailed, HTTPStatus: http.StatusBadGateway, Message: "the upstream failed to complete the request"}
}

// ErrNoCapacity reports that the model is served but no worker can take the
// request right now (none eligible, or all at their concurrency limit).
func ErrNoCapacity(model string, eligibleWorkers int) *Error {
	return &Error{
		Code: CodeNoCapacity, HTTPStatus: http.StatusServiceUnavailable,
		Message: "no worker has capacity for the model `" + truncate(model, 64) + "` right now",
		Model:   truncate(model, 64), EligibleWorkers: &eligibleWorkers, RetryAfter: 1,
	}
}

// Reasons of an overload shed (ErrOverloaded). They are the Retry-After classes of ADR-020 and the reason label
// values of admission_rejections_total.
const (
	OverloadGlobalLimit  = "global_limit"
	OverloadQueueFull    = "queue_full"
	OverloadQueueTimeout = "queue_timeout"
	OverloadDraining     = "draining"
)

// ErrOverloaded reports a request shed by the gateway's admission control (Phase 15): the global request cap, a full
// model queue (or a tenant's share of it), a wait that ran out, or a gateway that is shutting down. It is a 503
// NO_CAPACITY with Retry-After, never a 429, which stays reserved for a tenant exceeding its own quota (RFC 9110
// 15.6.4 against RFC 6585). The body has the shape of every NO_CAPACITY error; model and eligible_workers are present
// when known (a negative eligible count, or an empty model, leaves the field out). The message carries the reason.
func ErrOverloaded(reason, model string, eligibleWorkers, retryAfter int) *Error {
	var msg string
	switch reason {
	case OverloadGlobalLimit:
		msg = "the gateway is at its limit of concurrent requests; retry shortly"
	case OverloadQueueTimeout:
		msg = "no worker had capacity for the model `" + truncate(model, 64) + "` within the queue wait limit"
	case OverloadDraining:
		msg = "the gateway is shutting down; retry on another instance"
	default:
		msg = "the queue for the model `" + truncate(model, 64) + "` is full; retry shortly"
	}
	e := &Error{Code: CodeNoCapacity, HTTPStatus: http.StatusServiceUnavailable, Message: msg, Model: truncate(model, 64), RetryAfter: max(retryAfter, 1)}
	if eligibleWorkers >= 0 {
		e.EligibleWorkers = &eligibleWorkers
	}
	return e
}

// ErrUnauthorized reports a missing or unusable API key. Its body is identical whatever the
// reason (absent, malformed, unknown, wrong secret, revoked, expired) so a response never
// reveals whether a key exists.
func ErrUnauthorized() *Error {
	return &Error{Code: CodeUnauthorized, HTTPStatus: http.StatusUnauthorized, Message: "invalid or missing API key", WWWAuthenticate: "Bearer"}
}

// ErrForbidden reports a valid caller that may not use the service, such as a suspended tenant.
func ErrForbidden() *Error {
	return &Error{Code: CodeForbidden, HTTPStatus: http.StatusForbidden, Message: "this API key is not permitted to use the service"}
}

// ErrModelForbidden reports a model the caller's tenant may not use. Its text is the same for a
// model that exists and one that does not, so a tenant cannot probe the catalogue.
func ErrModelForbidden(model string) *Error {
	return &Error{Code: CodeForbidden, HTTPStatus: http.StatusForbidden, Message: "the model `" + truncate(model, 64) + "` does not exist or is not available to this API key"}
}

// ErrAuthUnavailable reports that keys cannot be verified right now (the key store is down and
// the key is not cached). It is not a 401: the key may well be valid.
func ErrAuthUnavailable() *Error {
	return &Error{Code: CodeAuthUnavailable, HTTPStatus: http.StatusServiceUnavailable, Message: "authentication is temporarily unavailable; retry shortly", RetryAfter: 2}
}

// ErrRateLimited reports a request over a quota. limit names the quota (requests, tokens, concurrency or
// model) and nothing else; retryAfter is in seconds and at least 1.
func ErrRateLimited(limit string, retryAfter int) *Error {
	return &Error{Code: CodeRateLimited, HTTPStatus: http.StatusTooManyRequests, RetryAfter: max(1, retryAfter),
		Message: "rate limit exceeded: " + limitText(limit) + "; retry after the time in the Retry-After header"}
}

func limitText(limit string) string {
	switch limit {
	case "requests":
		return "too many requests per minute"
	case "tokens":
		return "too many tokens per minute"
	case "concurrency":
		return "too many concurrent requests"
	case "model":
		return "too many requests for this model"
	}
	return "limit reached"
}

// ErrRateLimitUnavailable reports that quotas cannot be checked right now (Redis is down and the gateway
// fails closed). It is not a 429: the caller may be within its quota.
func ErrRateLimitUnavailable(retryAfter int) *Error {
	return &Error{Code: CodeRateLimitUnavail, HTTPStatus: http.StatusServiceUnavailable, RetryAfter: max(1, retryAfter),
		Message: "rate limiting is temporarily unavailable; retry shortly"}
}

// ErrInternal reports an unexpected gateway failure.
func ErrInternal() *Error {
	return &Error{Code: CodeInternalError, HTTPStatus: http.StatusInternalServerError, Message: "internal server error"}
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`

	Model           string `json:"model,omitempty"`
	EligibleWorkers *int   `json:"eligible_workers,omitempty"`
}

// Body returns the OpenAI-shaped JSON error body for e.
func (e *Error) Body() []byte {
	b, _ := json.Marshal(errorBody{errorDetail{Message: e.Message, Type: errorType(e.HTTPStatus), Code: e.Code, Model: e.Model, EligibleWorkers: e.EligibleWorkers}})
	return b
}

// WriteError writes e as a JSON response.
func WriteError(w http.ResponseWriter, e *Error) {
	w.Header().Set("Content-Type", "application/json")
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfter))
	}
	if e.WWWAuthenticate != "" {
		w.Header().Set("WWW-Authenticate", e.WWWAuthenticate)
	}
	w.WriteHeader(e.HTTPStatus)
	_, _ = w.Write(e.Body())
}

func errorType(status int) string {
	if status >= 500 {
		return "api_error"
	}
	if status == http.StatusTooManyRequests {
		return "rate_limit_error"
	}
	return "invalid_request_error"
}
