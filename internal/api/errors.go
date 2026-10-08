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
	w.WriteHeader(e.HTTPStatus)
	_, _ = w.Write(e.Body())
}

func errorType(status int) string {
	if status >= 500 {
		return "api_error"
	}
	return "invalid_request_error"
}
