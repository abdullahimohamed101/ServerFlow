// Package api holds the OpenAI-compatible wire types, request validation,
// and the error model for the public gateway API. Wire types stay in this
// package; the rest of the system uses protocol.InferenceRequest.
package api

import (
	"encoding/json"
	"net/http"
)

// Error codes from spec section 51 that the gateway can produce today.
const (
	CodeInvalidRequest    = "INVALID_REQUEST"
	CodeModelNotFound     = "MODEL_NOT_FOUND"
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
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// ErrInvalidRequest reports a malformed or invalid client request.
func ErrInvalidRequest(msg string) *Error {
	return &Error{CodeInvalidRequest, http.StatusBadRequest, msg}
}

// ErrBodyTooLarge reports a request body over the configured limit.
func ErrBodyTooLarge() *Error {
	return &Error{CodeInvalidRequest, http.StatusRequestEntityTooLarge, "request body exceeds the maximum allowed size"}
}

// ErrModelNotFound reports a model the gateway does not serve.
func ErrModelNotFound(model string) *Error {
	return &Error{CodeModelNotFound, http.StatusNotFound, "the model `" + truncate(model, 64) + "` does not exist"}
}

// ErrUpstreamTimeout reports that the upstream did not respond in time.
func ErrUpstreamTimeout() *Error {
	return &Error{CodeUpstreamTimeout, http.StatusGatewayTimeout, "the upstream did not respond in time"}
}

// ErrWorkerUnavailable reports that the upstream could not be reached.
func ErrWorkerUnavailable() *Error {
	return &Error{CodeWorkerUnavailable, http.StatusServiceUnavailable, "no inference upstream is available"}
}

// ErrInferenceFailed reports an upstream transport or protocol failure.
func ErrInferenceFailed() *Error {
	return &Error{CodeInferenceFailed, http.StatusBadGateway, "the upstream failed to complete the request"}
}

// ErrInternal reports an unexpected gateway failure.
func ErrInternal() *Error {
	return &Error{CodeInternalError, http.StatusInternalServerError, "internal server error"}
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

// Body returns the OpenAI-shaped JSON error body for e.
func (e *Error) Body() []byte {
	b, _ := json.Marshal(errorBody{errorDetail{Message: e.Message, Type: errorType(e.HTTPStatus), Code: e.Code}})
	return b
}

// WriteError writes e as a JSON response.
func WriteError(w http.ResponseWriter, e *Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.HTTPStatus)
	_, _ = w.Write(e.Body())
}

func errorType(status int) string {
	if status >= 500 {
		return "api_error"
	}
	return "invalid_request_error"
}
