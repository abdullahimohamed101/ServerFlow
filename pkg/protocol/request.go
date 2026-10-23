// Package protocol defines the public, vendor-neutral types shared across
// ServerFlow components. It must not import any internal package.
package protocol

// HeaderRequestID is the response header carrying the gateway-assigned
// request ID.
const HeaderRequestID = "X-Request-ID"

// HeaderAttemptID names the request header that carries the gateway's attempt ID to a worker, so
// the worker's own logs can be tied to one attempt of a request (spec section 7).
const HeaderAttemptID = "X-Attempt-ID"

// Message is one chat turn in a normalized request.
type Message struct {
	Role    string
	Content string
}

// InferenceRequest is the normalized internal representation of an
// inference request. Scheduling, admission, and cost estimation depend on
// this type, never on OpenAI wire structures. Zero MaxTokens means the
// client did not set a limit.
//
// EstInputTokens and EstOutputTokens are the gateway's own cost estimate (internal/cost, spec section 17), set once
// by the gateway handler after the body is parsed: the estimated prompt tokens and the output budget (max_tokens, or
// the gateway's limit when the client named none). They are Go-only (json:"-"): never read from or written to a wire
// format, so a client cannot supply them. Zero means "not estimated" (a request built outside the handler).
type InferenceRequest struct {
	RequestID   string
	TenantID    string
	Model       string
	Messages    []Message
	Prompt      string
	Stream      bool
	MaxTokens   int
	Temperature float64
	Priority    int

	EstInputTokens  int `json:"-"`
	EstOutputTokens int `json:"-"`
}
