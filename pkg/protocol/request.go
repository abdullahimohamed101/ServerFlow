// Package protocol defines the public, vendor-neutral types shared across
// ServerFlow components. It must not import any internal package.
package protocol

// HeaderRequestID is the response header carrying the gateway-assigned
// request ID.
const HeaderRequestID = "X-Request-ID"

// Message is one chat turn in a normalized request.
type Message struct {
	Role    string
	Content string
}

// InferenceRequest is the normalized internal representation of an
// inference request. Scheduling, admission, and cost estimation depend on
// this type, never on OpenAI wire structures. Zero MaxTokens means the
// client did not set a limit.
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
}
