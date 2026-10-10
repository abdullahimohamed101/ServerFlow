package gateway

import "time"

// The event values handed to an Observer. They hold plain values only (strings, numbers, durations, times):
// never the request or response body, a prompt, an API key, or a pointer into gateway state, so an observer
// can neither leak a secret nor change how a request is served. Add a field only when a phase needs it.

// MaxTraceHeaderBytes caps each propagated trace header. The values are otherwise unvalidated; an observer
// that parses them must tolerate garbage.
const MaxTraceHeaderBytes = 512

// TraceHeaders are the W3C trace context headers of an incoming request, as the client sent them.
type TraceHeaders struct {
	Traceparent string
	Tracestate  string
}

// RequestStart is the request as the middleware accepted it. It fires for inference requests
// (POST /v1/chat/completions), before authentication.
type RequestStart struct {
	ID           string
	Method       string
	Path         string
	Time         time.Time
	TraceHeaders TraceHeaders
}

// Admission describes a request that passed authentication, validation and rate limiting. Model is what the
// client asked for: in registry mode it has not yet been matched against the registry.
type Admission struct {
	RequestID string
	Model     string
	Stream    bool
	TenantID  string // empty when authentication is off
	// EstimatedCost is the token estimate charged to rate limits (0 when rate limiting is off).
	EstimatedCost int
	// RateLimitChecked is true when a limiter decided the request; RateLimitDuration is how long it took and
	// RateLimitBypassed says it was admitted unchecked because the limiter's store was unavailable.
	RateLimitChecked  bool
	RateLimitStart    time.Time
	RateLimitDuration time.Duration
	RateLimitBypassed bool
}

// Rejection kinds.
const (
	RejectAuth       = "auth"
	RejectRateLimit  = "rate_limit"
	RejectValidation = "validation"
	RejectModel      = "model"
	RejectCapacity   = "capacity"
	RejectInternal   = "internal" // the gateway itself is misconfigured (a required component was not wired)
)

// Rejection is a request refused before it reached a worker.
type Rejection struct {
	RequestID string
	TenantID  string
	// Kind is one of the Reject* constants. Reason is a short fixed token: the authentication failure
	// reason, the rate limit that refused (requests, tokens, concurrency, model, unavailable), or for
	// validation, model and capacity refusals the lower-case error code.
	Kind   string
	Reason string
	Status int
	// DecisionStart and DecisionDuration say when the deciding component began and how long it took: the
	// limiter for rate_limit, the scheduler for model and capacity refusals in registry mode, else zero.
	DecisionStart    time.Time
	DecisionDuration time.Duration
	// Model is the model the registry confirmed exists, set for capacity refusals; empty when the model
	// is unknown or was never checked (it is a metrics label, so client text never goes here).
	Model string
}

// AttemptStart is a worker having been chosen (registry mode) or the upstream about to be called (static mode).
type AttemptStart struct {
	RequestID string
	AttemptID string
	Number    int // 1-based
	// WorkerID is empty in static mode, where the single configured upstream is the whole fleet.
	WorkerID string
	Model    string
	Strategy string // scheduler strategy; empty in static mode
	// SelectDuration is how long choosing the worker took; SinceRequestStart is the request's age when the
	// attempt began.
	SelectDuration    time.Duration
	SinceRequestStart time.Duration
	WorkerState       string // the state the registry reported for the worker; empty in static mode
	// WorkerEligible is whether the gateway's registry view judged the chosen worker eligible when it was
	// chosen. A registry-mode selection with this false is a routing bug (scheduler_ineligible_selections_total).
	WorkerEligible bool
}

// FirstToken marks the first streamed chunk reaching the client.
type FirstToken struct {
	RequestID string
	AttemptID string
	TTFT      time.Duration // from the request being accepted
}

// Attempt outcomes (also the Prometheus label values of inference_attempts_total).
const (
	AttemptOK           = outcomeOK
	AttemptFailed       = outcomeFailed
	AttemptRetried      = outcomeRetried
	AttemptClientClosed = outcomeClientClosed
)

// AttemptEnd closes an attempt.
type AttemptEnd struct {
	RequestID string
	AttemptID string
	Number    int
	WorkerID  string // empty in static mode
	Model     string
	Outcome   string // one of the Attempt* constants
	Duration  time.Duration
	// Class is why the attempt failed or was retried: connect, reset, empty_stream, status_NNN, or "".
	Class string
	// WillRetry is true when the request moves to another worker. NextWorkerUnavailable is true when a retry
	// was wanted but no other worker could take it, so this attempt's response is the client's.
	WillRetry             bool
	NextWorkerUnavailable bool
}

// Completion is the final outcome of an inference request.
type Completion struct {
	RequestID string
	// Model is empty unless the model was confirmed to exist (it becomes a metrics label).
	Model     string
	Stream    bool
	TenantID  string
	Status    int // 499 when the client left, 502 when the connection was aborted after the response began
	ErrorCode string
	Duration  time.Duration
	Attempts  int           // attempts started
	TTFT      time.Duration // 0 unless a streamed chunk was sent
	// Handled is true when the request got past authentication into the inference handler; a refused
	// authentication completes with Handled=false.
	Handled bool
}
