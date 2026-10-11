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
	// APIKeyID is the ID (key_...) of the key that authenticated the request; never the key or its prefix. Empty
	// when authentication is off.
	APIKeyID string
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
	// RejectOverload is a request shed because the gateway or the model's queue is full or draining (Phase 15,
	// not yet fired). It is not a scheduling decision and must not feed scheduler_decisions_total.
	RejectOverload = "overload"
)

// Reasons of a RejectOverload rejection (Phase 15, not yet fired).
const (
	OverloadGlobalLimit  = "global_limit"
	OverloadQueueFull    = "queue_full"
	OverloadQueueTimeout = "queue_timeout"
	OverloadDraining     = "draining"
)

// Rejection is a request refused before it reached a worker.
type Rejection struct {
	RequestID string
	TenantID  string
	// Kind is one of the Reject* constants. Reason is a short fixed token: the authentication failure
	// reason, the rate limit that refused (requests, tokens, concurrency, model, unavailable), for
	// validation, model and capacity refusals the lower-case error code, or for overload one of the
	// Overload* constants.
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
	// Probe is true when the attempt is a circuit breaker's trial request to a worker it had taken out of
	// routing. QueueWait is how long the request waited in the admission queue before this attempt (it is
	// excluded from the gateway overhead measurement). Both are zero until Phase 15 fires them.
	Probe     bool
	QueueWait time.Duration
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
	// NextRouteResult says why a wanted retry found no worker: one of the NextRoute* constants, or "" when
	// the retry was routed or none was wanted (and when the client left during the second routing). It feeds
	// the scheduling decision counters, as a first routing's refusal does.
	NextRouteResult string
	// RetryDenied says why a wanted retry was not made although another worker might exist: one of the
	// RetryDenied* constants, or "". Never set until Phase 15 adds the retry budget and deadline.
	RetryDenied string
}

// Values of AttemptEnd.NextRouteResult.
const (
	NextRouteNoCapacity        = "no_capacity"        // every other worker is excluded, ineligible or full
	NextRouteWorkerUnavailable = "worker_unavailable" // the registry view was missing or too old
	NextRouteError             = "error"              // anything else (the model vanished, the scheduler failed)
)

// Values of AttemptEnd.RetryDenied.
const (
	RetryDeniedBudget   = "budget"
	RetryDeniedDeadline = "deadline"
)

// Results of a QueueWait.
const (
	QueueServed     = "served"      // the request got a worker slot
	QueueTimeout    = "timeout"     // it waited the maximum and was shed
	QueueClientGone = "client_gone" // the client left while it waited
	QueueDrained    = "drained"     // the gateway began shutting down
)

// QueueWait is a request that waited in the admission queue (Phase 15, not yet fired). It fires once per
// request that actually waited, and never for one that did not.
type QueueWait struct {
	RequestID string
	Model     string // the registry-confirmed model
	Waited    time.Duration
	Result    string // one of the Queue* constants
}

// BreakerChange is a worker's circuit breaker changing state (Phase 15, not yet fired). It is delivered on a
// background context and is not part of any request.
type BreakerChange struct {
	WorkerID string
	Model    string
	To       string // closed, open or half_open
	Reason   string // a short fixed token naming what caused the change
	Failures int    // faults counted when it opened
	CoolDown time.Duration
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
	// TokensSource says where the token counts came from: TokensUsage (the worker's usage object: both counts),
	// TokensChunks (streamed content chunks counted: OutputTokens only, an approximation), or "" (nothing was
	// reported: both counts are meaningless). Phase 12 only.
	TokensSource string
	InputTokens  int64
	OutputTokens int64
}

// Token count sources for Completion.TokensSource.
const (
	TokensUsage  = "usage"
	TokensChunks = "chunks"
)
