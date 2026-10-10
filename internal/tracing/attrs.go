package tracing

import "go.opentelemetry.io/otel/attribute"

// Attribute keys. Every key a span, span event or link may carry is a constant here, and AllowedKeys lists
// them all; TestEmittedKeysAreAllowListed fails when an exported span uses a key that is not in the list
// (ADR-018). Keys that would carry a secret or unbounded text are deliberately absent: prompt or response
// text, any header value, the API key or its prefix, the API key ID, the tenant name, a client address,
// a raw URL or query, a worker address, and error strings.
const (
	KeyHTTPMethod       = attribute.Key("http.request.method")
	KeyHTTPRoute        = attribute.Key("http.route")
	KeyHTTPStatusCode   = attribute.Key("http.response.status_code")
	KeyRequestID        = attribute.Key("serverflow.request_id")
	KeyAttemptID        = attribute.Key("serverflow.attempt_id")
	KeyAttempt          = attribute.Key("serverflow.attempt")
	KeyAttempts         = attribute.Key("serverflow.attempts")
	KeyWorkerID         = attribute.Key("serverflow.worker_id")
	KeyModel            = attribute.Key("serverflow.model")
	KeyStream           = attribute.Key("serverflow.stream")
	KeyTenantID         = attribute.Key("serverflow.tenant_id")
	KeyErrorCode        = attribute.Key("serverflow.error_code")
	KeyRejectKind       = attribute.Key("serverflow.reject.kind")
	KeyAttemptOutcome   = attribute.Key("serverflow.attempt.outcome")
	KeyAttemptClass     = attribute.Key("serverflow.attempt.class")
	KeyNextWorkerAbsent = attribute.Key("serverflow.retry.next_worker_unavailable")
	KeyNextAttempt      = attribute.Key("serverflow.retry.next_attempt")
	KeyRateOutcome      = attribute.Key("serverflow.rate_limit.outcome")
	KeyRateLimit        = attribute.Key("serverflow.rate_limit.limit")
	KeyEstCost          = attribute.Key("serverflow.est_cost")
	KeyStrategy         = attribute.Key("serverflow.scheduler.strategy")
	KeySelectOutcome    = attribute.Key("serverflow.scheduler.outcome")
	KeyTTFTMillis       = attribute.Key("serverflow.ttft_ms")
	KeyClientClosed     = attribute.Key("serverflow.client_closed")
	KeyLink             = attribute.Key("serverflow.link")
	KeyQueueMillis      = attribute.Key("queue_ms")
	KeyPromptTokens     = attribute.Key("serverflow.prompt_tokens")
	KeyOutputTokens     = attribute.Key("serverflow.output_tokens")
	KeyInjectedFailure  = attribute.Key("serverflow.injected_failure")
	KeyServiceName      = attribute.Key("service.name")
	KeyServiceVersion   = attribute.Key("service.version")
	KeyServiceInstance  = attribute.Key("service.instance.id")
)

// AllowedKeys is the complete attribute vocabulary, resource attributes included.
var AllowedKeys = map[attribute.Key]struct{}{
	KeyHTTPMethod: {}, KeyHTTPRoute: {}, KeyHTTPStatusCode: {}, KeyRequestID: {}, KeyAttemptID: {}, KeyAttempt: {},
	KeyAttempts: {}, KeyWorkerID: {}, KeyModel: {}, KeyStream: {}, KeyTenantID: {}, KeyErrorCode: {}, KeyRejectKind: {},
	KeyAttemptOutcome: {}, KeyAttemptClass: {}, KeyNextWorkerAbsent: {}, KeyNextAttempt: {}, KeyRateOutcome: {}, KeyRateLimit: {},
	KeyEstCost: {}, KeyStrategy: {}, KeySelectOutcome: {}, KeyTTFTMillis: {}, KeyClientClosed: {}, KeyLink: {}, KeyQueueMillis: {},
	KeyPromptTokens: {}, KeyOutputTokens: {}, KeyInjectedFailure: {}, KeyServiceName: {}, KeyServiceVersion: {}, KeyServiceInstance: {},
}

// Span names. All are fixed strings: a name never carries request data.
const (
	SpanReceive       = "gateway.receive"
	SpanRateLimit     = "rate_limit"
	SpanSelect        = "scheduler.select"
	SpanForward       = "worker.forward"
	SpanFirstToken    = "first_token"
	SpanCompletion    = "completion"
	SpanInference     = "inference"
	SpanQueueWait     = "queue_wait"
	EventRetry        = "retry"
	EventFirstToken   = "first_token"
	LinkRetryOf       = "retry_of"
	LinkClientTrace   = "client_traceparent"
	MaxAttrValueBytes = 128 // longest string any attribute may carry (the SDK limit enforces it too)
)
