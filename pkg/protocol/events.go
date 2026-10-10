package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// This file is the contract of the inference lifecycle events published to Kafka (Phase 12, ADR-019). The
// publisher (internal/events) and the consumer (internal/usage) share these types and nothing else, so
// neither imports the other's packages. Evolution rules are in docs/events/inference-lifecycle-v1.md.
//
// Events are content-free by construction: every field is a primitive, an ID, a count or an enum value.
// There is deliberately no field that could hold a prompt, a response, a header, an API key or free text.

// Topic and version of the lifecycle stream.
const (
	// EventsTopicV1 is the topic that carries SchemaVersion 1 events. The topic name carries the major version.
	EventsTopicV1 = "inference.lifecycle.v1"
	// EventSchemaVersion is the schema version this code writes.
	EventSchemaVersion = 1
	// MaxEventBytes caps one encoded event. A larger one is refused by Encode and by Decode.
	MaxEventBytes = 16 << 10
	// MaxEventTokens bounds every token count in an event (input, output, estimate): ten billion. No request comes
	// near it (the largest context windows are about a million tokens), it is below the 12 digits the gateway's
	// scanner will read, and it keeps sums of many rows far from the int64 range. A bigger claim is a forged or
	// corrupt event and is rejected like any other invalid one.
	MaxEventTokens = 10_000_000_000
	// MaxEventMillis bounds durations in an event (a year of milliseconds is 3.2e10; this allows about 31 years).
	MaxEventMillis = 1_000_000_000_000
	// MaxEventAttempts caps attempts[] in a terminal event.
	MaxEventAttempts = 16
)

// Event types.
const (
	EventReceived   = "inference.request.received"
	EventRouted     = "inference.request.routed"
	EventFirstToken = "inference.request.first_token"
	EventCompleted  = "inference.request.completed"
	EventFailed     = "inference.request.failed"
)

// Failure classes of a failed request: a closed set, so no upstream text reaches an event.
const (
	FailureNoCapacity        = "no_capacity"
	FailureWorkerUnavailable = "worker_unavailable"
	FailureWorkerError       = "worker_error"
	FailureTimeout           = "timeout"
	FailureClientClosed      = "client_closed"
	FailureInternal          = "internal"
)

// Sources of token counts (see TerminalData).
const (
	TokensFromUsage    = "usage"    // the worker's usage object
	TokensFromChunks   = "chunks"   // a count of streamed content chunks, an approximation of output tokens
	TokensFromEstimate = "estimate" // nothing was reported; only EstimatedCostTokens is meaningful
)

// Errors returned by Validate, Encode and Decode. Callers match them with errors.Is.
var (
	ErrEventInvalid            = errors.New("invalid event")
	ErrEventTooLarge           = errors.New("event too large")
	ErrEventVersionUnsupported = errors.New("event schema version not supported")
)

// Length caps for the string fields.
const (
	maxEventID    = 80
	maxEventShort = 64
	maxEventModel = 128
)

// Event is one lifecycle event: an envelope plus exactly one payload, selected by EventType.
type Event struct {
	EventID       string
	EventType     string
	SchemaVersion int
	Timestamp     time.Time // when it happened on the gateway; UTC
	Source        string    // the gateway instance
	RequestID     string
	AttemptID     string // the attempt concerned; empty for received
	TenantID      string // empty when authentication is off
	APIKeyID      string // the key_ ID, never the key or its prefix
	Model         string
	WorkerID      string // empty before routing

	Received   *ReceivedData
	Routed     *RoutedData
	FirstToken *FirstTokenData
	Terminal   *TerminalData // for completed and failed
}

// ReceivedData is the payload of inference.request.received.
type ReceivedData struct {
	Stream              bool  `json:"stream"`
	EstimatedCostTokens int64 `json:"estimated_cost_tokens"`
}

// RoutedData is the payload of inference.request.routed.
type RoutedData struct {
	AttemptNumber int    `json:"attempt_number"`
	Strategy      string `json:"strategy,omitempty"`
}

// FirstTokenData is the payload of inference.request.first_token.
type FirstTokenData struct {
	TTFTMS int64 `json:"ttft_ms"`
}

// AttemptData is one entry of attempts[]: the append-only history of a request's tries.
type AttemptData struct {
	AttemptID    string `json:"attempt_id"`
	Number       int    `json:"number"`
	WorkerID     string `json:"worker_id,omitempty"`
	Outcome      string `json:"outcome"`
	FailureClass string `json:"failure_class,omitempty"`
	DurationMS   int64  `json:"duration_ms"`
}

// TerminalData is the payload of completed and failed. InputTokens and OutputTokens are nil when unknown
// (unknown is not zero); TokensSource says how good the numbers are.
type TerminalData struct {
	Stream              bool          `json:"stream"`
	HTTPStatus          int           `json:"http_status"`
	DurationMS          int64         `json:"duration_ms"`
	TTFTMS              int64         `json:"ttft_ms,omitempty"`
	InputTokens         *int64        `json:"input_tokens,omitempty"`
	OutputTokens        *int64        `json:"output_tokens,omitempty"`
	TokensSource        string        `json:"tokens_source"`
	EstimatedCostTokens int64         `json:"estimated_cost_tokens"`
	FailureClass        string        `json:"failure_class,omitempty"` // failed events only
	Attempts            []AttemptData `json:"attempts,omitempty"`
}

// IsTerminal reports whether the type ends a request.
func IsTerminalEventType(t string) bool { return t == EventCompleted || t == EventFailed }

// NewEventID derives the event ID from what makes an event unique. It is deterministic, so emitting the same
// logical event twice yields the same ID and cannot be counted twice (spec section 22).
func NewEventID(requestID, eventType, attemptID string) string {
	h := sha256.New()
	h.Write([]byte(requestID))
	h.Write([]byte{0})
	h.Write([]byte(eventType))
	h.Write([]byte{0})
	h.Write([]byte(attemptID))
	return "evt_" + hex.EncodeToString(h.Sum(nil))[:32]
}

// wire is the JSON shape: the envelope with the payload under one key.
type wire struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	Timestamp     time.Time       `json:"timestamp"`
	Source        string          `json:"source"`
	RequestID     string          `json:"request_id"`
	AttemptID     string          `json:"attempt_id,omitempty"`
	TenantID      string          `json:"tenant_id,omitempty"`
	APIKeyID      string          `json:"api_key_id,omitempty"`
	Model         string          `json:"model"`
	WorkerID      string          `json:"worker_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrEventInvalid, fmt.Sprintf(format, args...))
}

func checkString(name, v string, max int, required bool) error {
	if required && v == "" {
		return invalid("%s is empty", name)
	}
	if len(v) > max {
		return invalid("%s is longer than %d bytes", name, max)
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] == 0x7f {
			return invalid("%s has a control character", name)
		}
	}
	return nil
}

func checkPrefixed(name, v, prefix string, required bool) error {
	if err := checkString(name, v, maxEventID, required); err != nil {
		return err
	}
	if v != "" && !strings.HasPrefix(v, prefix) {
		return invalid("%s does not start with %q", name, prefix)
	}
	return nil
}

// Validate checks an event the way a consumer must before trusting it. It never echoes a field's value.
func (e Event) Validate() error {
	if e.SchemaVersion != EventSchemaVersion {
		if e.SchemaVersion > EventSchemaVersion {
			return fmt.Errorf("%w: %d", ErrEventVersionUnsupported, e.SchemaVersion)
		}
		return invalid("schema_version %d", e.SchemaVersion)
	}
	if err := checkPrefixed("event_id", e.EventID, "evt_", true); err != nil {
		return err
	}
	if err := checkPrefixed("request_id", e.RequestID, "req_", true); err != nil {
		return err
	}
	if err := checkPrefixed("attempt_id", e.AttemptID, "att_", false); err != nil {
		return err
	}
	if err := checkPrefixed("api_key_id", e.APIKeyID, "key_", false); err != nil {
		return err
	}
	if err := checkPrefixed("tenant_id", e.TenantID, "ten_", false); err != nil {
		return err
	}
	for _, f := range []struct {
		name, v string
		max     int
		req     bool
	}{
		{"source", e.Source, maxEventShort, true},
		{"model", e.Model, maxEventModel, false},
		{"worker_id", e.WorkerID, maxEventShort, false},
	} {
		if err := checkString(f.name, f.v, f.max, f.req); err != nil {
			return err
		}
	}
	if e.Timestamp.IsZero() {
		return invalid("timestamp is zero")
	}
	if want := NewEventID(e.RequestID, e.EventType, e.AttemptID); e.EventID != want {
		return invalid("event_id does not match request_id, event_type and attempt_id")
	}
	return e.validatePayload()
}

func (e Event) validatePayload() error {
	payloads := 0
	for _, set := range []bool{e.Received != nil, e.Routed != nil, e.FirstToken != nil, e.Terminal != nil} {
		if set {
			payloads++
		}
	}
	if payloads != 1 {
		return invalid("exactly one payload is required")
	}
	need := func(ok bool) error {
		if !ok {
			return invalid("payload does not match event_type")
		}
		return nil
	}
	switch e.EventType {
	case EventReceived:
		if err := need(e.Received != nil); err != nil {
			return err
		}
		if e.Received.EstimatedCostTokens < 0 || e.Received.EstimatedCostTokens > MaxEventTokens {
			return invalid("estimated_cost_tokens is out of range")
		}
	case EventRouted:
		if err := need(e.Routed != nil); err != nil {
			return err
		}
		if e.AttemptID == "" || e.Routed.AttemptNumber < 1 {
			return invalid("routed needs attempt_id and attempt_number")
		}
		return checkString("strategy", e.Routed.Strategy, maxEventShort, false)
	case EventFirstToken:
		if err := need(e.FirstToken != nil); err != nil {
			return err
		}
		if e.FirstToken.TTFTMS < 0 || e.FirstToken.TTFTMS > MaxEventMillis {
			return invalid("ttft_ms is out of range")
		}
	case EventCompleted, EventFailed:
		if err := need(e.Terminal != nil); err != nil {
			return err
		}
		return e.validateTerminal()
	default:
		return invalid("unknown event_type")
	}
	return nil
}

func (e Event) validateTerminal() error {
	t := e.Terminal
	if t.HTTPStatus < 100 || t.HTTPStatus > 599 {
		return invalid("http_status out of range")
	}
	if t.DurationMS < 0 || t.DurationMS > MaxEventMillis || t.TTFTMS < 0 || t.TTFTMS > MaxEventMillis || t.EstimatedCostTokens < 0 || t.EstimatedCostTokens > MaxEventTokens {
		return invalid("a duration or count is out of range")
	}
	for _, n := range []*int64{t.InputTokens, t.OutputTokens} {
		if n != nil && (*n < 0 || *n > MaxEventTokens) {
			return invalid("a token count is out of range")
		}
	}
	switch t.TokensSource {
	case TokensFromUsage, TokensFromChunks, TokensFromEstimate:
	default:
		return invalid("tokens_source is not one of usage, chunks, estimate")
	}
	if t.TokensSource == TokensFromEstimate && (t.InputTokens != nil || t.OutputTokens != nil) {
		return invalid("token counts present with tokens_source estimate")
	}
	if e.EventType == EventFailed {
		switch t.FailureClass {
		case FailureNoCapacity, FailureWorkerUnavailable, FailureWorkerError, FailureTimeout, FailureClientClosed, FailureInternal:
		default:
			return invalid("failed event has an unknown failure_class")
		}
	} else if t.FailureClass != "" {
		return invalid("completed event has a failure_class")
	}
	if len(t.Attempts) > MaxEventAttempts {
		return invalid("more than %d attempts", MaxEventAttempts)
	}
	for _, a := range t.Attempts {
		if err := checkPrefixed("attempts.attempt_id", a.AttemptID, "att_", true); err != nil {
			return err
		}
		if err := checkString("attempts.worker_id", a.WorkerID, maxEventShort, false); err != nil {
			return err
		}
		if err := checkString("attempts.outcome", a.Outcome, maxEventShort, true); err != nil {
			return err
		}
		if err := checkString("attempts.failure_class", a.FailureClass, maxEventShort, false); err != nil {
			return err
		}
		if a.Number < 1 || a.DurationMS < 0 || a.DurationMS > MaxEventMillis {
			return invalid("attempts has a bad number or duration")
		}
	}
	return nil
}

// Encode validates an event and returns its JSON. It refuses an event without a correct ID, an over-long
// string, or an encoding over MaxEventBytes; it never drops a field silently.
func Encode(e Event) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	var payload any
	switch {
	case e.Received != nil:
		payload = e.Received
	case e.Routed != nil:
		payload = e.Routed
	case e.FirstToken != nil:
		payload = e.FirstToken
	default:
		payload = e.Terminal
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, invalid("payload: %v", err)
	}
	out, err := json.Marshal(wire{
		EventID: e.EventID, EventType: e.EventType, SchemaVersion: e.SchemaVersion, Timestamp: e.Timestamp.UTC(),
		Source: e.Source, RequestID: e.RequestID, AttemptID: e.AttemptID, TenantID: e.TenantID, APIKeyID: e.APIKeyID,
		Model: e.Model, WorkerID: e.WorkerID, Payload: raw,
	})
	if err != nil {
		return nil, invalid("encode: %v", err)
	}
	if len(out) > MaxEventBytes {
		return nil, fmt.Errorf("%w: %d bytes", ErrEventTooLarge, len(out))
	}
	return out, nil
}

// Decode parses and validates one event. Unknown fields are ignored (additive evolution); a schema_version
// newer than this code supports returns ErrEventVersionUnsupported without interpreting the rest.
func Decode(b []byte) (Event, error) {
	if len(b) > MaxEventBytes {
		return Event{}, fmt.Errorf("%w: %d bytes", ErrEventTooLarge, len(b))
	}
	// Read the version first so a future shape is rejected as such, not as a confusing field error.
	var head struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return Event{}, invalid("not a JSON object")
	}
	if head.SchemaVersion > EventSchemaVersion {
		return Event{}, fmt.Errorf("%w: %d", ErrEventVersionUnsupported, head.SchemaVersion)
	}
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return Event{}, invalid("malformed event")
	}
	e := Event{
		EventID: w.EventID, EventType: w.EventType, SchemaVersion: w.SchemaVersion, Timestamp: w.Timestamp.UTC(),
		Source: w.Source, RequestID: w.RequestID, AttemptID: w.AttemptID, TenantID: w.TenantID, APIKeyID: w.APIKeyID,
		Model: w.Model, WorkerID: w.WorkerID,
	}
	dec := func(dst any) error {
		if len(bytes.TrimSpace(w.Payload)) == 0 {
			return invalid("payload is missing")
		}
		if err := json.Unmarshal(w.Payload, dst); err != nil {
			return invalid("payload is malformed")
		}
		return nil
	}
	var err error
	switch e.EventType {
	case EventReceived:
		e.Received = new(ReceivedData)
		err = dec(e.Received)
	case EventRouted:
		e.Routed = new(RoutedData)
		err = dec(e.Routed)
	case EventFirstToken:
		e.FirstToken = new(FirstTokenData)
		err = dec(e.FirstToken)
	case EventCompleted, EventFailed:
		e.Terminal = new(TerminalData)
		err = dec(e.Terminal)
	default:
		return Event{}, invalid("unknown event_type")
	}
	if err != nil {
		return Event{}, err
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}
