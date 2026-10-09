package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"serverflow/pkg/protocol"
)

// Limits bounds what ParseChatRequest accepts.
type Limits struct {
	Models         []string
	MaxTokensLimit int
	// AnyModel skips the check against Models. The gateway sets it when the
	// worker registry, not a static list, decides which models exist.
	AnyModel bool
	// Allowed, when set, restricts the request to models it accepts (a tenant's allow-list). It is
	// checked before the model is looked up, so a model that does not exist and one that is
	// merely not allowed get the same answer (ErrModelForbidden).
	Allowed func(model string) bool
}

// Fields the gateway validates. Go's encoding/json matches keys
// case-insensitively and keeps the last duplicate, while Python upstreams
// such as vLLM match exactly. Because the original body is forwarded
// unchanged (ADR-003), any ambiguity here would let a client show the gateway
// one value and the upstream another, so ambiguous bodies are rejected.
var (
	requestFields = []string{"model", "messages", "stream", "max_tokens", "max_completion_tokens", "temperature"}
	messageFields = []string{"role", "content", "tool_calls", "function_call"}
)

var validRoles = map[string]bool{
	"system": true, "developer": true, "user": true, "assistant": true, "tool": true,
	"function": true, // legacy OpenAI role, still accepted by OpenAI-compatible servers
}

// ParseChatRequest validates an OpenAI chat completion request body and
// normalizes it. Fields the gateway does not model are ignored here; the
// caller forwards the original body upstream unchanged. The returned
// error, if any, is an *Error.
func ParseChatRequest(body []byte, lim Limits) (*protocol.InferenceRequest, error) {
	fields, err := strictObject(body, requestFields)
	if err != nil {
		return nil, ErrInvalidRequest(err.Error())
	}

	var model string
	if raw, ok := fields["model"]; !ok {
		return nil, ErrInvalidRequest("`model` is required")
	} else if err := json.Unmarshal(raw, &model); err != nil || model == "" {
		return nil, ErrInvalidRequest("`model` must be a non-empty string")
	}
	if lim.Allowed != nil && !lim.Allowed(model) {
		return nil, ErrModelForbidden(model)
	}
	if !lim.AnyModel && !contains(lim.Models, model) {
		return nil, ErrModelNotFound(model)
	}

	var rawMsgs []json.RawMessage
	if raw, ok := fields["messages"]; !ok || json.Unmarshal(raw, &rawMsgs) != nil || len(rawMsgs) == 0 {
		return nil, ErrInvalidRequest("`messages` must be an array with at least one message")
	}
	msgs := make([]protocol.Message, 0, len(rawMsgs))
	for i, rm := range rawMsgs {
		m, err := parseMessage(rm)
		if err != nil {
			return nil, ErrInvalidRequest(fmt.Sprintf("messages[%d]: %s", i, err))
		}
		msgs = append(msgs, m)
	}

	var stream bool
	if err := decodeField(fields, "stream", &stream); err != nil {
		return nil, err
	}

	// Every token field that is present must be within the limit, not just
	// the first: the upstream may honor either one. max_completion_tokens
	// takes precedence, as it does in vLLM.
	maxTokens := 0
	for _, name := range []string{"max_tokens", "max_completion_tokens"} {
		var n *int
		if err := decodeField(fields, name, &n); err != nil {
			return nil, err
		}
		if n == nil {
			continue
		}
		if *n <= 0 || *n > lim.MaxTokensLimit {
			return nil, ErrInvalidRequest(fmt.Sprintf("`%s` must be between 1 and %d", name, lim.MaxTokensLimit))
		}
		maxTokens = *n
	}

	var temp *float64
	if err := decodeField(fields, "temperature", &temp); err != nil {
		return nil, err
	}
	temperature := 0.0
	if temp != nil {
		if *temp < 0 || *temp > 2 {
			return nil, ErrInvalidRequest("`temperature` must be between 0 and 2")
		}
		temperature = *temp
	}

	return &protocol.InferenceRequest{
		Model:       model,
		Messages:    msgs,
		Stream:      stream,
		MaxTokens:   maxTokens,
		Temperature: temperature,
	}, nil
}

func parseMessage(raw json.RawMessage) (protocol.Message, error) {
	fields, err := strictObject(raw, messageFields)
	if err != nil {
		return protocol.Message{}, err
	}
	var role string
	if err := decodeRaw(fields["role"], &role); err != nil || !validRoles[role] {
		return protocol.Message{}, fmt.Errorf("role %q is not supported", truncate(role, 32))
	}
	// An assistant message that carries tool calls legitimately has null or
	// missing content; every other message needs content.
	content, hasContent := fields["content"]
	if !hasContent || string(content) == "null" {
		if role == "assistant" && (present(fields["tool_calls"]) || present(fields["function_call"])) {
			return protocol.Message{Role: role}, nil
		}
		return protocol.Message{}, fmt.Errorf("content is required")
	}
	text, err := messageText(content)
	if err != nil {
		return protocol.Message{}, fmt.Errorf("content %s", err)
	}
	return protocol.Message{Role: role, Content: text}, nil
}

// strictObject decodes a JSON object into its raw top-level fields. It
// rejects non-objects, trailing data, duplicate keys, and keys that match one
// of known only case-insensitively.
func strictObject(raw []byte, known []string) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if d, ok := tok.(json.Delim); err != nil || !ok || d != '{' {
		return nil, fmt.Errorf("body must be a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for dec.More() {
		kt, err := dec.Token()
		key, isString := kt.(string)
		if err != nil || !isString {
			return nil, fmt.Errorf("body is not valid JSON")
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("body is not valid JSON")
		}
		if _, dup := fields[key]; dup {
			return nil, fmt.Errorf("duplicate key %q is ambiguous", truncate(key, 64))
		}
		fields[key] = v
	}
	if _, err := dec.Token(); err != nil { // closing brace
		return nil, fmt.Errorf("body is not valid JSON")
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("body has data after the JSON object")
	}
	for key := range fields {
		lower := strings.ToLower(key)
		for _, k := range known {
			if lower == k && key != k {
				return nil, fmt.Errorf("key %q is ambiguous with %q; keys must be lowercase", truncate(key, 64), k)
			}
		}
	}
	return fields, nil
}

// decodeField decodes fields[name] into dst if present. JSON null leaves dst
// at its zero value, which for pointer destinations means "not set".
func decodeField(fields map[string]json.RawMessage, name string, dst any) *Error {
	raw, ok := fields[name]
	if !ok {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return ErrInvalidRequest(fmt.Sprintf("`%s` has the wrong type", name))
	}
	return nil
}

func decodeRaw(raw json.RawMessage, dst any) error {
	if raw == nil {
		return fmt.Errorf("missing")
	}
	return json.Unmarshal(raw, dst)
}

func present(raw json.RawMessage) bool { return raw != nil && string(raw) != "null" }

// messageText returns the text of a message content value, which OpenAI
// allows to be a string or an array of content parts.
func messageText(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("must be a string or an array of content parts")
	}
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p.Text)
	}
	return b.String(), nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// truncate shortens client-supplied text echoed in error messages so a
// request cannot make the gateway reflect a megabyte back at it.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
