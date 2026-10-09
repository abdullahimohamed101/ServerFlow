package api

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

var testLimits = Limits{Models: []string{"qwen-7b", "llama-8b"}, MaxTokensLimit: 1000}

func TestParseChatRequestValid(t *testing.T) {
	body := `{"model":"qwen-7b","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}],"stream":true,"max_tokens":50,"temperature":0.7,"top_p":0.9}`
	req, err := ParseChatRequest([]byte(body), testLimits)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Model != "qwen-7b" || !req.Stream || req.MaxTokens != 50 || req.Temperature != 0.7 {
		t.Fatalf("unexpected request %+v", req)
	}
	if len(req.Messages) != 2 || req.Messages[1].Role != "user" || req.Messages[1].Content != "hi" {
		t.Fatalf("unexpected messages %+v", req.Messages)
	}
}

func TestParseChatRequestContentParts(t *testing.T) {
	body := `{"model":"qwen-7b","messages":[{"role":"user","content":[{"type":"text","text":"hello "},{"type":"text","text":"world"}]}]}`
	req, err := ParseChatRequest([]byte(body), testLimits)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := req.Messages[0].Content; got != "hello world" {
		t.Fatalf("expected joined text, got %q", got)
	}
}

func TestParseChatRequestMaxCompletionTokens(t *testing.T) {
	body := `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":20}`
	req, err := ParseChatRequest([]byte(body), testLimits)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.MaxTokens != 20 {
		t.Fatalf("expected max tokens 20, got %d", req.MaxTokens)
	}
}

func TestParseChatRequestErrors(t *testing.T) {
	user := `[{"role":"user","content":"hi"}]`
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"not json", `{nope`, 400, CodeInvalidRequest},
		{"trailing data", `{"model":"qwen-7b","messages":` + user + `} x`, 400, CodeInvalidRequest},
		{"wrong type", `{"model":5,"messages":` + user + `}`, 400, CodeInvalidRequest},
		{"missing model", `{"messages":` + user + `}`, 400, CodeInvalidRequest},
		{"unknown model", `{"model":"nope","messages":` + user + `}`, 404, CodeModelNotFound},
		{"missing messages", `{"model":"qwen-7b"}`, 400, CodeInvalidRequest},
		{"empty messages", `{"model":"qwen-7b","messages":[]}`, 400, CodeInvalidRequest},
		{"bad role", `{"model":"qwen-7b","messages":[{"role":"robot","content":"hi"}]}`, 400, CodeInvalidRequest},
		{"missing content", `{"model":"qwen-7b","messages":[{"role":"user"}]}`, 400, CodeInvalidRequest},
		{"null content", `{"model":"qwen-7b","messages":[{"role":"user","content":null}]}`, 400, CodeInvalidRequest},
		{"bad content type", `{"model":"qwen-7b","messages":[{"role":"user","content":5}]}`, 400, CodeInvalidRequest},
		{"zero max_tokens", `{"model":"qwen-7b","messages":` + user + `,"max_tokens":0}`, 400, CodeInvalidRequest},
		{"negative max_tokens", `{"model":"qwen-7b","messages":` + user + `,"max_tokens":-1}`, 400, CodeInvalidRequest},
		{"too many max_tokens", `{"model":"qwen-7b","messages":` + user + `,"max_tokens":1001}`, 400, CodeInvalidRequest},
		{"too many max_completion_tokens", `{"model":"qwen-7b","messages":` + user + `,"max_completion_tokens":1001}`, 400, CodeInvalidRequest},
		{"temperature too high", `{"model":"qwen-7b","messages":` + user + `,"temperature":2.5}`, 400, CodeInvalidRequest},
		{"temperature negative", `{"model":"qwen-7b","messages":` + user + `,"temperature":-0.1}`, 400, CodeInvalidRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseChatRequest([]byte(tt.body), testLimits)
			var apiErr *Error
			if !errors.As(err, &apiErr) {
				t.Fatalf("expected *Error, got %v", err)
			}
			if apiErr.HTTPStatus != tt.wantStatus || apiErr.Code != tt.wantCode {
				t.Fatalf("got %d %s, want %d %s", apiErr.HTTPStatus, apiErr.Code, tt.wantStatus, tt.wantCode)
			}
		})
	}
}

func TestErrorBodyShape(t *testing.T) {
	var got struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(ErrModelNotFound("x").Body(), &got); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if got.Error.Code != CodeModelNotFound || got.Error.Type != "invalid_request_error" || !strings.Contains(got.Error.Message, "x") {
		t.Fatalf("unexpected body %+v", got)
	}
}

func TestErrorStatusAndType(t *testing.T) {
	tests := []struct {
		err      *Error
		status   int
		wantType string
	}{
		{ErrInvalidRequest("m"), 400, "invalid_request_error"},
		{ErrBodyTooLarge(), 413, "invalid_request_error"},
		{ErrModelNotFound("m"), 404, "invalid_request_error"},
		{ErrUpstreamTimeout(), 504, "api_error"},
		{ErrWorkerUnavailable(), 503, "api_error"},
		{ErrInferenceFailed(), 502, "api_error"},
		{ErrInternal(), 500, "api_error"},
	}
	for _, tt := range tests {
		t.Run(tt.err.Code, func(t *testing.T) {
			rec := httptest.NewRecorder()
			WriteError(rec, tt.err)
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d", rec.Code, tt.status)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type %q", ct)
			}
			var got struct {
				Error struct{ Type string } `json:"error"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &got)
			if got.Error.Type != tt.wantType {
				t.Fatalf("type %q, want %q", got.Error.Type, tt.wantType)
			}
		})
	}
}

func TestModelsBody(t *testing.T) {
	var got struct {
		Object string `json:"object"`
		Data   []struct {
			ID     string `json:"id"`
			Object string `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(ModelsBody([]string{"a", "b"}), &got); err != nil {
		t.Fatal(err)
	}
	if got.Object != "list" || len(got.Data) != 2 || got.Data[0].ID != "a" || got.Data[1].Object != "model" {
		t.Fatalf("unexpected models body %+v", got)
	}
}

// --- parser-differential and compatibility fixes (independent review) ---------

func TestParseChatRequestRejectsAmbiguousKeys(t *testing.T) {
	user := `"messages":[{"role":"user","content":"hi"}]`
	tests := []struct{ name, body string }{
		// A case-insensitive decoder (Go) and a case-sensitive upstream
		// (Python) would read these differently; the gateway must refuse them.
		{"case variant of model", `{"model":"nope","MODEL":"qwen-7b",` + user + `}`},
		{"only a case variant of model", `{"Model":"qwen-7b",` + user + `}`},
		{"case variant of max_tokens", `{"model":"qwen-7b",` + user + `,"max_tokens":999999,"Max_Tokens":5}`},
		{"case variant of messages", `{"model":"qwen-7b","Messages":[{"role":"user","content":"hi"}],` + user + `}`},
		{"case variant of stream", `{"model":"qwen-7b",` + user + `,"stream":false,"Stream":true}`},
		{"duplicate model", `{"model":"qwen-7b","model":"qwen-7b",` + user + `}`},
		{"duplicate max_tokens", `{"model":"qwen-7b",` + user + `,"max_tokens":5,"max_tokens":50}`},
		{"case variant inside a message", `{"model":"qwen-7b","messages":[{"role":"system","Role":"user","content":"hi"}]}`},
		{"duplicate key inside a message", `{"model":"qwen-7b","messages":[{"role":"user","content":"a","content":"b"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseChatRequest([]byte(tt.body), testLimits)
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.Code != CodeInvalidRequest {
				t.Fatalf("expected INVALID_REQUEST, got %v", err)
			}
		})
	}
}

func TestParseChatRequestChecksEveryTokenField(t *testing.T) {
	user := `"model":"qwen-7b","messages":[{"role":"user","content":"hi"}]`
	for name, body := range map[string]string{
		"max_tokens ok, max_completion_tokens over": `{` + user + `,"max_tokens":10,"max_completion_tokens":1001}`,
		"max_completion_tokens ok, max_tokens over": `{` + user + `,"max_completion_tokens":10,"max_tokens":1001}`,
		"max_tokens ok, max_completion_tokens zero": `{` + user + `,"max_tokens":10,"max_completion_tokens":0}`,
		"max_tokens negative, completion ok":        `{` + user + `,"max_tokens":-5,"max_completion_tokens":10}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseChatRequest([]byte(body), testLimits); err == nil {
				t.Fatal("a token limit was bypassed via the second field")
			}
		})
	}

	// null means "not set" in the OpenAI API, and max_completion_tokens wins
	// when both are valid (that is what vLLM honors).
	ok := `{` + user + `,"max_tokens":null,"max_completion_tokens":20}`
	req, err := ParseChatRequest([]byte(ok), testLimits)
	if err != nil || req.MaxTokens != 20 {
		t.Fatalf("got %+v, %v", req, err)
	}
	both := `{` + user + `,"max_tokens":10,"max_completion_tokens":30}`
	if req, err := ParseChatRequest([]byte(both), testLimits); err != nil || req.MaxTokens != 30 {
		t.Fatalf("expected max_completion_tokens to take precedence, got %+v, %v", req, err)
	}
}

func TestParseChatRequestAssistantToolCallContent(t *testing.T) {
	ok := `{"model":"qwen-7b","messages":[{"role":"user","content":"weather?"},` +
		`{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"c1","content":"sunny"}]}`
	if _, err := ParseChatRequest([]byte(ok), testLimits); err != nil {
		t.Fatalf("assistant tool-call message with null content must be accepted: %v", err)
	}
	for name, body := range map[string]string{
		"assistant null content without tool_calls": `{"model":"qwen-7b","messages":[{"role":"assistant","content":null}]}`,
		"user null content":                         `{"model":"qwen-7b","messages":[{"role":"user","content":null,"tool_calls":[]}]}`,
		"assistant missing content, no tool_calls":  `{"model":"qwen-7b","messages":[{"role":"assistant"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseChatRequest([]byte(body), testLimits); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestParseChatRequestRejectsNonObjects(t *testing.T) {
	for _, body := range []string{`[]`, `null`, `"x"`, `5`, ``, `{"model":"qwen-7b"} {"model":"x"}`} {
		if _, err := ParseChatRequest([]byte(body), testLimits); err == nil {
			t.Fatalf("expected an error for %q", body)
		}
	}
}

func TestEchoedValuesAreTruncated(t *testing.T) {
	long := strings.Repeat("a", 5000)
	if msg := ErrModelNotFound(long).Message; len(msg) > 200 {
		t.Fatalf("model name echoed unbounded (%d bytes)", len(msg))
	}
	body := `{"model":"qwen-7b","messages":[{"role":"` + long + `","content":"x"}]}`
	_, err := ParseChatRequest([]byte(body), testLimits)
	var apiErr *Error
	if !errors.As(err, &apiErr) || len(apiErr.Message) > 200 {
		t.Fatalf("role echoed unbounded: %v", err)
	}
}

func TestParseChatRequestAcceptsLegacyFunctionRole(t *testing.T) {
	body := `{"model":"qwen-7b","messages":[{"role":"user","content":"hi"},{"role":"function","name":"f","content":"result"}]}`
	if _, err := ParseChatRequest([]byte(body), testLimits); err != nil {
		t.Fatalf("the legacy function role is still valid OpenAI: %v", err)
	}
}

func TestRateLimitErrorsAlwaysAdviseAtLeastOneSecond(t *testing.T) {
	for _, after := range []int{-5, 0, 1, 30} {
		want := max(1, after)
		for _, e := range []*Error{ErrRateLimited("requests", after), ErrRateLimitUnavailable(after)} {
			if e.RetryAfter != want {
				t.Fatalf("%s with %d: RetryAfter %d, want %d", e.Code, after, e.RetryAfter, want)
			}
			rec := httptest.NewRecorder()
			WriteError(rec, e)
			if got := rec.Header().Get("Retry-After"); got != strconv.Itoa(want) {
				t.Fatalf("%s with %d: header %q, want %d (never 0)", e.Code, after, got, want)
			}
		}
	}
	e := ErrRateLimited("tokens", 3)
	if e.HTTPStatus != 429 || e.Code != "RATE_LIMITED" || !strings.Contains(string(e.Body()), "rate_limit_error") || !strings.Contains(e.Message, "tokens") {
		t.Fatalf("%+v %s", e, e.Body())
	}
	if u := ErrRateLimitUnavailable(2); u.HTTPStatus != 503 || u.Code != "RATE_LIMIT_UNAVAILABLE" {
		t.Fatalf("%+v", u)
	}
}

func TestOverlongModelNameIsNotFoundEvenWhenAnyModelIsAllowed(t *testing.T) {
	long := strings.Repeat("m", 1<<20)
	body := `{"model":"` + long + `","messages":[{"role":"user","content":"hi"}]}`
	for _, lim := range []Limits{{AnyModel: true, MaxTokensLimit: 100}, {Models: []string{long}, MaxTokensLimit: 100}, {AnyModel: true, MaxTokensLimit: 100, Allowed: func(string) bool { return false }}} {
		_, err := ParseChatRequest([]byte(body), lim)
		var e *Error
		if !errors.As(err, &e) || e.Code != CodeModelNotFound || e.HTTPStatus != 404 || len(e.Message) > 200 {
			t.Fatalf("got %v", err)
		}
	}
	// A name at the limit still works.
	ok := strings.Repeat("m", 128)
	if _, err := ParseChatRequest([]byte(`{"model":"`+ok+`","messages":[{"role":"user","content":"hi"}]}`), Limits{AnyModel: true, MaxTokensLimit: 100}); err != nil {
		t.Fatal(err)
	}
}
