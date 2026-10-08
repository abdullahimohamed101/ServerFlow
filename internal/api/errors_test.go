package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNoCapacityCarriesTheSpecDetailAndRetryAfter(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, ErrNoCapacity("qwen-7b", 0))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" ||
		rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, headers %v", rec.Code, rec.Header())
	}
	var body struct {
		Error struct {
			Code, Type, Model string
			EligibleWorkers   *int `json:"eligible_workers"`
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "NO_CAPACITY" || body.Error.Model != "qwen-7b" || body.Error.EligibleWorkers == nil ||
		*body.Error.EligibleWorkers != 0 || body.Error.Type != "api_error" {
		t.Fatalf("body %s", rec.Body.String())
	}
}

func TestNoCapacityTruncatesTheModelName(t *testing.T) {
	e := ErrNoCapacity(strings.Repeat("m", 500), 2)
	if len(e.Model) > 70 || len(e.Message) > 200 {
		t.Fatalf("a client-chosen model name must be bounded: %d %d", len(e.Model), len(e.Message))
	}
}

func TestOtherErrorsHaveNoRetryAfterOrCapacityDetail(t *testing.T) {
	for _, e := range []*Error{ErrModelNotFound("m"), ErrWorkerUnavailable(), ErrInferenceFailed(), ErrUpstreamTimeout(), ErrInvalidRequest("x")} {
		rec := httptest.NewRecorder()
		WriteError(rec, e)
		if rec.Header().Get("Retry-After") != "" || strings.Contains(rec.Body.String(), "eligible_workers") || strings.Contains(rec.Body.String(), `"model"`) {
			t.Errorf("%s: unexpected detail: %v %s", e.Code, rec.Header(), rec.Body.String())
		}
	}
}

func TestAuthErrors(t *testing.T) {
	write := func(e *Error) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		WriteError(rec, e)
		return rec
	}
	u := write(ErrUnauthorized())
	if u.Code != 401 || u.Header().Get("WWW-Authenticate") != "Bearer" || !strings.Contains(u.Body.String(), `"code":"UNAUTHORIZED"`) {
		t.Fatalf("401: %d %v %s", u.Code, u.Header(), u.Body.String())
	}
	if u2 := write(ErrUnauthorized()); u2.Body.String() != u.Body.String() {
		t.Fatal("the 401 body must not vary")
	}
	f := write(ErrForbidden())
	if f.Code != 403 || f.Header().Get("WWW-Authenticate") != "" || !strings.Contains(f.Body.String(), `"code":"FORBIDDEN"`) {
		t.Fatalf("403: %d %v %s", f.Code, f.Header(), f.Body.String())
	}
	a := write(ErrAuthUnavailable())
	if a.Code != 503 || a.Header().Get("Retry-After") == "" || !strings.Contains(a.Body.String(), `"code":"AUTH_UNAVAILABLE"`) || !strings.Contains(a.Body.String(), "api_error") {
		t.Fatalf("503: %d %v %s", a.Code, a.Header(), a.Body.String())
	}
	m := ErrModelForbidden(strings.Repeat("x", 500))
	if len(m.Message) > 200 || m.HTTPStatus != 403 || m.Code != CodeForbidden {
		t.Fatalf("model forbidden: %+v", m)
	}
}

func TestParseChatRequestAllowList(t *testing.T) {
	body := func(model string) []byte {
		return []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`)
	}
	allow := func(m string) bool { return m == "ok" }
	lim := Limits{Models: []string{"ok", "other"}, MaxTokensLimit: 10, Allowed: allow}
	if r, err := ParseChatRequest(body("ok"), lim); err != nil || r.Model != "ok" {
		t.Fatalf("allowed model: %v", err)
	}
	// A configured-but-disallowed model and a nonexistent one get the same error.
	_, e1 := ParseChatRequest(body("other"), lim)
	_, e2 := ParseChatRequest(body("other"), Limits{Models: nil, MaxTokensLimit: 10, Allowed: func(string) bool { return false }})
	if e1 == nil || e1.(*Error).Code != CodeForbidden || e1.(*Error).Message != e2.(*Error).Message {
		t.Fatalf("allow-list errors differ: %v / %v", e1, e2)
	}
	_, e3 := ParseChatRequest(body("nonexistent"), lim)
	if e3 == nil || e3.(*Error).Code != CodeForbidden || e3.(*Error).Message != ErrModelForbidden("nonexistent").Message {
		t.Fatalf("a nonexistent model must look like a disallowed one: %v", e3)
	}
	// Without an allow-list nothing changes.
	if _, err := ParseChatRequest(body("nonexistent"), Limits{Models: []string{"ok"}, MaxTokensLimit: 10}); err == nil || err.(*Error).Code != CodeModelNotFound {
		t.Fatalf("unrestricted: %v", err)
	}
}
