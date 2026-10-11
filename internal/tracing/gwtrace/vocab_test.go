package gwtrace

import (
	"strings"
	"testing"
)

func TestErrorCodeIsAClosedSet(t *testing.T) {
	for _, c := range []string{"UNAUTHORIZED", "MODEL_NOT_FOUND", "NO_CAPACITY", "INVALID_REQUEST", "INFERENCE_FAILED", "WORKER_UNAVAILABLE", "INTERNAL_ERROR",
		"RATE_LIMITED", "RATE_LIMIT_UNAVAILABLE", "UPSTREAM_TIMEOUT", "FORBIDDEN", "AUTH_UNAVAILABLE"} {
		if got := errorCode(c); got != c {
			t.Errorf("errorCode(%q) = %q, a real code must pass through", c, got)
		}
	}
	for _, c := range []string{"canary-code", "unauthorized", "UNAUTHORIZED ", "NO_CAPACITY\nx", strings.Repeat("A", 500), "10.0.0.1:8000"} {
		if got := errorCode(c); got != "other" {
			t.Errorf("errorCode(%q) = %q, anything outside the set must be \"other\"", c, got)
		}
	}
	if errorCode("") != "" {
		t.Error("an empty code stays empty")
	}
}

func TestSafeIDRejectsAnythingThatIsNotAnID(t *testing.T) {
	for _, ok := range []string{"ten_abc", "att_0123456789abcdef", "mock-ok.1:x", strings.Repeat("a", 64)} {
		if safeID(ok) != ok {
			t.Errorf("safeID(%q) must pass", ok)
		}
	}
	for _, bad := range []string{"", strings.Repeat("a", 65), "has space", "new\nline", "Bearer sk-x y", "über", "a/b", "a=b", "<script>"} {
		if got := safeID(bad); got != "invalid" {
			t.Errorf("safeID(%q) = %q, want invalid", bad, got)
		}
	}
}

func TestValidModelRejectsAnythingThatIsNotAModelName(t *testing.T) {
	if validModel("qwen-7b") != "qwen-7b" || validModel("org/model:Q4_K.gguf") != "org/model:Q4_K.gguf" || validModel(strings.Repeat("m", 128)) == "" {
		t.Error("ordinary model names must pass")
	}
	for _, bad := range []string{"", strings.Repeat("m", 129), "has space", "tab\t", "new\nline", "del\x7f", "über", "\x00"} {
		if got := validModel(bad); got != "" {
			t.Errorf("validModel(%q) = %q, want it dropped", bad, got)
		}
	}
}

func TestTokenIsBoundedAndSafe(t *testing.T) {
	if token("status_503", 24) != "status_503" || token("", 24) != "" {
		t.Error("ordinary classes must pass")
	}
	for _, bad := range []string{"Status", "a b", strings.Repeat("a", 25), "a:b", "x\n"} {
		if got := token(bad, 24); got != "other" {
			t.Errorf("token(%q) = %q", bad, got)
		}
	}
}
