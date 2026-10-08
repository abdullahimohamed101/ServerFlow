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
