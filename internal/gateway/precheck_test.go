package gateway

import (
	"strings"
	"testing"

	"serverflow/pkg/protocol"
)

// Phase 15, PR A, D3 (debt 2): in registry mode an unknown model is answered 404 before the rate limiter, so it
// costs the tenant no request, token or concurrency quota. A model that is served (by any worker, eligible or
// not) is charged as before, and with no usable snapshot the request still reaches the limiter.

func limitedRegEnv(t *testing.T, ws ...protocol.WorkerSnapshot) (*regEnv, *fakeLimiter, *recObserver) {
	t.Helper()
	e := newRegEnv(t, "round-robin", ws)
	lim := &fakeLimiter{}
	e.gw.SetLimiter(lim)
	rec := &recObserver{}
	WithObserver(rec)(e.gw)
	return e, lim, rec
}

func TestAnUnknownModelIsNotChargedToTheLimiter(t *testing.T) {
	good := newScriptWorker(t, "a", okJSON("a"))
	e, lim, rec := limitedRegEnv(t, good.snapshot("qwen-7b"))
	resp, body := e.chat(t, "no-such-model")
	if resp.StatusCode != 404 || errorCode(t, body) != "MODEL_NOT_FOUND" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if n := len(lim.calls()); n != 0 || lim.allowed.Load() != 0 {
		t.Fatalf("the limiter was consulted %d times (%d leases) for a model that does not exist", n, lim.allowed.Load())
	}
	wantSeq(t, rec.await(t), "start", "admit", "reject:model:model_not_found:404", "done:404")
	if a := rec.adm[0]; a.RateLimitChecked || a.EstimatedCost != 0 {
		t.Fatalf("no rate limit decision was made: %+v", a)
	}
	if good.hits.Load() != 0 {
		t.Fatal("no worker is involved")
	}
}

func TestAKnownModelIsChargedAsBefore(t *testing.T) {
	good := newScriptWorker(t, "a", okJSON("a"))
	e, lim, rec := limitedRegEnv(t, good.snapshot("qwen-7b"))
	if resp, body := e.chat(t, "qwen-7b"); resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	calls := lim.calls()
	if len(calls) != 1 || calls[0].Model != "qwen-7b" || calls[0].Cost <= 0 {
		t.Fatalf("a served model is charged once: %+v", calls)
	}
	wantSeq(t, rec.await(t), "start", "admit", "attempt+1:a", "attempt-1:a:ok", "done:200")
	if !rec.adm[0].RateLimitChecked {
		t.Fatal("the admission records the rate limit decision")
	}
}

func TestWithoutAUsableSnapshotTheRequestStillReachesTheLimiter(t *testing.T) {
	e, lim, _ := limitedRegEnv(t) // no workers listed and no refresh: there is no snapshot
	resp, body := e.chat(t, "no-such-model")
	if resp.StatusCode != 503 || errorCode(t, body) != "WORKER_UNAVAILABLE" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if n := len(lim.calls()); n != 1 {
		t.Fatalf("with no snapshot nothing is known about the model, so the limiter decides as today: %d calls", n)
	}
}

func TestAModelWhoseWorkersAreAllLostIsStillServedAndCharged(t *testing.T) {
	good := newScriptWorker(t, "a", okJSON("a"))
	lost := good.snapshot("llama-8b")
	lost.WorkerID, lost.State, lost.Health, lost.Eligible = "w-lost", protocol.StateLost, protocol.HealthLost, false
	e, lim, _ := limitedRegEnv(t, good.snapshot("qwen-7b"), lost)
	resp, body := e.chat(t, "llama-8b")
	if resp.StatusCode != 503 || errorCode(t, body) != "NO_CAPACITY" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if n := len(lim.calls()); n != 1 {
		t.Fatalf("a model that exists but has no eligible worker is charged as before: %d calls", n)
	}
}

func TestTheUnknownModelRefusalKeepsItsLogLineAndSchedulerSeries(t *testing.T) {
	good := newScriptWorker(t, "a", okJSON("a"))
	e, _, _ := limitedRegEnv(t, good.snapshot("qwen-7b"))
	e.chat(t, "no-such-model")
	var found bool
	for _, l := range e.logs.logLines(t) {
		if l["msg"] == "no worker selected" {
			found = true
			if l["level"] != "DEBUG" || l["error_code"] != "MODEL_NOT_FOUND" || l["strategy"] != "round-robin" || l["request_id"] == nil || l["attempt_id"] == nil {
				t.Fatalf("log line: %v", l)
			}
		}
	}
	if !found {
		t.Fatalf("the refusal is logged as before:\n%s", e.logs.String())
	}
	body := scrape(t, e.gw)
	if !strings.Contains(body, `scheduler_decisions_total{model="unknown",result="no_model",strategy="round-robin"} 1`) {
		t.Fatalf("the refusal is still a no_model decision:\n%s", grepLines(body, "scheduler_decision"))
	}
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
