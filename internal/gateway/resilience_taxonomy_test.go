package gateway

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/pkg/protocol"
)

// Phase 15 D4 (acceptance 5): every row of the failure taxonomy, which attempts are retried, and what the breaker
// is told at the retry-decision point.

func hdrTimeout(d time.Duration) func(*config.Config) {
	return func(c *config.Config) { c.Gateway.UpstreamHeaderTimeout = d }
}

// binaryBody is deterministic bytes that are not text and hold no JSON.
func binaryBody(n int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.IntN(256))
	}
	return b
}

func raw503(body []byte) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		_, _ = w.Write(body)
	}
}

// hold reads the request and then blocks until the gateway gives up on it (the server only notices a closed
// connection once the body has been read).
func hold(r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	<-r.Context().Done()
}

// stall503 sends the 503 headers and a few bytes of a body and then holds the rest until the request ends.
func stall503(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(503)
	_, _ = io.WriteString(w, `{"error":{"mes`)
	w.(http.Flusher).Flush()
	<-r.Context().Done()
}

func TestTheFailureTaxonomyRowByRow(t *testing.T) {
	type row struct {
		name      string
		worker    func(t *testing.T) protocol.WorkerSnapshot // the worker tried first (sorts before the good one)
		cfg       func(*config.Config)
		retried   bool
		verdict   string
		wantFinal int // the client's status
	}
	srv := func(h func(http.ResponseWriter, *http.Request)) func(t *testing.T) protocol.WorkerSnapshot {
		return func(t *testing.T) protocol.WorkerSnapshot {
			return newScriptWorker(t, "a-first", h).snapshot("qwen-7b")
		}
	}
	rows := []row{
		{name: "connect refused", worker: func(t *testing.T) protocol.WorkerSnapshot { return refusing(t, "a-first") }, retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "reset before headers", worker: srv(dropConnection), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "empty first chunk", worker: srv(emptyStream), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "502", worker: srv(status(502, `{"error":{"code":"x"}}`)), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 queue_full", worker: srv(openAPI503("queue_full")), retried: true, verdict: VerdictNeutral, wantFinal: 200},
		{name: "503 draining", worker: srv(openAPI503("draining")), retried: true, verdict: VerdictNeutral, wantFinal: 200},
		{name: "503 not_ready", worker: srv(openAPI503("not_ready")), retried: true, verdict: VerdictNeutral, wantFinal: 200},
		{name: "503 unknown code", worker: srv(openAPI503("mock_failure")), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 no body", worker: srv(raw503(nil)), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 empty error object", worker: srv(raw503([]byte(`{"error":{}}`))), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 malformed json", worker: srv(raw503([]byte(`{"error":{"code":"queue_full"`))), retried: true, verdict: VerdictNeutral, wantFinal: 200}, // cut off after the code: the complete part is honoured
		{name: "503 truncated before the code", worker: srv(raw503([]byte(`{"error":{"message":"a`))), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 binary", worker: srv(raw503(binaryBody(4096))), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 code is not a string", worker: srv(raw503([]byte(`{"error":{"code":42}}`))), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 code first then 1 MiB", worker: srv(raw503([]byte(bodyWith("queue_full", 0)[:len(bodyWith("queue_full", 0))-1] + `,"pad":"` + strings.Repeat("x", 1<<20) + `"}`))), retried: true, verdict: VerdictNeutral, wantFinal: 200},
		{name: "503 code after 2 KiB of padding is out of the peek", worker: srv(raw503([]byte(bodyWith("queue_full", 2048)))), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 spoofed code in the message", worker: srv(raw503([]byte(`{"error":{"message":"code queue_full","code":"boom"}}`))), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "503 body that stalls", worker: srv(stall503), retried: true, verdict: VerdictFault, wantFinal: 200},
		{name: "header timeout", worker: srv(func(w http.ResponseWriter, r *http.Request) { hold(r) }), cfg: hdrTimeout(150 * time.Millisecond), retried: false, verdict: VerdictFault, wantFinal: 504},
		{name: "504 from the worker", worker: srv(status(504, `{"error":{"code":"x"}}`)), retried: false, verdict: VerdictFault, wantFinal: 504},
		{name: "500 is a ratio fault and never retried", worker: srv(status(500, `{"error":{"code":"x"}}`)), retried: false, verdict: VerdictFault, wantFinal: 500},
		{name: "404 relayed", worker: srv(status(404, `{"error":{"code":"x"}}`)), retried: false, verdict: VerdictNeutral, wantFinal: 404},
		{name: "400 relayed", worker: srv(status(400, `{"error":{"code":"x"}}`)), retried: false, verdict: VerdictNeutral, wantFinal: 400},
		{name: "200 ok", worker: srv(okJSON("a-first")), retried: false, verdict: VerdictOK, wantFinal: 200},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			good := newScriptWorker(t, "b-good", okJSON("b-good"))
			mut := []func(*config.Config){}
			if r.cfg != nil {
				mut = append(mut, r.cfg)
			}
			e := newResEnv(t, []protocol.WorkerSnapshot{r.worker(t), good.snapshot("qwen-7b")}, mut...)
			resp, body := e.chat(t, "qwen-7b")
			if resp.StatusCode != r.wantFinal {
				t.Fatalf("status %d (%s), want %d", resp.StatusCode, body, r.wantFinal)
			}
			e.settle(t, "a-first", "b-good")
			ends := e.rec.attemptEnds()
			if r.retried != (len(ends) == 2) {
				t.Fatalf("%d attempts, retried=%v wanted", len(ends), r.retried)
			}
			var first AttemptEnd
			for _, a := range ends {
				if a.Number == 1 {
					first = a
				}
			}
			if first.Verdict != r.verdict {
				t.Fatalf("verdict %q, want %q (%+v)", first.Verdict, r.verdict, first)
			}
			if r.retried && (!first.WillRetry || first.RetryDenied != "") {
				t.Fatalf("a retried attempt: %+v", first)
			}
		})
	}
}

// The peek puts what it read back: a held 503 that is relayed after all (no other worker) reaches the client
// byte for byte, whatever the body, and the peek is bounded to 1 KiB.
func TestAPeekedBodyReachesTheClientIntactWhenTheResponseIsRelayed(t *testing.T) {
	big := append([]byte(`{"error":{"code":"queue_full","message":"`), bytes.Repeat([]byte("y"), 1<<20)...)
	big = append(big, []byte(`"}}`)...)
	slowSplit := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		_, _ = io.WriteString(w, `{"error":{"code":"queue_`)
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond) // longer than the peek: the read is still pending when the gateway gives up waiting
		_, _ = io.WriteString(w, `full","message":"the rest"}}`)
	}
	cases := map[string]struct {
		h    func(http.ResponseWriter, *http.Request)
		want []byte
		slow bool
	}{
		"1 MiB":               {h: raw503(big), want: big},
		"binary":              {h: raw503(binaryBody(100000)), want: binaryBody(100000)},
		"short":               {h: raw503([]byte(`{"error":{"code":"x"}}`)), want: []byte(`{"error":{"code":"x"}}`)},
		"no body":             {h: raw503(nil), want: nil},
		"timed out then rest": {h: slowSplit, want: []byte(`{"error":{"code":"queue_full","message":"the rest"}}`), slow: true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			w := newScriptWorker(t, "only", c.h)
			e := newResEnv(t, []protocol.WorkerSnapshot{w.snapshot("qwen-7b")})
			resp, body := e.chat(t, "qwen-7b")
			if resp.StatusCode != 503 {
				t.Fatalf("status %d", resp.StatusCode)
			}
			if sha256.Sum256([]byte(body)) != sha256.Sum256(c.want) {
				t.Fatalf("the relayed body differs: %d bytes, want %d", len(body), len(c.want))
			}
			if e.rs.peeks.Load() != 1 {
				t.Fatalf("%d peeks: a retry was wanted, so the body is peeked once", e.rs.peeks.Load())
			}
			e.settle(t, "only")
			ends := e.rec.attemptEnds()
			if len(ends) != 1 || !ends[0].NextWorkerUnavailable || ends[0].NextRouteResult != NextRouteNoCapacity {
				t.Fatalf("one attempt with no other worker expected: %+v", ends)
			}
		})
	}
}

// A terminal 503 is relayed unread: no retry is possible when the attempts are used up or the budget refuses.
func TestATerminal503IsRelayedWithoutAPeek(t *testing.T) {
	t.Run("attempts exhausted", func(t *testing.T) {
		a := newScriptWorker(t, "a-1", openAPI503("queue_full"))
		b := newScriptWorker(t, "b-2", openAPI503("queue_full"))
		e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")})
		resp, _ := e.chat(t, "qwen-7b")
		if resp.StatusCode != 503 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if n := e.rs.peeks.Load(); n != 1 {
			t.Fatalf("%d peeks: the first attempt is peeked (a retry is possible), the second (last) is not", n)
		}
	})
	t.Run("max_attempts 1", func(t *testing.T) {
		a := newScriptWorker(t, "a-1", openAPI503("queue_full"))
		b := newScriptWorker(t, "b-2", okJSON("b-2"))
		e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")},
			func(c *config.Config) { c.Gateway.MaxAttempts = 1 })
		resp, _ := e.chat(t, "qwen-7b")
		if resp.StatusCode != 503 || e.rs.peeks.Load() != 0 {
			t.Fatalf("status %d, %d peeks", resp.StatusCode, e.rs.peeks.Load())
		}
	})
	t.Run("budget denied", func(t *testing.T) {
		a := newScriptWorker(t, "a-1", openAPI503("queue_full"))
		b := newScriptWorker(t, "b-2", okJSON("b-2"))
		e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")},
			func(c *config.Config) { c.Resilience.Retry.BudgetMin, c.Resilience.Retry.BudgetRatio = 0, 0 })
		resp, _ := e.chat(t, "qwen-7b")
		if resp.StatusCode != 503 || e.rs.peeks.Load() != 0 {
			t.Fatalf("status %d, %d peeks: a denied retry does not peek", resp.StatusCode, e.rs.peeks.Load())
		}
	})
}

// The peek never holds a retry longer than its bound, however the worker's body behaves.
func TestAStalledPeekCostsAtMostThePeekBound(t *testing.T) {
	a := newScriptWorker(t, "a-1", stall503)
	b := newScriptWorker(t, "b-2", okJSON("b-2"))
	e := newResEnv(t, []protocol.WorkerSnapshot{a.snapshot("qwen-7b"), b.snapshot("qwen-7b")})
	start := time.Now()
	resp, _ := e.chat(t, "qwen-7b")
	took := time.Since(start)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if took > 2*time.Second { // the bound is 40 ms in tests; the request must not wait for the stalled body
		t.Fatalf("took %v", took)
	}
	e.settle(t, "a-1", "b-2")
	if ends := e.rec.attemptEnds(); len(ends) != 2 || ends[0].Verdict != VerdictFault {
		t.Fatalf("a timed-out peek is a retryable fault: %+v", ends)
	}
}
