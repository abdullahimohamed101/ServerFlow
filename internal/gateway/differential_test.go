package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/pkg/protocol"
)

// The differential test (Phase 14 PR A, criterion 1, review item F14).
//
// It drives a fixed script of requests through several gateway set-ups and compares everything an outside
// observer can see (status, body, headers, what the workers were sent, what the limiter was asked to charge,
// the structured log lines, and the Prometheus series) with fixtures recorded ONCE from unmodified master
// (scripts/record-differential.sh). A change that is meant to alter nothing must leave every fixture byte for byte
// as recorded. Run with -record-differential only on a tree whose production code is the behaviour to preserve.
//
// Normalisation is deliberate and narrow, so CI on another Go version or another machine does not differ:
// request and attempt IDs, listener ports, timestamps, durations, the Date header, hop-by-hop framing headers
// (Content-Length, Transfer-Encoding, Connection), Go's own User-Agent and Accept-Encoding, and the Go runtime and
// process metric families (the toolchain version lives only there). Everything else is compared as is.
var recordDifferential = flag.Bool("record-differential", false, "rewrite testdata/differential/*.json from the current code (only on unmodified master behaviour)")

var (
	idRe   = regexp.MustCompile(`\b(req|att)_[0-9a-f]{16}\b`)
	hostRe = regexp.MustCompile(`127\.0\.0\.1:\d+`)
	// buildRe normalises the build-info labels (toolchain version, commit, version): CI builds with other Go versions.
	buildRe = regexp.MustCompile(`\b(go_version|goversion|commit|version|revision|goarch|goos)="[^"]*"`)
)

// bodyDigest keeps a fixture small: a body over 2 KiB is recorded as its length and SHA-256.
func bodyDigest(s string) string {
	if len(s) <= 2048 {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return fmt.Sprintf("<%d bytes sha256=%x>", len(s), sum)
}

func normText(s string) string {
	s = buildRe.ReplaceAllString(s, `$1="<build>"`)
	s = idRe.ReplaceAllString(s, "<$1-id>")
	return hostRe.ReplaceAllString(s, "<host>")
}

// volatileKey reports log keys whose values depend on timing or the run.
func volatileKey(k string) bool {
	if k == "time" {
		return true
	}
	for _, frag := range []string{"duration", "elapsed", "latency", "_ms", "_seconds", "_at"} {
		if strings.Contains(k, frag) {
			return true
		}
	}
	return false
}

func normValue(v any) any {
	switch x := v.(type) {
	case string:
		return normText(x)
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			if volatileKey(k) {
				out[k] = "<volatile>"
				continue
			}
			out[k] = normValue(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normValue(e)
		}
		return out
	}
	return v
}

func normLogs(t *testing.T, raw string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		b, _ := json.Marshal(normValue(m)) // map keys are sorted by encoding/json
		out = append(out, string(b))
	}
	return out
}

func normHeaders(h http.Header, drop ...string) map[string]string {
	skip := map[string]bool{"Date": true, "Content-Length": true, "Transfer-Encoding": true, "Connection": true,
		"User-Agent": true, "Accept-Encoding": true}
	for _, d := range drop {
		skip[http.CanonicalHeaderKey(d)] = true
	}
	out := map[string]string{}
	for k, v := range h {
		if skip[k] {
			continue
		}
		out[k] = normText(strings.Join(v, ","))
	}
	return out
}

type workerHit struct {
	Worker  string            `json:"worker"`
	Body    string            `json:"body"`
	Headers map[string]string `json:"headers"`
}

// hitLog records what the workers were sent, in arrival order.
type hitLog struct {
	mu   sync.Mutex
	hits []workerHit
}

func (l *hitLog) add(h workerHit) { l.mu.Lock(); l.hits = append(l.hits, h); l.mu.Unlock() }
func (l *hitLog) drain() []workerHit {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.hits
	l.hits = nil
	return out
}

type diffStep struct {
	Name         string            `json:"name"`
	Status       int               `json:"status"`
	Headers      map[string]string `json:"headers"`
	Body         string            `json:"body"`
	WorkerHits   []workerHit       `json:"worker_hits"`
	LimiterCosts []int             `json:"limiter_costs,omitempty"`
	Logs         []string          `json:"logs"`
}

type diffRecord struct {
	Variant string     `json:"variant"`
	Steps   []diffStep `json:"steps"`
	// Series are the gateway's own Prometheus series (name and label set, no values) after the whole script;
	// Counters adds the value of every *_total counter, which a sequential script makes deterministic.
	Series   []string          `json:"series"`
	Counters map[string]string `json:"counters"`
	Catalog  string            `json:"catalogue"`
}

type diffRequest struct {
	name   string
	method string
	path   string
	body   string
	hdr    map[string]string
	// bare requests do not get the variant's default headers (for example no API key).
	bare bool
}

func chatReq(name, body string) diffRequest {
	return diffRequest{name: name, method: http.MethodPost, path: chatCompletionsPath, body: body}
}

// diffScript is the fixed request script. It covers the paths the estimator, the router and the logs touch.
func diffScript() []diffRequest {
	nonASCII := strings.Repeat("héllo wörld 你好 ", 20)
	big := strings.Repeat("lorem ipsum dolor sit amet ", 8000) // about 216 KB, under the 1 MiB cap
	msg := func(model, content, extra string) string {
		b, _ := json.Marshal(content)
		return `{"model":"` + model + `","messages":[{"role":"user","content":` + string(b) + `}]` + extra + `}`
	}
	return []diffRequest{
		chatReq("plain", msg("qwen-7b", "hi", "")),
		chatReq("plain-again", msg("qwen-7b", "hello again", "")),
		chatReq("stream", msg("qwen-7b", "hi", `,"stream":true`)),
		chatReq("max-tokens-small", msg("qwen-7b", "hi", `,"max_tokens":16`)),
		chatReq("max-tokens-large", msg("qwen-7b", "hi", `,"max_tokens":100000`)),
		chatReq("max-completion-tokens", msg("qwen-7b", "hi", `,"max_completion_tokens":300`)),
		chatReq("non-ascii", msg("qwen-7b", nonASCII, "")),
		chatReq("big-body", msg("qwen-7b", big, `,"max_tokens":64`)),
		chatReq("two-messages", `{"model":"qwen-7b","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}],"temperature":0.2}`),
		chatReq("worker-500", msg("qwen-7b", "FAIL500", "")),
		chatReq("unknown-model", msg("nope", "hi", "")),
		chatReq("parked-model", msg("llama-8b", "hi", "")),
		chatReq("bad-json", `{"model":`),
		chatReq("no-messages", `{"model":"qwen-7b"}`),
		chatReq("empty-body", ``),
		chatReq("client-request-id", msg("qwen-7b", "hi", "")),
		{name: "get-models", method: http.MethodGet, path: "/v1/models"},
		{name: "get-readyz", method: http.MethodGet, path: "/readyz"},
		{name: "get-healthz", method: http.MethodGet, path: "/healthz"},
		{name: "wrong-method", method: http.MethodGet, path: chatCompletionsPath},
	}
}

// diffWorkerHandler is the behaviour every fake worker shares.
func diffWorkerHandler(id string, hits *hitLog) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		hits.add(workerHit{Worker: id, Body: bodyDigest(normText(string(b))), Headers: normHeaders(r.Header)})
		switch {
		case strings.Contains(string(b), "FAIL500"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"message":"boom","type":"server_error"}}`)
		case strings.Contains(string(b), `"stream":true`):
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "data: {\"worker\":\""+id+"\",\"i\":0}\n\ndata: [DONE]\n\n")
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"worker":"`+id+`","usage":{"prompt_tokens":3,"completion_tokens":5}}`)
		}
	}
}

type diffVariant struct {
	name    string
	limiter bool
	// build returns the gateway, its logs and the lister-independent hit log.
	build func(t *testing.T, hits *hitLog, lim *fakeLimiter) (*Server, *lockedBuffer)
	// script overrides the request script (nil: diffScript). headers, when set, supplies headers for every
	// request that is not bare; it is called per request, after build.
	script  func() []diffRequest
	headers func() map[string]string
}

func registryVariant(name, strategy string, reported []int, limiter bool) diffVariant {
	return diffVariant{name: name, limiter: limiter, build: func(t *testing.T, hits *hitLog, lim *fakeLimiter) (*Server, *lockedBuffer) {
		var ws []protocol.WorkerSnapshot
		for i, load := range reported {
			id := fmt.Sprintf("w-%c", 'a'+i)
			sw := newScriptWorker(t, id, diffWorkerHandler(id, hits))
			s := sw.snapshot("qwen-7b")
			s.Metrics.ActiveRequests = load
			s.Metrics.QueueDepth = load
			ws = append(ws, s)
		}
		// A worker that is up in the registry but whose port refuses connections, to exercise retries.
		if strategy == "round-robin" {
			ws = append(ws, refusing(t, "w-down"))
		}
		parked := ws[0]
		parked.WorkerID, parked.Model, parked.Eligible = "w-parked", "llama-8b", false
		ws = append(ws, parked)

		cfg := testConfig("http://127.0.0.1:1")
		diffGenerousTimeouts(&cfg)
		cfg.WorkerSource = config.WorkerSourceRegistry
		cfg.Models = nil
		logs := &lockedBuffer{}
		log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		gw, err := newRegistryServer(cfg, strategy, 5*time.Second, &fakeLister{workers: ws}, log)
		if err != nil {
			t.Fatal(err)
		}
		clock := &testClock{t: time.Unix(1_700_000_000, 0)}
		gw.router.cache.now = clock.Now
		if err := gw.router.cache.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		if limiter {
			gw.SetLimiter(lim)
		}
		return gw, logs
	}}
}

func staticVariant(name string, limiter bool) diffVariant {
	return diffVariant{name: name, limiter: limiter, build: func(t *testing.T, hits *hitLog, lim *fakeLimiter) (*Server, *lockedBuffer) {
		up := httptest.NewServer(diffWorkerHandler("static", hits))
		t.Cleanup(up.Close)
		cfg := testConfig(up.URL)
		diffGenerousTimeouts(&cfg)
		logs := &lockedBuffer{}
		log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		var opts []Option
		if limiter {
			opts = append(opts, WithLimiter(lim))
		}
		return New(cfg, log, opts...), logs
	}}
}

func diffVariants() []diffVariant {
	return []diffVariant{
		staticVariant("static", false),
		staticVariant("static-limiter", true),
		registryVariant("registry-round-robin", "round-robin", []int{0, 0}, false),
		registryVariant("registry-round-robin-limiter", "round-robin", []int{0, 0}, true),
		registryVariant("registry-least-active", "least-active", []int{2, 0, 1}, false),
		registryVariant("registry-least-queue", "least-queue", []int{1, 3, 0}, false),
		registryVariant("registry-random-single", "random", []int{0}, false),
	}
}

// seriesLines returns the gateway's own series identities and the values of its *_total counters.
func seriesLines(scrape string) (series []string, counters map[string]string) {
	counters = map[string]string{}
	for _, line := range strings.Split(scrape, "\n") {
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "go_") || strings.HasPrefix(line, "process_") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		id, val := normText(line[:i]), line[i+1:]
		name := id
		if j := strings.IndexByte(id, '{'); j >= 0 {
			name = id[:j]
		}
		series = append(series, id)
		if strings.HasSuffix(name, "_total") {
			counters[id] = val
		}
	}
	sort.Strings(series)
	return series, counters
}

func runDifferential(t *testing.T, v diffVariant) diffRecord {
	t.Helper()
	hits := &hitLog{}
	lim := &fakeLimiter{}
	gw, logs := v.build(t, hits, lim)
	srv := httptest.NewServer(gw.Handler())
	tr := &http.Transport{DisableCompression: true}
	t.Cleanup(func() { tr.CloseIdleConnections(); srv.Close() })
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}

	rec := diffRecord{Variant: v.name}
	logs.mu.Lock()
	logs.buf.Reset() // construction-time logs are not part of the script
	logs.mu.Unlock()
	hits.drain()
	costSeen := 0
	script := diffScript
	if v.script != nil {
		script = v.script
	}
	for _, rq := range script() {
		req, err := http.NewRequest(rq.method, srv.URL+rq.path, strings.NewReader(rq.body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if v.headers != nil && !rq.bare {
			for k, val := range v.headers() {
				req.Header.Set(k, val)
			}
		}
		for k, val := range rq.hdr {
			req.Header.Set(k, val)
		}
		if rq.name == "client-request-id" {
			req.Header.Set("X-Request-ID", "client-supplied-1")
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", rq.name, err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		step := diffStep{
			Name: rq.name, Status: resp.StatusCode, Headers: normHeaders(resp.Header), Body: bodyDigest(normText(string(b))),
			WorkerHits: hits.drain(),
		}
		if v.limiter {
			calls := lim.calls()
			for _, c := range calls[costSeen:] {
				step.LimiterCosts = append(step.LimiterCosts, int(c.Cost))
			}
			costSeen = len(calls)
		}
		// Log lines are written by the handler goroutine and the request log after the response is complete;
		// wait until the count is stable so a late line is not attributed to the next step.
		step.Logs = settledLogs(t, logs)
		rec.Steps = append(rec.Steps, step)
	}
	scr := scrape(t, gw)
	rec.Series, rec.Counters = seriesLines(scr)
	rec.Catalog = seriesCatalogue(t, gw)
	return rec
}

// settledLogs returns and clears the normalised log lines once no new line has arrived for a short while.
func settledLogs(t *testing.T, logs *lockedBuffer) []string {
	t.Helper()
	prev, stable := -1, 0
	for i := 0; i < 200 && stable < 3; i++ {
		n := len(logs.String())
		if n == prev {
			stable++
		} else {
			stable, prev = 0, n
		}
		time.Sleep(2 * time.Millisecond)
	}
	raw := logs.String()
	logs.mu.Lock()
	logs.buf.Reset()
	logs.mu.Unlock()
	return normLogs(t, raw)
}

func TestDifferentialAgainstRecordedFixtures(t *testing.T) {
	dir := filepath.Join("testdata", "differential")
	for _, v := range append(diffVariants(), wideVariants()...) {
		t.Run(v.name, func(t *testing.T) {
			got := runDifferential(t, v)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetIndent("", "  ")
			enc.SetEscapeHTML(false)
			if err := enc.Encode(got); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(dir, v.name+".json")
			if *recordDifferential {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(file, buf.Bytes(), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			wantRaw, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("missing fixture (record with scripts/record-differential.sh on unmodified master): %v", err)
			}
			if bytes.Equal(wantRaw, buf.Bytes()) {
				return
			}
			var want diffRecord
			if err := json.Unmarshal(wantRaw, &want); err != nil {
				t.Fatal(err)
			}
			t.Fatalf("%s differs from the recorded fixture: %s", v.name, firstDifference(want, got))
		})
	}
}

// firstDifference names the first step or section that differs, so a failure points at something.
func firstDifference(want, got diffRecord) string {
	for i := 0; i < len(want.Steps) && i < len(got.Steps); i++ {
		w, g := want.Steps[i], got.Steps[i]
		wb, _ := json.Marshal(w)
		gb, _ := json.Marshal(g)
		if !bytes.Equal(wb, gb) {
			return "step " + strconv.Quote(w.Name) + "\n want: " + string(wb) + "\n  got: " + string(gb)
		}
	}
	if len(want.Steps) != len(got.Steps) {
		return fmt.Sprintf("step count %d != %d", len(want.Steps), len(got.Steps))
	}
	if strings.Join(want.Series, "\n") != strings.Join(got.Series, "\n") {
		return "the /metrics series differ:\n want: " + strings.Join(want.Series, "\n       ") + "\n  got: " + strings.Join(got.Series, "\n       ")
	}
	if want.Catalog != got.Catalog {
		return "the series catalogue differs"
	}
	return "the counter values differ"
}

// diffGenerousTimeouts: no recorded step is about a timeout, so the gateway's upstream timeouts only matter as a
// source of flakes: a 1 MiB body proxied under the race detector on a loaded runner (the nightly CPU-load job) can take
// longer than testConfig's 2 s header timeout and turn a recorded 200 into a 504. The values are not part of any
// recorded output.
func diffGenerousTimeouts(cfg *config.GatewayConfig) {
	cfg.UpstreamHeaderTimeout = 60 * time.Second
	cfg.UpstreamIdleTimeout = 60 * time.Second
}
