package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/events"
	"serverflow/internal/events/eventstest"
	"serverflow/internal/gateway"
	"serverflow/internal/mockworker"
	"serverflow/pkg/protocol"
)

// These tests drive a real gateway with the events observer and a recording sink (no broker needed): they pin
// which events each kind of request leaves behind (D3) and that no content reaches them (D4).

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func newEventsObserver(t *testing.T, log *slog.Logger) (*events.Observer, *eventstest.RecordingSink) {
	t.Helper()
	sink := &eventstest.RecordingSink{}
	obs := events.NewObserver(sink, events.Config{BufferSize: 1000, Source: "gw-it"}, log)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = obs.Close(c)
	})
	return obs, sink
}

func staticGatewayWithEvents(t *testing.T, upstream string, mutate func(*config.GatewayConfig), log *slog.Logger) (string, *events.Observer, *eventstest.RecordingSink) {
	t.Helper()
	obs, sink := newEventsObserver(t, log)
	cfg := config.Default().Gateway
	cfg.UpstreamURL, cfg.Models, cfg.UpstreamHeaderTimeout = upstream, []string{model}, 5*time.Second
	if mutate != nil {
		mutate(&cfg)
	}
	ts := httptest.NewServer(gateway.New(cfg, log, gateway.WithObserver(obs)).Handler())
	t.Cleanup(ts.Close)
	return ts.URL, obs, sink
}

// settle waits until n events have been produced, then a moment longer to catch any that should not exist.
func settle(t *testing.T, sink *eventstest.RecordingSink, n int) []protocol.Event {
	t.Helper()
	eventually(t, 5*time.Second, func() bool { return len(sink.Records()) >= n }, fmt.Sprintf("%d events", n))
	time.Sleep(50 * time.Millisecond)
	evs, ok := sink.Events()
	if !ok {
		t.Fatal("an event did not decode")
	}
	return evs
}

func typesOf(evs []protocol.Event) string {
	var s []string
	for _, e := range evs {
		s = append(s, strings.TrimPrefix(e.EventType, "inference.request."))
	}
	return strings.Join(s, ",")
}

func terminalCount(evs []protocol.Event) int {
	n := 0
	for _, e := range evs {
		if protocol.IsTerminalEventType(e.EventType) {
			n++
		}
	}
	return n
}

func TestEventSequencesThroughAStaticGateway(t *testing.T) {
	log := quiet()
	t.Run("non-streaming success carries the worker's usage", func(t *testing.T) {
		mock, _ := startMock(t)
		gw, _, sink := staticGatewayWithEvents(t, mock.URL, nil, log)
		_ = mustPost(t, gw, chat(false, "hi")).Body.Close()
		evs := settle(t, sink, 3)
		if typesOf(evs) != "received,routed,completed" || terminalCount(evs) != 1 {
			t.Fatalf("sequence %s", typesOf(evs))
		}
		tt := evs[2].Terminal
		if tt.TokensSource != protocol.TokensFromUsage || tt.InputTokens == nil || tt.OutputTokens == nil || *tt.OutputTokens <= 0 || tt.HTTPStatus != 200 {
			t.Fatalf("terminal: %+v", tt)
		}
		if len(tt.Attempts) != 1 || tt.Attempts[0].Outcome != "ok" || evs[2].Model != model || evs[0].Model != model {
			t.Fatalf("attempts/model: %+v", evs[2])
		}
	})
	t.Run("streaming success has a first token and chunk counts", func(t *testing.T) {
		mock, _ := startMock(t)
		gw, _, sink := staticGatewayWithEvents(t, mock.URL, nil, log)
		resp := mustPost(t, gw, chat(true, "hi"))
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		evs := settle(t, sink, 4)
		if typesOf(evs) != "received,routed,first_token,completed" {
			t.Fatalf("sequence %s", typesOf(evs))
		}
		if tt := evs[3].Terminal; !tt.Stream || tt.TokensSource != protocol.TokensFromChunks || tt.OutputTokens == nil || *tt.OutputTokens != 10 || tt.InputTokens != nil || tt.TTFTMS <= 0 {
			t.Fatalf("terminal: %+v", tt)
		}
		if evs[2].FirstToken.TTFTMS <= 0 {
			t.Fatalf("first token: %+v", evs[2].FirstToken)
		}
	})
	for name, mode := range map[string]mockworker.FailureMode{"upstream error": mockworker.ModeError, "dropped connection": mockworker.ModeDrop, "unavailable": mockworker.ModeUnavailable} {
		t.Run(name+" is one failed event", func(t *testing.T) {
			mock, _ := startMock(t, func(c *mockworker.Config) { c.FailureRate, c.FailureMode = 1, mode })
			gw, _, sink := staticGatewayWithEvents(t, mock.URL, nil, log)
			resp := mustPost(t, gw, chat(false, "hi"))
			_ = resp.Body.Close()
			evs := settle(t, sink, 3)
			if typesOf(evs) != "received,routed,failed" || terminalCount(evs) != 1 {
				t.Fatalf("sequence %s", typesOf(evs))
			}
			if evs[2].Terminal.FailureClass == "" || evs[2].Terminal.TokensSource != protocol.TokensFromEstimate {
				t.Fatalf("terminal: %+v", evs[2].Terminal)
			}
		})
	}
	t.Run("a timeout is failed with class timeout", func(t *testing.T) {
		mock, _ := startMock(t, func(c *mockworker.Config) { c.TTFT = 2 * time.Second })
		gw, _, sink := staticGatewayWithEvents(t, mock.URL, func(c *config.GatewayConfig) { c.UpstreamHeaderTimeout = 150 * time.Millisecond }, log)
		resp := mustPost(t, gw, chat(false, "hi"))
		_ = resp.Body.Close()
		evs := settle(t, sink, 3)
		if typesOf(evs) != "received,routed,failed" || evs[2].Terminal.FailureClass != protocol.FailureTimeout {
			t.Fatalf("sequence %s %+v", typesOf(evs), evs[len(evs)-1].Terminal)
		}
	})
	t.Run("a client that leaves is failed with class client_closed", func(t *testing.T) {
		mock, _ := startMock(t, func(c *mockworker.Config) { c.OutputTokens, c.TokensPerSecond = 200, 50 })
		gw, _, sink := staticGatewayWithEvents(t, mock.URL, nil, log)
		ctx, cancel := context.WithCancel(context.Background())
		resp, err := post(ctx, gw, chat(true, "hi"))
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 64)
		_, _ = resp.Body.Read(buf)
		cancel()
		_ = resp.Body.Close()
		eventually(t, 5*time.Second, func() bool { return terminalCount(func() []protocol.Event { e, _ := sink.Events(); return e }()) == 1 }, "terminal event")
		evs, _ := sink.Events()
		last := evs[len(evs)-1]
		if last.EventType != protocol.EventFailed || last.Terminal.FailureClass != protocol.FailureClientClosed || last.Terminal.HTTPStatus != 499 {
			t.Fatalf("last: %s %+v", last.EventType, last.Terminal)
		}
	})
	t.Run("refusals before admission leave no events", func(t *testing.T) {
		mock, _ := startMock(t)
		gw, obs, sink := staticGatewayWithEvents(t, mock.URL, nil, log)
		for _, body := range []string{`{}`, `not json`, `{"model":"nope","messages":[{"role":"user","content":"x"}]}`, `{"model":"` + model + `","messages":[]}`} {
			resp := mustPost(t, gw, body)
			_ = resp.Body.Close()
			if resp.StatusCode < 400 {
				t.Fatalf("%s should be refused, got %d", body, resp.StatusCode)
			}
		}
		time.Sleep(100 * time.Millisecond)
		if n := len(sink.Records()); n != 0 || obs.Publisher().Depth() != 0 {
			t.Fatalf("refusals produced %d events", n)
		}
	})
}

func TestEventsAfterARetryListEveryAttemptInOrder(t *testing.T) {
	c := startRelaxedControlPlane(t)
	c.startModelNode(t, "q1", model, func(m *mockworker.Config) { m.FailureRate, m.FailureMode = 1, mockworker.ModeUnavailable })
	c.startModelNode(t, "q2", model, nil)
	obs, sink := newEventsObserver(t, quiet())
	cfg := config.Default()
	cfg.Gateway.WorkerSource, cfg.Gateway.ControlPlaneURL = config.WorkerSourceRegistry, c.url
	cfg.Gateway.RegistryRefresh, cfg.Gateway.RegistryMaxStaleness, cfg.Gateway.UpstreamHeaderTimeout = 25*time.Millisecond, 3*time.Second, 5*time.Second
	cfg.Worker.SuspectTimeout, cfg.ControlPlane.Token, cfg.Scheduler.Strategy = c.suspect, testToken, "round-robin"
	gw, err := gateway.NewRegistry(cfg, quiet(), gateway.WithObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	url := "http://" + ln.Addr().String()
	waitFor(t, 10*time.Second, "both workers eligible", func() bool { return c.eligible("q1") && c.eligible("q2") })
	// The gateway's own view of the registry lags the control plane's by a refresh or two: the first requests
	// can still be refused for capacity on a loaded machine.
	eventually(t, 15*time.Second, func() bool { code, _ := gwPost(url, false); return code == 200 }, "the gateway to route a request")

	var retried []protocol.Event
	for i := 0; i < 12 && retried == nil; i++ {
		code, _ := gwPost(url, i%2 == 0)
		if code != 200 {
			t.Fatalf("request %d failed: %d", i, code)
		}
	}
	eventually(t, 5*time.Second, func() bool {
		evs, _ := sink.Events()
		for _, e := range evs {
			if protocol.IsTerminalEventType(e.EventType) && len(e.Terminal.Attempts) == 2 {
				retried = append(retried, e)
			}
		}
		return len(retried) > 0
	}, "a request that needed two attempts")
	term := retried[0]
	a := term.Terminal.Attempts
	if a[0].Number != 1 || a[1].Number != 2 || a[0].Outcome != "retried" || a[1].Outcome != "ok" || a[0].WorkerID == a[1].WorkerID || a[0].AttemptID == a[1].AttemptID || a[0].WorkerID == "" {
		t.Fatalf("attempts: %+v", a)
	}
	evs, _ := sink.Events()
	var mine []protocol.Event
	for _, e := range evs {
		if e.RequestID == term.RequestID {
			mine = append(mine, e)
		}
	}
	got := typesOf(mine)
	if got != "received,routed,routed,completed" && got != "received,routed,routed,first_token,completed" {
		t.Fatalf("sequence of the retried request: %s", got)
	}
	if mine[1].AttemptID != a[0].AttemptID || mine[2].AttemptID != a[1].AttemptID || mine[1].WorkerID != a[0].WorkerID || term.AttemptID != a[1].AttemptID {
		t.Fatalf("routed events do not match attempts[]: %+v", mine)
	}
	perReq := map[string]int{}
	for _, e := range evs {
		if protocol.IsTerminalEventType(e.EventType) {
			perReq[e.RequestID]++
		}
	}
	for id, n := range perReq {
		if n != 1 {
			t.Fatalf("request %s has %d terminal events", id, n)
		}
	}
}

// The privacy canary (D4, acceptance 4): unique strings in the prompt, the system message, the response and the
// API key must not appear in any encoded event, in %+v output, or in the logs of the gateway or the publisher.
func TestNoPromptResponseOrKeyReachesAnEvent(t *testing.T) {
	const (
		promptCanary   = "CANARY-PROMPT-7d41e0"
		systemCanary   = "CANARY-SYSTEM-b3a9f2"
		responseCanary = "CANARY-RESPONSE-5c88d1"
		keyCanary      = "sk-CANARY-KEY-0f6e2a"
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Stream bool }
		b, _ := io.ReadAll(r.Body)
		req.Stream = bytes.Contains(b, []byte(`"stream":true`))
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":6}}\n\ndata: [DONE]\n\n", responseCanary)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":%q}}],"usage":{"prompt_tokens":5,"completion_tokens":6}}`, responseCanary)
	}))
	t.Cleanup(upstream.Close)
	logs := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	gw, _, sink := staticGatewayWithEvents(t, upstream.URL, nil, log)
	for _, stream := range []bool{false, true} {
		body := fmt.Sprintf(`{"model":%q,"stream":%v,"messages":[{"role":"system","content":%q},{"role":"user","content":%q}]}`, model, stream, systemCanary, promptCanary)
		req, _ := http.NewRequest(http.MethodPost, gw+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+keyCanary)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	evs := settle(t, sink, 7)
	var all bytes.Buffer
	for _, r := range sink.Records() {
		all.Write(r.Key)
		all.Write(r.Value)
	}
	for _, e := range evs {
		fmt.Fprintf(&all, "%v %+v %#v", e, e, e)
		if e.Terminal != nil {
			fmt.Fprintf(&all, "%+v", *e.Terminal)
		}
	}
	all.WriteString(logs.String())
	for _, c := range []string{promptCanary, systemCanary, responseCanary, keyCanary, "CANARY"} {
		if strings.Contains(all.String(), c) {
			t.Fatalf("canary %q reached an event, a dump of one, or a log", c)
		}
	}
	// The usage the worker reported did arrive, so the canary test really exercised the token path.
	found := false
	for _, e := range evs {
		if e.Terminal != nil && e.Terminal.TokensSource == protocol.TokensFromUsage && *e.Terminal.OutputTokens == 6 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected usage-sourced tokens in at least one terminal event")
	}
}
