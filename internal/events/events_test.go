package events_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"serverflow/internal/events"
	"serverflow/internal/events/eventstest"
	"serverflow/internal/gateway"
	"serverflow/pkg/protocol"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)) }

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func newObs(t *testing.T, sink events.Sink, buf int) *events.Observer {
	t.Helper()
	o := events.NewObserver(sink, events.Config{BufferSize: buf, Source: "gw-test"}, quiet())
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = o.Close(c)
	})
	return o
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out: " + msg)
}

// drive runs one request's observer calls the way the gateway makes them.
func drive(o *events.Observer, id string, c gateway.Completion, attempts int) {
	ctx := o.RequestStarted(context.Background(), gateway.RequestStart{ID: id})
	o.RequestAdmitted(ctx, gateway.Admission{RequestID: id, Model: "m", TenantID: "ten_a", APIKeyID: "key_a", EstimatedCost: 10, Stream: true})
	for i := 1; i <= attempts; i++ {
		att := fmt.Sprintf("att_%s_%d", id[4:], i)
		actx := o.AttemptStarted(ctx, gateway.AttemptStart{RequestID: id, AttemptID: att, Number: i, WorkerID: fmt.Sprintf("w%d", i), Model: "m", Strategy: "rr"})
		if i == attempts {
			o.FirstToken(actx, gateway.FirstToken{RequestID: id, AttemptID: att, TTFT: 5 * time.Millisecond})
			o.FirstToken(actx, gateway.FirstToken{RequestID: id, AttemptID: att, TTFT: 6 * time.Millisecond}) // must not repeat
		}
		out := gateway.AttemptOK
		if i < attempts {
			out = gateway.AttemptRetried
		}
		o.AttemptEnded(actx, gateway.AttemptEnd{RequestID: id, AttemptID: att, Number: i, WorkerID: fmt.Sprintf("w%d", i), Outcome: out, Duration: time.Millisecond, Class: "status_503"})
	}
	c.RequestID = id
	o.RequestCompleted(ctx, c)
}

func rid(i int) string { return fmt.Sprintf("req_%016x", i) }

func TestObserverEmitsTheSequenceAndOneTerminalEvent(t *testing.T) {
	sink := &eventstest.RecordingSink{}
	o := newObs(t, sink, 100)
	drive(o, rid(1), gateway.Completion{Model: "m", Stream: true, Status: 200, Duration: 20 * time.Millisecond, TTFT: 5 * time.Millisecond, Handled: true,
		TokensSource: gateway.TokensUsage, InputTokens: 7, OutputTokens: 9}, 2)
	eventually(t, func() bool { return len(sink.Records()) == 5 }, "five events")
	want := []string{protocol.EventReceived, protocol.EventRouted, protocol.EventRouted, protocol.EventFirstToken, protocol.EventCompleted}
	// routed(2) comes after the first attempt ended; order within the request is production order.
	got := sink.Types(rid(1))
	if strings.Join(got, ",") != strings.Join([]string{want[0], want[1], want[2], want[3], want[4]}, ",") {
		t.Fatalf("sequence = %v", got)
	}
	evs, ok := sink.Events()
	if !ok {
		t.Fatal("an event did not decode")
	}
	term := evs[4]
	if term.Terminal.TokensSource != protocol.TokensFromUsage || *term.Terminal.InputTokens != 7 || *term.Terminal.OutputTokens != 9 {
		t.Fatalf("tokens: %+v", term.Terminal)
	}
	if len(term.Terminal.Attempts) != 2 || term.Terminal.Attempts[0].Outcome != "retried" || term.Terminal.Attempts[1].Outcome != "ok" || term.Terminal.Attempts[0].Number != 1 {
		t.Fatalf("attempts: %+v", term.Terminal.Attempts)
	}
	if term.AttemptID != term.Terminal.Attempts[1].AttemptID || term.WorkerID != "w2" || term.TenantID != "ten_a" || term.APIKeyID != "key_a" || term.Source != "gw-test" {
		t.Fatalf("envelope: %+v", term)
	}
	if evs[0].AttemptID != "" || evs[0].Received.EstimatedCostTokens != 10 || !evs[0].Received.Stream {
		t.Fatalf("received: %+v", evs[0])
	}
	if evs[3].WorkerID != "w2" {
		t.Fatalf("first_token worker: %q", evs[3].WorkerID)
	}
}

func TestRefusedRequestsEmitNothing(t *testing.T) {
	sink := &eventstest.RecordingSink{}
	o := newObs(t, sink, 100)
	ctx := o.RequestStarted(context.Background(), gateway.RequestStart{ID: rid(2)})
	o.RequestRejected(ctx, gateway.Rejection{RequestID: rid(2), Kind: gateway.RejectAuth, Status: 401})
	o.RequestCompleted(ctx, gateway.Completion{RequestID: rid(2), Status: 401})
	// a context that never saw RequestStarted is ignored rather than panicking
	o.RequestAdmitted(context.Background(), gateway.Admission{RequestID: rid(3)})
	o.RequestCompleted(context.Background(), gateway.Completion{RequestID: rid(3), Status: 200})
	time.Sleep(30 * time.Millisecond)
	if n := len(sink.Records()); n != 0 {
		t.Fatalf("expected no events, got %v", sink.Types(""))
	}
}

func TestTerminalClassification(t *testing.T) {
	cases := []struct {
		name  string
		c     gateway.Completion
		typ   string
		class string
	}{
		{"ok", gateway.Completion{Status: 200}, protocol.EventCompleted, ""},
		{"no capacity", gateway.Completion{Status: 503, ErrorCode: "NO_CAPACITY"}, protocol.EventFailed, protocol.FailureNoCapacity},
		{"unavailable", gateway.Completion{Status: 503, ErrorCode: "WORKER_UNAVAILABLE"}, protocol.EventFailed, protocol.FailureWorkerUnavailable},
		{"timeout", gateway.Completion{Status: 504, ErrorCode: "UPSTREAM_TIMEOUT"}, protocol.EventFailed, protocol.FailureTimeout},
		{"worker error", gateway.Completion{Status: 502, ErrorCode: "INFERENCE_FAILED"}, protocol.EventFailed, protocol.FailureWorkerError},
		{"worker 500 passthrough", gateway.Completion{Status: 500}, protocol.EventFailed, protocol.FailureWorkerError},
		{"aborted stream", gateway.Completion{Status: 502}, protocol.EventFailed, protocol.FailureWorkerError},
		{"client closed", gateway.Completion{Status: 499}, protocol.EventFailed, protocol.FailureClientClosed},
		{"internal", gateway.Completion{Status: 500, ErrorCode: "INTERNAL_ERROR"}, protocol.EventFailed, protocol.FailureInternal},
		{"unknown code", gateway.Completion{Status: 400, ErrorCode: "SOMETHING"}, protocol.EventFailed, protocol.FailureInternal},
	}
	for i, tc := range cases {
		sink := &eventstest.RecordingSink{}
		o := newObs(t, sink, 10)
		tc.c.Handled = true
		drive(o, rid(10+i), tc.c, 1)
		eventually(t, func() bool { return len(sink.Records()) == 4 }, tc.name)
		evs, _ := sink.Events()
		last := evs[len(evs)-1]
		if last.EventType != tc.typ || last.Terminal.FailureClass != tc.class {
			t.Errorf("%s: got %s/%q", tc.name, last.EventType, last.Terminal.FailureClass)
		}
		if last.Terminal.TokensSource != protocol.TokensFromEstimate || last.Terminal.InputTokens != nil {
			t.Errorf("%s: tokens should be unknown: %+v", tc.name, last.Terminal)
		}
	}
}

func TestChunkCountsLabelledAndInputUnknown(t *testing.T) {
	sink := &eventstest.RecordingSink{}
	o := newObs(t, sink, 10)
	drive(o, rid(40), gateway.Completion{Status: 200, Handled: true, TokensSource: gateway.TokensChunks, OutputTokens: 33}, 1)
	eventually(t, func() bool { return len(sink.Records()) == 4 }, "events")
	evs, _ := sink.Events()
	tt := evs[3].Terminal
	if tt.TokensSource != protocol.TokensFromChunks || tt.InputTokens != nil || *tt.OutputTokens != 33 {
		t.Fatalf("%+v", tt)
	}
}

func TestUnconfirmedModelNeverReachesTheTerminalEvent(t *testing.T) {
	sink := &eventstest.RecordingSink{}
	o := newObs(t, sink, 10)
	drive(o, rid(41), gateway.Completion{Status: 404, ErrorCode: "MODEL_NOT_FOUND", Model: "", Handled: true}, 0)
	eventually(t, func() bool { return len(sink.Records()) == 2 }, "events")
	evs, _ := sink.Events()
	if evs[1].EventType != protocol.EventFailed || evs[1].Model != "" || evs[0].Model != "m" {
		t.Fatalf("received model %q terminal model %q", evs[0].Model, evs[1].Model)
	}
}

func metric(t *testing.T, o *events.Observer, name string, labels map[string]string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(o.Collectors()...)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matches(m, labels) {
				switch {
				case m.Counter != nil:
					return m.Counter.GetValue()
				case m.Gauge != nil:
					return m.Gauge.GetValue()
				case m.Histogram != nil:
					return float64(m.Histogram.GetSampleCount())
				}
			}
		}
	}
	return -1
}

func matches(m *dto.Metric, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, l := range m.Label {
			if l.GetName() == k && l.GetValue() == v {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// A sink that never returns must not slow the request path, and the memory it can pin is bounded (D10).
func TestStuckSinkNeverBlocksAndDropsAreCounted(t *testing.T) {
	sink := eventstest.NewBlockingSink()
	t.Cleanup(sink.Release)
	const buffer = 1000
	o := events.NewObserver(sink, events.Config{BufferSize: buffer, Source: "gw"}, quiet())
	before := runtime.NumGoroutine()
	const calls = 1_000_000
	ctx := o.RequestStarted(context.Background(), gateway.RequestStart{ID: rid(1)})
	start := time.Now()
	var maxDepth int
	for i := 0; i < calls; i++ {
		o.RequestAdmitted(ctx, gateway.Admission{RequestID: rid(1), Model: "m"})
		if i%1000 == 0 {
			maxDepth = max(maxDepth, o.Publisher().Depth())
		}
	}
	elapsed := time.Since(start)
	if elapsed > 10*time.Second { // typically ~0.2s; the bound catches a blocking send
		t.Fatalf("%d observer calls took %v", calls, elapsed)
	}
	if maxDepth > buffer {
		t.Fatalf("buffer depth %d exceeded %d", maxDepth, buffer)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
	dropped := o.Publisher().Dropped(events.ReasonBufferFull)
	// One event is held by the stuck Produce and `buffer` wait in the queue; everything else was dropped.
	if got := uint64(calls) - dropped; got != uint64(buffer)+1 && got != uint64(buffer) {
		t.Fatalf("accepted %d, want %d (+1 held by the sink); dropped %d", got, buffer+1, dropped)
	}
	if v := metric(t, o, "event_publish_failures_total", map[string]string{"reason": "buffer_full"}); v != float64(dropped) {
		t.Fatalf("metric %v != dropped %d", v, dropped)
	}
	if v := metric(t, o, "events_buffer_depth", nil); v > buffer {
		t.Fatalf("depth gauge %v", v)
	}
	// Close with a stuck sink gives up on time and counts what it could not flush.
	cctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	err := o.Close(cctx)
	if time.Since(t0) > time.Second || err == nil {
		t.Fatalf("Close took %v err=%v", time.Since(t0), err)
	}
	if o.Publisher().Dropped(events.ReasonShutdown) < buffer {
		t.Fatalf("shutdown drops: %d", o.Publisher().Dropped(events.ReasonShutdown))
	}
}

// gateSink records produce order and holds the first record until released, to prove which events survive.
type gateSink struct {
	eventstest.RecordingSink
	gate    chan struct{}
	started chan struct{}
	once    sync.Once
	order   []string
	mu      sync.Mutex
}

func (g *gateSink) Produce(r events.Record, done func(error)) error {
	g.once.Do(func() { close(g.started); <-g.gate })
	g.mu.Lock()
	g.order = append(g.order, string(r.Key))
	g.mu.Unlock()
	done(nil)
	return nil
}

func TestFullBufferDropsTheNewestAndKeepsTheOldest(t *testing.T) {
	g := &gateSink{gate: make(chan struct{}), started: make(chan struct{})}
	pub := events.NewPublisher(g, events.Config{BufferSize: 3}, quiet())
	mk := func(i int) protocol.Event {
		id := rid(i)
		return protocol.Event{EventID: protocol.NewEventID(id, protocol.EventReceived, ""), EventType: protocol.EventReceived, SchemaVersion: 1,
			Timestamp: time.Now(), Source: "s", RequestID: id, Model: "m", Received: &protocol.ReceivedData{}}
	}
	pub.Enqueue(mk(0))
	<-g.started // event 0 is in the sink, blocked
	for i := 1; i <= 8; i++ {
		pub.Enqueue(mk(i))
	}
	close(g.gate)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if pub.Dropped(events.ReasonBufferFull) != 5 {
		t.Fatalf("dropped %d, want 5", pub.Dropped(events.ReasonBufferFull))
	}
	want := []string{rid(0), rid(1), rid(2), rid(3)}
	g.mu.Lock()
	defer g.mu.Unlock()
	if fmt.Sprint(g.order) != fmt.Sprint(want) {
		t.Fatalf("delivered %v, want the oldest %v", g.order, want)
	}
	// After Close, events are refused and counted.
	if pub.Enqueue(mk(99)) || pub.Dropped(events.ReasonShutdown) != 1 {
		t.Fatal("an event after Close must be dropped as shutdown")
	}
}

func TestCloseFlushesEverythingAccepted(t *testing.T) {
	sink := &eventstest.SlowSink{Delay: 30 * time.Millisecond}
	o := events.NewObserver(sink, events.Config{BufferSize: 1000, Source: "gw"}, quiet())
	for i := 0; i < 100; i++ {
		drive(o, rid(i), gateway.Completion{Status: 200, Handled: true}, 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := o.Publisher().Published(); got != 400 {
		t.Fatalf("published %d of 400", got)
	}
	if o.Publisher().Dropped(events.ReasonShutdown) != 0 {
		t.Fatal("nothing should have been lost")
	}
	if err := o.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestProducerFullAndDeliveryFailuresAreCountedByReason(t *testing.T) {
	full := &eventstest.FullSink{}
	o := newObs(t, full, 100)
	drive(o, rid(1), gateway.Completion{Status: 200, Handled: true}, 1)
	eventually(t, func() bool { return o.Publisher().Dropped(events.ReasonProducerFull) == 4 }, "producer_full")

	logs := &syncBuf{}
	fail := &eventstest.FailingSink{Err: errors.New("broker unreachable")}
	o2 := events.NewObserver(fail, events.Config{BufferSize: 100}, slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { _ = o2.Close(context.Background()) })
	for i := 0; i < 10; i++ {
		drive(o2, rid(100+i), gateway.Completion{Status: 200, Handled: true}, 1)
	}
	eventually(t, func() bool { return o2.Publisher().Dropped(events.ReasonDeliveryFailed) == 40 }, "delivery_failed")
	if n := strings.Count(logs.String(), "event delivery is failing"); n != 1 {
		t.Fatalf("the outage must be logged once, got %d", n)
	}
	if o2.Publisher().Published() != 0 {
		t.Fatal("nothing was delivered")
	}
}

func TestEncodeFailureIsCountedAndDoesNotStopTheStream(t *testing.T) {
	sink := &eventstest.RecordingSink{}
	o := newObs(t, sink, 10)
	ctx := o.RequestStarted(context.Background(), gateway.RequestStart{ID: rid(1)})
	o.RequestAdmitted(ctx, gateway.Admission{RequestID: rid(1), Model: "bad\nmodel"}) // control character: refused by Encode
	drive(o, rid(2), gateway.Completion{Status: 200, Handled: true}, 1)
	eventually(t, func() bool { return o.Publisher().Dropped(events.ReasonEncode) == 1 && len(sink.Records()) == 4 }, "encode drop then normal flow")
}

func TestRecoveryIsLoggedOnce(t *testing.T) {
	logs := &syncBuf{}
	var failing atomic.Bool
	failing.Store(true)
	sink := &toggleSink{failing: &failing}
	o := events.NewObserver(sink, events.Config{BufferSize: 100}, slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { _ = o.Close(context.Background()) })
	drive(o, rid(1), gateway.Completion{Status: 200, Handled: true}, 1)
	eventually(t, func() bool { return o.Publisher().Dropped(events.ReasonDeliveryFailed) == 4 }, "failures")
	failing.Store(false)
	drive(o, rid(2), gateway.Completion{Status: 200, Handled: true}, 1)
	drive(o, rid(3), gateway.Completion{Status: 200, Handled: true}, 1)
	eventually(t, func() bool { return o.Publisher().Published() == 8 }, "recovery")
	if strings.Count(logs.String(), "event delivery recovered") != 1 {
		t.Fatalf("recovery must be logged once:\n%s", logs.String())
	}
	if metric(t, o, "event_publish_latency_seconds", nil) != 8 {
		t.Fatal("latency histogram should have 8 observations")
	}
}

type toggleSink struct{ failing *atomic.Bool }

func (s *toggleSink) Produce(_ events.Record, done func(error)) error {
	if s.failing.Load() {
		done(errors.New("down"))
		return nil
	}
	done(nil)
	return nil
}
func (s *toggleSink) Flush(context.Context) error { return nil }
func (s *toggleSink) Close() error                { return nil }

func TestEventIDsAreStableWhenTheSameCallsRepeat(t *testing.T) {
	sink := &eventstest.RecordingSink{}
	o := newObs(t, sink, 100)
	drive(o, rid(1), gateway.Completion{Status: 200, Handled: true}, 1)
	drive(o, rid(1), gateway.Completion{Status: 200, Handled: true}, 1)
	eventually(t, func() bool { return len(sink.Records()) == 8 }, "events")
	evs, _ := sink.Events()
	for i := 0; i < 4; i++ {
		if evs[i].EventID != evs[i+4].EventID {
			t.Fatalf("event %d: IDs differ across repeats: %s %s", i, evs[i].EventID, evs[i+4].EventID)
		}
	}
}

// BenchmarkObserver pins the cost per lifecycle with a recording sink (D20).
func BenchmarkObserverLifecycle(b *testing.B) {
	sink := discardSink{}
	o := events.NewObserver(sink, events.Config{BufferSize: 1 << 16, Source: "gw"}, quiet())
	defer func() { _ = o.Close(context.Background()) }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := o.RequestStarted(context.Background(), gateway.RequestStart{ID: "req_0000000000000001"})
		o.RequestAdmitted(ctx, gateway.Admission{RequestID: "req_0000000000000001", Model: "m", TenantID: "ten_a", APIKeyID: "key_a", EstimatedCost: 10})
		actx := o.AttemptStarted(ctx, gateway.AttemptStart{RequestID: "req_0000000000000001", AttemptID: "att_0000000000000001", Number: 1, WorkerID: "w1"})
		o.FirstToken(actx, gateway.FirstToken{RequestID: "req_0000000000000001", AttemptID: "att_0000000000000001"})
		o.AttemptEnded(actx, gateway.AttemptEnd{RequestID: "req_0000000000000001", AttemptID: "att_0000000000000001", Number: 1, Outcome: "ok"})
		o.RequestCompleted(ctx, gateway.Completion{RequestID: "req_0000000000000001", Status: 200, Handled: true})
	}
}

type discardSink struct{}

func (discardSink) Produce(_ events.Record, done func(error)) error { done(nil); return nil }
func (discardSink) Flush(context.Context) error                     { return nil }
func (discardSink) Close() error                                    { return nil }
