package usage_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"serverflow/internal/events/eventstest"
	"serverflow/internal/usage"
	"serverflow/pkg/protocol"
)

const topic = "inference.lifecycle.v1"

func terminal(i int, typ string, tokens bool) []byte {
	id := fmt.Sprintf("req_%016x", i)
	e := protocol.Event{EventID: protocol.NewEventID(id, typ, "att_0000000000000001"), EventType: typ, SchemaVersion: 1,
		Timestamp: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), Source: "gw", RequestID: id, AttemptID: "att_0000000000000001",
		TenantID: "ten_aaaa", Model: "m", WorkerID: "w1",
		Terminal: &protocol.TerminalData{HTTPStatus: 200, DurationMS: 10, TokensSource: protocol.TokensFromEstimate, EstimatedCostTokens: 5,
			Attempts: []protocol.AttemptData{{AttemptID: "att_0000000000000001", Number: 1, Outcome: "ok"}}}}
	if typ == protocol.EventFailed {
		e.Terminal.HTTPStatus, e.Terminal.FailureClass = 503, protocol.FailureNoCapacity
	}
	if tokens {
		in, out := int64(3), int64(4)
		e.Terminal.InputTokens, e.Terminal.OutputTokens, e.Terminal.TokensSource = &in, &out, protocol.TokensFromUsage
	}
	b, err := protocol.Encode(e)
	if err != nil {
		panic(err)
	}
	return b
}

func other(i int, typ string) []byte {
	id := fmt.Sprintf("req_%016x", i)
	e := protocol.Event{EventID: protocol.NewEventID(id, typ, ""), EventType: typ, SchemaVersion: 1, Timestamp: time.Now(), Source: "gw", RequestID: id,
		Received: &protocol.ReceivedData{}}
	b, _ := protocol.Encode(e)
	return b
}

func key(i int) []byte { return []byte(fmt.Sprintf("req_%016x", i)) }

func start(t *testing.T, b *eventstest.MemBroker, st usage.Store, group string, mut func(*usage.Config)) (*usage.Consumer, *eventstest.MemSource, context.CancelFunc, chan error) {
	t.Helper()
	src := b.NewSource(group)
	cfg := usage.Config{BatchSize: 10, BatchTimeout: 20 * time.Millisecond, GroupID: group, Topic: topic, BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
		Logger: slog.New(slog.DiscardHandler)}
	if mut != nil {
		mut(&cfg)
	}
	c := usage.New(src, st, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(cancel)
	return c, src, cancel, done
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("timed out: " + msg)
}

func TestOnlyTerminalEventsCreateRowsAndOffsetsPassTheRest(t *testing.T) {
	b := eventstest.NewMemBroker(topic, 3)
	st := eventstest.NewMemStore()
	for i := 0; i < 30; i++ {
		b.Append(key(i), other(i, protocol.EventReceived))
		b.Append(key(i), terminal(i, []string{protocol.EventCompleted, protocol.EventFailed}[i%2], i%3 == 0))
	}
	c, _, cancel, done := start(t, b, st, "g", nil)
	eventually(t, func() bool { return b.Lag("g") == 0 }, "lag drains")
	cancel()
	<-done
	if st.Count() != 30 || c.Count(usage.ResultInserted) != 30 || c.Count(usage.ResultSkipped) != 30 {
		t.Fatalf("rows=%d inserted=%d skipped=%d", st.Count(), c.Count(usage.ResultInserted), c.Count(usage.ResultSkipped))
	}
	failed, withTok := 0, 0
	for _, r := range st.Rows() {
		if r.Outcome == "failed" {
			failed++
			if r.FailureClass == "" {
				t.Fatal("failed row without class")
			}
		}
		if r.TokensSource == "usage" && r.InputTokens != nil && *r.InputTokens == 3 {
			withTok++
		}
	}
	if failed != 15 || withTok != 10 {
		t.Fatalf("failed=%d withTokens=%d", failed, withTok)
	}
}

func TestReplayAndDuplicatesAreAbsorbed(t *testing.T) {
	b := eventstest.NewMemBroker(topic, 2)
	st := eventstest.NewMemStore()
	for i := 0; i < 20; i++ {
		b.Append(key(i), terminal(i, protocol.EventCompleted, true))
		b.Append(key(i), terminal(i, protocol.EventCompleted, true)) // the same event twice
	}
	c, _, cancel, done := start(t, b, st, "g1", nil)
	eventually(t, func() bool { return b.Lag("g1") == 0 }, "first pass")
	cancel()
	<-done
	if st.Count() != 20 || c.Count(usage.ResultDuplicate) != 20 {
		t.Fatalf("rows=%d dup=%d", st.Count(), c.Count(usage.ResultDuplicate))
	}
	// a fresh group re-reads everything: totals unchanged, every event counted duplicate
	c2, _, cancel2, done2 := start(t, b, st, "g2", nil)
	eventually(t, func() bool { return b.Lag("g2") == 0 }, "replay")
	cancel2()
	<-done2
	if st.Count() != 20 || c2.Count(usage.ResultInserted) != 0 || c2.Count(usage.ResultDuplicate) != 40 {
		t.Fatalf("replay: rows=%d ins=%d dup=%d", st.Count(), c2.Count(usage.ResultInserted), c2.Count(usage.ResultDuplicate))
	}
}

func TestOffsetsAreCommittedOnlyAfterTheDatabase(t *testing.T) {
	b := eventstest.NewMemBroker(topic, 1)
	st := eventstest.NewMemStore()
	for i := 0; i < 10; i++ {
		b.Append(key(i), terminal(i, protocol.EventCompleted, false))
	}
	// crash between the DB commit and the offset commit
	crash := errors.New("crash")
	_, src, _, done := start(t, b, st, "g", func(c *usage.Config) { c.BeforeCommit = func() error { return crash } })
	if err := <-done; !errors.Is(err, crash) {
		t.Fatalf("Run = %v", err)
	}
	if st.Count() != 10 || b.Lag("g") != 10 || src.Commits != 0 {
		t.Fatalf("rows=%d lag=%d commits=%d: rows must be durable and offsets uncommitted", st.Count(), b.Lag("g"), src.Commits)
	}
	// the restarted consumer reads the batch again: no loss, no double count
	c2, _, cancel, done2 := start(t, b, st, "g", nil)
	eventually(t, func() bool { return b.Lag("g") == 0 }, "recovery")
	cancel()
	<-done2
	if st.Count() != 10 || c2.Count(usage.ResultInserted) != 0 || c2.Count(usage.ResultDuplicate) != 10 {
		t.Fatalf("rows=%d ins=%d dup=%d", st.Count(), c2.Count(usage.ResultInserted), c2.Count(usage.ResultDuplicate))
	}
}

func TestDatabaseFailureNeverCommitsAndRecovers(t *testing.T) {
	b := eventstest.NewMemBroker(topic, 1)
	st := eventstest.NewMemStore()
	st.FailNext = 5
	for i := 0; i < 10; i++ {
		b.Append(key(i), terminal(i, protocol.EventCompleted, false))
	}
	c, src, cancel, done := start(t, b, st, "g", nil)
	eventually(t, func() bool { return st.Count() == 10 }, "recovery after the database comes back")
	eventually(t, func() bool { return b.Lag("g") == 0 }, "commit after recovery")
	cancel()
	<-done
	if c.Count(usage.ResultInserted) != 10 || src.Commits == 0 {
		t.Fatalf("inserted=%d", c.Count(usage.ResultInserted))
	}
}

func TestPoisonRecordsAreRejectedAndTheStreamKeepsMoving(t *testing.T) {
	b := eventstest.NewMemBroker(topic, 1)
	st := eventstest.NewMemStore()
	b.Append(key(0), []byte("not json at all"))
	b.Append(key(1), []byte(`{"schema_version":1,"event_type":"inference.request.completed"}`))
	var m map[string]any
	_ = json.Unmarshal(terminal(2, protocol.EventCompleted, false), &m)
	m["schema_version"] = 7
	future, _ := json.Marshal(m)
	b.Append(key(2), future)
	b.Append(key(3), []byte(strings.Repeat("x", protocol.MaxEventBytes+1)))
	b.Append(key(4), terminal(4, protocol.EventCompleted, false))
	c, _, cancel, done := start(t, b, st, "g", nil)
	eventually(t, func() bool { return b.Lag("g") == 0 }, "lag drains past the poison")
	cancel()
	<-done
	reasons := map[string]bool{}
	for _, r := range st.Rejects {
		reasons[r.Reason] = true
	}
	for _, want := range []string{usage.RejectDecode, usage.RejectInvalid, usage.RejectVersion, usage.RejectOversize} {
		if !reasons[want] {
			t.Errorf("missing reject reason %s: %+v", want, st.Rejects)
		}
	}
	if st.Count() != 1 || c.Count(usage.ResultRejected) != 4 {
		t.Fatalf("rows=%d rejected=%d", st.Count(), c.Count(usage.ResultRejected))
	}
	for _, r := range st.Rejects {
		if fmt.Sprintf("%+v", r) == "" || strings.Contains(fmt.Sprintf("%+v", r), "not json") {
			t.Fatal("a reject must not carry the payload")
		}
	}
}

func TestOneRowTheDatabaseRefusesIsRejectedAlone(t *testing.T) {
	b := eventstest.NewMemBroker(topic, 1)
	st := eventstest.NewMemStore()
	st.BadRequestID = fmt.Sprintf("req_%016x", 3)
	for i := 0; i < 6; i++ {
		b.Append(key(i), terminal(i, protocol.EventCompleted, false))
	}
	c, _, cancel, done := start(t, b, st, "g", nil)
	eventually(t, func() bool { return b.Lag("g") == 0 }, "drains")
	cancel()
	<-done
	if st.Count() != 5 || c.Count(usage.ResultRejected) != 1 || len(st.Rejects) != 1 {
		t.Fatalf("rows=%d rejected=%d", st.Count(), c.Count(usage.ResultRejected))
	}
}

func TestShutdownFinishesTheBatchInHand(t *testing.T) {
	b := eventstest.NewMemBroker(topic, 1)
	st := eventstest.NewMemStore()
	for i := 0; i < 5; i++ {
		b.Append(key(i), terminal(i, protocol.EventCompleted, false))
	}
	_, _, cancel, done := start(t, b, st, "g", func(c *usage.Config) { c.BatchSize = 100; c.BatchTimeout = time.Hour })
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if st.Count() != 5 || b.Lag("g") != 0 {
		t.Fatalf("rows=%d lag=%d: the batch in hand must be written and committed at shutdown", st.Count(), b.Lag("g"))
	}
}
