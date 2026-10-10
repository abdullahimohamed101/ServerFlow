package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/events"
	"serverflow/internal/kafka"
	"serverflow/internal/kafka/kafkatest"
	"serverflow/pkg/protocol"
)

// These tests need a real broker (scripts/dev-kafka.sh); without one they skip, and scripts/quality.sh refuses to
// let that pass in CI.

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func reqID(i int) string { return fmt.Sprintf("req_%016x", i) }

func receivedEvent(i int) protocol.Event {
	id := reqID(i)
	return protocol.Event{EventID: protocol.NewEventID(id, protocol.EventReceived, ""), EventType: protocol.EventReceived, SchemaVersion: 1,
		Timestamp: time.Now(), Source: "t", RequestID: id, Model: "m", Received: &protocol.ReceivedData{EstimatedCostTokens: int64(i)}}
}

// diag, when set, adds the current state to a timeout message.
var diag func() string

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	extra := ""
	if diag != nil {
		extra = " [" + diag() + "]"
	}
	t.Fatalf("timed out after %v: %s%s", d, msg, extra)
}

func producerFor(t *testing.T, cfg kafka.Config, topic string) *kafka.Producer {
	t.Helper()
	cfg.Topic = topic
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestProducerDeliversToRealBroker(t *testing.T) {
	cfg := kafkatest.Config(t)
	topic := kafkatest.NewTopic(t, cfg, 6)
	pub := events.NewPublisher(producerFor(t, cfg, topic), events.Config{BufferSize: 2000}, slog.New(slog.DiscardHandler))
	const requests, perRequest = 100, 3
	for i := 0; i < requests; i++ {
		for k := 0; k < perRequest; k++ { // three events of one request: same key, so one partition, in order
			e := receivedEvent(i)
			e.EventType = []string{protocol.EventReceived, protocol.EventRouted, protocol.EventCompleted}[k]
			e.AttemptID = []string{"", "att_0000000000000001", "att_0000000000000001"}[k]
			e.EventID = protocol.NewEventID(e.RequestID, e.EventType, e.AttemptID)
			e.Received = nil
			switch k {
			case 0:
				e.Received = &protocol.ReceivedData{}
			case 1:
				e.Routed = &protocol.RoutedData{AttemptNumber: 1}
			default:
				e.Terminal = &protocol.TerminalData{HTTPStatus: 200, TokensSource: protocol.TokensFromEstimate}
			}
			if !pub.Enqueue(e) {
				t.Fatal("enqueue refused")
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if pub.Published() != requests*perRequest || pub.Dropped(events.ReasonDeliveryFailed) != 0 {
		t.Fatalf("published %d dropped %d", pub.Published(), pub.Dropped(events.ReasonDeliveryFailed))
	}
	msgs := kafkatest.ReadN(t, cfg, topic, requests*perRequest, 30*time.Second)
	part := map[string]int32{}
	order := map[string][]string{}
	for _, m := range msgs {
		e, err := protocol.Decode(m.Value)
		if err != nil {
			t.Fatalf("undecodable record: %v", err)
		}
		if string(m.Key) != e.RequestID {
			t.Fatalf("key %q != request id %q", m.Key, e.RequestID)
		}
		if p, ok := part[e.RequestID]; ok && p != m.Partition {
			t.Fatalf("request %s spread over partitions %d and %d", e.RequestID, p, m.Partition)
		}
		part[e.RequestID] = m.Partition
		order[e.RequestID] = append(order[e.RequestID], e.EventType)
	}
	want := strings.Join([]string{protocol.EventReceived, protocol.EventRouted, protocol.EventCompleted}, ",")
	distinct := map[int32]bool{}
	for id, o := range order {
		if strings.Join(o, ",") != want {
			t.Fatalf("request %s arrived as %v", id, o)
		}
		distinct[part[id]] = true
	}
	if len(order) != requests || len(distinct) < 2 {
		t.Fatalf("requests=%d partitions used=%d: keys are not spreading", len(order), len(distinct))
	}
}

func TestUnauthenticatedClientIsRefusedByTheTestBroker(t *testing.T) {
	cfg := kafkatest.Config(t)
	cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword = "none", "", ""
	a, err := kafka.NewAdmin(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := a.Partitions(ctx, "inference.lifecycle.v1"); err == nil {
		t.Fatal("the test broker must require authentication, as CI does; this guards test clients that forget credentials")
	}
	cfg = kafkatest.Config(t)
	cfg.SASLPassword = "wrong-" + cfg.SASLPassword
	a2, _ := kafka.NewAdmin(cfg)
	defer a2.Close()
	if _, err := a2.Partitions(ctx, "inference.lifecycle.v1"); err == nil {
		t.Fatal("a wrong password must be refused")
	}
}

func TestMissingTopicDropsAreCountedAndNothingBlocks(t *testing.T) {
	cfg := kafkatest.Config(t)
	cfg.DeliveryTimeout = 3 * time.Second
	logs := &syncBuf{}
	pub := events.NewPublisher(producerFor(t, cfg, kafkatest.Unique("sf-missing")), events.Config{BufferSize: 100}, slog.New(slog.NewTextHandler(logs, nil)))
	start := time.Now()
	for i := 0; i < 10; i++ {
		pub.Enqueue(receivedEvent(i))
	}
	if time.Since(start) > time.Second {
		t.Fatal("Enqueue blocked")
	}
	waitFor(t, 30*time.Second, func() bool { return pub.Dropped(events.ReasonDeliveryFailed) == 10 }, "ten delivery failures for a missing topic")
	if n := strings.Count(logs.String(), "event delivery is failing"); n != 1 {
		t.Fatalf("the missing topic must be logged once, got %d", n)
	}
	if pub.Published() != 0 {
		t.Fatal("nothing can be published to a topic that does not exist")
	}
}

// The outage matrix against a real broker of the test's own: stopped (a crash), black-holed (a frozen process),
// and back again. Requests are modelled by Enqueue calls, whose latency must not move; every event is either
// delivered or counted as dropped; recovery is logged once; nothing leaks.
func TestBrokerOutageDoesNotBlockAndDropsAreCounted(t *testing.T) {
	b := kafkatest.StartPrivateBroker(t)
	cfg := b.Config()
	cfg.DeliveryTimeout = 4 * time.Second
	topic := kafkatest.NewTopic(t, cfg, 3)
	baseline := runtime.NumGoroutine()
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	cfg.Logger, cfg.Topic = log, topic
	sink, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	pub := events.NewPublisher(sink, events.Config{BufferSize: 5000}, log)

	var maxEnqueue time.Duration
	sent := 0
	send := func(n int) {
		for i := 0; i < n; i++ {
			sent++
			s := time.Now()
			pub.Enqueue(receivedEvent(sent))
			maxEnqueue = max(maxEnqueue, time.Since(s))
			time.Sleep(2 * time.Millisecond)
		}
	}
	state := func() string {
		return fmt.Sprintf("sent=%d published=%d delivery_failed=%d producer_full=%d buffer_full=%d encode=%d depth=%d",
			sent, pub.Published(), pub.Dropped(events.ReasonDeliveryFailed), pub.Dropped(events.ReasonProducerFull), pub.Dropped(events.ReasonBufferFull), pub.Dropped(events.ReasonEncode), pub.Depth())
	}
	diag = state
	t.Cleanup(func() { diag = nil })
	accounted := func() uint64 {
		return pub.Published() + pub.Dropped(events.ReasonDeliveryFailed) + pub.Dropped(events.ReasonProducerFull) + pub.Dropped(events.ReasonBufferFull) + pub.Dropped(events.ReasonEncode)
	}

	// 1. healthy
	send(20)
	waitFor(t, 20*time.Second, func() bool { return pub.Published() == 20 }, "20 events delivered while the broker is up")

	// 2. the broker crashes
	b.Stop()
	send(100)
	waitFor(t, 40*time.Second, func() bool { return accounted() == uint64(sent) }, "every event sent during the crash is delivered or counted")
	if pub.Dropped(events.ReasonDeliveryFailed) == 0 {
		t.Fatal("events sent to a dead broker must be counted as delivery failures")
	}
	if n := strings.Count(logs.String(), "kafka is unreachable"); n != 1 {
		t.Fatalf("the outage must be logged exactly once, got %d", n)
	}

	// 3. the broker returns: new events arrive and the recovery is logged once
	b.Start()
	before := pub.Published()
	waitFor(t, 60*time.Second, func() bool {
		send(5)
		return pub.Published() >= before+5
	}, "events published after the broker came back")
	waitFor(t, 20*time.Second, func() bool { return strings.Count(logs.String(), "kafka connection recovered") == 1 }, "the recovery is logged once")

	// 4. the broker freezes (a black hole): connections stay open, nothing answers
	b.Pause()
	send(100)
	waitFor(t, 60*time.Second, func() bool { return accounted() == uint64(sent) }, "every event sent into the black hole is delivered or counted")
	b.Unpause()
	before = pub.Published()
	waitFor(t, 60*time.Second, func() bool {
		send(5)
		return pub.Published() >= before+5
	}, "events published after the freeze")

	if maxEnqueue > 100*time.Millisecond {
		t.Fatalf("an Enqueue took %v during the outages; it must never wait on Kafka", maxEnqueue)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pub.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = sink.Close()
	waitFor(t, 20*time.Second, func() bool { return runtime.NumGoroutine() <= baseline+3 }, fmt.Sprintf("goroutines return to the baseline (now %d, baseline %d)", runtime.NumGoroutine(), baseline))
	t.Logf("sent %d published %d dropped(delivery_failed=%d producer_full=%d) max enqueue %v", sent, pub.Published(),
		pub.Dropped(events.ReasonDeliveryFailed), pub.Dropped(events.ReasonProducerFull), maxEnqueue)
}

// The consumer commits only when told to: an uncommitted batch is delivered again to the next member of the group,
// a committed one is not. The usage consumer relies on this to commit after its database commit.
func TestConsumerGroupCommitsAfterDatabase(t *testing.T) {
	cfg := kafkatest.Config(t)
	topic := kafkatest.NewTopic(t, cfg, 3)
	adm := kafkatest.Admin(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const total = 30
	for i := 0; i < total; i++ {
		if err := adm.Produce(ctx, topic, []byte(reqID(i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	group := kafkatest.Unique("sf-group")
	cfg.Topic, cfg.GroupID, cfg.StartOffset, cfg.LagInterval = topic, group, "earliest", 100*time.Millisecond

	drain := func(c *kafka.Consumer, want int, commit bool) (n int) {
		deadline := time.Now().Add(30 * time.Second)
		for n < want && time.Now().Before(deadline) {
			pc, pcancel := context.WithTimeout(ctx, time.Second)
			msgs, _ := c.Poll(pc, 100)
			pcancel()
			if len(msgs) == 0 {
				continue
			}
			n += len(msgs)
			if commit {
				next := map[int32]int64{}
				for _, m := range msgs {
					next[m.Partition] = max(next[m.Partition], m.Offset+1)
				}
				var offs []usageOffset
				for p, o := range next {
					offs = append(offs, usageOffset{Topic: topic, Partition: p, Offset: o})
				}
				if err := c.Commit(ctx, toUsage(offs)); err != nil {
					t.Fatalf("commit: %v", err)
				}
			}
		}
		return n
	}

	// First member reads everything and commits nothing.
	c1, err := kafka.NewConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(c1, total, false); got != total {
		t.Fatalf("first member read %d of %d", got, total)
	}
	c1.Close()
	lag, err := adm.GroupLag(ctx, group, topic)
	if err != nil || lag != total {
		t.Fatalf("lag with nothing committed = %d (%v), want %d", lag, err, total)
	}

	// The next member of the group is given all of it again, and this time commits.
	c2, err := kafka.NewConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(c2, total, true); got != total {
		t.Fatalf("second member re-read %d of %d: uncommitted records must be redelivered", got, total)
	}
	waitFor(t, 20*time.Second, func() bool { return c2.Lag() == 0 }, "consumer lag after committing everything")
	c2.Close()
	if lag, err = adm.GroupLag(ctx, group, topic); err != nil || lag != 0 {
		t.Fatalf("lag after commit = %d (%v), want 0", lag, err)
	}

	// A third member finds nothing left.
	c3, err := kafka.NewConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	pc, pcancel := context.WithTimeout(ctx, 5*time.Second)
	defer pcancel()
	if msgs, _ := c3.Poll(pc, 100); len(msgs) != 0 {
		t.Fatalf("a committed group was redelivered %d records", len(msgs))
	}
}

// consumer_lag comes from the broker's committed offsets against partition end offsets, not from what this process
// happened to poll: a backlog is visible before the first poll, it falls as batches are committed, and a stopped
// poller does not make it zero.
func TestConsumerLagIsTheGroupsRealBacklog(t *testing.T) {
	cfg := kafkatest.Config(t)
	topic := kafkatest.NewTopic(t, cfg, 4)
	adm := kafkatest.Admin(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const total = 200
	for i := 0; i < total; i++ {
		if err := adm.Produce(ctx, topic, []byte(reqID(i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	cfg.Topic, cfg.GroupID, cfg.StartOffset, cfg.LagInterval = topic, kafkatest.Unique("sf-lag"), "earliest", 100*time.Millisecond
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Nothing has been polled: the backlog is still all of it.
	waitFor(t, 20*time.Second, func() bool { return c.Lag() == total }, fmt.Sprintf("lag %d before any poll (have %d)", total, c.Lag()))
	// Process half and commit: the lag follows the commit, even though polling now stops.
	done := 0
	offs := map[int32]int64{}
	for done < total/2 {
		pc, pcancel := context.WithTimeout(ctx, time.Second)
		msgs, _ := c.Poll(pc, 10)
		pcancel()
		for _, m := range msgs {
			offs[m.Partition] = max(offs[m.Partition], m.Offset+1)
		}
		done += len(msgs)
		if len(msgs) > 0 {
			var o []usageOffset
			for p, off := range offs {
				o = append(o, usageOffset{Topic: topic, Partition: p, Offset: off})
			}
			if err := c.Commit(ctx, toUsage(o)); err != nil {
				t.Fatal(err)
			}
		}
	}
	want := int64(total - done)
	waitFor(t, 20*time.Second, func() bool { return c.Lag() == want }, fmt.Sprintf("lag %d after committing %d (have %d)", want, done, c.Lag()))
	// Drain the rest.
	for done < total {
		pc, pcancel := context.WithTimeout(ctx, time.Second)
		msgs, _ := c.Poll(pc, 50)
		pcancel()
		for _, m := range msgs {
			offs[m.Partition] = max(offs[m.Partition], m.Offset+1)
		}
		done += len(msgs)
		if len(msgs) > 0 {
			var o []usageOffset
			for p, off := range offs {
				o = append(o, usageOffset{Topic: topic, Partition: p, Offset: off})
			}
			if err := c.Commit(ctx, toUsage(o)); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitFor(t, 20*time.Second, func() bool { return c.Lag() == 0 }, fmt.Sprintf("lag 0 after everything is committed (have %d)", c.Lag()))
}
