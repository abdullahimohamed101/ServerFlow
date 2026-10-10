package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"serverflow/internal/config"
	"serverflow/internal/events"
	"serverflow/internal/gateway"
	"serverflow/internal/kafka"
	"serverflow/internal/kafka/kafkatest"
	"serverflow/internal/mockworker"
	"serverflow/internal/postgres"
	"serverflow/internal/postgres/postgrestest"
	"serverflow/internal/usage"
	"serverflow/pkg/protocol"
)

// kafkaStack is a gateway publishing to a real topic, a migrated database, and the means to run usage consumers.
type kafkaStack struct {
	t     *testing.T
	cfg   kafka.Config // topic and no group
	topic string
	gw    string
	obs   *events.Observer
	store *postgres.Store
	admin *kafka.Admin
}

func newKafkaStack(t *testing.T) *kafkaStack {
	t.Helper()
	kcfg := kafkatest.Config(t)
	dsn := postgrestest.NewDSN(t)
	topic := kafkatest.NewTopic(t, kcfg, 6)
	kcfg.Topic = topic
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, postgres.Config{DSN: dsn, MaxConns: 8, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if _, err := store.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}
	sink, err := kafka.NewProducer(kcfg)
	if err != nil {
		t.Fatal(err)
	}
	obs := events.NewObserver(sink, events.Config{BufferSize: 20000, Source: "gw-e2e"}, quiet())
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = obs.Close(c)
		_ = sink.Close()
	})

	mock, _ := startMock(t, func(c *mockworker.Config) { c.TTFT = time.Millisecond; c.TokensPerSecond = 5000 })
	gcfg := config.Default().Gateway
	gcfg.UpstreamURL, gcfg.Models, gcfg.UpstreamHeaderTimeout = mock.URL, []string{model}, 5*time.Second
	ts := httptest.NewServer(gateway.New(gcfg, slog.New(slog.DiscardHandler), gateway.WithObserver(obs)).Handler())
	t.Cleanup(ts.Close)
	return &kafkaStack{t: t, cfg: kcfg, topic: topic, gw: ts.URL, obs: obs, store: store, admin: kafkatest.Admin(t, kcfg)}
}

// send makes n non-streaming requests with some concurrency and fails the test if any is not a 200.
func (s *kafkaStack) send(n int) {
	s.t.Helper()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	errs := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			resp, err := http.Post(s.gw+"/v1/chat/completions", "application/json", strings.NewReader(chat(false, "hello")))
			if err != nil {
				errs <- err.Error()
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 {
				errs <- fmt.Sprint(resp.StatusCode)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		s.t.Fatalf("request failed: %s", e)
	}
}

// waitPublished waits until the broker holds n events (3 per non-streaming request).
func (s *kafkaStack) waitPublished(n int64) {
	s.t.Helper()
	eventually(s.t, 30*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, err := s.admin.Records(ctx, s.topic)
		return err == nil && got >= n
	}, fmt.Sprintf("%d events on the topic", n))
}

type runningConsumer struct {
	c      *usage.Consumer
	src    *kafka.Consumer
	cancel context.CancelFunc
	done   chan error
}

func (s *kafkaStack) startConsumer(group, startOffset string, mut func(*usage.Config)) *runningConsumer {
	s.t.Helper()
	kcfg := s.cfg
	kcfg.GroupID, kcfg.StartOffset = group, startOffset
	src, err := kafka.NewConsumer(kcfg)
	if err != nil {
		s.t.Fatal(err)
	}
	ucfg := usage.Config{BatchSize: 100, BatchTimeout: 50 * time.Millisecond, GroupID: group, Topic: s.topic, BackoffMin: 10 * time.Millisecond}
	if mut != nil {
		mut(&ucfg)
	}
	c := usage.New(src, s.store, ucfg)
	ctx, cancel := context.WithCancel(context.Background())
	rc := &runningConsumer{c: c, src: src, cancel: cancel, done: make(chan error, 1)}
	go func() { rc.done <- c.Run(ctx) }()
	s.t.Cleanup(func() { rc.stop() })
	return rc
}

func (r *runningConsumer) stop() error {
	r.cancel()
	var err error
	select {
	case err = <-r.done:
	case <-time.After(20 * time.Second):
		err = fmt.Errorf("consumer did not stop")
	}
	r.src.Close()
	return err
}

type totals struct{ requests, failures, in, out, est int64 }

func (s *kafkaStack) totals() totals {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.store.UsageSummaries(ctx, time.Unix(0, 0), "")
	if err != nil {
		s.t.Fatal(err)
	}
	var t totals
	for _, r := range rows {
		t.requests += r.Requests
		t.failures += r.Failures
		t.in += r.InputTokens
		t.out += r.OutputTokens
		t.est += r.EstimatedCostTokens
	}
	return t
}

func (s *kafkaStack) waitRows(n int64) {
	s.t.Helper()
	eventually(s.t, 60*time.Second, func() bool { return s.totals().requests == n }, fmt.Sprintf("%d usage rows (have %d)", n, s.totals().requests))
}

func (s *kafkaStack) lag(group string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	l, err := s.admin.GroupLag(ctx, group, s.topic)
	if err != nil {
		s.t.Fatal(err)
	}
	return l
}

func TestGatewayEventsEndToEnd(t *testing.T) {
	s := newKafkaStack(t)
	group := kafkatest.Unique("sf-e2e")
	rc := s.startConsumer(group, "earliest", nil)
	s.send(30)
	s.waitRows(30)
	tt := s.totals()
	if tt.failures != 0 || tt.out <= 0 || tt.in <= 0 {
		t.Fatalf("totals: %+v", tt)
	}
	s.waitPublished(90)
	msgs := kafkatest.ReadN(t, s.cfg, s.topic, 90, 30*time.Second)
	types := map[string]int{}
	byReq := map[string][]string{}
	for _, m := range msgs {
		e, err := protocol.Decode(m.Value)
		if err != nil {
			t.Fatal(err)
		}
		types[e.EventType]++
		byReq[e.RequestID] = append(byReq[e.RequestID], strings.TrimPrefix(e.EventType, "inference.request."))
	}
	if types[protocol.EventReceived] != 30 || types[protocol.EventRouted] != 30 || types[protocol.EventCompleted] != 30 || types[protocol.EventFailed] != 0 {
		t.Fatalf("event types: %v", types)
	}
	for id, seq := range byReq {
		if strings.Join(seq, ",") != "received,routed,completed" {
			t.Fatalf("request %s arrived as %v", id, seq)
		}
	}
	if rc.c.Count(usage.ResultInserted) != 30 || rc.c.Count(usage.ResultSkipped) != 60 {
		t.Fatalf("consumer: inserted %d skipped %d", rc.c.Count(usage.ResultInserted), rc.c.Count(usage.ResultSkipped))
	}
	eventually(t, 20*time.Second, func() bool { return s.lag(group) == 0 }, "the group has committed everything")
}

// The Phase 12 acceptance demonstration (D15): stop the consumer, send requests, start it, totals equal the number
// sent; replay the whole topic under a fresh group and totals do not move.
func TestUsageStopAndReplay(t *testing.T) {
	began := time.Now()
	stage := func(what string) { t.Logf("%6.1fs %s", time.Since(began).Seconds(), what) }
	s := newKafkaStack(t)
	stage("stack up")
	group := kafkatest.Unique("sf-replay")
	c1 := s.startConsumer(group, "earliest", nil)
	s.send(50)
	s.waitRows(50)
	stage("50 rows")
	eventually(t, 20*time.Second, func() bool { return s.lag(group) == 0 }, "first 50 committed")
	stage("first 50 committed")

	if err := c1.stop(); err != nil {
		t.Fatalf("stopping the consumer: %v", err)
	}
	stage("consumer stopped")
	s.send(200)
	s.waitPublished(750)
	stage("200 sent and published")
	if got := s.totals().requests; got != 50 {
		t.Fatalf("rows grew to %d while the consumer was stopped", got)
	}
	if lag := s.lag(group); lag < 200 {
		t.Fatalf("lag %d while stopped, want at least the 200 requests sent (600 events)", lag)
	} else {
		t.Logf("lag while the consumer was stopped: %d events", lag)
	}

	c2 := s.startConsumer(group, "earliest", nil)
	s.waitRows(250)
	stage("250 rows")
	eventually(t, 30*time.Second, func() bool { return s.lag(group) == 0 }, "lag drains to 0")
	stage("lag 0")
	if c2.c.Count(usage.ResultInserted) != 200 {
		t.Fatalf("the restarted consumer inserted %d, want exactly the 200 missed", c2.c.Count(usage.ResultInserted))
	}
	before := s.totals()
	if before.requests != 250 {
		t.Fatalf("totals: %+v", before)
	}
	if err := c2.stop(); err != nil {
		t.Fatal(err)
	}

	// Replay: a fresh group re-reads every event from the start.
	replay := kafkatest.Unique("sf-replay-fresh")
	stage("c2 stopped")
	c3 := s.startConsumer(replay, "earliest", nil)
	eventually(t, 60*time.Second, func() bool { return s.lag(replay) == 0 && c3.c.Count(usage.ResultDuplicate) == 250 }, "the replay reads all 250 terminal events as duplicates")
	stage("replay done")
	if c3.c.Count(usage.ResultInserted) != 0 {
		t.Fatalf("the replay inserted %d rows", c3.c.Count(usage.ResultInserted))
	}
	if after := s.totals(); after != before {
		t.Fatalf("replay changed the totals: before %+v after %+v", before, after)
	}
	if err := c3.stop(); err != nil {
		t.Fatal(err)
	}
}

// A crash between the database commit and the offset commit loses nothing and counts nothing twice.
func TestUsageCrashBetweenDatabaseAndOffsetCommit(t *testing.T) {
	s := newKafkaStack(t)
	group := kafkatest.Unique("sf-crash")
	s.send(40)
	s.waitPublished(120)
	crashed := false
	c1 := s.startConsumer(group, "earliest", func(c *usage.Config) {
		c.BeforeCommit = func() error {
			if !crashed {
				crashed = true
				return usage.ErrStopped
			}
			return nil
		}
	})
	select {
	case err := <-c1.done:
		if err != usage.ErrStopped {
			t.Fatalf("Run = %v", err)
		}
		c1.done <- nil // so the cleanup's stop() does not wait for it again
	case <-time.After(30 * time.Second):
		t.Fatal("the consumer did not reach the crash point")
	}
	c1.src.Close()
	inDB := s.totals().requests
	if inDB == 0 {
		t.Fatal("the batch must be durable before the crash point")
	}
	if lag := s.lag(group); lag != 120 {
		t.Fatalf("nothing may be committed at the crash: lag %d", lag)
	}
	c2 := s.startConsumer(group, "earliest", nil)
	s.waitRows(40)
	eventually(t, 30*time.Second, func() bool { return s.lag(group) == 0 }, "recovered")
	if c2.c.Count(usage.ResultDuplicate) != uint64(inDB) {
		t.Fatalf("rows redelivered after the crash = %d, want %d absorbed as duplicates", c2.c.Count(usage.ResultDuplicate), inDB)
	}
	if s.totals().requests != 40 {
		t.Fatalf("rows = %d", s.totals().requests)
	}
}
