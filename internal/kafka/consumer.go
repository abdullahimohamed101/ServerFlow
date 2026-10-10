package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"serverflow/internal/usage"
)

// Consumer is the usage.Source for Kafka: a member of a consumer group with automatic commits off. Offsets
// move only when Commit is called, after the caller's database commit. Rebalances are held back while a batch
// is being processed (between Poll and Commit), so one member never works on partitions another has been given.
type Consumer struct {
	cfg    Config
	cl     *kgo.Client
	closed atomic.Bool
	polled atomic.Bool // a batch is out: rebalances are blocked until Commit or an empty Poll

	// lag is the group's backlog as the broker last reported it (committed offsets against partition end
	// offsets), refreshed by lagLoop. It is not derived from what this process has polled.
	lag     atomic.Int64
	lagStop chan struct{}
	lagDone chan struct{}
}

var _ usage.Source = (*Consumer)(nil)

// NewConsumer joins cfg.GroupID on cfg.Topic. A group with no committed offsets starts at cfg.StartOffset.
func NewConsumer(cfg Config) (*Consumer, error) {
	if cfg.Topic == "" || cfg.GroupID == "" {
		return nil, errors.New("kafka: consumer needs a topic and a group")
	}
	opts, err := cfg.baseOpts(newHealth(cfg))
	if err != nil {
		return nil, cfg.safe(err)
	}
	reset := kgo.NewOffset().AtStart()
	if cfg.StartOffset == "latest" {
		reset = kgo.NewOffset().AtEnd()
	}
	opts = append(opts,
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(reset),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.SessionTimeout(20*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, cfg.safe(err)
	}
	c := &Consumer{cfg: cfg, cl: cl, lagStop: make(chan struct{}), lagDone: make(chan struct{})}
	go c.lagLoop()
	return c, nil
}

// lagLoop recomputes the group's lag on a fixed cadence (Config.LagInterval, default 5s), each time bounded by a
// timeout. A failed refresh (broker unreachable) keeps the last value: the gauge is stale, not reset to zero, and
// goes on being stale for as long as the broker cannot be asked.
func (c *Consumer) lagLoop() {
	defer close(c.lagDone)
	every := c.cfg.LagInterval
	if every <= 0 {
		every = 5 * time.Second
	}
	adm := &Admin{cfg: c.cfg, cl: c.cl}
	refresh := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if n, err := adm.GroupLag(ctx, c.cfg.GroupID, c.cfg.Topic); err == nil {
			c.lag.Store(n)
		}
	}
	refresh()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-c.lagStop:
			return
		case <-t.C:
			refresh()
		}
	}
}

// Poll implements usage.Source.
func (c *Consumer) Poll(ctx context.Context, max int) ([]usage.Message, error) {
	if c.closed.Load() {
		return nil, usage.ErrClosed
	}
	if c.polled.Swap(false) {
		c.cl.AllowRebalance() // the previous batch was never committed; do not hold the group hostage
	}
	fetches := c.cl.PollRecords(ctx, max)
	if fetches.IsClientClosed() {
		return nil, usage.ErrClosed
	}
	if err := ctx.Err(); err != nil && fetches.NumRecords() == 0 {
		c.cl.AllowRebalance()
		return nil, nil
	}
	var firstErr error
	for _, fe := range fetches.Errors() {
		if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
			continue
		}
		if firstErr == nil {
			firstErr = c.cfg.safe(fe.Err)
		}
	}
	var out []usage.Message
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, r := range p.Records {
			out = append(out, usage.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value, Timestamp: r.Timestamp})
		}
	})
	if len(out) == 0 {
		c.cl.AllowRebalance()
		return nil, firstErr
	}
	c.polled.Store(true)
	return out, nil
}

// Commit implements usage.Source.
func (c *Consumer) Commit(ctx context.Context, upTo []usage.Offset) error {
	if c.closed.Load() {
		return usage.ErrClosed
	}
	defer func() {
		if c.polled.Swap(false) {
			c.cl.AllowRebalance()
		}
	}()
	offs := map[string]map[int32]kgo.EpochOffset{}
	for _, o := range upTo {
		if offs[o.Topic] == nil {
			offs[o.Topic] = map[int32]kgo.EpochOffset{}
		}
		offs[o.Topic][o.Partition] = kgo.EpochOffset{Epoch: -1, Offset: o.Offset}
	}
	var commitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	c.cl.CommitOffsets(ctx, offs, func(_ *kgo.Client, _ *kmsgOffsetCommitRequest, resp *kmsgOffsetCommitResponse, err error) {
		defer wg.Done()
		if err != nil {
			commitErr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if e := kerrFor(p.ErrorCode); e != nil && commitErr == nil {
					commitErr = e
				}
			}
		}
	})
	wg.Wait()
	if commitErr != nil {
		return c.cfg.safe(commitErr)
	}
	return nil
}

// Lag implements usage.Source: the group's backlog on the topic, from the broker's committed offsets and partition
// end offsets, refreshed every Config.LagInterval (default 5 seconds). A partition with no commit counts from its log
// start, so a brand-new group shows the whole retained log until its first commit.
func (c *Consumer) Lag() int64 { return c.lag.Load() }

// Close implements usage.Source.
func (c *Consumer) Close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.lagStop)
	<-c.lagDone
	c.cl.CloseAllowingRebalance()
}

type (
	kmsgOffsetCommitRequest  = kmsg.OffsetCommitRequest
	kmsgOffsetCommitResponse = kmsg.OffsetCommitResponse
)

func kerrFor(code int16) error { return kerr.ErrorForCode(code) }

// Format implements fmt.Formatter: every verb prints the redacted configuration, never the password.
func (x *Consumer) Format(f fmt.State, _ rune) { redactedFormat(f, "Consumer", x.cfg) }

// LogValue implements slog.LogValuer.
func (x *Consumer) LogValue() slog.Value { return slog.StringValue("kafka.Consumer" + x.cfg.String()) }
