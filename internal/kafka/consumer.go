package kafka

import (
	"context"
	"errors"
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
	mu     sync.Mutex
	hw     map[tp]int64 // high watermark seen per partition
	next   map[tp]int64 // next offset to process per partition
	closed atomic.Bool
	polled atomic.Bool // a batch is out: rebalances are blocked until Commit or an empty Poll
}

type tp struct {
	topic string
	part  int32
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
	return &Consumer{cfg: cfg, cl: cl, hw: map[tp]int64{}, next: map[tp]int64{}}, nil
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
	c.mu.Lock()
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		k := tp{p.Topic, p.Partition}
		if _, known := c.next[k]; !known {
			// The first record fetched is where this member's position is; before that the position is not
			// known, and guessing it would invent lag.
			if len(p.Records) == 0 {
				return
			}
			c.next[k] = p.Records[0].Offset
		}
		c.hw[k] = p.HighWatermark
		for _, r := range p.Records {
			out = append(out, usage.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value, Timestamp: r.Timestamp})
		}
	})
	c.mu.Unlock()
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
	c.mu.Lock()
	for _, o := range upTo {
		c.next[tp{o.Topic, o.Partition}] = o.Offset
	}
	c.mu.Unlock()
	return nil
}

// Lag implements usage.Source: the sum over partitions of the high watermark seen minus the next offset to process.
func (c *Consumer) Lag() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var lag int64
	for k, hw := range c.hw {
		if d := hw - c.next[k]; d > 0 {
			lag += d
		}
	}
	return lag
}

// Close implements usage.Source.
func (c *Consumer) Close() {
	if c.closed.Swap(true) {
		return
	}
	c.cl.CloseAllowingRebalance()
}

type (
	kmsgOffsetCommitRequest  = kmsg.OffsetCommitRequest
	kmsgOffsetCommitResponse = kmsg.OffsetCommitResponse
)

func kerrFor(code int16) error { return kerr.ErrorForCode(code) }
