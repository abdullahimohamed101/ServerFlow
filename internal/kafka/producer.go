package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"serverflow/internal/events"
)

// Producer is the events.Sink for Kafka. It writes with acks=all and the idempotent producer, never blocks in
// Produce, and bounds its memory: at most MaxBufferedRecords unacknowledged records, and a record that is not
// acknowledged within DeliveryTimeout is given up on and reported as an error.
type Producer struct {
	cfg   Config
	cl    *kgo.Client
	max   int64
	count atomic.Int64 // records handed to the client and not yet acknowledged or failed
}

var _ events.Sink = (*Producer)(nil)

// NewProducer builds the producer. It does not connect: a broker that is down at start is not an error, and the
// client keeps retrying with backoff in the background.
func NewProducer(cfg Config) (*Producer, error) {
	if cfg.Topic == "" {
		return nil, errors.New("kafka: no topic")
	}
	opts, err := cfg.baseOpts(newHealth(cfg))
	if err != nil {
		return nil, cfg.safe(err)
	}
	codec, err := compression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	maxRecs := cfg.MaxBufferedRecords
	if maxRecs < 1 {
		maxRecs = 10_000
	}
	delivery := cfg.DeliveryTimeout
	if delivery <= 0 {
		delivery = 30 * time.Second
	}
	opts = append(opts,
		kgo.RequiredAcks(kgo.AllISRAcks()), // with idempotence on (the default), this is required and kept explicit
		kgo.ProducerBatchCompression(codec),
		// The client's own limit sits above ours so that Produce, not the client, decides when to refuse.
		kgo.MaxBufferedRecords(maxRecs+1024),
		kgo.RecordDeliveryTimeout(delivery),
		// Without this, a record whose produce request was in flight when the broker died is never failed, so a dead
		// broker would pin memory for ever. Bounded delivery matters more here than duplicate avoidance: a failed
		// record is counted and not re-sent by us, and the usage consumer absorbs duplicates (D9).
		kgo.AllowIdempotentProduceCancellation(),
		kgo.UnknownTopicRetries(1),
	)
	if cfg.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(cfg.Linger))
	}
	if cfg.BatchMaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(cfg.BatchMaxBytes)))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, cfg.safe(err)
	}
	return &Producer{cfg: cfg, cl: cl, max: int64(maxRecs)}, nil
}

// Produce queues the record and returns at once. It returns events.ErrFull when MaxBufferedRecords records are
// already waiting for the broker.
func (p *Producer) Produce(r events.Record, done func(error)) error {
	if p.count.Add(1) > p.max {
		p.count.Add(-1)
		return events.ErrFull
	}
	rec := &kgo.Record{Topic: p.cfg.Topic, Key: r.Key, Value: r.Value}
	p.cl.TryProduce(context.Background(), rec, func(_ *kgo.Record, err error) {
		p.count.Add(-1)
		done(p.cfg.safe(err))
	})
	return nil
}

// Flush waits for every produced record to be acknowledged or failed, or for ctx to end.
func (p *Producer) Flush(ctx context.Context) error { return p.cfg.safe(p.cl.Flush(ctx)) }

// Close shuts the client down without flushing; records still buffered are failed.
func (p *Producer) Close() error {
	p.cl.Close()
	return nil
}
