package usage

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/pkg/protocol"
)

// Results of handling one record (the label of usage_consumer_records_total). The set is closed.
const (
	ResultInserted  = "inserted"
	ResultDuplicate = "duplicate"
	ResultSkipped   = "skipped"
	ResultRejected  = "rejected"
)

var results = []string{ResultInserted, ResultDuplicate, ResultSkipped, ResultRejected}

// Config configures the consumer loop.
type Config struct {
	// BatchSize and BatchTimeout bound one batch: it is written when it holds BatchSize records or BatchTimeout
	// has passed since its first record, whichever is first.
	BatchSize    int
	BatchTimeout time.Duration
	// GroupID and Topic label the lag metric.
	GroupID string
	Topic   string
	// BackoffMin and BackoffMax bound the wait between retries of a failing database (default 100ms and 10s).
	BackoffMin, BackoffMax time.Duration
	// CommitTimeout bounds the offset commit, and ShutdownGrace the time allowed to finish a batch after the
	// context ends (defaults 10s and 5s).
	CommitTimeout, ShutdownGrace time.Duration
	Logger                       *slog.Logger
	// BeforeCommit, if set, runs after a batch is durable in the database and before its offsets are
	// committed. A non-nil error stops Run at that point without committing, which is how tests crash the
	// consumer between the two. Production leaves it nil.
	BeforeCommit func() error
}

// Consumer reads terminal lifecycle events from a Source and records them idempotently in a Store.
//
// The order that matters (D13): rows are written and committed in the database FIRST, and offsets are committed
// only afterwards. A crash between the two replays the batch, and the Store's conflict handling absorbs it. A
// record that cannot be used is recorded as a reject (coordinates only) and skipped, so one poison message never
// blocks a partition.
type Consumer struct {
	src   Source
	store Store
	cfg   Config
	log   *slog.Logger

	counts     [4]atomic.Uint64
	dbErrors   atomic.Uint64
	dbDown     atomic.Bool
	lastWarn   atomic.Int64
	lastReject atomic.Int64
	collect    []prometheus.Collector
}

// New builds a Consumer.
func New(src Source, store Store, cfg Config) *Consumer {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 500
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = time.Second
	}
	if cfg.BackoffMin <= 0 {
		cfg.BackoffMin = 100 * time.Millisecond
	}
	if cfg.BackoffMax < cfg.BackoffMin {
		cfg.BackoffMax = 10 * time.Second
	}
	if cfg.CommitTimeout <= 0 {
		cfg.CommitTimeout = 10 * time.Second
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 5 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c := &Consumer{src: src, store: store, cfg: cfg, log: log.With("component", "usage-consumer")}
	for i, r := range results {
		c.collect = append(c.collect, prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "usage_consumer_records_total", Help: "Records handled by the usage consumer, by result.",
			ConstLabels: prometheus.Labels{"result": r},
		}, func() float64 { return float64(c.counts[i].Load()) }))
	}
	c.collect = append(c.collect,
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "usage_consumer_db_errors_total", Help: "Failed attempts to write to the database.",
		}, func() float64 { return float64(c.dbErrors.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "consumer_lag", Help: "Records on the topic that the consumer group has not processed, summed over partitions.",
			ConstLabels: prometheus.Labels{"group": cfg.GroupID, "topic": cfg.Topic},
		}, func() float64 { return float64(src.Lag()) }),
	)
	return c
}

// Collectors returns the Prometheus collectors. Labels are bounded (result; group and topic are fixed).
func (c *Consumer) Collectors() []prometheus.Collector { return c.collect }

// Count returns how many records ended with a result.
func (c *Consumer) Count(result string) uint64 {
	for i, r := range results {
		if r == result {
			return c.counts[i].Load()
		}
	}
	return 0
}

func (c *Consumer) add(result string, n int) {
	for i, r := range results {
		if r == result {
			c.counts[i].Add(uint64(n))
		}
	}
}

// ErrStopped is returned by Run when BeforeCommit asked it to stop.
var ErrStopped = errors.New("usage: stopped by BeforeCommit")

// Run consumes until ctx ends (returning nil) or BeforeCommit stops it (returning its error).
func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		batch := c.collectBatch(ctx)
		if len(batch) == 0 {
			continue
		}
		pctx := ctx
		if ctx.Err() != nil {
			// Shutting down with records in hand: finish them if the database allows, within a bound.
			var cancel context.CancelFunc
			pctx, cancel = context.WithTimeout(context.Background(), c.cfg.ShutdownGrace)
			defer cancel()
		}
		if err := c.process(pctx, batch); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil // abandoned: nothing was committed, so it will be read again
			}
			return err
		}
	}
	return nil
}

// collectBatch polls until the batch is full, its timer ends, or ctx ends. It returns what it has.
func (c *Consumer) collectBatch(ctx context.Context) []Message {
	var batch []Message
	// pollCtx ends when the batch timer fires (started by the first record) or ctx ends.
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for len(batch) < c.cfg.BatchSize && pollCtx.Err() == nil {
		msgs, err := c.src.Poll(pollCtx, c.cfg.BatchSize-len(batch))
		if err != nil {
			if errors.Is(err, ErrClosed) {
				return batch
			}
			c.warnThrottled("polling failed; waiting before trying again", err)
			if len(msgs) == 0 {
				select {
				case <-pollCtx.Done():
				case <-time.After(c.cfg.BackoffMin):
				}
			}
		}
		batch = append(batch, msgs...)
		if len(batch) > 0 && timer == nil {
			timer = time.AfterFunc(c.cfg.BatchTimeout, cancel)
		}
	}
	return batch
}

// logRejects says that records were skipped, at most once every ten seconds, with coordinates and reasons only: the
// payload of a record that could not be used is never logged (it may be garbage, or someone else's data).
func (c *Consumer) logRejects(rs []Reject) {
	now := time.Now().UnixNano()
	last := c.lastReject.Load()
	if (last == 0 || now-last > int64(10*time.Second)) && c.lastReject.CompareAndSwap(last, now) {
		first := rs[0]
		c.log.Warn("records could not be used and were skipped", "count", len(rs), "first_topic", first.Topic,
			"first_partition", first.Partition, "first_offset", first.Offset, "first_reason", first.Reason)
	}
}

func (c *Consumer) warnThrottled(msg string, err error) {
	now := time.Now().UnixNano()
	last := c.lastWarn.Load()
	if (last == 0 || now-last > int64(10*time.Second)) && c.lastWarn.CompareAndSwap(last, now) {
		c.log.Warn(msg, "error", err.Error())
	}
}

// process makes one batch durable, then commits its offsets.
func (c *Consumer) process(ctx context.Context, batch []Message) error {
	var rows []Row
	var rejects []Reject
	skipped := 0
	next := map[[2]int64]Offset{} // keyed by partition; topics are one here but kept in the offset
	for _, m := range batch {
		k := [2]int64{int64(m.Partition), 0}
		if o, ok := next[k]; !ok || m.Offset+1 > o.Offset {
			next[k] = Offset{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset + 1}
		}
		row, reject, skip := c.classify(m)
		switch {
		case reject != nil:
			rejects = append(rejects, *reject)
		case skip:
			skipped++
		default:
			rows = append(rows, row)
		}
	}

	if len(rejects) > 0 {
		c.logRejects(rejects)
		if err := c.retry(ctx, func() error { return c.store.RecordRejects(ctx, rejects) }); err != nil {
			return err
		}
	}
	inserted, bad, err := c.insert(ctx, rows)
	if err != nil {
		return err
	}
	if len(bad) > 0 {
		rejects = append(rejects, bad...)
	}
	c.add(ResultInserted, inserted)
	c.add(ResultDuplicate, len(rows)-len(bad)-inserted)
	c.add(ResultSkipped, skipped)
	c.add(ResultRejected, len(rejects))

	if c.cfg.BeforeCommit != nil {
		if err := c.cfg.BeforeCommit(); err != nil {
			return err
		}
	}
	offs := make([]Offset, 0, len(next))
	for _, o := range next {
		offs = append(offs, o)
	}
	c.commit(offs)
	return nil
}

// classify turns a message into a row, a reject, or a skip.
func (c *Consumer) classify(m Message) (row Row, reject *Reject, skip bool) {
	rej := func(reason string) (Row, *Reject, bool) {
		return Row{}, &Reject{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Reason: reason}, false
	}
	if len(m.Value) > protocol.MaxEventBytes {
		return rej(RejectOversize)
	}
	e, err := protocol.Decode(m.Value)
	switch {
	case errors.Is(err, protocol.ErrEventVersionUnsupported):
		return rej(RejectVersion)
	case errors.Is(err, protocol.ErrEventTooLarge):
		return rej(RejectOversize)
	case err != nil && !json.Valid(m.Value):
		return rej(RejectDecode)
	case err != nil:
		return rej(RejectInvalid)
	}
	if !protocol.IsTerminalEventType(e.EventType) {
		return Row{}, nil, true
	}
	t := e.Terminal
	outcome := "completed"
	if e.EventType == protocol.EventFailed {
		outcome = "failed"
	}
	return Row{
		EventID: e.EventID, RequestID: e.RequestID, TenantID: e.TenantID, APIKeyID: e.APIKeyID, Model: e.Model, WorkerID: e.WorkerID,
		Outcome: outcome, FailureClass: t.FailureClass, HTTPStatus: t.HTTPStatus, InputTokens: t.InputTokens, OutputTokens: t.OutputTokens,
		TokensSource: t.TokensSource, EstimatedCostTokens: t.EstimatedCostTokens, Attempts: len(t.Attempts), TTFTMS: t.TTFTMS,
		DurationMS: t.DurationMS, OccurredAt: e.Timestamp, Partition: m.Partition, Offset: m.Offset,
	}, nil, false
}

// insert writes the rows, retrying while the database is unavailable. If the database refuses the content of
// some row, the batch is written row by row so only that row is rejected.
func (c *Consumer) insert(ctx context.Context, rows []Row) (inserted int, bad []Reject, err error) {
	if len(rows) == 0 {
		return 0, nil, nil
	}
	var n int
	err = c.retry(ctx, func() error {
		var e error
		n, e = c.store.InsertUsage(ctx, rows)
		return e
	})
	if err == nil {
		return n, nil, nil
	}
	if !errors.Is(err, ErrBadData) {
		return 0, nil, err
	}
	for _, r := range rows {
		var one int
		e := c.retry(ctx, func() error {
			var ie error
			one, ie = c.store.InsertUsage(ctx, []Row{r})
			return ie
		})
		switch {
		case e == nil:
			inserted += one
		case errors.Is(e, ErrBadData):
			bad = append(bad, Reject{Topic: c.cfg.Topic, Partition: r.Partition, Offset: r.Offset, Reason: RejectInvalid})
		default:
			return 0, nil, e
		}
	}
	if len(bad) > 0 {
		if err := c.retry(ctx, func() error { return c.store.RecordRejects(ctx, bad) }); err != nil {
			return 0, nil, err
		}
	}
	return inserted, bad, nil
}

// retry runs op until it succeeds, fails with bad data, or ctx ends, backing off between database failures. It
// logs when the database goes away and when it comes back, once each.
func (c *Consumer) retry(ctx context.Context, op func() error) error {
	wait := c.cfg.BackoffMin
	for {
		err := op()
		if err == nil || errors.Is(err, ErrBadData) {
			if err == nil && c.dbDown.CompareAndSwap(true, false) {
				c.log.Info("database recovered; consuming again")
			}
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.dbErrors.Add(1)
		if c.dbDown.CompareAndSwap(false, true) {
			c.log.Warn("database write failed; polling is paused and offsets are not committed until it recovers", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, c.cfg.BackoffMax)
	}
}

// commit commits offsets after the database commit. A failure is logged and not fatal: the rows are durable, and
// a batch read again after a failed commit is absorbed by the Store's conflict handling.
func (c *Consumer) commit(offs []Offset) {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		cctx, cancel := context.WithTimeout(context.Background(), c.cfg.CommitTimeout)
		err = c.src.Commit(cctx, offs)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(c.cfg.BackoffMin)
	}
	c.warnThrottled("offset commit failed; the batch may be read again, which is harmless", err)
}
