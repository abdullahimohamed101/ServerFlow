package events

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"serverflow/pkg/protocol"
)

// Reasons an event was not delivered (the label of event_publish_failures_total). The set is closed.
const (
	ReasonBufferFull     = "buffer_full"     // the in-memory buffer between the request path and the drain goroutine was full
	ReasonProducerFull   = "producer_full"   // the sink's own bounded buffer was full
	ReasonDeliveryFailed = "delivery_failed" // the broker did not acknowledge within the delivery timeout
	ReasonEncode         = "encode"          // the event failed validation or exceeded the size cap
	ReasonShutdown       = "shutdown"        // arrived after Close, or was not flushed in time
)

var failureReasons = []string{ReasonBufferFull, ReasonProducerFull, ReasonDeliveryFailed, ReasonEncode, ReasonShutdown}

// dropLogInterval is how often drops of one reason are logged; the metric counts every one.
const dropLogInterval = 10 * time.Second

// Config configures a Publisher (and the Observer built on it).
type Config struct {
	// BufferSize bounds the in-memory queue of events waiting for the drain goroutine. When it is full the
	// newest event is dropped. Must be positive.
	BufferSize int
	// Source identifies this gateway instance in every event.
	Source string
	// Now returns the current time; nil means time.Now. For tests.
	Now func() time.Time
}

type item struct {
	ev protocol.Event
	at time.Time
}

// Publisher is a bounded, non-blocking queue in front of a Sink. Enqueue never blocks and takes no lock.
type Publisher struct {
	sink Sink
	log  *slog.Logger
	now  func() time.Time

	ch      chan item
	stop    chan struct{}
	done    chan struct{}
	closed  atomic.Bool
	closeMu sync.Mutex // serialises Close calls only

	inflight  atomic.Int64 // records handed to the sink and not yet acknowledged
	abandoned atomic.Bool  // Close gave up on the in-flight records; late acknowledgements are not counted again
	failing   atomic.Bool  // the sink is currently failing deliveries (for the once-per-outage log)
	lastDrop  [5]atomic.Int64

	published atomic.Uint64
	failures  [5]atomic.Uint64
	latency   prometheus.Histogram
	collect   []prometheus.Collector
}

// NewPublisher starts a Publisher draining into sink. Call Close to stop it.
func NewPublisher(sink Sink, cfg Config, log *slog.Logger) *Publisher {
	if cfg.BufferSize < 1 {
		cfg.BufferSize = 1
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	p := &Publisher{
		sink: sink, log: log.With("component", "events"), now: cfg.Now,
		ch: make(chan item, cfg.BufferSize), stop: make(chan struct{}), done: make(chan struct{}),
	}
	if p.now == nil {
		p.now = time.Now
	}
	p.latency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "event_publish_latency_seconds",
		Help:    "Time from an event being accepted by the publisher to the broker acknowledging it.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	})
	p.collect = []prometheus.Collector{
		p.latency,
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "events_published_total", Help: "Events acknowledged by the broker.",
		}, func() float64 { return float64(p.published.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "events_buffer_depth", Help: "Events waiting in the publisher's in-memory buffer.",
		}, func() float64 { return float64(len(p.ch)) }),
	}
	for i, r := range failureReasons {
		p.collect = append(p.collect, prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "event_publish_failures_total", Help: "Events that were not delivered, by reason.",
			ConstLabels: prometheus.Labels{"reason": r},
		}, func() float64 { return float64(p.failures[i].Load()) }))
	}
	go p.run()
	return p
}

// Collectors returns the publisher's Prometheus collectors. Labels are bounded (reason only).
func (p *Publisher) Collectors() []prometheus.Collector { return p.collect }

// Enqueue offers an event. It never blocks: when the buffer is full, or the publisher is closed, the event is
// dropped and counted. It reports whether the event was accepted.
func (p *Publisher) Enqueue(ev protocol.Event) bool {
	if p.closed.Load() {
		p.drop(ReasonShutdown, 1)
		return false
	}
	select {
	case p.ch <- item{ev: ev, at: p.now()}:
		return true
	default:
		p.drop(ReasonBufferFull, 1)
		return false
	}
}

// Depth is the number of events currently buffered.
func (p *Publisher) Depth() int { return len(p.ch) }

// Dropped returns the number of events dropped for a reason.
func (p *Publisher) Dropped(reason string) uint64 {
	for i, r := range failureReasons {
		if r == reason {
			return p.failures[i].Load()
		}
	}
	return 0
}

// Published returns the number of events the broker acknowledged.
func (p *Publisher) Published() uint64 { return p.published.Load() }

func (p *Publisher) drop(reason string, n uint64) {
	for i, r := range failureReasons {
		if r != reason {
			continue
		}
		total := p.failures[i].Add(n)
		now := p.now().UnixNano()
		last := p.lastDrop[i].Load()
		if (last == 0 || now-last >= int64(dropLogInterval)) && p.lastDrop[i].CompareAndSwap(last, now) {
			p.log.Warn("events dropped; inference is not affected", "reason", reason, "dropped_total", total)
		}
		return
	}
}

func (p *Publisher) run() {
	defer close(p.done)
	for {
		select {
		case it := <-p.ch:
			p.send(it)
		case <-p.stop:
			for {
				select {
				case it := <-p.ch:
					p.send(it)
				default:
					return
				}
			}
		}
	}
}

func (p *Publisher) send(it item) {
	b, err := protocol.Encode(it.ev)
	if err != nil {
		p.drop(ReasonEncode, 1)
		// The error names the field, never its value (protocol.Validate).
		p.log.Debug("event not encoded", "event_type", it.ev.EventType, "request_id", it.ev.RequestID, "error", err)
		return
	}
	rec := Record{Key: []byte(it.ev.RequestID), Value: b, EnqueuedAt: it.at}
	p.inflight.Add(1)
	err = p.sink.Produce(rec, func(derr error) { p.delivered(rec, derr) })
	if err != nil {
		p.inflight.Add(-1)
		if errors.Is(err, ErrFull) {
			p.drop(ReasonProducerFull, 1)
		} else {
			p.drop(ReasonDeliveryFailed, 1)
		}
	}
}

func (p *Publisher) delivered(rec Record, err error) {
	p.inflight.Add(-1)
	if p.abandoned.Load() {
		return // already counted as dropped at shutdown
	}
	if err != nil {
		p.drop(ReasonDeliveryFailed, 1)
		if p.failing.CompareAndSwap(false, true) {
			p.log.Warn("event delivery is failing; inference is not affected", "error", err.Error())
		}
		return
	}
	p.published.Add(1)
	p.latency.Observe(p.now().Sub(rec.EnqueuedAt).Seconds())
	if p.failing.CompareAndSwap(true, false) {
		p.log.Info("event delivery recovered")
	}
}

// Close stops accepting events, drains what is buffered into the sink and flushes the sink, all bounded by ctx.
// Whatever is not delivered in time is counted as dropped (reason shutdown) and logged with its count. It is
// safe to call more than once; the sink is closed by the caller.
func (p *Publisher) Close(ctx context.Context) error {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	if p.closed.Swap(true) {
		return nil
	}
	close(p.stop)
	var err error
	select {
	case <-p.done:
		err = p.sink.Flush(ctx)
	case <-ctx.Done():
		err = ctx.Err()
	}
	p.abandoned.Store(true)
	lost := uint64(len(p.ch)) + uint64(max(p.inflight.Load(), 0))
	if lost > 0 {
		p.drop(ReasonShutdown, lost)
		p.log.Warn("events were not flushed before shutdown", "count", lost)
	}
	return err
}
