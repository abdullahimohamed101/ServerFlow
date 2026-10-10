package tracing

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Stats counts spans through the pipeline. Ended counts sampled spans that finished; each is then exported,
// failed (its batch could not be exported), dropped (the queue was full or the process was shutting down),
// or still waiting.
type Stats struct {
	Ended    int64
	Exported int64
	Failed   int64 // spans in batches the exporter rejected
	Dropped  int64
	Failures int64 // export attempts that failed (batches)
	Queued   int64
}

// batcher is a bounded, non-blocking span processor. The SDK's BatchSpanProcessor is not used because it
// does not report how many spans it dropped, and the drop count is the operator's only signal that a
// collector outage is losing data (ADR-018). OnEnd never blocks: a full queue drops the span.
type batcher struct {
	exp      sdktrace.SpanExporter
	res      *resource.Resource // what every exported span carries; see fixedResource
	queue    chan sdktrace.ReadOnlySpan
	maxBatch int
	delay    time.Duration
	timeout  time.Duration
	errlog   *failureLog

	flushReq chan chan struct{}
	stop     chan struct{} // closed by Shutdown
	done     chan struct{} // closed when the worker has exited
	closed   atomic.Bool
	once     sync.Once
	// shutdownDeadline is read by the worker when it drains; set before stop is closed.
	deadlineMu sync.Mutex
	deadline   time.Time

	ended, exported, failed, dropped, failures atomic.Int64
}

func newBatcher(exp sdktrace.SpanExporter, res *resource.Resource, queueSize, maxBatch int, delay, timeout time.Duration, log *slog.Logger) *batcher {
	b := &batcher{
		exp: exp, res: res, queue: make(chan sdktrace.ReadOnlySpan, queueSize), maxBatch: maxBatch, delay: delay, timeout: timeout,
		errlog: newFailureLog(log), flushReq: make(chan chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go b.run()
	return b
}

func (b *batcher) stats() Stats {
	return Stats{Ended: b.ended.Load(), Exported: b.exported.Load(), Failed: b.failed.Load(), Dropped: b.dropped.Load(),
		Failures: b.failures.Load(), Queued: int64(len(b.queue))}
}

// OnStart implements sdktrace.SpanProcessor.
func (b *batcher) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

// OnEnd implements sdktrace.SpanProcessor. It never blocks.
func (b *batcher) OnEnd(s sdktrace.ReadOnlySpan) {
	if !s.SpanContext().IsSampled() {
		return
	}
	b.ended.Add(1)
	if b.closed.Load() {
		b.dropped.Add(1)
		return
	}
	select {
	case b.queue <- s:
	default:
		b.dropped.Add(1)
	}
}

func (b *batcher) run() {
	defer close(b.done)
	batch := make([]sdktrace.ReadOnlySpan, 0, b.maxBatch)
	timer := time.NewTimer(b.delay)
	defer timer.Stop()
	flush := func(ctx context.Context) {
		if len(batch) > 0 {
			b.export(ctx, batch)
			batch = batch[:0]
		}
	}
	for {
		select {
		case s := <-b.queue:
			batch = append(batch, s)
			if len(batch) >= b.maxBatch {
				flush(context.Background())
				resetTimer(timer, b.delay)
			}
		case <-timer.C:
			flush(context.Background())
			timer.Reset(b.delay)
		case ack := <-b.flushReq:
			b.drain(&batch, context.Background())
			close(ack)
			resetTimer(timer, b.delay)
		case <-b.stop:
			b.deadlineMu.Lock()
			dl := b.deadline
			b.deadlineMu.Unlock()
			ctx, cancel := context.WithDeadline(context.Background(), dl)
			b.drain(&batch, ctx)
			cancel()
			// Whatever is still queued (the deadline passed first, or spans raced in) is lost.
			for {
				select {
				case <-b.queue:
					b.dropped.Add(1)
				default:
					return
				}
			}
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// drain exports everything queued so far, in batches, until ctx ends.
func (b *batcher) drain(batch *[]sdktrace.ReadOnlySpan, ctx context.Context) {
	for {
		for len(*batch) < b.maxBatch {
			select {
			case s := <-b.queue:
				*batch = append(*batch, s)
				continue
			default:
			}
			break
		}
		if len(*batch) == 0 {
			return
		}
		if ctx.Err() != nil {
			b.dropped.Add(int64(len(*batch)))
			*batch = (*batch)[:0]
			continue // count the rest of the queue as dropped via the loop above
		}
		b.export(ctx, *batch)
		*batch = (*batch)[:0]
	}
}

func (b *batcher) export(parent context.Context, batch []sdktrace.ReadOnlySpan) {
	ctx, cancel := context.WithTimeout(parent, b.timeout)
	defer cancel()
	n := int64(len(batch))
	err := b.exp.ExportSpans(ctx, b.withFixedResource(batch))
	if err != nil {
		b.failed.Add(n)
		b.failures.Add(1)
		b.errlog.failure(err)
		return
	}
	b.exported.Add(n)
	b.errlog.success()
}

// fixedResource replaces a span's resource. sdktrace.WithResource merges in OTEL_RESOURCE_ATTRIBUTES and
// OTEL_SERVICE_NAME from the environment; exporting the resource we built instead means the environment
// cannot add attributes to what leaves the process.
type fixedResource struct {
	sdktrace.ReadOnlySpan
	res *resource.Resource
}

func (f fixedResource) Resource() *resource.Resource { return f.res }

func (b *batcher) withFixedResource(batch []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	if b.res == nil {
		return batch
	}
	out := make([]sdktrace.ReadOnlySpan, len(batch))
	for i, s := range batch {
		out[i] = fixedResource{ReadOnlySpan: s, res: b.res}
	}
	return out
}

// ForceFlush implements sdktrace.SpanProcessor: it exports what is queued now.
func (b *batcher) ForceFlush(ctx context.Context) error {
	if b.closed.Load() {
		return nil
	}
	ack := make(chan struct{})
	select {
	case b.flushReq <- ack:
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown implements sdktrace.SpanProcessor: it stops accepting spans, exports what is queued until ctx
// ends (spans not exported by then count as dropped), and shuts the exporter down.
func (b *batcher) Shutdown(ctx context.Context) error {
	b.once.Do(func() {
		b.closed.Store(true)
		dl, ok := ctx.Deadline()
		if !ok {
			dl = time.Now().Add(b.timeout)
		}
		b.deadlineMu.Lock()
		b.deadline = dl
		b.deadlineMu.Unlock()
		close(b.stop)
	})
	select {
	case <-b.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.exp.Shutdown(ctx)
}

// failureLog logs exporter failures without flooding: the first failure, then at most one line a minute
// while failures continue, and one line when exporting works again. It logs a short class, never the
// error text, which can carry the collector's address.
type failureLog struct {
	log      *slog.Logger
	mu       sync.Mutex
	failing  bool
	lastLog  time.Time
	since    time.Time
	suppress int
	now      func() time.Time
}

const failureLogInterval = time.Minute

func newFailureLog(log *slog.Logger) *failureLog {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &failureLog{log: log, now: time.Now}
}

func (f *failureLog) failure(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	if !f.failing {
		f.failing, f.since, f.lastLog = true, now, now
		f.log.Warn("trace export failing; spans are being dropped and requests are unaffected", "component", "tracing", "reason", classify(err))
		return
	}
	if now.Sub(f.lastLog) < failureLogInterval {
		f.suppress++
		return
	}
	f.lastLog = now
	f.log.Warn("trace export still failing", "component", "tracing", "reason", classify(err), "failing_for", now.Sub(f.since).Round(time.Second).String(), "suppressed_lines", f.suppress)
	f.suppress = 0
}

func (f *failureLog) success() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failing {
		return
	}
	f.failing = false
	f.log.Info("trace export recovered", "component", "tracing", "failed_for", f.now().Sub(f.since).Round(time.Second).String())
}

// classify reduces an export error to a short fixed token.
func classify(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return "export_error"
}
