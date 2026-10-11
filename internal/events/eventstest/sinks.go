// Package eventstest provides fakes for testing event publishing and consuming without a broker.
package eventstest

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"serverflow/internal/events"
	"serverflow/pkg/protocol"
)

// RecordingSink acknowledges every record at once and keeps them.
type RecordingSink struct {
	mu   sync.Mutex
	recs []events.Record
}

func (s *RecordingSink) Produce(r events.Record, done func(error)) error {
	s.mu.Lock()
	s.recs = append(s.recs, r)
	s.mu.Unlock()
	done(nil)
	return nil
}
func (s *RecordingSink) Flush(context.Context) error { return nil }
func (s *RecordingSink) Close() error                { return nil }

// Records returns a copy of what was produced.
func (s *RecordingSink) Records() []events.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Record(nil), s.recs...)
}

// Events decodes everything produced, in order. Undecodable records fail the caller through ok=false.
func (s *RecordingSink) Events() (out []protocol.Event, ok bool) {
	ok = true
	for _, r := range s.Records() {
		e, err := protocol.Decode(r.Value)
		if err != nil {
			ok = false
			continue
		}
		out = append(out, e)
	}
	return out, ok
}

// Types lists the event types produced for one request, in order ("" matches every request).
func (s *RecordingSink) Types(requestID string) []string {
	evs, _ := s.Events()
	var out []string
	for _, e := range evs {
		if requestID == "" || e.RequestID == requestID {
			out = append(out, e.EventType)
		}
	}
	return out
}

// BlockingSink never returns from Produce until Release is called: a stuck sink.
type BlockingSink struct {
	once    sync.Once
	release chan struct{}
	Calls   atomic.Int64
}

func NewBlockingSink() *BlockingSink { return &BlockingSink{release: make(chan struct{})} }

func (s *BlockingSink) Produce(_ events.Record, done func(error)) error {
	s.Calls.Add(1)
	<-s.release
	done(nil)
	return nil
}
func (s *BlockingSink) Flush(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (s *BlockingSink) Close() error { return nil }

// Release unblocks every Produce, now and later.
func (s *BlockingSink) Release() { s.once.Do(func() { close(s.release) }) }

// FullSink refuses every record with events.ErrFull.
type FullSink struct{ RecordingSink }

func (s *FullSink) Produce(events.Record, func(error)) error { return events.ErrFull }

// FailingSink accepts records and reports each as failed to deliver.
type FailingSink struct {
	Err   error
	Calls atomic.Int64
}

func (s *FailingSink) Produce(_ events.Record, done func(error)) error {
	s.Calls.Add(1)
	done(s.Err)
	return nil
}
func (s *FailingSink) Flush(context.Context) error { return nil }
func (s *FailingSink) Close() error                { return nil }

// SlowSink acknowledges each record after Delay, without blocking Produce.
type SlowSink struct {
	Delay time.Duration
	wg    sync.WaitGroup
	RecordingSink
}

func (s *SlowSink) Produce(r events.Record, done func(error)) error {
	s.mu.Lock()
	s.recs = append(s.recs, r)
	s.mu.Unlock()
	s.wg.Add(1)
	time.AfterFunc(s.Delay, func() { defer s.wg.Done(); done(nil) })
	return nil
}
func (s *SlowSink) Flush(ctx context.Context) error {
	c := make(chan struct{})
	go func() { s.wg.Wait(); close(c) }()
	select {
	case <-c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
