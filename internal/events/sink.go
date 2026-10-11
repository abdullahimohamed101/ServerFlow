// Package events publishes the inference lifecycle (Phase 12, ADR-019): a gateway.Observer maps what happens
// to a request to small content-free events, a bounded publisher hands them to a Sink (Kafka in production)
// without ever blocking the request path, and drops are counted, never hidden.
//
// The package knows nothing about Kafka. internal/kafka provides the production Sink; eventstest provides the
// ones used by tests.
package events

import (
	"context"
	"errors"
	"time"
)

// ErrFull is returned by Sink.Produce when the sink's own bounded buffer cannot take another record.
var ErrFull = errors.New("events: sink buffer is full")

// Record is one encoded event on its way to the broker. Key is the request ID, so all events of one request
// share a partition and stay in order.
type Record struct {
	Key        []byte
	Value      []byte
	EnqueuedAt time.Time // when the observer accepted the event; used for the publish latency histogram
}

// Sink delivers records. Implementations must be safe for concurrent use and must never block in Produce.
type Sink interface {
	// Produce queues r for delivery and returns at once. done is called exactly once, from any goroutine, when
	// the record was acknowledged (nil) or given up on (an error). It returns ErrFull, without calling done,
	// when the sink cannot buffer another record.
	Produce(r Record, done func(error)) error
	// Flush waits until every record handed to Produce has had done called, or ctx ends.
	Flush(ctx context.Context) error
	// Close releases the sink's resources. It does not flush.
	Close() error
}
