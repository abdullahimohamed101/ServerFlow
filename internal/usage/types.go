// Package usage turns terminal lifecycle events into durable usage rows (Phase 12, ADR-019). The consumer loop
// depends on two interfaces, Source (Kafka in production) and Store (PostgreSQL), so every rule that matters
// (idempotence, commit after the database, poison handling, backoff) is tested without either.
package usage

import (
	"context"
	"errors"
	"time"
)

// Message is one record read from the topic.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Offset names the next offset to read in a partition: committing {P, N} means every record below N is done.
type Offset struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Source delivers messages and takes commits. Implementations must make Poll return promptly when ctx ends.
type Source interface {
	// Poll returns up to max messages, waiting for ctx to end or for at least one to arrive. It returns
	// (nil, nil) when ctx ended with nothing to return.
	Poll(ctx context.Context, max int) ([]Message, error)
	// Commit records that every message below the given offsets has been durably processed.
	Commit(ctx context.Context, upTo []Offset) error
	// Lag is the number of records the source knows exist that have not been processed, summed over partitions.
	Lag() int64
	// Close leaves the group. It does not commit.
	Close()
}

// Row is one usage record, built from a terminal event.
type Row struct {
	EventID             string
	RequestID           string
	TenantID            string
	APIKeyID            string
	Model               string
	WorkerID            string
	Outcome             string // completed or failed
	FailureClass        string
	HTTPStatus          int
	InputTokens         *int64
	OutputTokens        *int64
	TokensSource        string
	EstimatedCostTokens int64
	Attempts            int
	TTFTMS              int64
	DurationMS          int64
	OccurredAt          time.Time
	Partition           int32
	Offset              int64
}

// Reject records a message that could not be used. It never holds the message's payload.
type Reject struct {
	Topic     string
	Partition int32
	Offset    int64
	Reason    string
}

// Reject reasons (a closed set; they are metric-safe).
const (
	RejectDecode      = "decode"
	RejectInvalid     = "invalid"
	RejectVersion     = "version"
	RejectOversize    = "oversize"
	RejectDuplicateID = "conflict"
)

// Store writes usage durably. Both methods must be safe to repeat: a batch may be written again after a crash.
type Store interface {
	// InsertUsage writes the rows in one transaction, ignoring rows that already exist, and returns how many
	// were new. The rest were duplicates.
	InsertUsage(ctx context.Context, rows []Row) (inserted int, err error)
	// RecordRejects stores poison-message coordinates (no payload). Repeating one is harmless.
	RecordRejects(ctx context.Context, rejects []Reject) error
}

// ErrClosed is returned by a Source after Close.
var ErrClosed = errors.New("usage: source closed")
