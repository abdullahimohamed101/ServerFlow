package eventstest

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"serverflow/internal/events"
	"serverflow/internal/usage"
)

// MemBroker is an in-memory topic: records are placed on a partition by key hash, offsets grow per partition, and
// consumer groups keep committed offsets. It is an events.Sink (Produce) and hands out usage.Sources.
type MemBroker struct {
	Topic string
	mu    sync.Mutex
	parts [][]usage.Message
	group map[string]map[int32]int64 // committed (next) offsets
	wake  chan struct{}
}

func NewMemBroker(topic string, partitions int) *MemBroker {
	return &MemBroker{Topic: topic, parts: make([][]usage.Message, partitions), group: map[string]map[int32]int64{}, wake: make(chan struct{})}
}

// Append adds a raw record (also used to inject poison messages).
func (b *MemBroker) Append(key, value []byte) {
	h := fnv.New32a()
	_, _ = h.Write(key)
	p := int32(h.Sum32() % uint32(len(b.parts)))
	b.mu.Lock()
	b.parts[p] = append(b.parts[p], usage.Message{Topic: b.Topic, Partition: p, Offset: int64(len(b.parts[p])), Key: key, Value: value, Timestamp: time.Now()})
	old := b.wake
	b.wake = make(chan struct{})
	b.mu.Unlock()
	close(old)
}

// Produce implements events.Sink.
func (b *MemBroker) Produce(r events.Record, done func(error)) error {
	b.Append(r.Key, r.Value)
	done(nil)
	return nil
}
func (b *MemBroker) Flush(context.Context) error { return nil }
func (b *MemBroker) Close() error                { return nil }

// Total is the number of records on the topic.
func (b *MemBroker) Total() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var n int64
	for _, p := range b.parts {
		n += int64(len(p))
	}
	return n
}

// Lag is the number of records the group has not committed past.
func (b *MemBroker) Lag(group string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var n int64
	for i, p := range b.parts {
		n += int64(len(p)) - b.group[group][int32(i)]
	}
	return n
}

// NewSource joins a group. A member starts from the group's committed offsets, so uncommitted work is redelivered
// to the next member, as with Kafka.
func (b *MemBroker) NewSource(group string) *MemSource {
	b.mu.Lock()
	defer b.mu.Unlock()
	pos := map[int32]int64{}
	for i := range b.parts {
		pos[int32(i)] = b.group[group][int32(i)]
	}
	return &MemSource{b: b, group: group, pos: pos}
}

// MemSource is one member of a group.
type MemSource struct {
	b      *MemBroker
	group  string
	pos    map[int32]int64
	closed bool
	// CommitErr, if set, is returned by Commit (and nothing is committed).
	CommitErr error
	Commits   int
}

func (s *MemSource) Poll(ctx context.Context, max int) ([]usage.Message, error) {
	for {
		s.b.mu.Lock()
		var out []usage.Message
		for i, p := range s.b.parts {
			for s.pos[int32(i)] < int64(len(p)) && len(out) < max {
				out = append(out, p[s.pos[int32(i)]])
				s.pos[int32(i)]++
			}
		}
		wake := s.b.wake
		closed := s.closed
		s.b.mu.Unlock()
		if closed {
			return nil, usage.ErrClosed
		}
		if len(out) > 0 {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil
		case <-wake:
		}
	}
}

func (s *MemSource) Commit(_ context.Context, upTo []usage.Offset) error {
	if s.CommitErr != nil {
		return s.CommitErr
	}
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	if s.b.group[s.group] == nil {
		s.b.group[s.group] = map[int32]int64{}
	}
	for _, o := range upTo {
		if o.Offset > s.b.group[s.group][o.Partition] {
			s.b.group[s.group][o.Partition] = o.Offset
		}
	}
	s.Commits++
	return nil
}

func (s *MemSource) Lag() int64 { return s.b.Lag(s.group) }

func (s *MemSource) Close() {
	s.b.mu.Lock()
	s.closed = true
	s.b.mu.Unlock()
}

// MemStore is an in-memory usage.Store with the same uniqueness rules as the table.
type MemStore struct {
	mu      sync.Mutex
	byEvent map[string]usage.Row
	byReq   map[string]bool
	Rejects []usage.Reject
	// FailNext makes the next n store calls fail like an unreachable database.
	FailNext int
	// BadRequestID makes any batch containing this request ID fail with usage.ErrBadData.
	BadRequestID string
	Calls        int
}

func NewMemStore() *MemStore {
	return &MemStore{byEvent: map[string]usage.Row{}, byReq: map[string]bool{}}
}

type dbDown struct{}

func (dbDown) Error() string { return "memstore: database unreachable" }

func (s *MemStore) InsertUsage(_ context.Context, rows []usage.Row) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Calls++
	if s.FailNext > 0 {
		s.FailNext--
		return 0, dbDown{}
	}
	for _, r := range rows {
		if s.BadRequestID != "" && r.RequestID == s.BadRequestID {
			return 0, usage.ErrBadData
		}
	}
	n := 0
	for _, r := range rows {
		if _, dup := s.byEvent[r.EventID]; dup || s.byReq[r.RequestID] {
			continue
		}
		s.byEvent[r.EventID], s.byReq[r.RequestID] = r, true
		n++
	}
	return n, nil
}

func (s *MemStore) RecordRejects(_ context.Context, rs []usage.Reject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.FailNext > 0 {
		s.FailNext--
		return dbDown{}
	}
	s.Rejects = append(s.Rejects, rs...)
	return nil
}

func (s *MemStore) Count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.byEvent) }
func (s *MemStore) Rows() []usage.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []usage.Row
	for _, r := range s.byEvent {
		out = append(out, r)
	}
	return out
}
