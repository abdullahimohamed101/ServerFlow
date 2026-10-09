package redis_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"serverflow/internal/redis"
	"serverflow/internal/redis/redistest"
)

func runRecorder(t *testing.T, r *redis.Recorder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func TestRecorderWritesWithATTL(t *testing.T) {
	c := redistest.NewClient(t)
	r := redis.NewRecorder(c, 90*time.Minute)
	runRecorder(t, r)
	id := redistest.Unique("req")
	r.Record(id, "ten_acme", "qwen-7b", "w1", 2)
	deadline := time.Now().Add(5 * time.Second)
	for r.Written() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	v, err := c.Get(context.Background(), "request:{"+id+"}")
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Tenant, Model, Worker string
		Attempt               int
	}
	if err := json.Unmarshal([]byte(strings.ToLower(v)), &got); err != nil || got.Tenant != "ten_acme" || got.Worker != "w1" || got.Attempt != 2 {
		t.Fatalf("%s %v", v, err)
	}
	if ttl, _ := c.TTL(context.Background(), "request:{"+id+"}"); ttl <= 89*time.Minute || ttl > 90*time.Minute {
		t.Fatalf("ttl %v", ttl)
	}
}

func TestRecorderNeverBlocksAndDropsWhenFullOrDown(t *testing.T) {
	cfg := redistest.Config(t)
	proxy := redistest.NewProxy(t, cfg.Address)
	cfg.Address = proxy.Addr()
	c := redistest.NewClientWith(t, cfg)
	r := redis.NewRecorder(c, time.Minute) // no writers running: the queue fills
	start := time.Now()
	for i := 0; i < 20000; i++ {
		r.Record("id", "t", "m", "w", 1)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Record blocked")
	}
	if r.Dropped() == 0 {
		t.Fatal("a full queue must drop")
	}
	// While Redis is down, records are skipped without queueing.
	proxy.SetMode(redistest.Blackhole)
	_, _ = c.Run(context.Background(), echo, []string{"k"}, 1) // marks the client down
	before := r.Dropped()
	r.Record("id2", "t", "m", "w", 1)
	if r.Dropped() != before+1 {
		t.Fatal("a record during an outage was not skipped")
	}
}
