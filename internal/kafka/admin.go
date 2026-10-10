package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Admin performs the few administrative calls the tests and runbook tooling need. The gateway never creates
// topics (D7); production topics are created by an operator.
type Admin struct {
	cfg Config
	cl  *kgo.Client
}

// NewAdmin connects lazily, like the producer.
func NewAdmin(cfg Config) (*Admin, error) {
	opts, err := cfg.baseOpts(nil)
	if err != nil {
		return nil, cfg.safe(err)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, cfg.safe(err)
	}
	return &Admin{cfg: cfg, cl: cl}, nil
}

// Close releases the connection.
func (a *Admin) Close() { a.cl.Close() }

// CreateTopic creates a topic with the given partitions and seven-day retention; an existing topic is not an error.
func (a *Admin) CreateTopic(ctx context.Context, name string, partitions int) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = name, int32(partitions), -1
	for k, v := range map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"} {
		c := kmsg.NewCreateTopicsRequestTopicConfig()
		c.Name, c.Value = k, &v
		t.Configs = append(t.Configs, c)
	}
	req.Topics = append(req.Topics, t)
	req.TimeoutMillis = 15000
	resp, err := req.RequestWith(ctx, a.cl)
	if err != nil {
		return a.cfg.safe(err)
	}
	for _, rt := range resp.Topics {
		if err := kerr.ErrorForCode(rt.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka: create topic: %w", err)
		}
	}
	return nil
}

// DeleteTopic deletes a topic; a missing topic is not an error.
func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{name}
	rt := kmsg.NewDeleteTopicsRequestTopic()
	rt.Topic = &name
	req.Topics = append(req.Topics, rt)
	req.TimeoutMillis = 15000
	resp, err := req.RequestWith(ctx, a.cl)
	if err != nil {
		return a.cfg.safe(err)
	}
	for _, r := range resp.Topics {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("kafka: delete topic: %w", err)
		}
	}
	return nil
}

// offsets returns the per-partition offset of the topic at a timestamp (-1 latest, -2 earliest).
func (a *Admin) offsets(ctx context.Context, topic string, partitions []int32, ts int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for _, p := range partitions {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition, rp.Timestamp = p, ts
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	req.SetVersion(1)
	resp, err := req.RequestWith(ctx, a.cl)
	if err != nil {
		return nil, a.cfg.safe(err)
	}
	out := map[int32]int64{}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("kafka: list offsets: %w", err)
			}
			out[p.Partition] = p.Offset
		}
	}
	return out, nil
}

// Partitions returns the partition numbers of a topic.
func (a *Admin) Partitions(ctx context.Context, topic string) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = &topic
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, a.cl)
	if err != nil {
		return nil, a.cfg.safe(err)
	}
	var out []int32
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			return nil, fmt.Errorf("kafka: metadata: %w", err)
		}
		for _, p := range t.Partitions {
			out = append(out, p.Partition)
		}
	}
	return out, nil
}

// EndOffsets returns the number of records ever written to the topic's partitions (sum of end offsets minus
// start offsets), which is the number still readable.
func (a *Admin) Records(ctx context.Context, topic string) (int64, error) {
	parts, err := a.Partitions(ctx, topic)
	if err != nil {
		return 0, err
	}
	end, err := a.offsets(ctx, topic, parts, -1)
	if err != nil {
		return 0, err
	}
	start, err := a.offsets(ctx, topic, parts, -2)
	if err != nil {
		return 0, err
	}
	var n int64
	for p, e := range end {
		n += e - start[p]
	}
	return n, nil
}

// GroupLag returns how many records of the topic the group has not yet committed past, summed over partitions.
// A partition the group has not committed counts from its start offset.
func (a *Admin) GroupLag(ctx context.Context, group, topic string) (int64, error) {
	parts, err := a.Partitions(ctx, topic)
	if err != nil {
		return 0, err
	}
	end, err := a.offsets(ctx, topic, parts, -1)
	if err != nil {
		return 0, err
	}
	start, err := a.offsets(ctx, topic, parts, -2)
	if err != nil {
		return 0, err
	}
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic, rt.Partitions = topic, parts
	req.Topics = append(req.Topics, rt)
	req.SetVersion(5)
	resp, err := req.RequestWith(ctx, a.cl)
	if err != nil {
		return 0, a.cfg.safe(err)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return 0, fmt.Errorf("kafka: offset fetch: %w", err)
	}
	committed := map[int32]int64{}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			committed[p.Partition] = p.Offset
		}
	}
	var lag int64
	for p, e := range end {
		c, ok := committed[p]
		if !ok || c < start[p] {
			c = start[p]
		}
		lag += e - c
	}
	return lag, nil
}

// Produce sends one record synchronously through the admin connection (for tests that need a raw record on the
// topic, such as a poison message).
func (a *Admin) Produce(ctx context.Context, topic string, key, value []byte) error {
	res := a.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value})
	return a.cfg.safe(res.FirstErr())
}
