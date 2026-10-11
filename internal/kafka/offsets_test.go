package kafka_test

import "serverflow/internal/usage"

type usageOffset = usage.Offset

func toUsage(in []usageOffset) []usage.Offset { return in }
