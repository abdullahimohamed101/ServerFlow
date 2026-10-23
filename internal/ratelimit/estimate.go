package ratelimit

import (
	"serverflow/internal/cost"
	"serverflow/pkg/protocol"
)

// The cost estimator moved to internal/cost (Phase 14, ADR-022) so the scheduler can use the same numbers without
// importing the rate limiter and its Redis dependency. These are thin delegates with the original names and
// semantics; parity_test.go pins them to the original implementation, so billing does not move.

// MaxCost bounds an estimate so no arithmetic downstream can overflow.
const MaxCost = cost.MaxCost

// EstimateCost estimates a request's token cost: input tokens plus the output budget. See cost.EstimateCost.
func EstimateCost(msgs []protocol.Message, prompt string, maxTokens, limit int) int {
	return cost.EstimateCost(msgs, prompt, maxTokens, limit)
}

// EstimateRequestCost is EstimateCost for a request whose whole body is known. See cost.EstimateRequestCost.
func EstimateRequestCost(msgs []protocol.Message, prompt string, bodyBytes, maxTokens, limit int) int {
	return cost.EstimateRequestCost(msgs, prompt, bodyBytes, maxTokens, limit)
}
