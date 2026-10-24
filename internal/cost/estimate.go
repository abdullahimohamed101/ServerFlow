// Package cost estimates what a request will cost in tokens (spec section 17). It is pure (no I/O, no clock, no
// state) and is the single estimator for everything that needs one: rate limiting (internal/ratelimit delegates
// here) and, from Phase 14, the scheduler's work accounting (ADR-022). Billing must not move when it is used for
// a new purpose, so the arithmetic is pinned against the original implementation by parity tests in
// internal/ratelimit.
package cost

import (
	"math"
	"unicode/utf8"

	"serverflow/pkg/protocol"
)

// Cost estimation (spec section 17): input_tokens + max_tokens, deliberately simple. No tokenizer is consulted and
// nothing is refunded after the response. It is an approximation, not a bound: it is typically LOW for very dense
// text (digits, base64, hex, minified JSON and code can take 1.5 to 3 characters per token; the 1 MiB request body cap is
// the outer bound on the input), and when max_tokens is absent the reply is charged at gateway.max_tokens_limit while the
// backend, which receives the body unchanged (ADR-003), may generate up to its own limit. See ADR-015.
const (
	// charsPerToken: English prose is about four characters per token; dense ASCII (code, digits, base64,
	// hex, minified JSON) is 1.5 to 3. Three is a compromise that leans towards charging more for text that
	// is mostly not prose; it is still low for the densest text.
	charsPerToken = 3
	// perMessageOverhead covers role and framing tokens (OpenAI documents about 3-4 per message).
	perMessageOverhead = 4
	// replyPriming covers the tokens that prime the assistant's reply.
	replyPriming = 3
	// MaxCost bounds an estimate so no arithmetic downstream can overflow.
	MaxCost = 1 << 32
)

// EstimateCost estimates a request's token cost: input tokens plus the output budget.
//
// Input tokens: ASCII bytes count one token per three bytes (rounded up per piece of text), every
// non-ASCII character counts as a whole token (multi-byte scripts are expensive to tokenize), each
// message adds a few framing tokens, and the reply adds a few more. Output tokens: maxTokens when the
// client set it (the larger of max_tokens and max_completion_tokens when both are present), otherwise limit
// (gateway.max_tokens_limit, an assumption: the backend may generate more when the request names no limit). A
// non-positive limit contributes nothing. The result is at least 1 and at most MaxCost, and is
// monotonic: more text or a larger maxTokens never lowers it.
func EstimateCost(msgs []protocol.Message, prompt string, maxTokens, limit int) int {
	return EstimateRequestCost(msgs, prompt, 0, maxTokens, limit)
}

// EstimateRequestCost is EstimateCost for a request whose whole body is known: bodyBytes is the size of the
// request body as received. The parsed messages carry only the text; image parts, tool call arguments, the tools
// list and every other field a client can send are not in them, and would otherwise cost nothing (a 500 KB request
// of tool definitions was estimated at a few dozen tokens). Every byte of the body that is not already counted as
// message text is therefore charged at the same three bytes per token. That over-charges the JSON framing a little
// and images a lot (a base64 image is charged by its encoded size), which errs towards charging, the safe direction.
func EstimateRequestCost(msgs []protocol.Message, prompt string, bodyBytes, maxTokens, limit int) int {
	in, out := Estimate(msgs, prompt, bodyBytes, maxTokens, limit)
	return in + out
}

// Estimate is EstimateRequestCost split into its two parts: the input tokens and the output budget. The two always
// sum to exactly what EstimateRequestCost returns (including the MaxCost bound, which is applied to the sum: the
// output part is the room left under it), so a caller that wants both parts and one that wants the total agree.
func Estimate(msgs []protocol.Message, prompt string, bodyBytes, maxTokens, limit int) (input, output int) {
	var in uint64
	add := func(n uint64) {
		in += n
		if in > MaxCost {
			in = MaxCost
		}
	}
	var counted uint64 // bytes of text already charged above
	for _, m := range msgs {
		add(perMessageOverhead)
		add(textTokens(m.Role))
		add(textTokens(m.Content))
		counted += uint64(len(m.Role)) + uint64(len(m.Content))
	}
	add(textTokens(prompt))
	counted += uint64(len(prompt))
	if bodyBytes > 0 && uint64(bodyBytes) > counted {
		add((uint64(bodyBytes) - counted + charsPerToken - 1) / charsPerToken)
	}
	add(replyPriming)

	out := maxTokens
	if out <= 0 {
		out = limit
	}
	var o uint64
	if out > 0 {
		o = uint64(out)
	}
	// The original loop saturated at MaxCost after every addition, so the total is min(input + output, MaxCost).
	if room := MaxCost - in; o > room {
		o = room
	}
	if in+o < 1 {
		in = 1 // unreachable while replyPriming > 0; kept so the "at least 1" contract does not depend on it
	}
	return int(in), int(o)
}

// textTokens estimates the tokens in s without copying it.
func textTokens(s string) uint64 {
	var ascii, wide uint64
	for i := 0; i < len(s); {
		if s[i] < utf8.RuneSelf {
			ascii++
			i++
			continue
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		wide++ // an invalid byte decodes as one RuneError of width 1 and also counts as a token
		i += size
	}
	return (ascii+charsPerToken-1)/charsPerToken + wide
}

// Weights turn an estimate into the work a request is expected to put on a worker (ADR-022, D3). They apply to
// the scheduler's accounting only: the rate limiter is always charged the unweighted estimate.
type Weights struct {
	// InputWeight scales the input tokens (prefill tokens are usually cheaper than decode tokens, so a value
	// below 1 is plausible). Default 1.
	InputWeight float64
	// OutputFraction scales the output budget (max_tokens, or the gateway limit when absent): the share of it
	// the replies are expected to use. In (0, 1]. Default 1, the pessimistic "charge the whole budget" estimate.
	OutputFraction float64
}

// DefaultWeights are the plan's defaults: input counted fully, the whole output budget counted.
func DefaultWeights() Weights { return Weights{InputWeight: 1, OutputFraction: 1} }

// maxWeight bounds a weight taken from a caller that did not validate it, so the arithmetic cannot overflow.
const maxWeight = 1e4

// Work is the estimated work of a request in tokens: InputWeight times the input estimate plus OutputFraction
// times the output budget, rounded up per part. It is at least 1 for a request with any estimate (so the ledger
// never holds a free request) and bounded by 2*MaxCost*maxWeight. Negative or non-finite weights count as the
// defaults; weights above maxWeight are clamped.
func Work(estInput, estOutput int, w Weights) int64 {
	in, out := float64(max(estInput, 0)), float64(max(estOutput, 0))
	iw, of := sane(w.InputWeight, 1), sane(w.OutputFraction, 1)
	total := int64(math.Ceil(iw*in)) + int64(math.Ceil(of*out))
	if total < 1 && (estInput > 0 || estOutput > 0) {
		total = 1
	}
	return total
}

func sane(v, def float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return def
	}
	return math.Min(v, maxWeight)
}
