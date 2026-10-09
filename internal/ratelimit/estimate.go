package ratelimit

import (
	"unicode/utf8"

	"serverflow/pkg/protocol"
)

// Cost estimation (spec section 17): input_tokens + max_tokens, deliberately simple and deliberately an
// over-estimate. No model is consulted and nothing is refunded after the response.
const (
	// charsPerToken is the usual rule of thumb for English text; it errs low on tokens, so the
	// non-ASCII rule below and the per-message overhead push the estimate up.
	charsPerToken = 4
	// perMessageOverhead covers role and framing tokens (OpenAI documents about 3-4 per message).
	perMessageOverhead = 4
	// replyPriming covers the tokens that prime the assistant's reply.
	replyPriming = 3
	// MaxCost bounds an estimate so no arithmetic downstream can overflow.
	MaxCost = 1 << 32
)

// EstimateCost estimates a request's token cost: input tokens plus the output budget.
//
// Input tokens: ASCII bytes count one token per four bytes (rounded up per piece of text), every
// non-ASCII character counts as a whole token (multi-byte scripts are expensive to tokenize), each
// message adds a few framing tokens, and the reply adds a few more. Output tokens: maxTokens when the
// client set it, otherwise limit (gateway.max_tokens_limit, the most the gateway would allow). A
// non-positive limit contributes nothing. The result is at least 1 and at most MaxCost, and is
// monotonic: more text or a larger maxTokens never lowers it.
func EstimateCost(msgs []protocol.Message, prompt string, maxTokens, limit int) int {
	var total uint64
	add := func(n uint64) {
		total += n
		if total > MaxCost {
			total = MaxCost
		}
	}
	for _, m := range msgs {
		add(perMessageOverhead)
		add(textTokens(m.Role))
		add(textTokens(m.Content))
	}
	add(textTokens(prompt))
	add(replyPriming)

	out := maxTokens
	if out <= 0 {
		out = limit
	}
	if out > 0 {
		add(uint64(out))
	}
	if total < 1 {
		total = 1
	}
	return int(total)
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
