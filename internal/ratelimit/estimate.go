package ratelimit

import (
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
