package gateway

import (
	"bytes"

	"serverflow/pkg/protocol"
)

// tokenScanner extracts token counts from a response that is already flowing to the client (Phase 12, D8). It
// never changes the bytes and never stores or logs text: it keeps a small sliding window of the most recent bytes
// (overwritten as the response goes by, discarded with the request) and a counter. Only integers leave it.
//
//   - A "usage" object with prompt_tokens and completion_tokens (a non-streaming response, or the final chunk of a
//     stream when the client set stream_options.include_usage) gives TokensUsage: both counts, as the worker
//     reported them. Servers that put usage before the choices are not seen; the request then falls back below.
//   - Otherwise, for a stream, the number of chunks that carried non-empty delta content approximates the output
//     tokens (TokensChunks); the input count is unknown.
//   - Otherwise nothing is known and the source stays empty (the event says "estimate").
type tokenScanner struct {
	tail   []byte // the last bytes seen, at most tokenWindow
	carry  [contentKeyLen]byte
	ncarry int
	chunks int64
}

const (
	tokenWindow   = 2048
	contentKey    = `"content":"`
	contentKeyLen = len(contentKey)
)

var (
	contentKeyBytes = []byte(contentKey)
	usageKey        = []byte(`"usage"`)
	promptKey       = []byte(`"prompt_tokens"`)
	completionKey   = []byte(`"completion_tokens"`)
)

// feed sees each piece of the response body, in order.
func (t *tokenScanner) feed(b []byte) {
	if len(b) == 0 {
		return
	}
	if t.tail == nil {
		t.tail = make([]byte, 0, tokenWindow)
	}
	if len(b) >= tokenWindow {
		t.tail = append(t.tail[:0], b[len(b)-tokenWindow:]...)
	} else {
		if over := len(t.tail) + len(b) - tokenWindow; over > 0 {
			t.tail = append(t.tail[:0], t.tail[over:]...)
		}
		t.tail = append(t.tail, b...)
	}
	// Count "content":"<something> across read boundaries without copying the body. A key that began in the
	// carry (the last bytes of the previous piece) is completed from the start of this one; every other key lies
	// wholly inside b. A key that ends exactly at the end of a piece is left for the next, which sees the byte
	// after it. The carry is exactly the key's length, so it holds a whole key only in that case.
	if t.ncarry > 0 {
		var head [2 * contentKeyLen]byte
		n := copy(head[:], t.carry[:t.ncarry])
		n += copy(head[n:], b[:min(len(b), contentKeyLen+1)])
		t.chunks += countKeys(head[:n], t.ncarry)
	}
	t.chunks += countKeys(b, len(b))
	if len(b) >= len(t.carry) {
		t.ncarry = copy(t.carry[:], b[len(b)-len(t.carry):])
	} else {
		var all [2 * contentKeyLen]byte
		n := copy(all[:], t.carry[:t.ncarry])
		n += copy(all[n:], b)
		t.ncarry = copy(t.carry[:], all[max(0, n-len(t.carry)):n])
	}
}

// countKeys counts the keys in b that start before limit and are followed by a byte that is not a closing quote.
// A key at the very end of b is not counted: its following byte has not arrived.
func countKeys(b []byte, limit int) int64 {
	var n int64
	pos := 0
	for {
		i := bytes.Index(b[pos:], contentKeyBytes)
		if i < 0 {
			return n
		}
		start := pos + i
		if start >= limit {
			return n
		}
		after := start + contentKeyLen
		if after < len(b) && b[after] != '"' {
			n++
		}
		pos = after
	}
}

// result returns the source (TokensUsage, TokensChunks or "") and the counts. A usage object is used only if it is
// unambiguous: exactly one "usage" object in the window, each of prompt_tokens and completion_tokens present
// exactly once with a plain non-negative integer no larger than protocol.MaxEventTokens. Anything else (a fraction,
// an exponent, a sign, a leading zero, a number too large, a duplicate key, a string) is not guessed at or
// truncated: the request falls back to chunks or to "estimate".
//
// Decision (fix round): counts far above the request's max_tokens are NOT rejected. A worker may count more than
// max_tokens (reasoning tokens, a different tokenizer) or the request may carry no max_tokens; the workers are
// registered, authenticated parts of the system, and the absolute bound already stops absurd values from reaching
// the database. The label tokens_source=usage means "as the worker reported it", not "audited".
func (t *tokenScanner) result(stream bool) (source string, in, out int64) {
	if p, c, ok := parseUsage(t.tail); ok {
		return TokensUsage, p, c
	}
	if stream && t.chunks > 0 {
		return TokensChunks, 0, t.chunks
	}
	return "", 0, 0
}

// parseUsage finds the one usage object in b and reads its two counts.
func parseUsage(b []byte) (prompt, completion int64, ok bool) {
	objStart := -1
	for off := 0; ; {
		i := bytes.Index(b[off:], usageKey)
		if i < 0 {
			break
		}
		pos := off + i + len(usageKey)
		rest := b[pos:]
		for len(rest) > 0 && (rest[0] == ' ' || rest[0] == ':' || rest[0] == '\t' || rest[0] == '\n' || rest[0] == '\r') {
			rest = rest[1:]
		}
		if len(rest) > 0 && rest[0] == '{' {
			if objStart >= 0 {
				return 0, 0, false // two usage objects: ambiguous
			}
			objStart = len(b) - len(rest)
		}
		off = pos
	}
	if objStart < 0 {
		return 0, 0, false
	}
	obj, closed := balanced(b[objStart:])
	if !closed {
		return 0, 0, false
	}
	var ok1, ok2 bool
	prompt, ok1 = uniqueCount(obj, promptKey)
	completion, ok2 = uniqueCount(obj, completionKey)
	return prompt, completion, ok1 && ok2
}

// balanced returns the object that starts at b[0] == '{' up to its matching brace, skipping braces inside strings.
func balanced(b []byte) (obj []byte, closed bool) {
	depth, inStr := 0, false
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch {
		case inStr:
			if c == '\\' {
				i++
			} else if c == '"' {
				inStr = false
			}
		case c == '"':
			inStr = true
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return b[:i+1], true
			}
		}
	}
	return nil, false
}

// uniqueCount reads the integer that follows key, which must occur exactly once in obj.
func uniqueCount(obj, key []byte) (int64, bool) {
	i := bytes.Index(obj, key)
	if i < 0 || bytes.Contains(obj[i+len(key):], key) {
		return 0, false
	}
	b := obj[i+len(key):]
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	if len(b) == 0 || b[0] != ':' {
		return 0, false
	}
	b = b[1:]
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	n, digits := int64(0), 0
	for digits < len(b) && b[digits] >= '0' && b[digits] <= '9' {
		if digits >= 11 { // more digits than the largest allowed value has
			return 0, false
		}
		n = n*10 + int64(b[digits]-'0')
		digits++
	}
	if digits == 0 || (digits > 1 && b[0] == '0') || n > protocol.MaxEventTokens || digits == len(b) {
		return 0, false
	}
	switch b[digits] { // the number must end here: not a fraction, an exponent or anything else
	case ',', '}', ' ', '\t', '\n', '\r':
		return n, true
	}
	return 0, false
}
