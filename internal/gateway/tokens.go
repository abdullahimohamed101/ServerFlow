package gateway

import "bytes"

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
	usageKey      = []byte(`"usage"`)
	promptKey     = []byte(`"prompt_tokens"`)
	completionKey = []byte(`"completion_tokens"`)
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
	// Count "content":"<something> across read boundaries. The carry is exactly the key's length, so it holds a
	// whole key only when the key ends the previous piece, whose next byte was not yet seen and so not counted.
	var joined []byte
	if t.ncarry > 0 {
		joined = append(append(make([]byte, 0, t.ncarry+len(b)), t.carry[:t.ncarry]...), b...)
	} else {
		joined = b
	}
	for rest := joined; ; {
		i := bytes.Index(rest, []byte(contentKey))
		if i < 0 {
			break
		}
		after := i + contentKeyLen
		if after < len(rest) {
			if rest[after] != '"' {
				t.chunks++
			}
		} else {
			// The next byte has not arrived: look at it with the next piece by keeping the key in the carry.
			break
		}
		rest = rest[after:]
	}
	keep := min(len(joined), len(t.carry))
	t.ncarry = copy(t.carry[:], joined[len(joined)-keep:])
}

// result returns the source (TokensUsage, TokensChunks or "") and the counts.
func (t *tokenScanner) result(stream bool) (source string, in, out int64) {
	if i := bytes.LastIndex(t.tail, usageKey); i >= 0 {
		rest := t.tail[i+len(usageKey):]
		for len(rest) > 0 && (rest[0] == ' ' || rest[0] == ':') {
			rest = rest[1:]
		}
		if len(rest) > 0 && rest[0] == '{' {
			p, okp := intAfter(rest, promptKey)
			c, okc := intAfter(rest, completionKey)
			if okp && okc {
				return TokensUsage, p, c
			}
		}
	}
	if stream && t.chunks > 0 {
		return TokensChunks, 0, t.chunks
	}
	return "", 0, 0
}

// intAfter reads the non-negative integer that follows key and a colon.
func intAfter(b, key []byte) (int64, bool) {
	i := bytes.Index(b, key)
	if i < 0 {
		return 0, false
	}
	b = b[i+len(key):]
	for len(b) > 0 && (b[0] == ' ' || b[0] == ':') {
		b = b[1:]
	}
	var n int64
	digits := 0
	for _, c := range b {
		if c < '0' || c > '9' || digits >= 12 {
			break
		}
		n = n*10 + int64(c-'0')
		digits++
	}
	return n, digits > 0
}
