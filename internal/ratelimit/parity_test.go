package ratelimit

import (
	"math"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"serverflow/pkg/protocol"
)

// The reference below is the Phase 8 estimator exactly as it was before it moved to internal/cost (copied
// verbatim from master at 32b20d4, only the identifiers renamed). It is kept here so the delegating functions are
// compared with the ORIGINAL arithmetic, not with themselves: billing must not move (Phase 14 criterion 2).
const (
	refCharsPerToken      = 3
	refPerMessageOverhead = 4
	refReplyPriming       = 3
	refMaxCost            = 1 << 32
)

func refEstimateRequestCost(msgs []protocol.Message, prompt string, bodyBytes, maxTokens, limit int) int {
	var total uint64
	add := func(n uint64) {
		total += n
		if total > refMaxCost {
			total = refMaxCost
		}
	}
	var counted uint64 // bytes of text already charged above
	for _, m := range msgs {
		add(refPerMessageOverhead)
		add(refTextTokens(m.Role))
		add(refTextTokens(m.Content))
		counted += uint64(len(m.Role)) + uint64(len(m.Content))
	}
	add(refTextTokens(prompt))
	counted += uint64(len(prompt))
	if bodyBytes > 0 && uint64(bodyBytes) > counted {
		add((uint64(bodyBytes) - counted + refCharsPerToken - 1) / refCharsPerToken)
	}
	add(refReplyPriming)

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

// refTextTokens estimates the tokens in s without copying it.
func refTextTokens(s string) uint64 {
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
	return (ascii+refCharsPerToken-1)/refCharsPerToken + wide
}

func refMsgs(contents ...string) []protocol.Message {
	var out []protocol.Message
	for _, c := range contents {
		out = append(out, protocol.Message{Role: "user", Content: c})
	}
	return out
}

func TestEstimatorDelegatesWithTheOriginalNumbers(t *testing.T) {
	if MaxCost != refMaxCost {
		t.Fatalf("MaxCost = %d, want %d", MaxCost, refMaxCost)
	}
	big := strings.Repeat("lorem ipsum ", 50_000)
	wide := strings.Repeat("héllo 你好 ", 400)
	table := []struct {
		name      string
		msgs      []protocol.Message
		prompt    string
		bodyBytes int
		maxTokens int
		limit     int
	}{
		{"empty", nil, "", 0, 0, 0},
		{"empty with limit", nil, "", 0, 0, 4096},
		{"hello", refMsgs("hello world"), "", 0, 100, 4096},
		{"absent max_tokens", refMsgs("hi"), "", 0, 0, 4096},
		{"negative max_tokens", refMsgs("hi"), "", 0, -5, 4096},
		{"negative everything", nil, "", -1, -5, -5},
		{"prompt", nil, big, 0, 10, 4096},
		{"body bytes larger than text", refMsgs("hello"), "", 500_000, 10, 4096},
		{"body bytes smaller than text", refMsgs(big), "", 5, 10, 4096},
		{"body bytes equal to text", refMsgs("abc"), "", 3 + 4, 10, 4096},
		{"non-ascii", refMsgs(wide), "", len(wide) + 80, 10, 4096},
		{"invalid utf-8", refMsgs("a\xff\xfe\x80b"), "", 0, 1, 1},
		{"max int max_tokens", refMsgs("x"), "", 0, math.MaxInt, 1},
		{"max int limit", refMsgs("x"), "", 0, 0, math.MaxInt},
		{"max int body", refMsgs("x"), "", math.MaxInt, 1, 1},
		{"huge text", refMsgs(big, big, big), big, 1 << 20, math.MaxInt, math.MaxInt},
		{"many empty messages", make([]protocol.Message, 100_000), "", 0, 1, 1},
	}
	for _, c := range table {
		t.Run(c.name, func(t *testing.T) {
			want := refEstimateRequestCost(c.msgs, c.prompt, c.bodyBytes, c.maxTokens, c.limit)
			if got := EstimateRequestCost(c.msgs, c.prompt, c.bodyBytes, c.maxTokens, c.limit); got != want {
				t.Fatalf("EstimateRequestCost = %d, original = %d", got, want)
			}
			if want, got := refEstimateRequestCost(c.msgs, c.prompt, 0, c.maxTokens, c.limit), EstimateCost(c.msgs, c.prompt, c.maxTokens, c.limit); got != want {
				t.Fatalf("EstimateCost = %d, original = %d", got, want)
			}
		})
	}
}

// randomText mixes ASCII, multi-byte runes and invalid bytes.
func randomText(r *rand.Rand, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		switch r.Intn(8) {
		case 0:
			b.WriteRune(rune(0x80 + r.Intn(0x2000)))
		case 1:
			b.WriteByte(byte(0x80 + r.Intn(0x80))) // frequently invalid UTF-8
		default:
			b.WriteByte(byte('a' + r.Intn(26)))
		}
	}
	return b.String()
}

func TestEstimatorParityWithTheOriginalOverRandomRequests(t *testing.T) {
	r := rand.New(rand.NewSource(14))
	extremes := []int{math.MinInt, -1, 0, 1, 2, 3, 4, 4096, 1 << 20, 1 << 31, 1 << 32, 1<<32 + 1, math.MaxInt32, math.MaxInt}
	pick := func() int {
		if r.Intn(4) == 0 {
			return extremes[r.Intn(len(extremes))]
		}
		return r.Intn(20_000) - 100
	}
	const n = 100_000
	for i := 0; i < n; i++ {
		msgs := make([]protocol.Message, r.Intn(6))
		for j := range msgs {
			msgs[j] = protocol.Message{Role: randomText(r, r.Intn(10)), Content: randomText(r, r.Intn(400))}
		}
		prompt := ""
		if r.Intn(3) == 0 {
			prompt = randomText(r, r.Intn(400))
		}
		body, maxTokens, limit := pick(), pick(), pick()
		if r.Intn(2) == 0 {
			body = r.Intn(3000) // exercise the "body larger than counted text" branch often
		}
		want := refEstimateRequestCost(msgs, prompt, body, maxTokens, limit)
		if got := EstimateRequestCost(msgs, prompt, body, maxTokens, limit); got != want {
			t.Fatalf("case %d: EstimateRequestCost = %d, original = %d (msgs=%d prompt=%d body=%d max=%d limit=%d)", i, got, want, len(msgs), len(prompt), body, maxTokens, limit)
		}
		if want, got := refEstimateRequestCost(msgs, prompt, 0, maxTokens, limit), EstimateCost(msgs, prompt, maxTokens, limit); got != want {
			t.Fatalf("case %d: EstimateCost = %d, original = %d", i, got, want)
		}
	}
}
