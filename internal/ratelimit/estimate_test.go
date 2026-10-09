package ratelimit

import (
	"math"
	"math/rand"
	"strings"
	"testing"

	"serverflow/pkg/protocol"
)

func msgs(contents ...string) []protocol.Message {
	var out []protocol.Message
	for _, c := range contents {
		out = append(out, protocol.Message{Role: "user", Content: c})
	}
	return out
}

func TestEstimateCostKnownValues(t *testing.T) {
	// One message "hello world" (11 ASCII bytes = 4 tokens at 3 per token) + role "user" (2) + 4 framing + 3 priming + 100 output.
	if got := EstimateCost(msgs("hello world"), "", 100, 4096); got != 4+2+4+3+100 {
		t.Fatalf("got %d", got)
	}
	// An absent max_tokens costs the gateway limit, the gateway's assumption (a backend may generate more).
	if got := EstimateCost(msgs("hi"), "", 0, 4096); got != 1+2+4+3+4096 {
		t.Fatalf("absent max_tokens: got %d", got)
	}
}

func TestEstimateCostMinimumAndZeroLimit(t *testing.T) {
	if got := EstimateCost(nil, "", 0, 0); got < 1 {
		t.Fatalf("cost %d must be at least 1", got)
	}
	if got := EstimateCost(nil, "", -5, -5); got < 1 {
		t.Fatalf("negative inputs: %d", got)
	}
}

func TestEstimateCostMultiByteChargesMoreThanBytesOverFour(t *testing.T) {
	cjk := strings.Repeat("漢", 100) // 300 bytes
	ascii := strings.Repeat("a", 100)
	if EstimateCost(msgs(cjk), "", 1, 1) <= EstimateCost(msgs(ascii), "", 1, 1) {
		t.Fatal("non-ASCII text must cost more than the same number of ASCII characters")
	}
	if got := EstimateCost(msgs(cjk), "", 1, 1); got < 100 {
		t.Fatalf("100 CJK characters estimated at only %d tokens", got)
	}
	// Invalid UTF-8 must not panic or be free.
	bad := string([]byte{0xff, 0xfe, 0xfd, 0xc0, 0x80})
	if EstimateCost(msgs(bad), "", 1, 1) <= EstimateCost(msgs(""), "", 1, 1) {
		t.Fatal("invalid UTF-8 was free")
	}
}

func TestEstimateCostMonotonic(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		base := randText(r, r.Intn(200))
		extra := randText(r, r.Intn(50)+1)
		mt := r.Intn(5000) + 1
		a := EstimateCost(msgs(base), "", mt, 4096)
		if b := EstimateCost(msgs(base+extra), "", mt, 4096); b < a {
			t.Fatalf("longer text cost less: %d < %d", b, a)
		}
		if b := EstimateCost(msgs(base, extra), "", mt, 4096); b < a {
			t.Fatalf("an extra message cost less: %d < %d", b, a)
		}
		if b := EstimateCost(msgs(base), "", mt+r.Intn(100)+1, 4096); b <= a {
			t.Fatalf("a bigger max_tokens did not cost more: %d <= %d", b, a)
		}
		if b := EstimateCost(msgs(base), extra, mt, 4096); b < a {
			t.Fatalf("a prompt cost less: %d < %d", b, a)
		}
	}
}

func randText(r *rand.Rand, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		switch r.Intn(4) {
		case 0:
			b.WriteRune(rune(0x4e00 + r.Intn(500)))
		case 1:
			b.WriteRune('é')
		default:
			b.WriteByte(byte('a' + r.Intn(26)))
		}
	}
	return b.String()
}

func TestEstimateCostBoundedAndNoOverflow(t *testing.T) {
	huge := strings.Repeat("x", 64<<20)
	got := EstimateCost(msgs(huge, huge), huge, math.MaxInt, math.MaxInt)
	if got < 1 || got > MaxCost {
		t.Fatalf("got %d, want within [1,%d]", got, MaxCost)
	}
	if got := EstimateCost(msgs("x"), "", math.MaxInt, 1); got != MaxCost {
		t.Fatalf("a max_tokens of MaxInt must clamp to MaxCost, got %d", got)
	}
	many := make([]protocol.Message, 100000)
	if got := EstimateCost(many, "", 1, 1); got < 100000*perMessageOverhead {
		t.Fatalf("many empty messages undercounted: %d", got)
	}
}

func TestEstimateAssumesThreeCharsPerToken(t *testing.T) {
	text := strings.Repeat("the quick brown fox ", 500) // 10,000 ASCII chars
	in := EstimateCost(msgs(text), "", 1, 1) - 1
	if in < len(text)/3 {
		t.Fatalf("input estimated at %d tokens for %d characters", in, len(text))
	}
}
