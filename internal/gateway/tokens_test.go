package gateway

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"testing"
)

func scanAll(stream bool, body string, split int) (string, int64, int64) {
	var t tokenScanner
	b := []byte(body)
	for len(b) > 0 {
		n := min(split, len(b))
		t.feed(b[:n])
		b = b[n:]
	}
	return t.result(stream)
}

const nonStream = `{"id":"x","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"SECRET answer text"}}],"usage":{"prompt_tokens":18,"completion_tokens":120,"total_tokens":138}}`

func sseBody(chunks int, withUsage bool) string {
	var sb strings.Builder
	sb.WriteString("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}],\"usage\":null}\n\n")
	for i := 0; i < chunks; i++ {
		fmt.Fprintf(&sb, "data: {\"choices\":[{\"delta\":{\"content\":\"tok%d\"}}],\"usage\":null}\n\n", i)
	}
	if withUsage {
		sb.WriteString("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":77,\"total_tokens\":86}}\n\n")
	}
	sb.WriteString("data: [DONE]\n\n")
	return sb.String()
}

func TestScannerReadsUsageFromANonStreamingBodyAtAnyChunking(t *testing.T) {
	for _, split := range []int{1, 3, 7, 64, 100000} {
		src, in, out := scanAll(false, nonStream, split)
		if src != TokensUsage || in != 18 || out != 120 {
			t.Errorf("split %d: %s %d %d", split, src, in, out)
		}
	}
}

func TestScannerReadsTheFinalUsageChunkOfAStream(t *testing.T) {
	for _, split := range []int{1, 5, 13, 200, 100000} {
		src, in, out := scanAll(true, sseBody(40, true), split)
		if src != TokensUsage || in != 9 || out != 77 {
			t.Errorf("split %d: %s %d %d", split, src, in, out)
		}
	}
}

func TestScannerCountsContentChunksWhenThereIsNoUsage(t *testing.T) {
	for _, split := range []int{1, 2, 11, 12, 13, 50, 100000} {
		src, in, out := scanAll(true, sseBody(40, false), split)
		if src != TokensChunks || in != 0 || out != 40 {
			t.Errorf("split %d: %s in=%d out=%d (want chunks, 40; the empty role chunk is not counted)", split, src, in, out)
		}
	}
	// a non-streaming body without usage reports nothing, whatever it contains
	if src, _, _ := scanAll(false, `{"choices":[{"message":{"content":"hi"}}]}`, 8); src != "" {
		t.Errorf("source = %q", src)
	}
	if src, _, _ := scanAll(true, "data: [DONE]\n\n", 4); src != "" {
		t.Errorf("source = %q", src)
	}
}

func TestScannerSurvivesGarbageAndHugeBodies(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 200; i++ {
		b := make([]byte, r.Intn(5000))
		r.Read(b)
		scanAll(r.Intn(2) == 0, string(b), 1+r.Intn(300))
	}
	for _, body := range []string{`"usage":{"prompt_tokens":99999999999999999999,"completion_tokens":1}`, `"usage":{"prompt_tokens":-3,"completion_tokens":1}`, `"usage":null`, `"usage"`, `"usage":{`} {
		if src, in, _ := scanAll(false, body, 5); src == TokensUsage && (in < 0 || in > 999999999999) {
			t.Errorf("%q gave absurd %d", body, in)
		}
	}
	var t0 tokenScanner
	big := strings.Repeat("a", 10<<20)
	t0.feed([]byte(big))
	if len(t0.tail) > tokenWindow || cap(t0.tail) > 2*tokenWindow {
		t.Fatalf("window grew to %d/%d", len(t0.tail), cap(t0.tail))
	}
}

func TestScannerDoesNotAllocatePerChunkOnTheHotPath(t *testing.T) {
	var s tokenScanner
	chunk := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
	s.feed(chunk)
	allocs := testing.AllocsPerRun(1000, func() { s.feed(chunk) })
	if allocs > 2 {
		t.Fatalf("%v allocations per chunk", allocs)
	}
}

func TestCompletionCarriesTheTokenCountsOfAUsageBody(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(nonStream))
	}), WithObserver(rec))
	_, body := postChat(t, c, url, plainBody)
	if body != nonStream {
		t.Fatal("the body must reach the client unchanged")
	}
	rec.await(t)
	if d := rec.done[0]; d.TokensSource != TokensUsage || d.InputTokens != 18 || d.OutputTokens != 120 {
		t.Fatalf("completion: %+v", d)
	}
}

func TestCompletionCarriesChunkCountsForAStreamAndNothingForAnError(t *testing.T) {
	rec := &recObserver{}
	url, c, _ := staticObsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(sseBody(12, false)))
	}), WithObserver(rec))
	_, body := postChat(t, c, url, streamBody)
	if body != sseBody(12, false) {
		t.Fatal("the stream must reach the client unchanged")
	}
	rec.await(t)
	if d := rec.done[0]; d.TokensSource != TokensChunks || d.OutputTokens != 12 || d.InputTokens != 0 {
		t.Fatalf("completion: %+v", d)
	}
	rec2 := &recObserver{}
	url2, c2, _ := staticObsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":2}}`))
	}), WithObserver(rec2))
	postChat(t, c2, url2, plainBody)
	rec2.await(t)
	if d := rec2.done[0]; d.TokensSource != "" {
		t.Fatalf("an error response must report no tokens: %+v", d)
	}
}

// BenchmarkTokenScanner is the cost of the token pass per streamed chunk and per non-streaming body (D8).
func BenchmarkTokenScannerStreamChunk(b *testing.B) {
	var s tokenScanner
	chunk := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}],\"usage\":null}\n\n")
	b.ReportAllocs()
	b.SetBytes(int64(len(chunk)))
	for i := 0; i < b.N; i++ {
		s.feed(chunk)
	}
}

func BenchmarkTokenScannerBody64KiB(b *testing.B) {
	var s tokenScanner
	body := []byte(strings.Repeat("x", 64<<10-len(nonStream)) + nonStream)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for i := 0; i < b.N; i++ {
		s.feed(body)
		s.result(false)
	}
}

func TestScannerCountsAreIndependentOfHowTheBodyIsCut(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	body := []byte(sseBody(60, false))
	_, _, want := func() (string, int64, int64) { return scanAll(true, string(body), len(body)) }()
	for i := 0; i < 500; i++ {
		var s tokenScanner
		for rest := body; len(rest) > 0; {
			n := min(1+r.Intn(40), len(rest))
			s.feed(rest[:n])
			rest = rest[n:]
		}
		if _, _, got := s.result(true); got != want || want != 60 {
			t.Fatalf("random cut %d: counted %d, want %d (60 chunks)", i, got, want)
		}
	}
}
