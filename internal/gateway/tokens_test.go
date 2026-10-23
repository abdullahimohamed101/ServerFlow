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

// Hostile or odd usage objects are never "repaired": the request falls back to estimate instead.
func TestScannerRefusesAmbiguousOrMangledUsage(t *testing.T) {
	usage := func(inner string) string { return `{"choices":[],"usage":{` + inner + `}}` }
	bad := map[string]string{
		"13 digits":          usage(`"prompt_tokens":1234567890123,"completion_tokens":5`),
		"just over the cap":  usage(`"prompt_tokens":10000000001,"completion_tokens":5`),
		"fraction":           usage(`"prompt_tokens":5.5,"completion_tokens":5`),
		"exponent":           usage(`"prompt_tokens":7.9e3,"completion_tokens":5`),
		"capital exponent":   usage(`"prompt_tokens":2E3,"completion_tokens":5`),
		"negative":           usage(`"prompt_tokens":-3,"completion_tokens":5`),
		"plus sign":          usage(`"prompt_tokens":+3,"completion_tokens":5`),
		"leading zero":       usage(`"prompt_tokens":007,"completion_tokens":5`),
		"string value":       usage(`"prompt_tokens":"5","completion_tokens":5`),
		"null value":         usage(`"prompt_tokens":null,"completion_tokens":5`),
		"duplicate prompt":   usage(`"prompt_tokens":5,"prompt_tokens":6,"completion_tokens":5`),
		"duplicate complete": usage(`"prompt_tokens":5,"completion_tokens":5,"completion_tokens":9`),
		"missing completion": usage(`"prompt_tokens":5`),
		"two usage objects":  `{"usage":{"prompt_tokens":1,"completion_tokens":2},"usage":{"prompt_tokens":3,"completion_tokens":4}}`,
		"unterminated":       `{"usage":{"prompt_tokens":1,"completion_tokens":2`,
		"number at the end":  `{"usage":{"prompt_tokens":1,"completion_tokens":2`,
	}
	for name, body := range bad {
		for _, split := range []int{1, 9, 1 << 20} {
			if src, in, out := scanAll(false, body, split); src != "" {
				t.Errorf("%s (split %d): accepted as %s %d/%d", name, split, src, in, out)
			}
		}
	}
	good := map[string]string{
		"plain":              usage(`"prompt_tokens":18,"completion_tokens":120,"total_tokens":138`),
		"spaces and newline": usage("\"prompt_tokens\" : 18 ,\n\"completion_tokens\":\t120"),
		"at the cap":         usage(`"prompt_tokens":10000000000,"completion_tokens":0`),
		"nested details":     usage(`"prompt_tokens":18,"completion_tokens":120,"completion_tokens_details":{"reasoning_tokens":4},"prompt_tokens_details":{"cached_tokens":2}`),
	}
	for name, body := range good {
		if src, _, _ := scanAll(false, body, 7); src != TokensUsage {
			t.Errorf("%s: not accepted", name)
		}
	}
	// A hostile usage chunk in a stream falls back to the chunk count, not to a number.
	hostile := sseBody(10, false) + "data: {\"usage\":{\"prompt_tokens\":1.5,\"completion_tokens\":2}}\n\n"
	if src, _, out := scanAll(true, hostile, 13); src != TokensChunks || out != 10 {
		t.Errorf("stream with a hostile usage chunk: %s %d", src, out)
	}
}

// The scanner must not change a byte of what the client receives, whatever the worker sends.
func TestHostileUsageBodiesReachTheClientByteIdentical(t *testing.T) {
	for _, body := range []string{
		`{"usage":{"prompt_tokens":1234567890123,"completion_tokens":5.5}}`,
		`{"usage":{"prompt_tokens":1,"prompt_tokens":2,"completion_tokens":7.9e3}}`,
		sseBody(5, false) + "data: {\"usage\":{\"prompt_tokens\":-1}}\n\n",
	} {
		stream := strings.HasPrefix(body, "data:")
		rec := &recObserver{}
		url, c, _ := staticObsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if stream {
				w.Header().Set("Content-Type", "text/event-stream")
			} else {
				w.Header().Set("Content-Type", "application/json")
			}
			_, _ = w.Write([]byte(body))
		}), WithObserver(rec))
		req := plainBody
		if stream {
			req = streamBody
		}
		_, got := postChat(t, c, url, req)
		if got != body {
			t.Fatalf("the client received different bytes:\n got %q\nwant %q", got, body)
		}
		rec.await(t)
		if d := rec.done[0]; d.TokensSource == TokensUsage {
			t.Fatalf("hostile usage accepted: %+v", d)
		}
	}
}

// JSON string escapes: the scanner works on raw bytes, so a quote, a backslash or a \u escape inside a string value
// must neither hide a real key nor forge one. Every case is fed whole, in 1-byte pieces and in odd-sized pieces, because
// a key (or an escape) can straddle two reads.
func TestScannerAndEscapedQuotesBackslashesAndUnicodeEscapes(t *testing.T) {
	delta := func(content string) string {
		return `data: {"choices":[{"delta":{"content":` + content + `}}]}` + "\n\n"
	}
	chunks := []struct {
		name string
		body string
		want int64 // content chunks counted
	}{
		{"plain", delta(`"hi"`), 1},
		{"empty content is not a chunk", delta(`""`), 0},
		{"null content is not a chunk", delta(`null`), 0},
		{"content that is a single escaped quote", delta(`"\""`), 1},
		{"escaped quote first", delta(`"\"hi\""`), 1},
		{"escaped quote last", delta(`"hi\""`), 1},
		{"escaped backslash last", delta(`"x\\"`), 1},
		{"only an escaped backslash", delta(`"\\"`), 1},
		{"unicode escape", delta(`"\u0041"`), 1},
		{"unicode escape of a quote", delta(`"\u0022"`), 1},
		{"unicode escape of a backslash", delta(`"\u005c"`), 1},
		{"surrogate pair", delta(`"\ud83d\ude00"`), 1},
		{"newline and tab escapes", delta(`"\n\t"`), 1},
		{"text that looks like a content key, escaped", delta(`"say \"content\":\"x\""`), 1},
		{"text that looks like a content key, behind an escaped backslash", delta(`"\\\"content\":\"z"`), 1},
		{"text that looks like a usage object, escaped", delta(`"\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}"`), 1},
		{"key name that merely ends in content", `data: {"choices":[{"delta":{"reasoning_content":"thinking"}}]}` + "\n\n", 0},
		{"two chunks, the second empty", delta(`"a"`) + delta(`""`), 1},
		{"two chunks after an escaped quote", delta(`"\""`) + delta(`"b"`), 2},
		{"a key spelled with a unicode escape is not seen (an undercount, never an overcount)", `data: {"choices":[{"delta":{"cont\u0065nt":"x"}}]}` + "\n\n", 0},
	}
	for _, c := range chunks {
		for _, split := range []int{1, 2, 3, 5, 7, 11, len(c.body)} {
			src, in, out := scanAll(true, c.body, split)
			wantSrc := ""
			if c.want > 0 {
				wantSrc = TokensChunks
			}
			if src != wantSrc || out != c.want || in != 0 {
				t.Errorf("%s (split %d): got %q %d/%d, want %q 0/%d", c.name, split, src, in, out, wantSrc, c.want)
			}
		}
	}

	const ok = `"prompt_tokens":5,"completion_tokens":7`
	usages := []struct {
		name string
		body string
		src  string // TokensUsage or ""
	}{
		{"a closing brace inside an escaped-quote string", `{"usage":{` + ok + `,"note":"a \"} b"}}`, TokensUsage},
		{"a string ending in an escaped backslash before the brace", `{"usage":{` + ok + `,"note":"x\\"}}`, TokensUsage},
		{"a string of escaped backslashes and quotes", `{"usage":{"note":"\\\"\\","prompt_tokens":5,"completion_tokens":7}}`, TokensUsage},
		{"unicode escapes in a sibling string", `{"usage":{` + ok + `,"note":"\u007d\u0022"}}`, TokensUsage},
		{"a quoted key name inside a string is not a second key", `{"usage":{` + ok + `,"note":"\"completion_tokens\":99"}}`, TokensUsage},
		{"a quoted usage object inside content is not a second usage object", `{"choices":[{"message":{"content":"\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}"}}],"usage":{` + ok + `}}`, TokensUsage},
		{"a unicode-escaped usage object inside content is not a usage object", `{"choices":[{"message":{"content":"\u0022usage\u0022:{\u0022prompt_tokens\u0022:1,\u0022completion_tokens\u0022:2}"}}]}`, ""},
		{"injected usage text alone is never believed", `{"choices":[{"message":{"content":"\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}"}}]}`, ""},
		{"a key spelled with a unicode escape is not seen", `{"usage":{"prompt_tokens":5,"completion_tok\u0065ns":7}}`, ""},
		{"a second raw key next to an escaped one is a duplicate", `{"usage":{` + ok + `,"completion_tokens":9}}`, ""},
		{"an unterminated string swallows the brace", `{"usage":{` + ok + `,"note":"open}}`, ""},
		{"an escaped quote at the very end leaves the string open", `{"usage":{` + ok + `,"note":"x\"}}`, ""},
	}
	for _, c := range usages {
		for _, split := range []int{1, 2, 3, 5, 7, 11, len(c.body)} {
			src, in, out := scanAll(false, c.body, split)
			if src != c.src {
				t.Errorf("%s (split %d): source %q, want %q (%d/%d)", c.name, split, src, c.src, in, out)
				continue
			}
			if c.src == TokensUsage && (in != 5 || out != 7) {
				t.Errorf("%s (split %d): counts %d/%d, want 5/7", c.name, split, in, out)
			}
		}
	}
}

// The count boundaries: the largest allowed value is accepted, one more is not, and what looks like a number but is
// not a plain integer is refused, wherever the pieces are cut.
func TestScannerCountBoundaries(t *testing.T) {
	usage := func(p, c string) string { return `{"usage":{"prompt_tokens":` + p + `,"completion_tokens":` + c + `}}` }
	cases := []struct {
		name         string
		p, c         string
		ok           bool
		wantP, wantC int64
	}{
		{"zeros", "0", "0", true, 0, 0},
		{"ones", "1", "1", true, 1, 1},
		{"the maximum", "10000000000", "10000000000", true, 10_000_000_000, 10_000_000_000},
		{"maximum minus one", "9999999999", "9999999999", true, 9_999_999_999, 9_999_999_999},
		{"one over the maximum", "10000000001", "1", false, 0, 0},
		{"one over, in the completion", "1", "10000000001", false, 0, 0},
		{"eleven nines", "99999999999", "1", false, 0, 0},
		{"twelve digits", "100000000000", "1", false, 0, 0},
		{"a very long number", strings.Repeat("9", 400), "1", false, 0, 0},
		{"leading zero", "01", "1", false, 0, 0},
		{"two zeros", "00", "1", false, 0, 0},
		{"minus zero", "-0", "1", false, 0, 0},
		{"a quoted number", `"5"`, "1", false, 0, 0},
		{"an escaped-digit string", `"5"`, "1", false, 0, 0},
		{"a number followed by a letter", "5x", "1", false, 0, 0},
		{"a number followed by a unicode escape", `5\u0030`, "1", false, 0, 0},
		{"a number ending the body", "1", "5", false, 0, 0}, // replaced below: no closing brace
	}
	for _, c := range cases {
		body := usage(c.p, c.c)
		if c.name == "a number ending the body" {
			body = `{"usage":{"prompt_tokens":1,"completion_tokens":5`
		}
		for _, split := range []int{1, 4, 1 << 20} {
			src, in, out := scanAll(false, body, split)
			if c.ok {
				if src != TokensUsage || in != c.wantP || out != c.wantC {
					t.Errorf("%s (split %d): %q %d/%d, want usage %d/%d", c.name, split, src, in, out, c.wantP, c.wantC)
				}
			} else if src != "" {
				t.Errorf("%s (split %d): accepted as %q %d/%d", c.name, split, src, in, out)
			}
		}
	}
}

// The scanner keeps a 2048-byte window. A usage object that fits in it is read wherever it falls in a long body; one
// that does not fit is never guessed at.
func TestScannerWindowBoundary(t *testing.T) {
	usage := func(pad int) string {
		return `{"usage":{"note":"` + strings.Repeat("n", pad) + `","prompt_tokens":5,"completion_tokens":7}}`
	}
	long := strings.Repeat(`{"choices":[{"message":{"content":"padding padding"}}]}`, 200)
	for _, c := range []struct {
		name string
		body string
		src  string
	}{
		{"small object after a long body", long + usage(0), TokensUsage},
		{"object filling most of the window", long + usage(tokenWindow-200), TokensUsage},
		{"object exactly the window", long + usage(tokenWindow-len(usage(0))), TokensUsage},
		// The outer brace is not needed: reading starts at the "usage" key, which is the second byte of the body below.
		{"object one byte over the window (only the outer brace is lost)", long + usage(tokenWindow-len(usage(0))+1), TokensUsage},
		{"object two bytes over the window (the usage key is cut)", long + usage(tokenWindow-len(usage(0))+2), ""},
		{"object far over the window", long + usage(3*tokenWindow), ""},
	} {
		for _, split := range []int{1, 100, 2047, 2048, 2049, 1 << 20} {
			if src, in, out := scanAll(false, c.body, split); src != c.src || (c.src != "" && (in != 5 || out != 7)) {
				t.Errorf("%s (split %d): %q %d/%d, want %q", c.name, split, src, in, out, c.src)
			}
		}
	}
}
