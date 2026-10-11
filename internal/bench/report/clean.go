package report

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Clean makes a string from a result file safe to print on a terminal or in a report: invalid
// UTF-8, control characters (escape sequences, carriage returns, newlines), and invisible
// formatting characters (bidirectional overrides and the like) become '?', and the result is
// bounded in length. Result files are data from disk, which may not have been written by this
// harness.
func Clean(s string) string { return CleanN(s, 200) }

// CleanN is Clean with a longer bound, for the paragraphs of a report that are written by the harness itself
// (a scenario's description) rather than being short identifiers.
func CleanN(s string, maxLen int) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "?")
	}
	var b strings.Builder
	n := 0
	for _, r := range s {
		if n >= maxLen {
			b.WriteString("...")
			break
		}
		switch {
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == ' ' || r == ' ':
			b.WriteByte('?')
		default:
			b.WriteRune(r)
		}
		n++
	}
	return b.String()
}
