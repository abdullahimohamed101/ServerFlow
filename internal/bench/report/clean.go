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
func Clean(s string) string {
	const maxLen = 200
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
