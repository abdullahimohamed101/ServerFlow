package config

import (
	"fmt"
	"io"
)

// writeRedacted is the body of the fmt.Formatter of every config type that holds a secret. fmt consults String and
// GoString only for some verbs (%d, %t and others print the raw fields), so a Formatter is the only way to make every
// verb print the redacted form. %#v keeps its GoString shape; every other verb prints String.
func writeRedacted(f fmt.State, verb rune, str, goStr string) {
	if verb == 'v' && f.Flag('#') {
		_, _ = io.WriteString(f, goStr)
		return
	}
	_, _ = io.WriteString(f, str)
}
