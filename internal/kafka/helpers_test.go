package kafka

import (
	"encoding/base64"
	"net/url"
)

func fmtB64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
func urlEsc(s string) string { return url.QueryEscape(s) }
