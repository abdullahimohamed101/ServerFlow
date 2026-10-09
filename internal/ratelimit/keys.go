package ratelimit

import (
	"fmt"
	"strings"
)

// Redis key layout (cluster-friendly for the tenant keys, which share the hash tag {tenant}):
//
//	rl:{<tenant>}:req    request bucket
//	rl:{<tenant>}:tok    token bucket
//	rl:{<tenant>}:conc   concurrency leases (sorted set of lease id by expiry)
//	rl:model:{<model>}   per-model request bucket
//
// Tenant and model names are escaped (escapeKeyPart) so a name can neither close the hash tag early
// nor spell another key. The tenant keys of different tenants therefore never collide, and no tenant
// key can equal a model key. The combined script touches the tenant keys and a model key in one call,
// which Redis Cluster refuses across slots; Cluster is not supported (ADR-015).

// maxKeyPart bounds a tenant or model name used in a key.
const maxKeyPart = 256

func keyRequests(tenant string) string    { return "rl:{" + escapeKeyPart(tenant) + "}:req" }
func keyTokens(tenant string) string      { return "rl:{" + escapeKeyPart(tenant) + "}:tok" }
func keyConcurrency(tenant string) string { return "rl:{" + escapeKeyPart(tenant) + "}:conc" }
func keyModel(model string) string        { return "rl:model:{" + escapeKeyPart(model) + "}" }

// escapeKeyPart percent-encodes the characters that have meaning in a key ('{', '}', '%') and anything
// outside printable ASCII, so distinct names give distinct keys with no braces inside.
func escapeKeyPart(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x21 || c > 0x7e || c == '{' || c == '}' || c == '%' {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// validKeyPart reports whether a name is short enough to be used in a key.
func validKeyPart(s string) bool { return len(s) <= maxKeyPart }
