package gwtrace

import "serverflow/internal/gateway"

// Every string that reaches a span is either fixed by this package, taken from a closed set (pick), a
// short token of safe characters (token, safeID), or a validated model name. Anything else is replaced by
// "other" or dropped, so an attacker-controlled value cannot enter a trace.

var (
	rejectKinds = set(gateway.RejectAuth, gateway.RejectRateLimit, gateway.RejectValidation, gateway.RejectModel,
		gateway.RejectCapacity, gateway.RejectInternal)
	rateLimits      = set("requests", "tokens", "concurrency", "model", "unavailable")
	selectOutcomes  = set("no_capacity", "model_not_found", "model_forbidden", "worker_unavailable", "internal")
	strategies      = set("random", "round-robin", "least-active", "least-queue", "least-work")
	attemptOutcomes = set(gateway.AttemptOK, gateway.AttemptFailed, gateway.AttemptRetried, gateway.AttemptClientClosed)
)

func set(vs ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(vs))
	for _, v := range vs {
		m[v] = struct{}{}
	}
	return m
}

// pick returns v if it is in the closed set, else "other".
func pick(v string, allowed map[string]struct{}) string {
	if _, ok := allowed[v]; ok {
		return v
	}
	return "other"
}

func tokenByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

// token returns v if it is 1..max lower-case letters, digits, '_' or '-', else "other" (empty stays empty).
func token(v string, max int) string {
	if v == "" {
		return ""
	}
	if len(v) > max {
		return "other"
	}
	for i := 0; i < len(v); i++ {
		if !tokenByte(v[i]) {
			return "other"
		}
	}
	return v
}

// safeID returns v if it looks like an ID (letters, digits, '_', '-', '.', ':' up to 64 bytes), else "invalid".
func safeID(v string) string {
	if len(v) == 0 || len(v) > 64 {
		return "invalid"
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' || c == ':') {
			return "invalid"
		}
	}
	return v
}

// validModel returns a confirmed model name that is safe to record, else "". Model names are operator
// chosen (up to 128 printable characters without spaces).
func validModel(v string) string {
	if v == "" || len(v) > 128 {
		return ""
	}
	for i := 0; i < len(v); i++ {
		if v[i] <= 0x20 || v[i] >= 0x7f {
			return ""
		}
	}
	return v
}
