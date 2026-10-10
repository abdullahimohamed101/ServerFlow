package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"runtime/debug"
)

// BuildVersion is the main module version from the build info, or "dev".
func BuildVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

// NewInstanceID returns a random identifier for one process (service.instance.id).
func NewInstanceID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
