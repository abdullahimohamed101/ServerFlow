package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"runtime/debug"

	"serverflow/internal/version"
)

// BuildVersion is the linker-stamped version (internal/version), else the main module version from the build
// info, else "dev".
func BuildVersion() string {
	if version.Version != "" {
		return version.Version
	}
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
