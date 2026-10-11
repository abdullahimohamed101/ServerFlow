// Package version holds the build stamp of a binary. The container build sets both variables with
// -ldflags "-X serverflow/internal/version.Version=... -X serverflow/internal/version.Commit=..." because
// -buildvcs=false (reproducible builds, no .git in the build context) removes the VCS data that
// runtime/debug.ReadBuildInfo would otherwise provide. Unset (empty), callers fall back to the Go build
// information, so a plain `go build` behaves as before.
package version

// Version and Commit are set by the linker; they are plain strings so -X can write them.
var (
	Version = ""
	Commit  = ""
)
