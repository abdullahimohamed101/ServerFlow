// Package telemetry provides structured JSON logging for all ServerFlow
// components. It wraps the standard library log/slog so every binary
// produces machine-readable logs with consistent request context fields.
package telemetry

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger returns a JSON slog logger writing to w at the given
// level. The level string accepts "debug", "info", "warn", or "error".
func NewLogger(w io.Writer, level string) (*slog.Logger, error) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	case "info", "":
		l = slog.LevelInfo
	default:
		return nil, fmt.Errorf("unsupported log level %q", level)
	}

	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l})
	return slog.New(handler), nil
}

// NewDefaultLogger returns a JSON logger at the given level writing to
// standard error. It is a convenience for main functions.
func NewDefaultLogger(level string) (*slog.Logger, error) {
	return NewLogger(os.Stderr, level)
}
