package telemetry

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLoggerLevels(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		logger, err := NewLogger(&buf, tt.level)
		if err != nil {
			t.Fatalf("NewLogger(%q) error: %v", tt.level, err)
		}
		if logger == nil {
			t.Fatalf("NewLogger(%q) returned nil logger", tt.level)
		}
		_ = logger
	}
}

func TestNewLoggerRejectsUnknownLevel(t *testing.T) {
	_, err := NewLogger(io.Discard, "verbose")
	if err == nil {
		t.Fatal("expected error for unknown level, got nil")
	}
}

func TestLoggerEmitsJSON(t *testing.T) {
	var buf bytes.Buffer
	logger, err := NewLogger(&buf, "info")
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("gateway started", "component", "gateway", "port", 8080)

	line := strings.TrimSpace(buf.String())
	if !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
		t.Fatalf("expected JSON object, got %q", line)
	}
	if !strings.Contains(line, "\"msg\":\"gateway started\"") {
		t.Fatalf("expected msg field, got %q", line)
	}
	if !strings.Contains(line, "\"component\":\"gateway\"") {
		t.Fatalf("expected component field, got %q", line)
	}
}
