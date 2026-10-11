package chaos

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// loadAverage returns the machine's 1, 5 and 15 minute load averages as text, or "unknown". Benchmark
// numbers move with the load other processes put on a shared machine, so every run records it.
func loadAverage() string {
	switch runtime.GOOS {
	case "linux":
		if b, err := os.ReadFile("/proc/loadavg"); err == nil {
			if f := strings.Fields(string(b)); len(f) >= 3 {
				return strings.Join(f[:3], " ")
			}
		}
	case "darwin":
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sysctl", "-n", "vm.loadavg")
		cmd.WaitDelay = time.Second
		if out, err := cmd.Output(); err == nil {
			if s := strings.TrimSpace(strings.Trim(strings.TrimSpace(string(out)), "{}")); s != "" {
				return s
			}
		}
	}
	return "unknown"
}
