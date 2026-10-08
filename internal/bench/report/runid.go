package report

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var runIDPattern = regexp.MustCompile(`^run_(\d{3,9})$`)

// lastFile remembers the highest ID ever issued in a directory, so deleting the newest
// run does not let its ID be handed out again.
const lastFile = ".last_run"

// FormatRunID formats n as a run ID: run_001, run_002, ... run_1000.
func FormatRunID(n int) string { return fmt.Sprintf("run_%03d", n) }

// ParseRunID returns the number in a run ID.
func ParseRunID(id string) (int, bool) {
	m := runIDPattern.FindStringSubmatch(id)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	return n, err == nil
}

// CreateRun makes the next run directory under base and returns its ID and path. IDs are
// sequential per directory and never reused. It creates base when needed. Directory
// creation is atomic, so two harnesses sharing a directory get different IDs.
func CreateRun(base string) (id, dir string, err error) {
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", "", err
	}
	for range 100 {
		n, err := highest(base)
		if err != nil {
			return "", "", err
		}
		id = FormatRunID(n + 1)
		dir = filepath.Join(base, id)
		err = os.Mkdir(dir, 0o755)
		if err == nil {
			_ = os.WriteFile(filepath.Join(base, lastFile), []byte(strconv.Itoa(n+1)+"\n"), 0o644)
			return id, dir, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", "", err
		}
	}
	return "", "", fmt.Errorf("could not allocate a run ID in %s", base)
}

// highest is the largest run number that exists or was ever issued in base.
func highest(base string) (int, error) {
	entries, err := os.ReadDir(base)
	if err != nil {
		return 0, err
	}
	top := 0
	for _, e := range entries {
		if n, ok := ParseRunID(e.Name()); ok && e.IsDir() && n > top {
			top = n
		}
	}
	if b, err := os.ReadFile(filepath.Join(base, lastFile)); err == nil {
		if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n > top {
			top = n
		}
	}
	return top, nil
}

// RunIDs lists the run IDs under base in ascending order. A missing base has none.
func RunIDs(base string) ([]string, error) {
	entries, err := os.ReadDir(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if _, ok := ParseRunID(e.Name()); ok && e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, _ := ParseRunID(ids[i])
		b, _ := ParseRunID(ids[j])
		return a < b
	})
	return ids, nil
}
