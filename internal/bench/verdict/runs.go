package verdict

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"serverflow/internal/bench/report"
)

// maxRunDirs bounds how many run directories LoadRuns reads.
const maxRunDirs = 20000

// LoadRuns reads the result.json of every immediate subdirectory of dir that has one, in name order. A
// directory whose result cannot be read is reported in the second return value, not skipped silently, and
// the hypotheses that needed it will say a run is missing.
func LoadRuns(dir string) ([]report.Result, []string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	if len(ents) > maxRunDirs {
		return nil, nil, fmt.Errorf("%s has %d entries, more than the %d this tool reads", dir, len(ents), maxRunDirs)
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name() < ents[j].Name() })
	var runs []report.Result
	var problems []string
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		p := filepath.Join(dir, e.Name())
		if _, err := os.Lstat(filepath.Join(p, report.ResultFile)); err != nil {
			continue
		}
		r, err := report.Load(p)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", report.Clean(e.Name()), report.Clean(err.Error())))
			continue
		}
		runs = append(runs, r)
	}
	return runs, problems, nil
}
