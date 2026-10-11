// Command vulnfilter reads the output of `govulncheck -format json` and decides whether it is a failure.
// A finding fails the check when the code calls a vulnerable symbol (the same rule as govulncheck's text
// output, which only reports called vulnerabilities as "affected") unless it is in the exception list. An
// exception names BOTH the OSV id and the module, so it cannot hide the same advisory in another module or
// another advisory in the same module. Every run prints the exception list, so an exception is never silent.
//
//	govulncheck -format json ./... | go run ./scripts/vulnfilter -allow golang.org/x/net:GO-2026-6617
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type frame struct {
	Module   string `json:"module"`
	Version  string `json:"version"`
	Package  string `json:"package"`
	Function string `json:"function"`
}

type finding struct {
	OSV          string  `json:"osv"`
	FixedVersion string  `json:"fixed_version"`
	Trace        []frame `json:"trace"`
}

type message struct {
	Finding *finding `json:"finding"`
}

// exception is one accepted advisory in one module.
type exception struct{ Module, OSV string }

func (e exception) String() string { return e.OSV + " in " + e.Module }

// result is the verdict on one govulncheck run.
type result struct {
	Failures []string // called findings that are not excepted
	Excepted []string // exceptions that matched a called finding
	Unused   []string // exceptions that matched nothing in this run
}

// evaluate parses the JSON stream and applies the exceptions.
func evaluate(r io.Reader, allow []exception) (result, error) {
	type key struct{ module, osv string }
	called := map[key]finding{}
	dec := json.NewDecoder(r)
	for {
		var m message
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return result{}, fmt.Errorf("govulncheck output is not valid JSON: %w", err)
		}
		f := m.Finding
		if f == nil || len(f.Trace) == 0 || f.Trace[0].Function == "" {
			continue // an advisory in a module or package the code imports but does not call
		}
		k := key{f.Trace[0].Module, f.OSV}
		if _, ok := called[k]; !ok {
			called[k] = *f
		}
	}
	allowed := map[exception]bool{}
	for _, a := range allow {
		allowed[a] = true
	}
	var res result
	hit := map[exception]bool{}
	keys := make([]key, 0, len(called))
	for k := range called {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].module+keys[i].osv < keys[j].module+keys[j].osv })
	for _, k := range keys {
		f := called[k]
		desc := fmt.Sprintf("%s in %s@%s (fixed in %s), e.g. %s.%s", k.osv, k.module, f.Trace[0].Version, orNone(f.FixedVersion), f.Trace[0].Package, f.Trace[0].Function)
		if e := (exception{k.module, k.osv}); allowed[e] {
			hit[e] = true
			res.Excepted = append(res.Excepted, desc)
			continue
		}
		res.Failures = append(res.Failures, desc)
	}
	for _, a := range allow {
		if !hit[a] {
			res.Unused = append(res.Unused, a.String())
		}
	}
	return res, nil
}

func orNone(s string) string {
	if s == "" {
		return "no fixed version"
	}
	return s
}

type allowList []exception

func (a *allowList) String() string { return fmt.Sprint(len(*a)) }
func (a *allowList) Set(v string) error {
	i := strings.LastIndex(v, ":")
	if i <= 0 || i == len(v)-1 || !strings.HasPrefix(v[i+1:], "GO-") {
		return fmt.Errorf("an exception is module:OSV-id, for example golang.org/x/net:GO-2026-6617, got %q", v)
	}
	*a = append(*a, exception{Module: v[:i], OSV: v[i+1:]})
	return nil
}

func main() {
	var allow allowList
	flag.Var(&allow, "allow", "an accepted advisory as module:OSV-id (repeatable)")
	flag.Parse()
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr, allow))
}

func run(in io.Reader, out, errw io.Writer, allow []exception) int {
	if len(allow) > 0 {
		_, _ = fmt.Fprintln(out, "vuln: NOTICE: these advisories are accepted by an exception list (ADR-018); they are not fixed:")
		for _, a := range allow {
			_, _ = fmt.Fprintf(out, "vuln:   %s\n", a)
		}
	}
	res, err := evaluate(in, allow)
	if err != nil {
		_, _ = fmt.Fprintf(errw, "vuln: %v\n", err)
		return 2
	}
	for _, e := range res.Excepted {
		_, _ = fmt.Fprintf(out, "vuln: excepted finding: %s\n", e)
	}
	for _, u := range res.Unused {
		_, _ = fmt.Fprintf(out, "vuln: NOTICE: exception %s matched nothing in this run; if the dependency was fixed, delete it from scripts/quality.sh\n", u)
	}
	if len(res.Failures) > 0 {
		for _, f := range res.Failures {
			_, _ = fmt.Fprintf(errw, "vuln: FAIL: called vulnerability not in the exception list: %s\n", f)
		}
		return 1
	}
	_, _ = fmt.Fprintf(out, "vuln: no called vulnerabilities outside the exception list (%d excepted)\n", len(res.Excepted))
	return 0
}
