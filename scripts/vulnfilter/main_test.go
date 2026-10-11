package main

import (
	"bytes"
	"strings"
	"testing"
)

// line builds one line of govulncheck -format json output.
func line(osv, module, function string) string {
	fn := ""
	if function != "" {
		fn = `,"package":"p","function":"` + function + `"`
	}
	return `{"finding":{"osv":"` + osv + `","fixed_version":"v9.9.9","trace":[{"module":"` + module + `","version":"v1.0.0"` + fn + `}]}}` + "\n"
}

const preamble = `{"config":{"protocol_version":"v1.0.0"}}` + "\n" + `{"progress":{"message":"Scanning"}}` + "\n" + `{"osv":{"id":"GO-2026-6617"}}` + "\n"

var xnet = []exception{{"golang.org/x/net", "GO-2026-6617"}, {"golang.org/x/net", "GO-2026-6612"}}

func runOn(input string, allow []exception) (code int, out, errOut string) {
	var o, e bytes.Buffer
	code = run(strings.NewReader(input), &o, &e, allow)
	return code, o.String(), e.String()
}

func TestOnlyExceptedFindingsPassWithAVisibleNotice(t *testing.T) {
	in := preamble + line("GO-2026-6617", "golang.org/x/net", "Framer.ReadFrame") + line("GO-2026-6612", "golang.org/x/net", "Transport.RoundTrip")
	code, out, errOut := runOn(in, xnet)
	if code != 0 || errOut != "" {
		t.Fatalf("code %d, stderr %q", code, errOut)
	}
	for _, want := range []string{"NOTICE", "GO-2026-6617 in golang.org/x/net", "GO-2026-6612 in golang.org/x/net", "excepted finding"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestAnExceptedIDInAnotherModuleFails(t *testing.T) {
	in := preamble + line("GO-2026-6617", "golang.org/x/net", "F") + line("GO-2026-6617", "example.com/other", "G")
	code, _, errOut := runOn(in, xnet)
	if code != 1 || !strings.Contains(errOut, "example.com/other") || strings.Contains(errOut, "golang.org/x/net@") {
		t.Fatalf("code %d, stderr %q: the exception must not cover another module", code, errOut)
	}
}

func TestAnUnrelatedNewIDFails(t *testing.T) {
	in := preamble + line("GO-2026-6617", "golang.org/x/net", "F") + line("GO-2026-9999", "golang.org/x/net", "F")
	code, _, errOut := runOn(in, xnet)
	if code != 1 || !strings.Contains(errOut, "GO-2026-9999") {
		t.Fatalf("code %d, stderr %q: a new advisory in the same module must fail", code, errOut)
	}
}

func TestStdlibFindingsAreNotSilenced(t *testing.T) {
	code, _, errOut := runOn(preamble+line("GO-2026-6613", "stdlib", "Server.Serve"), xnet)
	if code != 1 || !strings.Contains(errOut, "stdlib") {
		t.Fatalf("code %d, stderr %q", code, errOut)
	}
}

func TestEmptyFindingsPass(t *testing.T) {
	for name, in := range map[string]string{"nothing at all": "", "only metadata": preamble} {
		code, out, errOut := runOn(in, xnet)
		if code != 0 || errOut != "" {
			t.Errorf("%s: code %d stderr %q", name, code, errOut)
		}
		if !strings.Contains(out, "NOTICE") || !strings.Contains(out, "matched nothing") {
			t.Errorf("%s: the exception list must still be printed, and unused exceptions flagged:\n%s", name, out)
		}
	}
}

func TestImportedButNotCalledFindingsDoNotFail(t *testing.T) {
	// A module-level finding has no function in its trace: the code imports the module but never calls the symbol.
	code, _, errOut := runOn(preamble+line("GO-2026-1111", "example.com/lib", ""), nil)
	if code != 0 || errOut != "" {
		t.Fatalf("code %d, stderr %q", code, errOut)
	}
}

func TestNoExceptionsMeansEveryCalledFindingFails(t *testing.T) {
	code, _, _ := runOn(preamble+line("GO-2026-6617", "golang.org/x/net", "F"), nil)
	if code != 1 {
		t.Fatalf("code %d", code)
	}
}

func TestGarbageInputFailsLoudly(t *testing.T) {
	code, _, errOut := runOn("this is not json", xnet)
	if code != 2 || !strings.Contains(errOut, "not valid JSON") {
		t.Fatalf("code %d, stderr %q", code, errOut)
	}
}

func TestAllowFlagSyntax(t *testing.T) {
	var a allowList
	if err := a.Set("golang.org/x/net:GO-2026-6617"); err != nil || a[0] != (exception{"golang.org/x/net", "GO-2026-6617"}) {
		t.Fatalf("%v %v", a, err)
	}
	for _, bad := range []string{"GO-2026-6617", "golang.org/x/net:", ":GO-2026-1", "golang.org/x/net:6617"} {
		if err := (&allowList{}).Set(bad); err == nil {
			t.Errorf("%q must be refused", bad)
		}
	}
}
