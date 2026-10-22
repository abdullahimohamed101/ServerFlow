package engine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestBuiltInProfilesAreValidAndDistinct(t *testing.T) {
	seen := map[string]string{}
	for _, name := range ProfileNames() {
		p, ok := Profile(name)
		if !ok || p.Name != name {
			t.Fatalf("profile %q: ok=%v name=%q", name, ok, p.Name)
		}
		if err := p.Validate(); err != nil {
			t.Errorf("profile %q: %v", name, err)
		}
		if !strings.HasPrefix(p.Provenance, "assumed") {
			t.Errorf("profile %q claims provenance %q; only measured data may claim more", name, p.Provenance)
		}
		h := p.Hash()
		if other, dup := seen[h]; dup {
			t.Errorf("profiles %q and %q hash alike", name, other)
		}
		seen[h] = name
	}
	if _, ok := Profile("nope"); ok {
		t.Error("unknown profile found")
	}
}

// Pinning the default profile's hash makes a silent change of the physics visible in review: results
// quote this hash.
func TestDefaultProfileHashIsPinned(t *testing.T) {
	p, _ := Profile("default")
	const want = "e7d211cd6bbb0eba991a71250b7eee231871cc3e732a40e0bdcdef49e1c8afec"
	if got := p.Hash(); got != want {
		t.Errorf("default profile hash changed to %s", got)
	}
}

func TestParamsRoundTripAndHash(t *testing.T) {
	p, _ := Profile("noisy")
	b, err := p.MarshalFile()
	if err != nil {
		t.Fatal(err)
	}
	q, err := ParseParams(strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, q) || p.Hash() != q.Hash() {
		t.Errorf("round trip changed the parameters: %+v vs %+v", p, q)
	}
	// Whitespace and key order in the file do not matter to the hash.
	var compact bytes.Buffer
	if err := json.Compact(&compact, b); err != nil {
		t.Fatal(err)
	}
	r, err := ParseParams(&compact)
	if err != nil || r.Hash() != p.Hash() {
		t.Errorf("reformatted file: %v hash %s vs %s", err, r.Hash(), p.Hash())
	}
}

func TestEveryFieldChangesTheHash(t *testing.T) {
	base, _ := Profile("default")
	v := reflect.ValueOf(&base).Elem()
	for i := range v.NumField() {
		q := base
		f := reflect.ValueOf(&q).Elem().Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(f.String() + "x")
		case reflect.Float64:
			f.SetFloat(f.Float() + 1)
		case reflect.Int, reflect.Int64:
			f.SetInt(f.Int() + 1)
		default:
			t.Fatalf("unhandled kind %v", f.Kind())
		}
		if q.Hash() == base.Hash() {
			t.Errorf("changing %s did not change the hash", v.Type().Field(i).Name)
		}
	}
}

func TestParseParamsRejectsBadFiles(t *testing.T) {
	good, _ := Profile("default")
	b, _ := good.MarshalFile()
	ok := string(b)
	cases := map[string]string{
		"unknown field":    strings.Replace(ok, `"name"`, `"nmae"`, 1),
		"wrong version":    strings.Replace(ok, `"version": 1`, `"version": 2`, 1),
		"no version":       strings.Replace(ok, `"version": 1,`, ``, 1),
		"trailing":         ok + `{}`,
		"not json":         `step_base_ms=1`,
		"empty":            ``,
		"zero step":        strings.NewReplacer(`"step_base_ms": 20`, `"step_base_ms": 0`, `"step_per_seq_ms": 1`, `"step_per_seq_ms": 0`).Replace(ok),
		"negative":         strings.Replace(ok, `"step_base_ms": 20`, `"step_base_ms": -1`, 1),
		"huge kv":          strings.Replace(ok, `"kv_budget_tokens": 32768`, `"kv_budget_tokens": 99999999999`, 1),
		"cv too big":       strings.Replace(ok, `"step_noise_cv": 0`, `"step_noise_cv": 5`, 1),
		"fraction too big": strings.Replace(ok, `"output_fraction": 0`, `"output_fraction": 1.5`, 1),
		"too large":        `{"version":1,"name":"` + strings.Repeat("a", 70000) + `"}`,
	}
	for name, in := range cases {
		if _, err := ParseParams(strings.NewReader(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := ParseParams(strings.NewReader(ok)); err != nil {
		t.Errorf("the good file was rejected: %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	good := Config{Params: quiet(), MaxSeqs: 4, MaxQueue: 0, OutputMeanTokens: 1}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*Config){
		"no seqs":       func(c *Config) { c.MaxSeqs = 0 },
		"too many seqs": func(c *Config) { c.MaxSeqs = 1 << 20 },
		"neg queue":     func(c *Config) { c.MaxQueue = -1 },
		"zero output":   func(c *Config) { c.OutputMeanTokens = 0 },
		"bad params":    func(c *Config) { c.Params.KVBudgetTokens = 0 },
	} {
		c := good
		mut(&c)
		if c.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
		if _, err := New(c); err == nil {
			t.Errorf("%s: New accepted", name)
		}
	}
}
