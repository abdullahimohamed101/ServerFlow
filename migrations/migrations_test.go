package migrations

import (
	"testing"
	"testing/fstest"
)

func TestEmbeddedMigrationsAreWellFormed(t *testing.T) {
	all, err := All()
	if err != nil || len(all) == 0 {
		t.Fatalf("All: %v (%d)", err, len(all))
	}
	if all[0].Version != 1 || all[0].Name != "initial" || len(all[0].Checksum) != 64 {
		t.Fatalf("unexpected first migration: %+v", all[0])
	}
}

func TestLoadRejectsBadSets(t *testing.T) {
	f := func(m map[string]string) fstest.MapFS {
		out := fstest.MapFS{}
		for k, v := range m {
			out[k] = &fstest.MapFile{Data: []byte(v)}
		}
		return out
	}
	for name, set := range map[string]map[string]string{
		"gap":       {"0001_a.sql": "select 1", "0003_b.sql": "select 1"},
		"not one":   {"0002_a.sql": "select 1"},
		"bad name":  {"0001_A.sql": "select 1"},
		"no number": {"init.sql": "select 1"},
		"empty":     {"0001_a.sql": ""},
	} {
		if _, err := load(f(set)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if m, err := load(f(map[string]string{"0002_b.sql": "select 2", "0001_a.sql": "select 1"})); err != nil || m[0].Name != "a" || m[1].Name != "b" {
		t.Errorf("valid set rejected or misordered: %v %v", m, err)
	}
}
