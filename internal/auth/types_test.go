package auth

import "testing"

func TestAllowsModelIsAnExactMatch(t *testing.T) {
	p := TenantPolicy{AllowedModels: []string{"m1", "llama-3-8b"}}
	for model, want := range map[string]bool{"m1": true, "llama-3-8b": true, "m10": false, "m": false, "m1 ": false, "M1": false, "llama-3-8b-instruct": false, "": false} {
		if got := p.AllowsModel(model); got != want {
			t.Errorf("AllowsModel(%q) = %v, want %v", model, got, want)
		}
	}
	if !(TenantPolicy{}).AllowsModel("anything") {
		t.Error("nil allow-list means every model")
	}
	if (TenantPolicy{AllowedModels: []string{}}).AllowsModel("anything") {
		t.Error("an empty allow-list means no model")
	}
}
