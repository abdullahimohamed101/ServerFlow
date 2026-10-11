package embedded

import (
	"testing"

	"serverflow/internal/config"
)

type withFeature struct {
	Other      int
	Resilience struct{ Mode string }
}

type withPointer struct{ Resilience *struct{ Mode string } }

type badShape struct{ Resilience struct{ Mode int } }

type noMode struct{ Resilience struct{ Enabled bool } }

func TestResilienceAbsentOnABuildWithoutTheFeature(t *testing.T) {
	// The real configuration of this build: master has no Resilience section.
	cfg := config.Default()
	for _, want := range []string{ResilienceDefault, ResilienceOn, ResilienceOff} {
		got, ignored, err := ApplyResilience(&cfg, want)
		if err != nil || got != "absent" {
			t.Fatalf("want %q: got %q, %v: a build without the feature must record absent, never off", want, got, err)
		}
		if ignored != (want != ResilienceDefault) {
			t.Fatalf("want %q: ignored=%v", want, ignored)
		}
	}
}

func TestResilienceAppliedWhenTheFeatureExists(t *testing.T) {
	var c withFeature
	c.Resilience.Mode = "on"
	for _, tc := range []struct{ want, effective string }{{"", "on"}, {"off", "off"}, {"on", "on"}} {
		got, ignored, err := applyResilience(&c, tc.want)
		if err != nil || ignored || got != tc.effective || c.Resilience.Mode != tc.effective {
			t.Fatalf("want %q: got %q ignored=%v err=%v, mode now %q", tc.want, got, ignored, err, c.Resilience.Mode)
		}
	}
	p := withPointer{Resilience: &struct{ Mode string }{Mode: "off"}}
	if got, _, err := applyResilience(&p, "on"); err != nil || got != "on" || p.Resilience.Mode != "on" {
		t.Fatalf("pointer section: %q %v", got, err)
	}
}

func TestResilienceRefusesAShapeItDoesNotKnow(t *testing.T) {
	for name, v := range map[string]any{"int mode": &badShape{}, "no mode": &noMode{}, "nil section": &withPointer{}, "not a pointer": withFeature{}, "not a struct": new(int)} {
		if _, _, err := applyResilience(v, "on"); err == nil {
			t.Errorf("%s: want an error rather than silently recording something wrong", name)
		}
	}
	if _, _, err := applyResilience(&withFeature{}, "maybe"); err == nil {
		t.Error("an unknown value was accepted")
	}
	if err := ValidResilience("auto"); err == nil {
		t.Error("auto accepted")
	}
}
