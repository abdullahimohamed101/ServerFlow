package embedded

import (
	"fmt"
	"reflect"

	"serverflow/internal/bench/report"
	"serverflow/internal/config"
)

// Resilience settings the harness flag accepts. The empty string keeps the build's default.
const (
	ResilienceDefault = ""
	ResilienceOn      = report.ResilienceOn
	ResilienceOff     = report.ResilienceOff
)

// ValidResilience reports whether want is a value of the --resilience flag.
func ValidResilience(want string) error {
	switch want {
	case ResilienceDefault, ResilienceOn, ResilienceOff:
		return nil
	}
	return fmt.Errorf("resilience %q is not valid (choose on or off)", want)
}

// ApplyResilience sets the gateway's resilience mode in cfg and returns the value in effect, to be
// recorded in the result's metadata.
//
// The harness is built to run on a gateway with and without the Phase 15 resilience feature, so it
// cannot name the feature's configuration type. It looks for a `Resilience` struct with a string
// `Mode` in config.Config by reflection: when there is none (master before the feature) the flag is
// a no-op and the effective value is "absent", never "off", so a before/after comparison can tell
// "the build had no feature" from "the feature was switched off". With the feature, an empty
// want keeps the default and reports it. ignored says the flag asked for a setting the build cannot
// honour.
func ApplyResilience(cfg *config.Config, want string) (effective string, ignored bool, err error) {
	return applyResilience(cfg, want)
}

func applyResilience(root any, want string) (effective string, ignored bool, err error) {
	if err := ValidResilience(want); err != nil {
		return "", false, err
	}
	rv := reflect.ValueOf(root)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Struct {
		return "", false, fmt.Errorf("resilience: need a pointer to a configuration struct, got %T", root)
	}
	field := rv.Elem().FieldByName("Resilience")
	if !field.IsValid() {
		return report.ResilienceAbsent, want != ResilienceDefault, nil
	}
	if field.Kind() == reflect.Pointer {
		if field.IsNil() {
			return "", false, fmt.Errorf("resilience: the configuration's Resilience section is nil")
		}
		field = field.Elem()
	}
	mode := reflect.Value{}
	if field.Kind() == reflect.Struct {
		mode = field.FieldByName("Mode")
	}
	if !mode.IsValid() || mode.Kind() != reflect.String || !mode.CanSet() {
		return "", false, fmt.Errorf("resilience: this build's Resilience configuration has no settable string Mode; update the harness (internal/bench/embedded/resilience.go)")
	}
	if want != ResilienceDefault {
		mode.SetString(want)
	}
	return mode.String(), false, nil
}
