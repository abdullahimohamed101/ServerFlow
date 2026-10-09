package main

import (
	"io"
	"log/slog"
	"testing"

	"serverflow/internal/config"
	"serverflow/internal/gateway"
)

func TestLookupCapLeavesTwoConnectionsFree(t *testing.T) {
	for conns, want := range map[int]int{4: 2, 5: 3, 10: 8, 100: 98} {
		if got := lookupCap(conns); got != want || got >= conns {
			t.Errorf("lookupCap(%d) = %d, want %d (and below the pool)", conns, got, want)
		}
	}
}

func TestCheckAuthWiring(t *testing.T) {
	for _, c := range []struct {
		mode     string
		required bool
		ok       bool
	}{
		{config.AuthModeRequired, true, true}, {config.AuthModeOff, false, true},
		{config.AuthModeRequired, false, false}, {config.AuthModeOff, true, false}, {"", true, false},
	} {
		if err := checkAuthWiring(c.mode, c.required); (err == nil) != c.ok {
			t.Errorf("mode %q required=%v: %v", c.mode, c.required, err)
		}
	}
}

// The wiring check is what stands between a configuration that requires keys and an open server.
func TestRequiredModeWithoutAnAuthenticatorIsCaughtBeforeServing(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()
	cfg.Auth.Mode = config.AuthModeRequired
	srv := gateway.NewFromConfig(cfg, log) // no authenticator supplied
	if !srv.AuthRequired() {
		t.Fatal("NewFromConfig must honour auth.mode=required")
	}
	if err := checkAuthWiring(cfg.Auth.Mode, srv.AuthRequired()); err != nil {
		t.Fatalf("a server that refuses to run unauthenticated is correctly wired: %v", err)
	}
	cfg.Auth.Mode = config.AuthModeOff
	if gateway.NewFromConfig(cfg, log).AuthRequired() {
		t.Fatal("auth.mode=off must stay open")
	}
}
