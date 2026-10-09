package postgres

import (
	"strings"
	"testing"
)

func TestCheckTransport(t *testing.T) {
	const remote = "db.example.com"
	cases := []struct {
		name     string
		dsn      string
		insecure bool
		ok       bool
	}{
		{"loopback disable", "postgres://u:p@127.0.0.1:55432/db?sslmode=disable", false, true},
		{"localhost disable", "postgres://u:p@localhost/db?sslmode=disable", false, true},
		{"ipv6 loopback", "postgres://u:p@[::1]:5432/db?sslmode=disable", false, true},
		{"unix socket", "postgres://u:p@/db?host=/var/run/postgresql&sslmode=disable", false, true},
		{"keyword unix socket", "host=/var/run/postgresql sslmode=disable", false, true},
		{"remote disable", "postgres://u:p@" + remote + "/db?sslmode=disable", false, false},
		{"remote unset sslmode", "postgres://u:p@" + remote + "/db", false, false},
		{"remote prefer", "postgres://u:p@" + remote + "/db?sslmode=prefer", false, false},
		{"remote allow", "postgres://u:p@" + remote + "/db?sslmode=allow", false, false},
		{"remote require", "postgres://u:p@" + remote + "/db?sslmode=require", false, true},
		{"remote verify-ca", "postgres://u:p@" + remote + "/db?sslmode=verify-ca&sslrootcert=system", false, true},
		{"remote verify-full", "postgres://u:p@" + remote + "/db?sslmode=verify-full", false, true},
		{"remote disable but allowed", "postgres://u:p@" + remote + "/db?sslmode=disable", true, true},
		{"remote ip", "postgres://u:p@10.1.2.3/db?sslmode=disable", false, false},
		{"hosts: local first, remote second", "postgres://u:p@127.0.0.1:1,db.example.com:2/db?sslmode=disable", false, false},
		{"hosts: remote with require", "postgres://u:p@127.0.0.1:1,db.example.com:2/db?sslmode=require", false, true},
		{"host query parameter", "postgres://u:p@/db?host=" + remote + "&sslmode=disable", false, false},
		{"keyword form remote", "host=" + remote + " user=u password='p q' dbname=d sslmode=disable", false, false},
		{"keyword form remote require", "host=" + remote + " user=u password='p q' dbname=d sslmode=require", false, true},
		{"keyword form remote, no sslmode", "host=" + remote + " user=u dbname=d", false, false},
		{"postgresql scheme", "postgresql://u:p@" + remote + "/db?sslmode=disable", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Keep the environment from changing what the DSN means.
			for _, v := range []string{"PGHOST", "PGSSLMODE", "PGHOSTADDR", "PGPORT"} {
				t.Setenv(v, "")
			}
			err := CheckTransport(c.dsn, c.insecure)
			if (err == nil) != c.ok {
				t.Fatalf("ok=%v, err=%v", c.ok, err)
			}
			if err != nil && (strings.Contains(err.Error(), "p@") || strings.Contains(err.Error(), remote) || strings.Contains(err.Error(), "p q")) {
				t.Fatalf("the error echoes the DSN: %v", err)
			}
		})
	}
}

func TestCheckTransportHonoursTheEnvironment(t *testing.T) {
	t.Setenv("PGHOST", "")
	t.Setenv("PGSSLMODE", "")
	// PGHOST supplies the host the DSN omits.
	t.Setenv("PGHOST", "db.example.com")
	if err := CheckTransport("postgres://u:p@/db?sslmode=disable", false); err == nil {
		t.Error("PGHOST to a remote host with sslmode=disable was accepted")
	}
	if err := CheckTransport("postgres://u:p@/db?sslmode=require", false); err != nil {
		t.Errorf("PGHOST with sslmode=require: %v", err)
	}
	t.Setenv("PGHOST", "")
	// PGSSLMODE supplies the sslmode the DSN omits.
	t.Setenv("PGSSLMODE", "disable")
	if err := CheckTransport("postgres://u:p@db.example.com/db", false); err == nil {
		t.Error("PGSSLMODE=disable to a remote host was accepted")
	}
	t.Setenv("PGSSLMODE", "verify-full")
	if err := CheckTransport("postgres://u:p@db.example.com/db", false); err != nil {
		t.Errorf("PGSSLMODE=verify-full: %v", err)
	}
}

func TestCheckTransportRejectsUnparseableDSNsWithoutEchoing(t *testing.T) {
	for _, dsn := range []string{"postgres://u:topsecret@%zz/db", "host=x password=topsecret bogus", "postgres://u:topsecret@h:notaport/db"} {
		err := CheckTransport(dsn, false)
		if err == nil || strings.Contains(err.Error(), "topsecret") {
			t.Errorf("%q: %v", dsn[:12], err)
		}
	}
}
