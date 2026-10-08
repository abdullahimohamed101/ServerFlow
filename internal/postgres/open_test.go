package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

// These need no database: they check that failures never carry the DSN's password.
func TestOpenErrorsDoNotLeakThePassword(t *testing.T) {
	const pw = "s3cr3tP4ssw0rd"
	for name, dsn := range map[string]string{
		"unparseable port":  "postgres://app:" + pw + "@db.example:notaport/serverflow",
		"unreachable":       "postgres://app:" + pw + "@127.0.0.1:1/serverflow?sslmode=disable",
		"keyword form":      "host=127.0.0.1 port=1 user=app password=" + pw + " dbname=x sslmode=disable",
		"bad keyword form":  "host=127.0.0.1 port=1 password=" + pw + " bogus",
		"unknown parameter": "postgres://app:" + pw + "@127.0.0.1:1/x?nosuchparam=1",
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		s, err := Open(ctx, Config{DSN: dsn, ConnectTimeout: time.Second})
		cancel()
		if err == nil {
			s.Close()
			t.Fatalf("%s: expected an error", name)
		}
		if strings.Contains(err.Error(), pw) {
			t.Errorf("%s: error leaks the password: %v", name, err)
		}
	}
}
