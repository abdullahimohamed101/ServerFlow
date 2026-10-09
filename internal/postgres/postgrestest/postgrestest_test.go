package postgrestest

import "testing"

func TestCheckLoopback(t *testing.T) {
	for _, v := range []string{"PGHOST", "PGSSLMODE", "PGPORT"} {
		t.Setenv(v, "")
	}
	ok := []string{
		"postgres://u@127.0.0.1:55432/db", "postgres://u@localhost/db", "postgres://u@[::1]/db",
		"postgres://u@/db?host=/tmp", "host=127.0.0.1 user=u", "postgres://u@127.0.0.1:1,localhost:2/db",
	}
	bad := []string{
		"postgres://u@prod.example.com/db", "postgres://u@10.0.0.5/db",
		"postgres://u@127.0.0.1/db?host=prod.example.com", // the query parameter wins over the authority
		"postgres://u@/db?host=prod.example.com",
		"postgres://u@127.0.0.1:1,prod.example.com:2/db", // a second host
		"host=prod.example.com user=u",
		"postgres://u@%zz/db",
	}
	for _, d := range ok {
		if err := checkLoopback(d); err != nil {
			t.Errorf("%q refused: %v", d, err)
		}
	}
	for _, d := range bad {
		if err := checkLoopback(d); err == nil {
			t.Errorf("%q accepted", d)
		}
	}
	t.Setenv("PGHOST", "prod.example.com")
	if err := checkLoopback("postgres://u@/db"); err == nil {
		t.Error("PGHOST to a remote host accepted")
	}
}
