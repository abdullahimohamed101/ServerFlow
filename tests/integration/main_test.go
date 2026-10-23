package integration

import (
	"net/http"
	"os"
	"testing"
	"time"
)

// Most of these tests talk to child processes through http.Get, http.Post and http.DefaultClient, which have no
// timeout: a child that accepts a connection and never answers (a frozen or deadlocked process) would block the test
// until go test's own 10-minute limit kills the whole binary with a goroutine dump and no verdict on the test. The
// longest legitimate exchange here is a stream of about 20 s, so a response that takes 90 s is a failure that names
// the URL it was waiting for (net/http reports it) instead of a hang.
func TestMain(m *testing.M) {
	http.DefaultClient.Timeout = 90 * time.Second
	os.Exit(m.Run())
}
