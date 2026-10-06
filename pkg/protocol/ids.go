package protocol

import (
	"crypto/rand"
	"encoding/hex"
)

// NewRequestID returns a new random request ID such as "req_9f2c4a1b7d3e5f60".
func NewRequestID() string { return newID("req_") }

// NewAttemptID returns a new random attempt ID such as "att_9f2c4a1b7d3e5f60".
// A request may have several attempts across workers; see spec section 7.
func NewAttemptID() string { return newID("att_") }

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; a failure here
		// means the process cannot generate safe IDs at all.
		panic("protocol: crypto/rand unavailable: " + err.Error())
	}
	return prefix + hex.EncodeToString(b[:])
}
