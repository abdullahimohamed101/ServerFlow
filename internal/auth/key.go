// Package auth identifies API callers. It defines the API key format and its
// hashing, and an Authenticator that verifies keys against a bounded cache in
// front of a KeyStore (implemented by internal/postgres). It never talks to a
// database itself and never logs or returns key material.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
)

// An API key looks like sf_<prefix>_<secret>: a public 8-character lowercase hex
// prefix that finds the key's row, and a 256-bit secret from crypto/rand, base64url
// encoded without padding. Only the prefix and SHA-256 of the secret's bytes are ever
// stored. The key is shown once, when created.
//
// SHA-256 is the right hash here only because the secret is long and random: there is
// nothing for an attacker to guess, so stretching (bcrypt, argon2) would add latency to
// every request without adding safety. It must never be used for human-chosen secrets,
// which is why nothing in ServerFlow accepts a user-supplied key.
const (
	keyScheme     = "sf_"
	prefixHexLen  = 8
	secretBytes   = 32
	secretEncLen  = 43 // base64.RawURLEncoding length of 32 bytes
	keyLen        = len(keyScheme) + prefixHexLen + 1 + secretEncLen
	hashLen       = sha256.Size
	prefixPrefix  = keyScheme
	prefixSepByte = '_'
)

// ErrMalformedKey is returned by ParseKey. It carries no part of the input.
var ErrMalformedKey = errors.New("auth: malformed API key")

// KeyLen is the length of every well-formed API key.
const KeyLen = keyLen

// GenerateKey creates a new random API key. It returns the plaintext (show it once, then
// drop it), the public prefix, and SHA-256 of the secret, which are the only parts to store.
// It panics if the system's random source fails, as no safe key can then be made.
func GenerateKey() (plaintext, prefix string, hash []byte) {
	var p [prefixHexLen / 2]byte
	var s [secretBytes]byte
	if _, err := rand.Read(p[:]); err != nil {
		panic("auth: crypto/rand unavailable: " + err.Error())
	}
	if _, err := rand.Read(s[:]); err != nil {
		panic("auth: crypto/rand unavailable: " + err.Error())
	}
	prefix = hex.EncodeToString(p[:])
	secret := base64.RawURLEncoding.EncodeToString(s[:])
	h := sha256.Sum256(s[:])
	return keyScheme + prefix + "_" + secret, prefix, h[:]
}

// ParseKey splits a presented key into its prefix and the hash of its secret. It accepts
// exactly one spelling of each key (fixed length, lowercase hex, canonical base64url) and
// rejects everything else with ErrMalformedKey, without echoing the input.
func ParseKey(key string) (prefix string, hash []byte, err error) {
	if len(key) != keyLen || key[:len(keyScheme)] != prefixPrefix || key[len(keyScheme)+prefixHexLen] != prefixSepByte {
		return "", nil, ErrMalformedKey
	}
	prefix = key[len(keyScheme) : len(keyScheme)+prefixHexLen]
	if !validPrefix(prefix) {
		return "", nil, ErrMalformedKey
	}
	enc := key[len(keyScheme)+prefixHexLen+1:]
	raw, derr := base64.RawURLEncoding.Strict().DecodeString(enc)
	if derr != nil || len(raw) != secretBytes {
		return "", nil, ErrMalformedKey
	}
	h := sha256.Sum256(raw)
	return prefix, h[:], nil
}

// ValidPrefix reports whether s is a well-formed key prefix.
func ValidPrefix(s string) bool { return validPrefix(s) }

func validPrefix(s string) bool {
	if len(s) != prefixHexLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// HashesEqual compares two secret hashes in constant time.
func HashesEqual(a, b []byte) bool {
	return len(a) == hashLen && len(b) == hashLen && subtle.ConstantTimeCompare(a, b) == 1
}
