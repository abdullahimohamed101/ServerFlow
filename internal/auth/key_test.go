package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"os"
	"strings"
	"testing"
)

func TestGenerateKeyShape(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		key, prefix, hash := GenerateKey()
		if len(key) != KeyLen || KeyLen != 55 {
			t.Fatalf("key length %d, want 55", len(key))
		}
		if !strings.HasPrefix(key, "sf_"+prefix+"_") {
			t.Fatalf("key does not embed its prefix")
		}
		if !ValidPrefix(prefix) {
			t.Fatalf("bad prefix %q", prefix)
		}
		secret, err := base64.RawURLEncoding.DecodeString(key[len("sf_")+8+1:])
		if err != nil || len(secret) != 32 {
			t.Fatalf("secret is not 32 bytes of base64url: %v len=%d", err, len(secret))
		}
		want := sha256.Sum256(secret)
		if !bytes.Equal(hash, want[:]) {
			t.Fatal("returned hash is not SHA-256 of the secret bytes")
		}
		if seen[key] {
			t.Fatal("duplicate key generated")
		}
		seen[key] = true
	}
}

func TestParseRoundTrip(t *testing.T) {
	key, prefix, hash := GenerateKey()
	p, h, err := ParseKey(key)
	if err != nil || p != prefix || !HashesEqual(h, hash) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestParseKeyRejectsMalformed(t *testing.T) {
	good, _, _ := GenerateKey()
	enc := good[len("sf_")+9:]
	mut := func(s string, i int, c byte) string { return s[:i] + string(c) + s[i+1:] }
	cases := map[string]string{
		"empty":            "",
		"scheme only":      "sf_",
		"wrong scheme":     "xx" + good[2:],
		"uppercase scheme": "SF" + good[2:],
		"truncated":        good[:len(good)-1],
		"extended":         good + "A",
		"huge":             strings.Repeat("a", 1<<20),
		"uppercase hex":    "sf_ABCDEF01_" + enc,
		"non-hex prefix":   "sf_zzzzzzzz_" + enc,
		"short prefix":     "sf_abcdef0_" + enc + "A",
		"bad separator":    mut(good, 3+8, '-'),
		"unicode":          "sf_abcdef01_" + strings.Repeat("é", 21) + "a",
		"padding":          good[:len(good)-1] + "=",
		"space":            " " + good,
		"trailing newline": good[:len(good)-1] + "\n",
		"nul":              mut(good, 20, 0),
		"sql":              "sf_';--abcd_" + enc,
	}
	for name, in := range cases {
		if _, _, err := ParseKey(in); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestParseErrorDoesNotEchoInput(t *testing.T) {
	secretish := "sf_deadbeef_SUPERSECRETVALUE"
	_, _, err := ParseKey(secretish)
	if err == nil || strings.Contains(err.Error(), "SUPERSECRET") || strings.Contains(err.Error(), "deadbeef") {
		t.Fatalf("error must not echo the key: %v", err)
	}
}

func TestHashesEqual(t *testing.T) {
	_, _, h := GenerateKey()
	_, _, other := GenerateKey()
	if !HashesEqual(h, append([]byte(nil), h...)) {
		t.Fatal("equal hashes compare unequal")
	}
	if HashesEqual(h, other) || HashesEqual(h, nil) || HashesEqual(nil, nil) || HashesEqual(h, h[:31]) {
		t.Fatal("unequal or short hashes compare equal")
	}
}

// The last base64 character of a 32-byte secret has two spare bits. A spelling that sets them
// decodes to the same bytes in a lenient decoder, so two different strings would be one key.
func TestParseKeyRefusesNonCanonicalTail(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	canonical := "sf_00000000_" + strings.Repeat("A", 42) + "A"
	if _, _, err := ParseKey(canonical); err != nil {
		t.Fatalf("canonical key refused: %v", err)
	}
	for i := 1; i < 4; i++ {
		if _, _, err := ParseKey(canonical[:len(canonical)-1] + string(alphabet[i])); err == nil {
			t.Fatalf("non-canonical tail %q accepted", alphabet[i])
		}
	}
}

// The constant-time property cannot be observed from a test, so check the code's structure: the
// hash comparison goes through crypto/subtle and nothing else compares hashes.
func TestHashComparisonIsStructurallyConstantTime(t *testing.T) {
	src, err := os.ReadFile("key.go")
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(string(src), "func HashesEqual")
	if i < 0 {
		t.Fatal("HashesEqual not found")
	}
	body := string(src)[i:]
	if j := strings.Index(body[1:], "\nfunc "); j >= 0 {
		body = body[:j+1]
	}
	for _, bad := range []string{"bytes.Equal", "string(a)", "string(b)", "reflect.", "||"} {
		if strings.Contains(body, bad) {
			t.Errorf("HashesEqual must not use %q", bad)
		}
	}
	if !strings.Contains(body, "subtle.ConstantTimeCompare(a, b) == 1") {
		t.Error("HashesEqual must use subtle.ConstantTimeCompare")
	}
	for _, f := range []string{"authenticator.go", "types.go"} {
		b, _ := os.ReadFile(f)
		for _, bad := range []string{"bytes.Equal", "SecretHash =="} {
			if strings.Contains(string(b), bad) {
				t.Errorf("%s compares secrets with %q", f, bad)
			}
		}
	}
}
