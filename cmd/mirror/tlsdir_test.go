package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

// A DIR below a directory anyone can write, without the sticky bit, could be
// renamed away and replaced between starts, so it is refused; under a sticky
// one (as /tmp is) it is fine.
func TestSelfSignedRefusesADirUnderAnOpenAncestor(t *testing.T) {
	posixOnly(t)
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(shared, "x", "tls")
	_, err := ensureSelfSigned(dir, clock.Real{}.Now())
	if err == nil || !strings.Contains(err.Error(), shared) || !strings.Contains(err.Error(), "sticky") {
		t.Fatalf("want a refusal naming %s, got %v", shared, err)
	}
	if err := os.Chmod(shared, 0o777|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureSelfSigned(dir, clock.Real{}.Now()); err != nil {
		t.Fatalf("under a sticky ancestor: %v", err)
	}
}

// DIR itself is opened without following a symlink: a link named DIR could be
// re-pointed by whoever owns it.
func TestSelfSignedRefusesASymlinkedDir(t *testing.T) {
	posixOnly(t)
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "tls")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	_, err := ensureSelfSigned(link, clock.Real{}.Now())
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("want a refusal of the symlink, got %v", err)
	}
}

// Only what writeAtomic leaves behind is removed: a regular file named like a
// temp file and nothing else (not a backup, a symlink or a directory).
func TestSelfSignedCleansOnlyItsOwnTemps(t *testing.T) {
	posixOnly(t)
	dir := t.TempDir()
	if _, err := ensureSelfSigned(dir, clock.Real{}.Now()); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "target")
	mustWrite(t, outside, "untouched", 0o600)
	mustWrite(t, filepath.Join(dir, ".server.key.123456"), "stale", 0o600)
	mustWrite(t, filepath.Join(dir, ".server.key.bak"), "a backup", 0o600)
	if err := os.Symlink(outside, filepath.Join(dir, ".server.pem.123")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, ".ca.pem.42"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureSelfSigned(dir, clock.Real{}.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, ".server.key.123456")); !os.IsNotExist(err) {
		t.Error("the stale temp file was kept")
	}
	for _, n := range []string{".server.key.bak", ".server.pem.123", ".ca.pem.42"} {
		if _, err := os.Lstat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s was removed", n)
		}
	}
	if b, _ := os.ReadFile(outside); string(b) != "untouched" {
		t.Error("the symlink's target changed")
	}
}

// A host the CA cannot be constrained to is refused before anything is made,
// with --tls-self-signed in the error.
func TestSelfSignedRefusesHostsItCannotCertify(t *testing.T) {
	for host, why := range map[string]string{
		"bücher.example":                "punycode",
		"*.example.com":                 "wildcard",
		"a..b":                          "DNS name",
		"-a.test":                       "DNS name",
		"under_score.test":              "DNS name",
		strings.Repeat("a", 64) + ".tt": "DNS name",
	} {
		dir := filepath.Join(t.TempDir(), "tls")
		_, err := ensureSelfSignedFor(dir, append(append([]string{}, selfSignedNames...), host), clock.Real{}.Now())
		if err == nil || !strings.HasPrefix(err.Error(), "--tls-self-signed: ") || !strings.Contains(err.Error(), why) {
			t.Errorf("%q: want a --tls-self-signed error about %s, got %v", host, why, err)
		}
		if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
			t.Errorf("%q: DIR was created before the refusal", host)
		}
	}
	// A single label is a host too; its constraint covers its subtree.
	if _, err := ensureSelfSignedFor(t.TempDir(), append(append([]string{}, selfSignedNames...), "mirror"), clock.Real{}.Now()); err != nil {
		t.Errorf("single-label host: %v", err)
	}
}
