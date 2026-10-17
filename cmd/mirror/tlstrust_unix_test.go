//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

// A FIFO where ca.pem was must not block a start: it is not read at all.
func TestSelfSignedDoesNotOpenAFIFO(t *testing.T) {
	dir := t.TempDir()
	f, err := ensureSelfSigned(dir, clock.Real{}.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f.CA); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(f.CA, 0o644); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := ensureSelfSigned(dir, clock.Real{}.Now()); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if info, err := os.Lstat(f.CA); err != nil || !info.Mode().IsRegular() {
			t.Errorf("ca.pem after the start: %v %v", info.Mode(), err)
		}
	case <-time.After(10 * time.Second):
		if w, err := os.OpenFile(f.CA, os.O_WRONLY, 0); err == nil {
			_ = w.Close() // let the blocked reader go
		}
		<-done
		t.Fatal("a FIFO named ca.pem blocked the start")
	}
}

func TestDirAndFileRules(t *testing.T) {
	if err := dirRule("d", 4242, false, 0o700); err == nil || !strings.Contains(err.Error(), "not to this user") {
		t.Errorf("another owner: %v", err)
	}
	for _, perm := range []os.FileMode{0o720, 0o702, 0o777} {
		if err := dirRule("d", 0, true, perm); err == nil {
			t.Errorf("mode %#o accepted", perm)
		}
	}
	if err := dirRule("d", 0, true, 0o755); err != nil {
		t.Errorf("0755: %v", err)
	}
	if fileRule(false, 0o600, true) || fileRule(true, 0o640, true) || fileRule(true, 0o664, false) || !fileRule(true, 0o644, false) || !fileRule(true, 0o600, true) {
		t.Error("fileRule")
	}
}

// A DIR that belongs to someone else is refused; making one needs root, so
// elsewhere only the predicate above is exercised.
func TestSelfSignedRefusesADirOfAnotherUser(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "theirs")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(dir, os.Geteuid()+4242, -1); err != nil {
		t.Logf("not root (%v): ownership is covered by TestOwnedByMe", err)
		return
	}
	_, err := ensureSelfSigned(dir, clock.Real{}.Now())
	if err == nil || !strings.Contains(err.Error(), dir) || !strings.Contains(err.Error(), "not to this user") {
		t.Fatalf("want a refusal naming %s, got %v", dir, err)
	}
}

// Under umask 0 the modes are still 0700, 0600 and 0644, and a key someone left
// readable in a private DIR is replaced, not reused.
func TestSelfSignedModesUnderUmaskZero(t *testing.T) {
	base := t.TempDir() // made before the umask changes: testing makes it 0777 &^ umask
	old := syscall.Umask(0)
	defer syscall.Umask(old)
	dir := filepath.Join(base, "a", "b")
	f, err := ensureSelfSigned(dir, clock.Real{}.Now())
	if err != nil {
		t.Fatal(err)
	}
	for p, want := range map[string]os.FileMode{dir: 0o700, filepath.Dir(dir): 0o700, f.Key: 0o600, f.Cert: 0o644, f.CA: 0o644} {
		if info, err := os.Lstat(p); err != nil || info.Mode().Perm() != want {
			t.Errorf("%s: %v (%v), want %#o", p, info.Mode().Perm(), err, want)
		}
	}
	if err := os.Chmod(f.Key, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureSelfSigned(dir, clock.Real{}.Now()); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(f.Key); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("a 0666 key was left %v (%v)", info.Mode().Perm(), err)
	}
}
