//go:build linux

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

// TestSelfSignedReadsTheDirItChecked replaces DIR while a start is between its
// checks and its reads, without depending on luck: the test holds DIR's lock,
// waits until /proc/locks shows the start queued behind it (so DIR has been
// checked), renames a different, unsafe directory into DIR's place, and then
// lets the start go. The start must serve what is in the directory it checked.
func TestSelfSignedReadsTheDirItChecked(t *testing.T) {
	base, now := t.TempDir(), clock.Real{}.Now()
	dir, evil := filepath.Join(base, "tls"), filepath.Join(base, "evil")
	a, err := ensureSelfSigned(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ensureSelfSigned(evil, now); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(evil, 0o777); err != nil { // a start would refuse it outright
		t.Fatal(err)
	}
	evilLeaf := readPEM(t, filepath.Join(evil, "server.pem")).Bytes

	lock, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(int(lock.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	type result struct {
		f   selfSignedFiles
		err error
	}
	done := make(chan result, 1)
	go func() { f, err := ensureSelfSigned(dir, now); done <- result{f, err} }()
	waitForLockWaiter(t, st.Ino)

	if err := os.Rename(dir, dir+".checked"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(evil, dir); err != nil {
		t.Fatal(err)
	}
	_ = lock.Close() // releases the flock
	r := <-done
	if r.err != nil {
		t.Fatalf("the start failed: %v", r.err)
	}
	if bytes.Equal(r.f.pair.Certificate[0], evilLeaf) {
		t.Fatal("served the set of a directory it never checked (mode 0777)")
	}
	if !bytes.Equal(r.f.pair.Certificate[0], readPEM(t, filepath.Join(dir+".checked", "server.pem")).Bytes) || a.CA == "" {
		t.Error("did not serve the set of the directory it checked")
	}
}

// waitForLockWaiter returns once /proc/locks shows a process queued for the
// flock on inode ino.
func waitForLockWaiter(t *testing.T, ino uint64) {
	t.Helper()
	needle := fmt.Sprintf(":%d ", ino)
	for i := 0; i < 30000; i++ { // about 30 s
		b, err := os.ReadFile("/proc/locks")
		if err != nil {
			t.Fatalf("/proc/locks: %v", err)
		}
		for _, line := range strings.Split(string(b), "\n") {
			if strings.Contains(line, "->") && strings.Contains(line, needle) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the start never queued on DIR's lock")
}
