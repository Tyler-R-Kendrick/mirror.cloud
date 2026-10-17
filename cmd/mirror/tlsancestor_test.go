//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

// startUnder makes an ancestor with mode (and owner uid, unless -1) and starts
// with DIR two levels below it, so DIR and its parent do not exist yet.
func startUnder(t *testing.T, mode os.FileMode, uid int) (anc string, err error) {
	t.Helper()
	anc = filepath.Join(t.TempDir(), "anc")
	if err := os.Mkdir(anc, 0o700); err != nil {
		t.Fatal(err)
	}
	if uid >= 0 {
		if err := os.Chown(anc, uid, -1); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(anc, mode); err != nil {
		t.Fatal(err)
	}
	_, err = ensureSelfSigned(filepath.Join(anc, "x", "tls"), clock.Real{}.Now())
	return anc, err
}

// The ancestor rule is ssh's StrictModes: every directory above DIR belongs to
// root or this user, and is not writable by group or others unless it is
// sticky (and so, by the first rule, root's or this user's). Anyone else who
// could write one could rename DIR away and put their own in its place, and a
// client pointed at DIR/ca.pem would trust whatever CA it found there. A
// refusal comes before anything is created.
func TestSelfSignedAncestorRules(t *testing.T) {
	for _, c := range []struct {
		name string
		mode os.FileMode
		ok   bool
	}{
		{"0775, group-writable", 0o775, false},
		{"2775, setgid and group-writable", 0o775 | os.ModeSetgid, false},
		{"0777, not sticky", 0o777, false},
		{"0757, others-writable", 0o757, false},
		{"1777, sticky and this user's", 0o777 | os.ModeSticky, true},
		{"0755, this user's", 0o755, true},
	} {
		anc, err := startUnder(t, c.mode, -1)
		if c.ok {
			if err != nil {
				t.Errorf("%s: %v", c.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), anc) {
			t.Errorf("%s: want a refusal naming %s, got %v", c.name, anc, err)
		}
		if _, statErr := os.Lstat(filepath.Join(anc, "x")); !os.IsNotExist(statErr) {
			t.Errorf("%s: the refusal came after DIR's parent was created", c.name)
		}
	}
}

// The same rule for ancestors owned by another user, which needs root to set
// up: a sticky directory's owner may still rename anything in it.
func TestSelfSignedAncestorOfAnotherUser(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to give a directory to another user; the non-root rules are TestSelfSignedAncestorRules")
	}
	for _, c := range []struct {
		name string
		mode os.FileMode
	}{
		{"1777, sticky but uid 4242's", 0o777 | os.ModeSticky},
		{"0755, uid 4242's", 0o755},
	} {
		anc, err := startUnder(t, c.mode, 4242)
		if err == nil || !strings.Contains(err.Error(), anc) || !strings.Contains(err.Error(), "uid 4242") {
			t.Errorf("%s: want a refusal naming %s and uid 4242, got %v", c.name, anc, err)
		}
	}
}

// server.key a hard link to another user's file with the right bytes and
// mode: the owner check on the descriptor refuses it, the set is remade, and
// the other user's file is left as it was.
func TestSelfSignedRefusesAHardLinkToAnotherUsersKey(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to give a file to another user; the owner rule is TestDirAndFileRules")
	}
	base := t.TempDir()
	f0, err := ensureSelfSigned(filepath.Join(base, "tls"), clock.Real{}.Now())
	if err != nil {
		t.Fatal(err)
	}
	key, _ := os.ReadFile(f0.Key)
	foreign := filepath.Join(base, "foreign.key")
	mustWrite(t, foreign, string(key), 0o600)
	if err := os.Chown(foreign, 4242, 4242); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f0.Key); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(foreign, f0.Key); err != nil {
		t.Fatal(err)
	}
	f1, err := ensureSelfSigned(filepath.Join(base, "tls"), clock.Real{}.Now())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(f0.pair.Certificate[0], f1.pair.Certificate[0]) {
		t.Error("reused a set whose server.key is a hard link to uid 4242's file")
	}
	var st syscall.Stat_t
	b, _ := os.ReadFile(foreign)
	if err := syscall.Stat(foreign, &st); err != nil || !bytes.Equal(b, key) || st.Uid != 4242 || st.Nlink != 1 {
		t.Errorf("the other user's file changed: uid %d, links %d, same bytes %v (%v)", st.Uid, st.Nlink, bytes.Equal(b, key), err)
	}
}
