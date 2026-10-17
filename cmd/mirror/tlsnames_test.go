package main

import (
	"bytes"
	"crypto/x509"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

func TestSelfSignedNamesFollowBindAndAdvertise(t *testing.T) {
	base := strings.Join(selfSignedNames, " ")
	for _, c := range []struct{ bind, adv, extra string }{
		{"127.0.0.1:4566", "", ""},
		{"0.0.0.0:4566", "", ""},
		{":4566", "", ""},
		{"[::]:4566", "", ""},
		{"localhost:4566", "", ""},
		{"192.0.2.10:4566", "", "192.0.2.10"},
		{"Mirror.Internal.:4566", "", "mirror.internal"},
		{"mirror.internal", "", "mirror.internal"},
		{"127.0.0.1:4566", "https://mirror.test:4566", "mirror.test"},
		{"127.0.0.1:4566", "mirror.test:4566", "mirror.test"},
		{"192.0.2.10:4566", "https://192.0.2.10:4566", "192.0.2.10"},
		{"127.0.0.1:4566", "http://[::1]:4566", ""},
	} {
		want := strings.TrimSpace(base + " " + c.extra)
		if got := strings.Join(selfSignedNamesFor(c.bind, c.adv), " "); got != want {
			t.Errorf("bind %q advertise %q: %q, want %q", c.bind, c.adv, got, want)
		}
	}
}

// A non-default host is in the leaf and in the CA's constraints, so the leaf
// verifies under its own CA; a start with other names replaces the set.
func TestSelfSignedCoversAnExtraHost(t *testing.T) {
	dir, now := t.TempDir(), clock.Real{}.Now()
	names := selfSignedNamesFor("192.0.2.10:4566", "https://mirror.test")
	f, err := ensureSelfSignedFor(dir, names, now)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(readPEM(t, f.CA).Bytes)
	leaf, _ := x509.ParseCertificate(readPEM(t, f.Cert).Bytes)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	for _, n := range names {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: n, CurrentTime: now}); err != nil {
			t.Errorf("%s: %v", n, err)
		}
	}
	if !slices.Contains(ca.PermittedDNSDomains, "mirror.test") {
		t.Errorf("CA constraints %v lack mirror.test", ca.PermittedDNSDomains)
	}
	before := fileBytes(t, f)
	if _, err := ensureSelfSigned(dir, now); err != nil || fileBytes(t, f) == before {
		t.Errorf("a start for other names reused the set (%v)", err)
	}
}

// Starts racing on one DIR take turns: the first makes the set, the rest reuse
// it, and every one serves the pair that ends up on disk.
func TestSelfSignedConcurrentStartsAgree(t *testing.T) {
	if !haveDirLock {
		t.Skip("no flock on this platform: concurrent starts are documented to fail closed, not to agree")
	}
	dir, now := filepath.Join(t.TempDir(), "tls"), clock.Real{}.Now()
	var wg sync.WaitGroup
	leaves := make([][]byte, 8)
	errs := make([]error, 8)
	for i := range leaves {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, err := ensureSelfSigned(dir, now)
			errs[i] = err
			if err == nil {
				leaves[i] = f.pair.Certificate[0]
			}
		}(i)
	}
	wg.Wait()
	onDisk := readPEM(t, filepath.Join(dir, "server.pem")).Bytes
	for i := range leaves {
		if errs[i] != nil || !bytes.Equal(leaves[i], onDisk) {
			t.Errorf("start %d: err %v, serves the leaf on disk: %v", i, errs[i], bytes.Equal(leaves[i], onDisk))
		}
	}
}
