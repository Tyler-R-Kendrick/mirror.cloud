package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

func posixOnly(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("file owners and modes are POSIX semantics; Windows has neither")
	}
}

func certPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// forge rewrites ca.pem and server.pem as someone able to write DIR could: a
// fresh CA built the way ours is (then changed by mod), and a leaf for
// leafNames on DIR's own key. With mod nil and every name it is
// indistinguishable from a set this process made, which the control case shows.
func forge(t *testing.T, f selfSignedFiles, mod func(*x509.Certificate), leafNames []string) (caPEM, leafPEM []byte) {
	t.Helper()
	keyPEM, err := os.ReadFile(f.Key)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(keyPEM)
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	now := clock.Real{}.Now()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "forged CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(10 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true, KeyUsage: x509.KeyUsageCertSign,
		PermittedDNSDomainsCritical: true, PermittedDNSDomains: []string{"api.vercel.com", "localhost"},
		PermittedIPRanges: []*net.IPNet{
			{IP: net.IPv4(127, 0, 0, 1).To4(), Mask: net.CIDRMask(32, 32)},
			{IP: net.IPv6loopback, Mask: net.CIDRMask(128, 128)},
		},
	}
	if mod != nil {
		mod(ca)
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	parent, _ := x509.ParseCertificate(caDER)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "api.vercel.com"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(10 * 24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, n := range leafNames {
		if ip := net.ParseIP(n); ip != nil {
			leaf.IPAddresses = append(leaf.IPAddresses, ip)
		} else {
			leaf.DNSNames = append(leaf.DNSNames, n)
		}
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, parent, k.(*ecdsa.PrivateKey).Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, leafPEM = certPEM(caDER), certPEM(leafDER)
	mustWrite(t, f.CA, string(caPEM), 0o644)
	mustWrite(t, f.Cert, string(leafPEM), 0o644)
	return caPEM, leafPEM
}

func without(names []string, drop string) []string {
	var out []string
	for _, n := range names {
		if n != drop {
			out = append(out, n)
		}
	}
	return out
}

// TestSelfSignedReusesOnlyACAOfItsOwnKind: reuse is where DIR is trusted, so
// what someone else put there must be replaced, however well it chains.
func TestSelfSignedReusesOnlyACAOfItsOwnKind(t *testing.T) {
	all := []string{"api.vercel.com", "localhost", "127.0.0.1", "::1"}
	cases := map[string]func(t *testing.T, f selfSignedFiles){
		"no name constraints": func(t *testing.T, f selfSignedFiles) {
			forge(t, f, func(c *x509.Certificate) { c.PermittedDNSDomains, c.PermittedIPRanges = nil, nil }, all)
		},
		"constraints that also permit github.com": func(t *testing.T, f selfSignedFiles) {
			forge(t, f, func(c *x509.Certificate) { c.PermittedDNSDomains = append(c.PermittedDNSDomains, "github.com") }, all)
		},
		"constraints not critical": func(t *testing.T, f selfSignedFiles) {
			forge(t, f, func(c *x509.Certificate) { c.PermittedDNSDomainsCritical = false }, all)
		},
		"a CA that may sign CAs": func(t *testing.T, f selfSignedFiles) {
			forge(t, f, func(c *x509.Certificate) { c.MaxPathLenZero, c.MaxPathLen = false, -1 }, all)
		},
		"a CA without certSign": func(t *testing.T, f selfSignedFiles) {
			forge(t, f, func(c *x509.Certificate) { c.KeyUsage = x509.KeyUsageDigitalSignature }, all)
		},
		"a second CA appended to ca.pem": func(t *testing.T, f selfSignedFiles) {
			caPEM, _ := forge(t, f, nil, all)
			mustWrite(t, f.CA, string(caPEM)+string(certPEM(mustCA(t))), 0o644)
		},
		"text before the CA": func(t *testing.T, f selfSignedFiles) {
			caPEM, _ := forge(t, f, nil, all)
			mustWrite(t, f.CA, "trust me\n"+string(caPEM), 0o644)
		},
		"a chain in server.pem": func(t *testing.T, f selfSignedFiles) {
			caPEM, leafPEM := forge(t, f, nil, all)
			mustWrite(t, f.Cert, string(leafPEM)+string(caPEM), 0o644)
		},
	}
	for _, drop := range all {
		cases["a leaf without "+drop] = func(t *testing.T, f selfSignedFiles) { forge(t, f, nil, without(all, drop)) }
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			dir, now := t.TempDir(), clock.Real{}.Now()
			f, err := ensureSelfSigned(dir, now)
			if err != nil {
				t.Fatal(err)
			}
			tamper(t, f)
			before := fileBytes(t, f)
			if _, err := ensureSelfSigned(dir, now); err != nil {
				t.Fatalf("want the set replaced, got %v", err)
			}
			if after := fileBytes(t, f); after[0] == before[0] || after[1] == before[1] {
				t.Fatal("a set this process did not make was reused")
			}
		})
	}

	// Control: a forged set exactly like ours is reused, so each case above is
	// replaced for the one thing it changes. Only DIR's owner can do this, and
	// DIR is refused when anyone else can write it.
	dir, now := t.TempDir(), clock.Real{}.Now()
	f, _ := ensureSelfSigned(dir, now)
	forge(t, f, nil, all)
	before := fileBytes(t, f)
	if _, err := ensureSelfSigned(dir, now); err != nil || fileBytes(t, f) != before {
		t.Fatalf("control: a set like ours was not reused (%v); the cases above prove nothing", err)
	}
}

func mustCA(t *testing.T) []byte {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(9), Subject: pkix.Name{CommonName: "other CA"},
		NotBefore: time.Unix(0, 0), NotAfter: clock.Real{}.Now().Add(365 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// TestSelfSignedReplacesFilesItDidNotWrite: a symlink is not followed (even to
// valid material), and a file others can write is not trusted.
func TestSelfSignedReplacesFilesItDidNotWrite(t *testing.T) {
	posixOnly(t)
	cases := map[string]func(t *testing.T, f selfSignedFiles){
		"ca.pem writable by others":    func(t *testing.T, f selfSignedFiles) { _ = os.Chmod(f.CA, 0o666) },
		"server.pem writable by group": func(t *testing.T, f selfSignedFiles) { _ = os.Chmod(f.Cert, 0o664) },
	}
	for _, which := range []string{"ca.pem", "server.pem", "server.key"} {
		cases[which+" a symlink to valid material"] = func(t *testing.T, f selfSignedFiles) {
			p := filepath.Join(filepath.Dir(f.CA), which)
			elsewhere := filepath.Join(t.TempDir(), which)
			if err := os.Rename(p, elsewhere); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(elsewhere, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			dir, now := t.TempDir(), clock.Real{}.Now()
			f, err := ensureSelfSigned(dir, now)
			if err != nil {
				t.Fatal(err)
			}
			tamper(t, f)
			before := fileBytes(t, f)
			if _, err := ensureSelfSigned(dir, now); err != nil {
				t.Fatalf("want the set replaced, got %v", err)
			}
			if after := fileBytes(t, f); after[0] == before[0] {
				t.Fatal("reused")
			}
			for p, want := range map[string]os.FileMode{f.CA: 0o644, f.Cert: 0o644, f.Key: 0o600} {
				if info, err := os.Lstat(p); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != want {
					t.Errorf("%s after replacement: %v %v", filepath.Base(p), info.Mode(), err)
				}
			}
		})
	}
}

func TestSelfSignedRefusesADirOthersCanWrite(t *testing.T) {
	posixOnly(t)
	for _, mode := range []os.FileMode{0o777, 0o770, 0o702} {
		dir, now := t.TempDir(), clock.Real{}.Now()
		f, err := ensureSelfSigned(dir, now)
		if err != nil {
			t.Fatal(err)
		}
		before := fileBytes(t, f)
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
		_, err = ensureSelfSigned(dir, now)
		if err == nil || !strings.Contains(err.Error(), dir) || !strings.Contains(err.Error(), "writable by group or others") {
			t.Errorf("mode %#o: want a refusal naming %s, got %v", mode, dir, err)
		}
		if fileBytes(t, f) != before {
			t.Errorf("mode %#o: the refusal changed DIR", mode)
		}
	}
	// Readable by others is fine: ca.pem and server.pem are public.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureSelfSigned(dir, clock.Real{}.Now()); err != nil {
		t.Errorf("0755: %v", err)
	}
}

func TestSelfSignedMakesDIRPrivateAndCleansUp(t *testing.T) {
	posixOnly(t)
	dir := filepath.Join(t.TempDir(), "tls")
	if _, err := ensureSelfSigned(dir, clock.Real{}.Now()); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		t.Errorf("new DIR mode %v (%v), want 0700", info.Mode().Perm(), err)
	}
	// What a start killed between CreateTemp and Rename leaves behind.
	for _, n := range []string{".server.key.123456", ".ca.pem.42", ".server.pem.7"} {
		mustWrite(t, filepath.Join(dir, n), "half written", 0o600)
	}
	if _, err := ensureSelfSigned(dir, clock.Real{}.Now()); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") && e.Name() != ".lock" {
			t.Errorf("stale %s left in DIR", e.Name())
		}
	}
}

func TestSelfSignedCAIsConstrainedAndNamedPerGeneration(t *testing.T) {
	now := clock.Real{}.Now()
	var cns []string
	for i := 0; i < 2; i++ {
		f, err := ensureSelfSigned(t.TempDir(), now)
		if err != nil {
			t.Fatal(err)
		}
		ca, err := x509.ParseCertificate(readPEM(t, f.CA).Bytes)
		if err != nil {
			t.Fatal(err)
		}
		cns = append(cns, ca.Subject.CommonName)
		var ranges []string
		for _, r := range ca.PermittedIPRanges {
			ranges = append(ranges, r.String())
		}
		if !ca.PermittedDNSDomainsCritical || strings.Join(ca.PermittedDNSDomains, " ") != "api.vercel.com localhost" ||
			strings.Join(ranges, " ") != "127.0.0.1/32 ::1/128" {
			t.Errorf("CA name constraints: critical %v, DNS %v, IP %v", ca.PermittedDNSDomainsCritical, ca.PermittedDNSDomains, ranges)
		}
	}
	if cns[0] == cns[1] || !strings.HasPrefix(cns[0], "mirror.cloud throwaway CA ") {
		t.Errorf("two generations named %q and %q: a trust store could not tell them apart", cns[0], cns[1])
	}
}

// TestUpServesTheCheckedPairFromMemory replaces the files between the check and
// the serving: up must serve what it checked, not whatever the paths hold later.
func TestUpServesTheCheckedPairFromMemory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	got := make(chan net.Listener, 1)
	oldListen, oldStderr := listen, os.Stderr
	devnull, _ := os.Open(os.DevNull)
	os.Stderr = devnull
	listen = func(network, address string) (net.Listener, error) {
		for _, n := range []string{"server.pem", "server.key"} {
			_ = os.WriteFile(filepath.Join(dir, n), []byte("replaced after the check"), 0o600)
		}
		ln, err := net.Listen(network, address)
		if err == nil {
			got <- ln
		}
		return ln, err
	}
	defer func() { listen, os.Stderr = oldListen, oldStderr; _ = devnull.Close() }()
	done := make(chan error, 1)
	go func() { done <- cmdUp([]string{"s3", "--bind", "127.0.0.1:0", "--tls-self-signed", dir}) }()
	var ln net.Listener
	select {
	case ln = <-got:
	case err := <-done:
		t.Fatalf("up returned before listening: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("up did not listen")
	}
	res, err := clientTrusting(t, filepath.Join(dir, "ca.pem"), "api.vercel.com").Get("https://" + ln.Addr().String() + "/_mirror/health")
	if err == nil {
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}
	_ = ln.Close()
	upErr := <-done
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("serving after the files changed: %v (up: %v)", err, upErr)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "server.pem")); !bytes.Equal(b, []byte("replaced after the check")) {
		t.Fatal("the test did not replace the files")
	}
}
