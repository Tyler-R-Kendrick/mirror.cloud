package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

// startUp runs `mirror up` with args until the returned stop is called. It
// replaces the listener seam so the test learns the port the kernel chose, and
// it captures what the command writes to stderr. stop closes the listener, which
// is how Serve returns, and reports stderr and cmdUp's error.
func startUp(t *testing.T, args ...string) (addr string, stop func() (string, error)) {
	t.Helper()
	got := make(chan net.Listener, 1)
	oldListen, oldStderr := listen, os.Stderr
	listen = func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			got <- ln
		}
		return ln, err
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	restore := func() string {
		_ = w.Close()
		os.Stderr, listen = oldStderr, oldListen
		var out bytes.Buffer
		_, _ = out.ReadFrom(r)
		_ = r.Close()
		return out.String()
	}
	done := make(chan error, 1)
	go func() { done <- cmdUp(args) }()
	select {
	case ln := <-got:
		return ln.Addr().String(), func() (string, error) {
			_ = ln.Close()
			err := <-done
			return restore(), err
		}
	case err := <-done:
		out := restore()
		t.Fatalf("cmdUp returned before it listened: %v\n%s", err, out)
	case <-time.After(60 * time.Second):
		t.Fatal("cmdUp did not listen within 60s")
	}
	return "", nil
}

func clientTrusting(t *testing.T, caPath, serverName string) *http.Client {
	t.Helper()
	pemBytes, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatalf("%s holds no certificate", caPath)
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: serverName},
	}}
}

func TestUpSelfSignedServesTLSToAClientThatTrustsTheCA(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	addr, stop := startUp(t, "s3", "--bind", "127.0.0.1:0", "--tls-self-signed", dir)
	ca := filepath.Join(dir, "ca.pem")

	res, err := clientTrusting(t, ca, "api.vercel.com").Get("https://" + addr + "/_mirror/health")
	if err != nil {
		stop()
		t.Fatalf("a client that trusts ca.pem and asks for api.vercel.com: %v", err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Errorf("GET /_mirror/health: status %d", res.StatusCode)
	}

	// The same server, a client that trusts nothing: it must refuse, and for
	// the right reason (an unknown authority, not a refused connection).
	bare := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "api.vercel.com"},
	}}
	_, err = bare.Get("https://" + addr + "/_mirror/health")
	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Errorf("a client that trusts no CA: want an unknown-authority error, got %v", err)
	}

	// And plain HTTP to the TLS port is not served.
	if res, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + addr + "/_mirror/health"); err == nil {
		defer res.Body.Close()
		if res.StatusCode == http.StatusOK {
			t.Errorf("plain HTTP on the TLS port answered 200")
		}
	}

	stderr, err := stop()
	if !errors.Is(err, net.ErrClosed) {
		t.Errorf("cmdUp after the listener closed: %v", err)
	}
	// The banner names the address that was asked for (--bind), not the one
	// the kernel picked, exactly as it does without TLS.
	if want := "listening on https://127.0.0.1:0\n"; !strings.Contains(stderr, want) {
		t.Errorf("startup output lacks %q:\n%s", want, stderr)
	}
	if want := "tls ca: " + ca + "\n"; !strings.Contains(stderr, want) {
		t.Errorf("startup output lacks %q:\n%s", want, stderr)
	}
}

func TestUpWithoutTLSStillServesPlainHTTP(t *testing.T) {
	addr, stop := startUp(t, "s3", "--bind", "127.0.0.1:0")
	res, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + addr + "/_mirror/health")
	if err == nil {
		_ = res.Body.Close()
	}
	stderr, _ := stop()
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("plain GET: %v", err)
	}
	if !strings.Contains(stderr, "listening on http://127.0.0.1:0\n") || strings.Contains(stderr, "tls ca:") {
		t.Errorf("startup output:\n%s", stderr)
	}
}

func TestUpRefusesSelfSignedBesideTLSCertAndKey(t *testing.T) {
	for _, extra := range [][]string{
		{"--tls-cert", "c.pem", "--tls-key", "k.pem"},
		{"--tls-cert", "c.pem"},
		{"--tls-key", "k.pem"},
	} {
		dir := filepath.Join(t.TempDir(), "tls")
		args := append([]string{"s3", "--bind", "127.0.0.1:0", "--tls-self-signed", dir}, extra...)
		err := cmdUp(args)
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Errorf("%v: want a mutually-exclusive error, got %v", extra, err)
		}
		if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
			t.Errorf("%v: the refusal came after %s was created", extra, dir)
		}
	}
}

func TestUsageDocumentsSelfSigned(t *testing.T) {
	var b bytes.Buffer
	usage(&b)
	if !strings.Contains(b.String(), "--tls-self-signed DIR") {
		t.Fatalf("usage does not document --tls-self-signed:\n%s", b.String())
	}
}

func readPEM(t *testing.T, path string) *pem.Block {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blk, rest := pem.Decode(b)
	if blk == nil || len(bytes.TrimSpace(rest)) != 0 {
		t.Fatalf("%s is not exactly one PEM block", path)
	}
	return blk
}

func TestSelfSignedMaterial(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "tls")
	now := clock.Real{}.Now().Truncate(time.Second)
	files, err := ensureSelfSigned(dir, now)
	if err != nil {
		t.Fatal(err)
	}

	// Exactly three files, no CA key and no temporary file left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if strings.Join(names, " ") != "ca.pem server.key server.pem" {
		t.Fatalf("directory holds %v", names)
	}
	if files.CA != filepath.Join(dir, "ca.pem") || files.Cert != filepath.Join(dir, "server.pem") || files.Key != filepath.Join(dir, "server.key") {
		t.Fatalf("paths %+v", files)
	}
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(files.Key); err != nil || info.Mode().Perm() != 0o600 {
			t.Errorf("server.key mode %v, %v", info.Mode().Perm(), err)
		}
	}

	ca, err := x509.ParseCertificate(readPEM(t, files.CA).Bytes)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(readPEM(t, files.Cert).Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !ca.IsCA || ca.MaxPathLen != 0 || !ca.MaxPathLenZero {
		t.Errorf("CA must be a CA that cannot sign another CA: %+v", ca)
	}
	if leaf.IsCA {
		t.Error("the leaf must not be a CA")
	}
	for _, c := range []*x509.Certificate{ca, leaf} {
		pub, ok := c.PublicKey.(*ecdsa.PublicKey)
		if !ok || pub.Curve != elliptic.P256() {
			t.Errorf("%s: want an ECDSA P-256 key, got %T", c.Subject.CommonName, c.PublicKey)
		}
	}
	if blk := readPEM(t, files.Key); blk.Type != "PRIVATE KEY" {
		t.Errorf("server.key PEM type %q", blk.Type)
	}
	if _, err := tls.LoadX509KeyPair(files.Cert, files.Key); err != nil {
		t.Errorf("server.pem and server.key are not a pair: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca)
	for _, name := range []string{"api.vercel.com", "localhost", "127.0.0.1", "::1"} {
		if err := leaf.VerifyHostname(name); err != nil {
			t.Errorf("leaf does not cover %s: %v", name, err)
		}
		opts := x509.VerifyOptions{Roots: pool, DNSName: name, CurrentTime: now.Add(time.Minute), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		if _, err := leaf.Verify(opts); err != nil {
			t.Errorf("leaf does not verify for %s against ca.pem: %v", name, err)
		}
	}
	for _, name := range []string{"example.com", "vercel.com", "evil.api.vercel.com", "10.0.0.1"} {
		if leaf.VerifyHostname(name) == nil {
			t.Errorf("leaf must not cover %s", name)
		}
	}
	if got := leaf.NotAfter.Sub(now); got < 29*24*time.Hour || got > 31*24*time.Hour {
		t.Errorf("leaf lifetime %v", got)
	}
	if !leaf.NotBefore.Before(now) {
		t.Errorf("leaf starts at %v, not before %v: a client with a slow clock would refuse it", leaf.NotBefore, now)
	}
}

func fileBytes(t *testing.T, f selfSignedFiles) [3]string {
	t.Helper()
	var out [3]string
	for i, p := range []string{f.CA, f.Cert, f.Key} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = string(b)
	}
	return out
}

func TestSelfSignedReuseKeepsTheCAStable(t *testing.T) {
	dir := t.TempDir()
	now := clock.Real{}.Now().Truncate(time.Second)
	first, err := ensureSelfSigned(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	want := fileBytes(t, first)

	// Valid for well over 24 hours: nothing is rewritten, so a machine that
	// already trusts ca.pem keeps trusting it across restarts.
	for _, later := range []time.Duration{time.Minute, 24 * time.Hour, 29*24*time.Hour - time.Hour} {
		again, err := ensureSelfSigned(dir, now.Add(later))
		if err != nil {
			t.Fatal(err)
		}
		if got := fileBytes(t, again); got != want {
			t.Fatalf("after %v the material was regenerated although more than 24h remained", later)
		}
	}

	// Inside the last 24 hours it is replaced, CA included: the CA's key was
	// never written, so a new leaf needs a new CA.
	renewed, err := ensureSelfSigned(dir, now.Add(30*24*time.Hour-23*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	got := fileBytes(t, renewed)
	if got[0] == want[0] || got[1] == want[1] || got[2] == want[2] {
		t.Fatalf("material with under 24h left was reused")
	}
}

func TestSelfSignedRegeneratesWhatItCannotTrust(t *testing.T) {
	other := t.TempDir()
	foreign, err := ensureSelfSigned(other, clock.Real{}.Now())
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(t *testing.T, f selfSignedFiles){
		"missing key":  func(t *testing.T, f selfSignedFiles) { _ = os.Remove(f.Key) },
		"missing ca":   func(t *testing.T, f selfSignedFiles) { _ = os.Remove(f.CA) },
		"missing cert": func(t *testing.T, f selfSignedFiles) { _ = os.Remove(f.Cert) },
		"garbage cert": func(t *testing.T, f selfSignedFiles) { mustWrite(t, f.Cert, "not a certificate", 0o644) },
		"garbage ca":   func(t *testing.T, f selfSignedFiles) { mustWrite(t, f.CA, "not a certificate", 0o644) },
		"key of another pair": func(t *testing.T, f selfSignedFiles) {
			b, err := os.ReadFile(foreign.Key)
			if err != nil {
				t.Fatal(err)
			}
			mustWrite(t, f.Key, string(b), 0o600)
		},
		"leaf signed by another CA": func(t *testing.T, f selfSignedFiles) {
			b, err := os.ReadFile(foreign.CA)
			if err != nil {
				t.Fatal(err)
			}
			mustWrite(t, f.CA, string(b), 0o644)
		},
	}
	if runtime.GOOS != "windows" {
		cases["key readable by others"] = func(t *testing.T, f selfSignedFiles) {
			if err := os.Chmod(f.Key, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			now := clock.Real{}.Now().Truncate(time.Second)
			files, err := ensureSelfSigned(dir, now)
			if err != nil {
				t.Fatal(err)
			}
			before := fileBytes(t, files)
			damage(t, files)
			fixed, err := ensureSelfSigned(dir, now)
			if err != nil {
				t.Fatalf("it should replace the material, not fail: %v", err)
			}
			after := fileBytes(t, fixed)
			if after[0] == before[0] && after[2] == before[2] {
				t.Fatal("damaged material was reused")
			}
			if runtime.GOOS != "windows" {
				if info, err := os.Stat(fixed.Key); err != nil || info.Mode().Perm() != 0o600 {
					t.Errorf("server.key mode %v, %v", info.Mode().Perm(), err)
				}
			}
			// What it wrote is trusted on the next start.
			if again, err := ensureSelfSigned(dir, now); err != nil || fileBytes(t, again) != after {
				t.Errorf("the replacement was not reused: %v", err)
			}
		})
	}
}

func TestSelfSignedDirectoryThatIsAFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "occupied")
	mustWrite(t, file, "x", 0o644)
	if _, err := ensureSelfSigned(file, clock.Real{}.Now()); err == nil {
		t.Fatal("a directory that is a file was accepted")
	}
}

func mustWrite(t *testing.T, p, body string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
}
