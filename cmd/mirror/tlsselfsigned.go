package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// `mirror up --tls-self-signed DIR` serves HTTPS with throwaway material it
// makes itself, for a client that must reach the emulator under the name
// api.vercel.com and refuses plain HTTP. DIR ends up holding exactly:
//
//	ca.pem      the CA a client trusts (NODE_EXTRA_CA_CERTS, SSL_CERT_FILE, ...)
//	server.pem  the leaf the server presents
//	server.key  the leaf's private key, mode 0600
//
// The CA's own key is generated in memory, signs the leaf, and is dropped: it
// is never written, so a CA that a machine has been told to trust cannot be used
// to mint a certificate for any other name. The cost is that a leaf cannot be
// reissued under an old CA, so material that is not reusable is replaced whole.
const (
	// selfSignedHost is the name the leaf is issued for. Besides it the leaf
	// covers the loopback names, so a client reaching the emulator by address
	// verifies too.
	selfSignedHost = "api.vercel.com"
	// selfSignedLifetime is how long a CA and its leaf are valid. It is short
	// because the material is cheap to remake and a trusted CA should not
	// outlive its purpose by years.
	selfSignedLifetime = 30 * 24 * time.Hour
	// selfSignedReuseMargin is the validity that must remain for a start to
	// reuse what is on disk. Below it the material is replaced, so a
	// long-running process never starts with a certificate about to lapse.
	selfSignedReuseMargin = 24 * time.Hour
	// selfSignedBackdate lets a client whose clock runs a little slow accept a
	// certificate made a moment ago.
	selfSignedBackdate = time.Hour
)

// errTLSConflict refuses two sources of TLS material, in up and in env.
var errTLSConflict = errors.New("--tls-self-signed and --tls-cert/--tls-key are mutually exclusive: use one source of TLS material")

// shellQuote returns s as one word for a POSIX shell that evals env's output.
func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_@%+=:,./-") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// selfSignedNames are the names a leaf must cover to be reused.
var selfSignedNames = []string{selfSignedHost, "localhost", "127.0.0.1", "::1"}

type selfSignedFiles struct{ CA, Cert, Key string }

func selfSignedIn(dir string) selfSignedFiles {
	return selfSignedFiles{
		CA:   filepath.Join(dir, "ca.pem"),
		Cert: filepath.Join(dir, "server.pem"),
		Key:  filepath.Join(dir, "server.key"),
	}
}

// ensureSelfSigned returns the paths of the material in dir, which it reuses
// when it is whole and has more than selfSignedReuseMargin left, and otherwise
// replaces. Paths are absolute so a caller may print them.
func ensureSelfSigned(dir string, now time.Time) (selfSignedFiles, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return selfSignedFiles{}, err
	}
	files := selfSignedIn(abs)
	if selfSignedReusable(files, now) {
		return files, nil
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return selfSignedFiles{}, fmt.Errorf("--tls-self-signed: %w", err)
	}
	ca, cert, key, err := newSelfSigned(now)
	if err != nil {
		return selfSignedFiles{}, err
	}
	// Key first: a start that dies between writes leaves a set that fails the
	// reuse check and is replaced, never one that is half trusted.
	for _, w := range []struct {
		path string
		body []byte
		mode os.FileMode
	}{{files.Key, key, 0o600}, {files.Cert, cert, 0o644}, {files.CA, ca, 0o644}} {
		if err := writeAtomic(w.path, w.body, w.mode); err != nil {
			return selfSignedFiles{}, fmt.Errorf("--tls-self-signed: %w", err)
		}
	}
	return files, nil
}

// selfSignedReusable reports whether the files are a pair this process would
// have made itself: the key matches the leaf, the leaf covers every name in
// selfSignedNames and chains to ca.pem, both are valid now and for the margin,
// and the key is not readable by anyone else.
func selfSignedReusable(f selfSignedFiles, now time.Time) bool {
	info, err := os.Stat(f.Key)
	if err != nil || (runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0) {
		return false
	}
	caPEM, err := os.ReadFile(f.CA)
	if err != nil {
		return false
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return false
	}
	pair, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil || len(pair.Certificate) == 0 {
		return false
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return false
	}
	for _, name := range selfSignedNames {
		for _, at := range []time.Time{now, now.Add(selfSignedReuseMargin)} {
			_, err := leaf.Verify(x509.VerifyOptions{
				Roots: pool, DNSName: name, CurrentTime: at,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			if err != nil {
				return false
			}
		}
	}
	return true
}

// newSelfSigned makes a CA and a leaf under it and returns their PEM, and the
// leaf's key as PKCS#8 PEM. The CA's key goes out of scope here.
func newSelfSigned(now time.Time) (caPEM, certPEM, keyPEM []byte, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	notBefore, notAfter := now.Add(-selfSignedBackdate), now.Add(selfSignedLifetime)

	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "mirror.cloud throwaway CA", Organization: []string{"mirror.cloud"}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, nil, err
	}

	leafTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: selfSignedHost, Organization: []string{"mirror.cloud"}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{selfSignedHost, "localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return nil, nil, nil, err
	}
	enc := func(typ string, der []byte) []byte { return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}) }
	return enc("CERTIFICATE", caDER), enc("CERTIFICATE", leafDER), enc("PRIVATE KEY", keyDER), nil
}

// serial is a random 128-bit certificate serial number.
func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		panic(err) // crypto/rand failing is not recoverable
	}
	return n
}

// writeAtomic writes path through a temporary file in the same directory, so a
// reader sees the old file or the new one and a key never exists with a looser
// mode than it was given.
func writeAtomic(path string, body []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}
