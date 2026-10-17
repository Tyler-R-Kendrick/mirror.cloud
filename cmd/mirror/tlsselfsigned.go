package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// `mirror up --tls-self-signed DIR` serves HTTPS with throwaway material it
// makes itself, for a client that must reach the emulator under the name
// api.vercel.com and refuses plain HTTP. DIR (made 0700; refused if anyone else
// can write it) holds:
//
//	ca.pem      the CA a client trusts (NODE_EXTRA_CA_CERTS, AWS_CA_BUNDLE)
//	server.pem  the leaf the server presents
//	server.key  the leaf's private key, mode 0600
//	.lock       where concurrent starts on one DIR take turns (flock)
//
// The CA's key exists only in memory while it signs the leaf and is never
// written, so the CA vouches for that one leaf. Its critical name constraints
// permit only the leaf's names, so a client that trusts ca.pem accepts it for
// nothing else. A start reuses DIR's files only when they are exactly a set it
// could have made (tlstrust.go); otherwise it replaces them whole, because a
// leaf cannot be reissued under a CA whose key is gone.
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

// selfSignedNames are the names every leaf covers; up adds the host it binds to
// or advertises (selfSignedNamesFor).
var selfSignedNames = []string{selfSignedHost, "localhost", "127.0.0.1", "::1"}

// selfSignedFiles are the paths in DIR and the pair to serve, which is built
// from the bytes that were checked or made, never read back from the paths.
type selfSignedFiles struct {
	CA, Cert, Key string
	pair          tls.Certificate
}

func selfSignedIn(dir string) selfSignedFiles {
	return selfSignedFiles{
		CA:   filepath.Join(dir, "ca.pem"),
		Cert: filepath.Join(dir, "server.pem"),
		Key:  filepath.Join(dir, "server.key"),
	}
}

// ensureSelfSigned is ensureSelfSignedFor with the default names.
func ensureSelfSigned(dir string, now time.Time) (selfSignedFiles, error) {
	return ensureSelfSignedFor(dir, selfSignedNames, now)
}

// ensureSelfSignedFor returns the material in dir for names: reused when
// selfSignedReusable accepts it, made afresh otherwise. dir is created 0700 and
// refused when someone else can write it; a start holds dir's lock throughout.
// Paths are absolute so a caller may print them.
func ensureSelfSignedFor(dir string, names []string, now time.Time) (selfSignedFiles, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return selfSignedFiles{}, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return selfSignedFiles{}, fmt.Errorf("--tls-self-signed: %w", err)
	}
	if err := checkDir(abs); err != nil {
		return selfSignedFiles{}, err
	}
	unlock, err := lockDir(abs)
	if err != nil {
		return selfSignedFiles{}, err
	}
	defer unlock()
	removeStaleTemps(abs)
	files := selfSignedIn(abs)
	if pair, ok := selfSignedReusable(files, names, now); ok {
		files.pair = pair
		return files, nil
	}
	ca, cert, key, err := newSelfSigned(names, now)
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
	files.pair, err = tls.X509KeyPair(cert, key)
	return files, err
}

// removeStaleTemps deletes the temporary files writeAtomic leaves behind when a
// start is killed between creating one and renaming it. It runs under dir's
// lock, so no other start is writing them.
func removeStaleTemps(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		for _, base := range []string{"ca.pem", "server.pem", "server.key"} {
			if strings.HasPrefix(e.Name(), "."+base+".") {
				_ = os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
}

// newSelfSigned makes a CA and a leaf under it for names and returns their PEM,
// and the leaf's key as PKCS#8 PEM. The CA's key goes out of scope here.
func newSelfSigned(names []string, now time.Time) (caPEM, certPEM, keyPEM []byte, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	notBefore, notAfter := now.Add(-selfSignedBackdate), now.Add(selfSignedLifetime)
	dns, ips := splitNames(names)

	// The serial in the name keeps successive CAs apart in a trust store.
	caSerial := serial()
	caTmpl := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			CommonName:   fmt.Sprintf("mirror.cloud throwaway CA %08x", caSerial.Uint64()&0xffffffff),
			Organization: []string{"mirror.cloud"},
		},
		NotBefore:                   notBefore,
		NotAfter:                    notAfter,
		IsCA:                        true,
		BasicConstraintsValid:       true,
		MaxPathLen:                  0,
		MaxPathLenZero:              true,
		KeyUsage:                    x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		PermittedDNSDomainsCritical: true,
		PermittedDNSDomains:         dns,
		PermittedIPRanges:           ipConstraints(ips),
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
		DNSNames:              dns,
		IPAddresses:           ips,
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
