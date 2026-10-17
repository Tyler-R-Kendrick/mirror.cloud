package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"time"
)

// Reusing what is in DIR is the one place its contents are trusted, so a start
// trusts only what it could have written itself, in a directory nobody else can
// write. Anything else is replaced, or, when DIR itself is unsafe, refused.

// checkDir refuses a DIR that anyone but this user could write: whatever they
// put there would be served, and its CA trusted, on the next start.
func checkDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("--tls-self-signed: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("--tls-self-signed: %s is not a directory", dir)
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	if uid, mine := ownedByMe(info); !mine {
		return fmt.Errorf("--tls-self-signed: %s belongs to uid %d, not to this user (uid %d); use a directory of your own", dir, uid, os.Geteuid())
	}
	if perm := info.Mode().Perm(); perm&0o022 != 0 {
		return fmt.Errorf("--tls-self-signed: %s is writable by group or others (mode %#o), so its certificates could be replaced; run chmod go-w on it or use another directory", dir, perm)
	}
	return nil
}

// trustedFile reports whether path is a regular file (not a symlink, FIFO or
// device, so reading it neither follows a link nor blocks) that belongs to this
// user and that nobody else can write, or, if private, read.
func trustedFile(path string, private bool) bool {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	mask := os.FileMode(0o022)
	if private {
		mask = 0o077
	}
	_, mine := ownedByMe(info)
	return mine && info.Mode().Perm()&mask == 0
}

// oneCertificate parses b as exactly one PEM CERTIFICATE block with nothing
// before or after it: a second certificate appended to ca.pem would be trusted
// by every client pointed at the file.
func oneCertificate(b []byte) (*x509.Certificate, bool) {
	b = bytes.TrimSpace(b)
	blk, rest := pem.Decode(b)
	if blk == nil || blk.Type != "CERTIFICATE" || len(blk.Headers) != 0 || len(rest) != 0 ||
		!bytes.HasPrefix(b, []byte("-----BEGIN CERTIFICATE-----")) {
		return nil, false
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	return c, err == nil
}

// splitNames separates the DNS names from the addresses in names.
func splitNames(names []string) (dns []string, ips []net.IP) {
	for _, n := range names {
		if ip := net.ParseIP(n); ip != nil {
			ips = append(ips, ip)
		} else {
			dns = append(dns, n)
		}
	}
	return dns, ips
}

// ipConstraints are the single-address ranges a CA permits for ips.
func ipConstraints(ips []net.IP) []*net.IPNet {
	nets := make([]*net.IPNet, 0, len(ips))
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			nets = append(nets, &net.IPNet{IP: v4, Mask: net.CIDRMask(32, 32)})
		} else {
			nets = append(nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)})
		}
	}
	return nets
}

// isOurCA reports whether ca is the kind of CA newSelfSigned makes for names:
// it signs leaves but no other CA, and its critical name constraints permit
// exactly names (DNS names and their subdomains, and single addresses), so even
// a client that trusts it everywhere accepts it for nothing else.
func isOurCA(ca *x509.Certificate, names []string) bool {
	if !ca.IsCA || !ca.BasicConstraintsValid || ca.MaxPathLen != 0 || !ca.MaxPathLenZero ||
		ca.KeyUsage&x509.KeyUsageCertSign == 0 || !ca.PermittedDNSDomainsCritical {
		return false
	}
	dns, ips := splitNames(names)
	return sameSet(ca.PermittedDNSDomains, dns) &&
		sameSet(netStrings(ca.PermittedIPRanges), netStrings(ipConstraints(ips)))
}

func netStrings(nets []*net.IPNet) []string {
	out := make([]string, len(nets))
	for i, n := range nets {
		out[i] = n.String()
	}
	return out
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// selfSignedReusable returns the pair in f when every file is trusted and the
// set is one this process would have made for names: ca.pem is one of our CAs,
// server.pem is one leaf that chains to it for every name, the key matches, and
// both are valid now and for selfSignedReuseMargin more. The pair is built from
// the bytes that were checked, so what is served is what was verified.
func selfSignedReusable(f selfSignedFiles, names []string, now time.Time) (tls.Certificate, bool) {
	if !trustedFile(f.CA, false) || !trustedFile(f.Cert, false) || !trustedFile(f.Key, true) {
		return tls.Certificate{}, false
	}
	caPEM, errCA := os.ReadFile(f.CA)
	certPEM, errCert := os.ReadFile(f.Cert)
	keyPEM, errKey := os.ReadFile(f.Key)
	if errCA != nil || errCert != nil || errKey != nil {
		return tls.Certificate{}, false
	}
	ca, ok := oneCertificate(caPEM)
	if !ok || !isOurCA(ca, names) {
		return tls.Certificate{}, false
	}
	leaf, ok := oneCertificate(certPEM)
	if !ok {
		return tls.Certificate{}, false
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, false
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	for _, name := range names {
		for _, at := range []time.Time{now, now.Add(selfSignedReuseMargin)} {
			_, err := leaf.Verify(x509.VerifyOptions{
				Roots: pool, DNSName: name, CurrentTime: at,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			if err != nil {
				return tls.Certificate{}, false
			}
		}
	}
	return pair, true
}
