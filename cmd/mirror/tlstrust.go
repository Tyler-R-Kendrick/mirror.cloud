package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"
)

// Reusing what is in DIR is the one place its contents are trusted, so a start
// trusts only what it could have written itself, in a directory nobody else can
// write. Anything else is replaced, or, when DIR itself is unsafe, refused. How
// DIR is opened and read is per platform (tls_unix.go, tls_other.go); the rules
// are here.

// dirRule refuses a DIR that anyone but this user could write: whatever they
// put there would be served, and its CA trusted, on the next start.
func dirRule(dir string, uid int, mine bool, perm os.FileMode) error {
	if !mine {
		return fmt.Errorf("--tls-self-signed: %s belongs to uid %d, not to this user (uid %d); use a directory of your own", dir, uid, os.Geteuid())
	}
	if perm&0o022 != 0 {
		return fmt.Errorf("--tls-self-signed: %s is writable by group or others (mode %#o), so its certificates could be replaced; run chmod go-w on it or use another directory", dir, perm)
	}
	return nil
}

// fileRule is the rule for a regular file in DIR: this user's, and writable by
// nobody else, nor readable when private.
func fileRule(mine bool, perm os.FileMode, private bool) bool {
	mask := os.FileMode(0o022)
	if private {
		mask = 0o077
	}
	return mine && perm&mask == 0
}

// checkAncestors refuses a DIR below a directory that others can write and
// that has no sticky bit: anyone who can write it can rename DIR away and put
// their own in its place between starts. A sticky one, as /tmp is, only lets
// owners rename their entries. Both the path given and the one it resolves to
// are walked, up to the root.
func checkAncestors(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	paths := []string{dir}
	if real, err := filepath.EvalSymlinks(dir); err == nil && real != dir {
		paths = append(paths, real)
	}
	for _, p := range paths {
		for a := filepath.Dir(p); ; a = filepath.Dir(a) {
			info, err := os.Stat(a)
			if err != nil {
				return fmt.Errorf("--tls-self-signed: %w", err)
			}
			if info.Mode().Perm()&0o002 != 0 && info.Mode()&os.ModeSticky == 0 {
				return fmt.Errorf("--tls-self-signed: %s is inside %s, which anyone can write and which has no sticky bit, so DIR could be replaced; use a directory below ones only you can write", dir, a)
			}
			if a == filepath.Dir(a) {
				break
			}
		}
	}
	return nil
}

// staleTemp matches the temporary files DIR's writes create: a dot, the file's
// name, a dot and digits, as os.CreateTemp and selfSignedDir.write name them.
var staleTemp = regexp.MustCompile(`^\.(ca\.pem|server\.pem|server\.key)\.[0-9]+$`)

func isStaleTemp(name string) bool { return staleTemp.MatchString(name) }

// checkName refuses a host the CA cannot be constrained to, before anything is
// made: an address with a zone, a wildcard, a non-ASCII name, or anything that
// is not dot-separated letter-digit-hyphen labels. A single label is a host
// like any other; a DNS name constraint always covers the name's subtree too.
func checkName(n string) error {
	fail := func(why string) error { return fmt.Errorf("--tls-self-signed: host %q %s", n, why) }
	if a, err := netip.ParseAddr(n); err == nil {
		if a.Zone() != "" {
			return fail("carries an IPv6 zone, which a certificate cannot name")
		}
		return nil
	}
	if strings.Contains(n, "*") {
		return fail("is a wildcard, not a host a client can dial")
	}
	for _, r := range n {
		if r > 0x7f {
			return fail("is not ASCII: give its punycode (A-label, xn--...) form")
		}
	}
	bad := len(n) == 0 || len(n) > 253
	for _, label := range strings.Split(n, ".") {
		bad = bad || len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' ||
			strings.Trim(label, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-") != ""
	}
	if bad {
		return fail("is not a valid DNS name (dot-separated labels of 1 to 63 letters, digits and hyphens)")
	}
	return nil
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

// validSet returns the pair in the three files' bytes when the set is one this
// process would have made for names: ca.pem is one of our CAs, server.pem is
// one leaf that chains to it for every name, the key matches, and both are
// valid now and for selfSignedReuseMargin more. The pair is built from these
// bytes, so what is served is what was verified.
func validSet(caPEM, certPEM, keyPEM []byte, names []string, now time.Time) (tls.Certificate, bool) {
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
