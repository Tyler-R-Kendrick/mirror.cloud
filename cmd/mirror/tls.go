package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
)

// How `up` and `env` use TLS. Making the self-signed material is
// tlsselfsigned.go.

// tlsUsage is the TLS part of up's flag list in usage.
const tlsUsage = `  --tls-cert PATH --tls-key PATH
                         serve HTTPS with this certificate and key
  --tls-self-signed DIR  serve HTTPS with a throwaway CA and a leaf for
                         api.vercel.com, localhost, 127.0.0.1 and ::1, kept in
                         DIR as ca.pem, server.pem and server.key (reused while
                         valid for another 24h); prints the CA path.
                         Not combinable with --tls-cert/--tls-key
`

// errTLSConflict refuses two sources of TLS material, in up and in env.
var errTLSConflict = errors.New("--tls-self-signed and --tls-cert/--tls-key are mutually exclusive: use one source of TLS material")

// tlsOptions are the TLS flags up and env share.
type tlsOptions struct{ cert, key, selfSigned *string }

func tlsFlags(fs *flag.FlagSet, cfg config.Config) tlsOptions {
	return tlsOptions{
		cert:       fs.String("tls-cert", cfg.TLSCert, ""),
		key:        fs.String("tls-key", cfg.TLSKey, ""),
		selfSigned: fs.String("tls-self-signed", "", ""),
	}
}

// apply gives up its TLS material: it sets cfg.TLSCert and cfg.TLSKey and,
// for --tls-self-signed, makes or reuses the files and returns ca.pem's path.
func (o tlsOptions) apply(cfg *config.Config) (ca string, err error) {
	cfg.TLSCert, cfg.TLSKey = *o.cert, *o.key
	if *o.selfSigned == "" {
		return "", nil
	}
	if cfg.TLSCert != "" || cfg.TLSKey != "" {
		return "", errTLSConflict
	}
	files, err := ensureSelfSigned(*o.selfSigned, clock.Real{}.Now())
	if err != nil {
		return "", err
	}
	cfg.TLSCert, cfg.TLSKey = files.Cert, files.Key
	return files.CA, nil
}

// forEnv tells env what up would serve with the same flags, without making
// anything: the scheme and, for --tls-self-signed, where its ca.pem is.
func (o tlsOptions) forEnv() (scheme, ca string, err error) {
	if *o.selfSigned != "" {
		if *o.cert != "" || *o.key != "" {
			return "", "", errTLSConflict
		}
		abs, err := filepath.Abs(*o.selfSigned)
		if err != nil {
			return "", "", err
		}
		return "https", selfSignedIn(abs).CA, nil
	}
	return tlsScheme(*o.cert), "", nil
}

// tlsScheme is the scheme up serves: https when it has a certificate.
func tlsScheme(cert string) string {
	if cert != "" {
		return "https"
	}
	return "http"
}

// storageEmulatorHost is STORAGE_EMULATOR_HOST for url: the bare host:port, or
// the whole URL when it is https, because a GCS client assumes http for a bare
// host:port.
func storageEmulatorHost(url string) string {
	if strings.HasPrefix(url, "https://") {
		return url
	}
	return strings.TrimPrefix(url, "http://")
}

// printCAExports adds the variables that make Node and AWS clients trust ca.
// SSL_CERT_FILE is left out: it replaces a process's trust store rather than
// adding to it.
func printCAExports(ca string) {
	if ca != "" {
		fmt.Printf("export NODE_EXTRA_CA_CERTS=%s\n", shellQuote(ca))
		fmt.Printf("export AWS_CA_BUNDLE=%s\n", shellQuote(ca))
	}
}

// shellQuote returns s as one word for a POSIX shell that evals env's output.
func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_@%+=:,./-") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// printTLSCA is up's startup line naming the CA a client must trust.
func printTLSCA(ca string) {
	if ca != "" {
		fmt.Fprintln(os.Stderr, "tls ca:", ca)
	}
}

// serve listens on addr and serves h, over TLS when cert is set.
func serve(addr string, h http.Handler, cert, key string) error {
	srv := &http.Server{Addr: addr, Handler: h}
	ln, err := listen("tcp", addr)
	if err != nil {
		return err
	}
	if cert != "" {
		return srv.ServeTLS(ln, cert, key)
	}
	return srv.Serve(ln)
}

// listen is net.Listen; a test replaces it to learn which port a --bind of :0
// was given.
var listen = net.Listen
