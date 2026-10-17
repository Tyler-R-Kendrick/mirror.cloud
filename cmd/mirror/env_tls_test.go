package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// envLines runs `mirror env` with args and returns its exports by name.
func envLines(t *testing.T, args ...string) (map[string]string, string, error) {
	t.Helper()
	t.Setenv("MIRROR_BIND", "127.0.0.1:4566")
	t.Setenv("MIRROR_ADVERTISE_URL", "")
	out, err := captureStdout(t, func() error { return cmdEnv(args) })
	vars := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if k, v, ok := strings.Cut(strings.TrimPrefix(line, "export "), "="); ok {
			vars[k] = v
		}
	}
	return vars, out, err
}

func TestEnvWithSelfSignedTLSPrintsHTTPSAndTheCA(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	vars, out, err := envLines(t, "--tls-self-signed", dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "VERCEL_API_URL", "STORAGE_EMULATOR_HOST"} {
		if vars[k] != "https://127.0.0.1:4566" {
			t.Errorf("%s=%q, want https://127.0.0.1:4566", k, vars[k])
		}
	}
	ca := filepath.Join(dir, "ca.pem")
	for _, k := range []string{"NODE_EXTRA_CA_CERTS", "AWS_CA_BUNDLE"} {
		if vars[k] != ca {
			t.Errorf("%s=%q, want %q", k, vars[k], ca)
		}
	}
	if strings.Contains(out, "http://") {
		t.Errorf("a plain http URL with TLS on:\n%s", out)
	}
	// env says where up keeps the CA; it does not make one.
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("env created %s", dir)
	}
}

func TestEnvWithTLSCertPrintsHTTPSWithoutACAItDoesNotKnow(t *testing.T) {
	vars, out, err := envLines(t, "--tls-cert", "c.pem", "--tls-key", "k.pem")
	if err != nil {
		t.Fatal(err)
	}
	if vars["AWS_ENDPOINT_URL"] != "https://127.0.0.1:4566" || vars["STORAGE_EMULATOR_HOST"] != "https://127.0.0.1:4566" {
		t.Errorf("want https URLs:\n%s", out)
	}
	if _, ok := vars["NODE_EXTRA_CA_CERTS"]; ok {
		t.Errorf("a CA line for a certificate whose CA is unknown:\n%s", out)
	}
}

func TestEnvWithoutTLSIsUnchanged(t *testing.T) {
	vars, out, err := envLines(t)
	if err != nil {
		t.Fatal(err)
	}
	if vars["AWS_ENDPOINT_URL"] != "http://127.0.0.1:4566" || vars["STORAGE_EMULATOR_HOST"] != "127.0.0.1:4566" {
		t.Errorf("plain output changed:\n%s", out)
	}
	if strings.Contains(out, "https://") || strings.Contains(out, "NODE_EXTRA_CA_CERTS") || strings.Contains(out, "AWS_CA_BUNDLE") {
		t.Errorf("TLS lines without TLS:\n%s", out)
	}
	// A host whose name starts with "http" still gets its scheme.
	for _, c := range []struct{ bind, flag, want string }{
		{"httpbin:4566", "", "httpbin:4566"},
		{"127.0.0.1:4566", "httpd.local:4566", "httpd.local:4566"},
	} {
		t.Setenv("MIRROR_BIND", c.bind)
		args := []string{}
		if c.flag != "" {
			args = []string{"--bind", c.flag}
		}
		out, err := captureStdout(t, func() error { return cmdEnv(args) })
		if err != nil || !strings.Contains(out, "AWS_ENDPOINT_URL=http://"+c.want+"\n") ||
			!strings.Contains(out, "STORAGE_EMULATOR_HOST="+c.want+"\n") {
			t.Errorf("bind %q --bind %q: %v\n%s", c.bind, c.flag, err, out)
		}
	}
}

// Every value env prints is evaluated by a shell, so each comes back as itself
// and nothing in it runs: CA paths and advertised URLs alike.
func TestEnvOutputSurvivesEval(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh to eval with")
	}
	base := t.TempDir()
	eval := func(t *testing.T, out, name string) string {
		t.Helper()
		cmd := exec.Command(sh, "-c", out+`printf %s "$`+name+`"`)
		cmd.Dir = base
		got, err := cmd.Output()
		if err != nil {
			t.Fatalf("eval: %v\n%s", err, out)
		}
		if _, err := os.Stat(filepath.Join(base, "PWNED")); err == nil {
			t.Fatalf("eval ran a command from:\n%s", out)
		}
		return string(got)
	}
	for _, n := range []string{"a\nb", "$(touch PWNED)", "`touch PWNED`", "it's", "x'\"$HOME\\", "-n", "~user", " lead", "tab\there", "semi;colon&amp|", "*glob?"} {
		dir := filepath.Join(base, n)
		_, out, err := envLines(t, "--tls-self-signed", dir)
		if err != nil {
			t.Fatal(err)
		}
		if got := eval(t, out, "NODE_EXTRA_CA_CERTS"); got != filepath.Join(dir, "ca.pem") {
			t.Errorf("%q: eval gave %q", n, got)
		}
	}
	adv := "http://mirror.test:4566/a b?x=1&y=$(touch PWNED)"
	t.Setenv("MIRROR_ADVERTISE_URL", adv)
	out, err := captureStdout(t, func() error { return cmdEnv(nil) })
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "VERCEL_API_URL"} {
		if got := eval(t, out, name); got != adv {
			t.Errorf("%s: eval gave %q, want %q", name, got, adv)
		}
	}
}

func TestEnvTLSWithAnAdvertisedAddress(t *testing.T) {
	t.Setenv("MIRROR_ADVERTISE_URL", "mirror.test:4566")
	out, err := captureStdout(t, func() error { return cmdEnv([]string{"--tls-cert", "c.pem"}) })
	if err != nil || !strings.Contains(out, "AWS_ENDPOINT_URL=https://mirror.test:4566\n") {
		t.Errorf("an advertised address without a scheme gets https: %v\n%s", err, out)
	}
	// A scheme written out is the operator's (a proxy may terminate TLS).
	t.Setenv("MIRROR_ADVERTISE_URL", "http://proxy.test:8080")
	out, err = captureStdout(t, func() error { return cmdEnv([]string{"--tls-cert", "c.pem"}) })
	if err != nil || !strings.Contains(out, "AWS_ENDPOINT_URL=http://proxy.test:8080\n") {
		t.Errorf("an explicit advertised scheme was rewritten: %v\n%s", err, out)
	}
}

func TestEnvRefusesSelfSignedBesideTLSCert(t *testing.T) {
	for _, extra := range [][]string{{"--tls-cert", "c.pem"}, {"--tls-key", "k.pem"}} {
		_, _, err := envLines(t, append([]string{"--tls-self-signed", t.TempDir()}, extra...)...)
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Errorf("%v: want a mutually-exclusive error, got %v", extra, err)
		}
	}
}

// The CA path is printed for eval: a path with a space or a quote must come
// back as itself.
func TestEnvQuotesTheCAPathForTheShell(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "it's a dir")
	_, out, err := envLines(t, "--tls-self-signed", dir)
	if err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(dir, "ca.pem")
	want := "export NODE_EXTRA_CA_CERTS='" + strings.ReplaceAll(ca, "'", `'\''`) + "'\n"
	if !strings.Contains(out, want) {
		t.Fatalf("want %q in\n%s", want, out)
	}
	if sh, err := exec.LookPath("sh"); err == nil {
		got, err := exec.Command(sh, "-c", out+`printf %s "$NODE_EXTRA_CA_CERTS"`).Output()
		if err != nil || string(got) != ca {
			t.Errorf("eval gave %q (%v), want %q", got, err, ca)
		}
	}
}
