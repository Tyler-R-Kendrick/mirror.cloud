// Package release tests scripts/release-archive.sh, which builds the archives
// .github/workflows/release.yml uploads. The workflow cannot run anywhere but
// GitHub, so everything about the archives that can be checked on the host is
// checked here: what is in them, that the same inputs give the same bytes, that
// the binary reports the tag, and what the checksum file looks like.
package release

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

const epoch = 1700000000 // an even second: a zip stores two-second resolution

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// script runs scripts/release-archive.sh with SOURCE_DATE_EPOCH set and returns
// its stdout, stderr and error.
func script(t *testing.T, env []string, args ...string) (string, string, error) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("release archives are made on Linux runners with GNU tar, gzip and Info-ZIP; the script is Linux-only")
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(repoRoot(t), "scripts", "release-archive.sh")}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func withEpoch(n string) []string { return []string{"SOURCE_DATE_EPOCH=" + n} }

func sha(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type member struct {
	name    string
	mode    int64
	mtime   time.Time
	uid     int
	gid     int
	uname   string
	gname   string
	content []byte
}

func readTarGz(t *testing.T, path string) []member {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	if gz.Name != "" || !gz.ModTime.IsZero() {
		t.Errorf("gzip header carries a name %q or a time %v", gz.Name, gz.ModTime)
	}
	tr := tar.NewReader(gz)
	var out []member
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Format != tar.FormatUSTAR {
			t.Errorf("%s: header format %v, want ustar (no pax records)", h.Name, h.Format)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, member{h.Name, h.Mode & 0o777, h.ModTime, h.Uid, h.Gid, h.Uname, h.Gname, body})
	}
}

func readZip(t *testing.T, path string) []member {
	t.Helper()
	zr, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	var out []member
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, member{name: f.Name, mode: int64(f.Mode().Perm()), mtime: f.Modified, content: body})
	}
	return out
}

func names(ms []member) string {
	var n []string
	for _, m := range ms {
		n = append(n, m.name)
	}
	return strings.Join(n, " ")
}

func hostBinary(t *testing.T, archive string) []byte {
	t.Helper()
	for _, m := range readTarGz(t, archive) {
		if m.name == "mirror" {
			return m.content
		}
	}
	t.Fatal("no mirror in " + archive)
	return nil
}

// TestArchiveOfTheHostPlatform builds the real binary the way the release does,
// for the platform the test runs on, and checks the archive and its
// reproducibility. It is the one test here that compiles ./cmd/mirror.
func TestArchiveOfTheHostPlatform(t *testing.T) {
	goos, goarch := runtime.GOOS, runtime.GOARCH
	tag := "v0.0.0-archive.test"
	work := t.TempDir()
	one, two := filepath.Join(work, "one"), filepath.Join(work, "two")

	out1, stderr, err := script(t, withEpoch("1700000000"), "archive", tag, goos, goarch, one)
	if err != nil {
		t.Fatalf("archive: %v\n%s", err, stderr)
	}
	if _, stderr, err := script(t, withEpoch("1700000000"), "archive", tag, goos, goarch, two); err != nil {
		t.Fatalf("second archive: %v\n%s", err, stderr)
	}

	name := "mirror_" + tag + "_" + goos + "_" + goarch + ".tar.gz"
	a, b := filepath.Join(one, name), filepath.Join(two, name)
	if sha(t, a) != sha(t, b) {
		t.Fatalf("two runs on the same commit and epoch differ:\n%s\n%s", sha(t, a), sha(t, b))
	}
	if want := sha(t, a) + "  " + name + "\n"; out1 != want {
		t.Errorf("the script printed %q, want %q", out1, want)
	}
	if entries, _ := os.ReadDir(one); len(entries) != 1 {
		t.Errorf("OUTDIR holds %d entries, want only the archive", len(entries))
	}

	ms := readTarGz(t, a)
	if names(ms) != "LICENSE mirror" {
		t.Fatalf("archive holds %q, want exactly LICENSE and mirror", names(ms))
	}
	license, err := os.ReadFile(filepath.Join(repoRoot(t), "LICENSE"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if m.mtime.Unix() != epoch || m.uid != 0 || m.gid != 0 || m.uname != "" || m.gname != "" {
			t.Errorf("%s: mtime %v uid %d gid %d uname %q gname %q", m.name, m.mtime, m.uid, m.gid, m.uname, m.gname)
		}
		switch m.name {
		case "LICENSE":
			if m.mode != 0o644 || !bytes.Equal(m.content, license) {
				t.Errorf("LICENSE: mode %o, matches the repository file: %v", m.mode, bytes.Equal(m.content, license))
			}
		case "mirror":
			if m.mode != 0o755 {
				t.Errorf("mirror: mode %o", m.mode)
			}
		}
	}

	// The binary reports the tag without its v, as `mirror version` has always
	// reported a version, and was built with no cgo and no build paths.
	bin := filepath.Join(work, "mirror")
	if err := os.WriteFile(bin, hostBinary(t, a), 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != strings.TrimPrefix(tag, "v") {
		t.Errorf("mirror version printed %q, want %q", got, strings.TrimPrefix(tag, "v"))
	}
	info, err := exec.Command("go", "version", "-m", bin).Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"-trimpath=true", "CGO_ENABLED=0"} {
		if !strings.Contains(string(info), want) {
			t.Errorf("go version -m lacks %s:\n%s", want, info)
		}
	}
	if strings.Contains(string(info), "vcs.") {
		t.Errorf("the binary records checkout state, so a dirty tree would change it:\n%s", info)
	}
	if strings.Contains(string(info), repoRoot(t)) {
		t.Errorf("the binary records the build directory %s", repoRoot(t))
	}

	// The epoch is an input: another one gives another archive, and the binary
	// inside it is the same bytes.
	other := filepath.Join(work, "other")
	if _, stderr, err := script(t, withEpoch("1700000002"), "pack", tag, goos, goarch, bin, other); err != nil {
		t.Fatalf("pack: %v\n%s", err, stderr)
	}
	if sha(t, filepath.Join(other, name)) == sha(t, a) {
		t.Error("SOURCE_DATE_EPOCH does not reach the archive")
	}
	if !bytes.Equal(hostBinary(t, filepath.Join(other, name)), hostBinary(t, a)) {
		t.Error("the binary changed with the epoch")
	}
}

func TestZipForWindows(t *testing.T) {
	work := t.TempDir()
	bin := filepath.Join(work, "built")
	if err := os.WriteFile(bin, []byte("MZ stand-in for a windows binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	var shas []string
	for _, d := range []string{"a", "b"} {
		if _, stderr, err := script(t, withEpoch("1700000000"), "pack", "v1.2.3", "windows", "amd64", bin, filepath.Join(work, d)); err != nil {
			t.Fatalf("pack: %v\n%s", err, stderr)
		}
		shas = append(shas, sha(t, filepath.Join(work, d, "mirror_v1.2.3_windows_amd64.zip")))
	}
	if shas[0] != shas[1] {
		t.Fatalf("two zips of the same inputs differ: %v", shas)
	}
	ms := readZip(t, filepath.Join(work, "a", "mirror_v1.2.3_windows_amd64.zip"))
	if names(ms) != "LICENSE mirror.exe" {
		t.Fatalf("zip holds %q, want exactly LICENSE and mirror.exe", names(ms))
	}
	for _, m := range ms {
		if m.mtime.Unix() != epoch {
			t.Errorf("%s: mtime %v, want %v", m.name, m.mtime.UTC(), time.Unix(epoch, 0).UTC())
		}
	}
	if string(ms[1].content) != "MZ stand-in for a windows binary" {
		t.Errorf("mirror.exe content %q", ms[1].content)
	}
}

var sumsLine = regexp.MustCompile(`^[0-9a-f]{64}  mirror_[A-Za-z0-9._+-]+\.(tar\.gz|zip)$`)

func TestSumsFile(t *testing.T) {
	work := t.TempDir()
	bin := filepath.Join(work, "built")
	if err := os.WriteFile(bin, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	dist := filepath.Join(work, "dist")
	// Packed out of order, with a name that sorts differently in the C locale
	// and in most others, and with a stray file the checksum must not cover.
	for _, p := range [][2]string{{"windows", "amd64"}, {"linux", "arm64"}, {"darwin", "arm64"}, {"linux", "amd64"}} {
		if _, stderr, err := script(t, withEpoch("1700000000"), "pack", "v1.2.3", p[0], p[1], bin, dist); err != nil {
			t.Fatalf("pack %v: %v\n%s", p, err, stderr)
		}
	}
	if err := os.WriteFile(filepath.Join(dist, "notes.txt"), []byte("not an archive"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, err := script(t, nil, "sums", dist)
	if err != nil {
		t.Fatalf("sums: %v\n%s", err, stderr)
	}
	body, err := os.ReadFile(filepath.Join(dist, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	if stdout != string(body) {
		t.Errorf("stdout is not the file")
	}
	if !strings.HasSuffix(string(body), "\n") {
		t.Error("SHA256SUMS does not end in a newline")
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("want 4 lines, got:\n%s", body)
	}
	var files []string
	for _, l := range lines {
		if !sumsLine.MatchString(l) {
			t.Errorf("line %q is not `sha256sum` output for an archive", l)
			continue
		}
		hash, file, _ := strings.Cut(l, "  ")
		files = append(files, file)
		if hash != sha(t, filepath.Join(dist, file)) {
			t.Errorf("%s: recorded %s", file, hash)
		}
	}
	if !sort.StringsAreSorted(files) {
		t.Errorf("lines are not sorted by file name (bytewise):\n%s", body)
	}

	// Run again: the checksum file is not one of its own inputs.
	again, _, err := script(t, nil, "sums", dist)
	if err != nil || again != stdout {
		t.Errorf("a second run changed the file: %v", err)
	}
	if _, _, err := script(t, nil, "sums", filepath.Join(work, "empty")); err == nil {
		t.Error("sums over a directory with no archives succeeded")
	}
}

func TestScriptRefusesWhatItCannotTrust(t *testing.T) {
	work := t.TempDir()
	bin := filepath.Join(work, "built")
	if err := os.WriteFile(bin, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(work, "out")
	cases := map[string]struct {
		env  []string
		args []string
	}{
		"tag without v":          {withEpoch("1"), []string{"pack", "1.2.3", "linux", "amd64", bin, out}},
		"tag with a space":       {withEpoch("1"), []string{"pack", "v1 2", "linux", "amd64", bin, out}},
		"tag that is a command":  {withEpoch("1"), []string{"pack", "v1;touch${IFS}pwned", "linux", "amd64", bin, out}},
		"tag with a path":        {withEpoch("1"), []string{"pack", "v1/../../x", "linux", "amd64", bin, out}},
		"goos with a slash":      {withEpoch("1"), []string{"pack", "v1.2.3", "linux/x", "amd64", bin, out}},
		"no epoch":               {[]string{"SOURCE_DATE_EPOCH="}, []string{"pack", "v1.2.3", "linux", "amd64", bin, out}},
		"epoch that is not time": {withEpoch("yesterday"), []string{"pack", "v1.2.3", "linux", "amd64", bin, out}},
		"missing binary":         {withEpoch("1"), []string{"pack", "v1.2.3", "linux", "amd64", filepath.Join(work, "nope"), out}},
		"unknown subcommand":     {withEpoch("1"), []string{"upload"}},
		"no subcommand":          {withEpoch("1"), nil},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			stdout, _, err := script(t, c.env, c.args...)
			if err == nil {
				t.Fatalf("succeeded, printed %q", stdout)
			}
			if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
				t.Errorf("exit status: %v, want 2", err)
			}
		})
	}
	if entries, _ := os.ReadDir(work); len(entries) != 1 {
		t.Errorf("a refusal left files behind in %s", work)
	}
	if _, err := os.Stat(filepath.Join(repoRoot(t), "pwned")); err == nil {
		t.Error("a tag was executed")
	}
}
