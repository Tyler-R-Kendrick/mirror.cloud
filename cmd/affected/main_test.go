package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunPrintsPackagesAndJSON(t *testing.T) {
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	var out, errb bytes.Buffer
	if err := run([]string{"-root", root}, strings.NewReader("internal/clock/clock.go\n\n"), &out, &errb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "./internal/clock\n") || !strings.Contains(errb.String(), "packages") {
		t.Errorf("stdout %q stderr %q", out.String(), errb.String())
	}
	out.Reset()
	errb.Reset()
	if err := run([]string{"-root", root}, strings.NewReader("go.mod\n"), &out, &errb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errb.String(), "every package") {
		t.Errorf("stderr %q", errb.String())
	}
	out.Reset()
	list := filepath.Join(t.TempDir(), "changed")
	os.WriteFile(list, []byte("README.md\n"), 0o644)
	if err := run([]string{"-root", root, "-json", "-files", list}, nil, &out, &errb); err != nil {
		t.Fatal(err)
	}
	var r struct {
		All      bool     `json:"all"`
		Packages []string `json:"packages"`
	}
	if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.All || r.Packages == nil || len(r.Packages) != 0 {
		t.Errorf("json %q: %v", out.String(), err)
	}
	// The default root is ".", a relative path, which is how CI runs it: a
	// file must still find its package.
	out.Reset()
	errb.Reset()
	if err := run([]string{"-root", filepath.Join("..", "..")}, strings.NewReader("internal/clock/clock.go\n"), &out, &errb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "./internal/clock\n") || strings.Contains(errb.String(), "every package") {
		t.Errorf("a relative root: stdout %q stderr %q", out.String(), errb.String())
	}
	if err := run([]string{"-files", filepath.Join(t.TempDir(), "absent")}, nil, &out, &errb); err == nil {
		t.Error("a missing -files was accepted")
	}
	if err := run([]string{"-nosuchflag"}, nil, &out, &errb); err == nil {
		t.Error("an unknown flag was accepted")
	}
	if err := run([]string{"-root", t.TempDir()}, strings.NewReader(""), &out, &errb); err == nil {
		t.Error("a directory with no module was accepted")
	}
}

func TestZReadsNULSeparatedNames(t *testing.T) {
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	var out, errb bytes.Buffer
	in := "internal/clock/caf\u00e9 notes.txt\x00docs/TESTING.md\x00"
	if err := run([]string{"-z", "-root", root}, strings.NewReader(in), &out, &errb); err != nil || !strings.Contains(out.String(), "./internal/clock\n") {
		t.Errorf("-z: %v %q %q", err, out.String(), errb.String())
	}
}
