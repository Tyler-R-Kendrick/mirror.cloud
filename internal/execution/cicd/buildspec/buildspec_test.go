package buildspec_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/buildspec"
)

func TestRunBuildWritesArtifact(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "make-artifact.sh")
	body := "#!/bin/sh\necho ok > artifact.txt\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	yaml := []byte(`
version: 0.2
phases:
  build:
    commands:
      - ./make-artifact.sh
`)
	spec, err := buildspec.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	res, err := buildspec.Run(context.Background(), spec, buildspec.Config{
		Dir:    dir,
		Output: &buf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit %d output %q", res.ExitCode, buf.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "artifact.txt")); err != nil {
		t.Fatalf("artifact: %v", err)
	}
}

func TestFailingCommandRunsFinally(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`
version: 0.2
phases:
  build:
    commands:
      - exit 7
finally:
  - echo finally-ran > finally.txt
`)
	spec, err := buildspec.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}

	res, err := buildspec.Run(context.Background(), spec, buildspec.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 7 {
		t.Fatalf("exit %d want 7", res.ExitCode)
	}
	if _, err := os.Stat(filepath.Join(dir, "finally.txt")); err != nil {
		t.Fatalf("finally did not run: %v", err)
	}
}

func TestParseRejectsBadVersion(t *testing.T) {
	_, err := buildspec.Parse([]byte("version: 0.1\nphases: {}\n"))
	if err == nil {
		t.Fatal("expected version error")
	}
}

func TestExportedVariablesAndArtifacts(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`version: 0.2
env:
  variables:
    HELLO: world
  exported-variables:
    - HELLO
phases:
  build:
    commands:
      - echo -n "$HELLO" > out.txt
artifacts:
  files:
    - out.txt
`)
	spec, err := buildspec.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	res, err := buildspec.Run(context.Background(), spec, buildspec.Config{Dir: dir})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("run %#v err %v", res, err)
	}
	body, err := os.ReadFile(filepath.Join(dir, "out.txt"))
	if err != nil || string(body) != "world" {
		t.Fatalf("out %q err %v", body, err)
	}
	exp, err := os.ReadFile(filepath.Join(dir, "exported-variables"))
	if err != nil || !strings.Contains(string(exp), "HELLO=world") {
		t.Fatalf("exported %q err %v", exp, err)
	}
}

func TestMissingArtifactFails(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`version: 0.2
phases:
  build:
    commands:
      - true
artifacts:
  files:
    - missing.txt
`)
	spec, err := buildspec.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	_, err = buildspec.Run(context.Background(), spec, buildspec.Config{Dir: dir})
	if err == nil {
		t.Fatal("want missing artifact error")
	}
}

func TestSecondaryArtifacts(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`version: 0.2
phases:
  build:
    commands:
      - echo a > primary.txt
      - echo b > extra.txt
artifacts:
  files:
    - primary.txt
secondary-artifacts:
  other:
    files:
      - extra.txt
`)
	spec, err := buildspec.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	res, err := buildspec.Run(context.Background(), spec, buildspec.Config{Dir: dir})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("run %#v err %v", res, err)
	}

	bad := []byte(`version: 0.2
phases:
  build:
    commands:
      - echo a > primary.txt
artifacts:
  files:
    - primary.txt
secondary-artifacts:
  other:
    files:
      - missing-extra.txt
`)
	spec2, err := buildspec.Parse(bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildspec.Run(context.Background(), spec2, buildspec.Config{Dir: t.TempDir()}); err == nil {
		t.Fatal("want missing secondary artifact error")
	}
}

func TestArtifactGlobRequiresMatch(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`version: 0.2
phases:
  build:
    commands:
      - mkdir -p nested && echo x > nested/out.bin
artifacts:
  files:
    - "**/*"
`)
	spec, err := buildspec.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	res, err := buildspec.Run(context.Background(), spec, buildspec.Config{Dir: dir})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("run %#v err %v", res, err)
	}

	empty := []byte(`version: 0.2
phases:
  build:
    commands:
      - true
artifacts:
  files:
    - "**/*.bin"
`)
	spec2, err := buildspec.Parse(empty)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildspec.Run(context.Background(), spec2, buildspec.Config{Dir: t.TempDir()}); err == nil {
		t.Fatal("want glob miss error")
	}
}
