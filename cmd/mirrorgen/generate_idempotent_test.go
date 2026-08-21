package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestGenerateFromSpecsIdempotent(t *testing.T) {
	root := moduleRoot(t)
	if _, err := os.Stat(filepath.Join(root, "specs", "aws")); err != nil {
		t.Skip("no vendored specs")
	}
	d1 := t.TempDir()
	d2 := t.TempDir()
	run := func(out string) {
		t.Helper()
		cmd := exec.Command("go", "run", "./cmd/mirrorgen", "--out", out)
		cmd.Dir = root
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("mirrorgen: %v\n%s", err, b)
		}
	}
	run(d1)
	run(d2)
	err := filepath.Walk(d1, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(d1, path)
		b1, _ := os.ReadFile(path)
		b2, err := os.ReadFile(filepath.Join(d2, rel))
		if err != nil {
			t.Errorf("missing in second tree: %s", rel)
			return nil
		}
		if string(b1) != string(b2) {
			t.Errorf("not idempotent: %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGenerateCatalogIdempotent(t *testing.T) {
	root := moduleRoot(t)
	d1 := t.TempDir()
	d2 := t.TempDir()
	run := func(out string) {
		t.Helper()
		cmd := exec.Command("go", "run", "./cmd/mirrorgen", "--catalog", "--out", out)
		cmd.Dir = root
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("mirrorgen: %v\n%s", err, b)
		}
	}
	run(d1)
	run(d2)
	err := filepath.Walk(d1, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(d1, path)
		b1, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		b2, err := os.ReadFile(filepath.Join(d2, rel))
		if err != nil {
			t.Errorf("missing in second tree: %s", rel)
			return nil
		}
		if string(b1) != string(b2) {
			t.Errorf("not idempotent: %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	for p := wd; p != "/"; p = filepath.Dir(p) {
		if _, err := os.Stat(filepath.Join(p, "go.mod")); err == nil {
			return p
		}
	}
	t.Fatal("go.mod not found")
	return ""
}
