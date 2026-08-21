package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFromEnvOverridesDefault(t *testing.T) {
	t.Setenv("MIRROR_BIND", "0.0.0.0:9")
	t.Setenv("MIRROR_SEED", "abc")
	c := FromEnv(Default())
	if c.Bind != "0.0.0.0:9" || c.Seed != "abc" {
		t.Fatalf("%+v", c)
	}
}

func TestFromFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "c.json")
	if err := os.WriteFile(p, []byte(`{"bind":"127.0.0.1:1","seed":"file"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	c := FromFile(Default(), p)
	if c.Bind != "127.0.0.1:1" || c.Seed != "file" {
		t.Fatalf("%+v", c)
	}
}
