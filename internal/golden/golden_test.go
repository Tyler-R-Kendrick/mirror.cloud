package golden

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAssertRoundTrip(t *testing.T) {
	dir := t.TempDir()
	old, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	t.Setenv("UPDATE_GOLDEN", "1")
	AssertJSON(t, map[string]any{"a": 1, "b": "x"})
	t.Setenv("UPDATE_GOLDEN", "0")
	AssertJSON(t, map[string]any{"a": 1, "b": "x"})
	p := filepath.Join("testdata", sanitize(t.Name())+".golden")
	if _, err := os.Stat(p); err != nil {
		t.Fatal(err)
	}
}

func TestClip(t *testing.T) {
	if clip([]byte("short")) != "short" || len(clip(make([]byte, 5000))) >= 5000 {
		t.Fatal("clip bounds")
	}
}
