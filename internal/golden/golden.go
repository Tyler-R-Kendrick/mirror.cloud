// Package golden is a stdlib Verify-equivalent.
//
//	golden.AssertJSON(t, got)
//
// First run (or UPDATE_GOLDEN=1) writes testdata/<TestName>.golden.
// Later runs fail if the bytes differ. JSON objects are marshaled with
// sorted keys so map iteration cannot flake.
package golden

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Assert compares got to the snapshot named after t.Name().
func Assert(t *testing.T, got []byte) {
	t.Helper()
	assertNamed(t, t.Name(), got)
}

// AssertJSON marshals v as indented JSON and snapshots it.
func AssertJSON(t *testing.T, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	Assert(t, b)
}

func assertNamed(t *testing.T, name string, got []byte) {
	t.Helper()
	got = bytes.ReplaceAll(got, []byte("\r\n"), []byte("\n"))
	path := filepath.Join("testdata", sanitize(name)+".golden")
	update := os.Getenv("UPDATE_GOLDEN") == "1" || os.Getenv("UPDATE_GOLDEN") == "true"
	want, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) || update {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, got, 0o644); err != nil {
				t.Fatal(err)
			}
			t.Logf("wrote snapshot %s", path)
			return
		}
		t.Fatal(err)
	}
	if update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want = bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))
	if bytes.Equal(want, got) {
		return
	}
	t.Errorf("snapshot mismatch %s\n--- want (%d bytes)\n%s\n--- got (%d bytes)\n%s", path, len(want), clip(want), len(got), clip(got))
}

func sanitize(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}

func clip(b []byte) string {
	const max = 4096
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "\n... (" + strconv.Itoa(len(b)-max) + " more bytes)"
}
