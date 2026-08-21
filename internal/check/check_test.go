package check

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoNondeterminismOutsideClockRand(t *testing.T) {
	root := findMod(t)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if strings.Contains(rel, "node_modules") || strings.Contains(rel, "/clock/") || strings.Contains(rel, "/rand/") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(b)
		for _, needle := range []string{"time" + ".Now(", "math" + "/rand", "uuid" + ".New("} {
			if strings.Contains(src, needle) {
				t.Errorf("%s contains %s", rel, needle)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPacksDoNotImportEdgeOrProto(t *testing.T) {
	root := findMod(t)
	svcRoot := filepath.Join(root, "internal", "services")
	fset := token.NewFileSet()
	err := filepath.WalkDir(svcRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, im := range f.Imports {
			p := strings.Trim(im.Path.Value, `"`)
			if strings.Contains(p, "/internal/edge") || strings.Contains(p, "/internal/proto") || strings.Contains(p, "/internal/generated") {
				t.Errorf("%s imports %s", path, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func findMod(t *testing.T) string {
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
