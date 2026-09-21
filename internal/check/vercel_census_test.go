package check

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Vercel parity is the same shape of ledger as S3/LocalStack and Azure/Azurite:
// the vendored Vercel REST document is the denominator, the mirror.set narrowing
// is a declared property of specs/mirror.set (never a silent edit), and the
// bundle's served operations are the numerator. YAML operation names are not a
// numerator either; the document's operationIds are. This dies if the vendored
// document's operation count drifts, if the narrowing changes without a review
// of this file, or if PARITY.md drops a denominator row.
func TestVercelCensusDenominators(t *testing.T) {
	root := findMod(t)

	doc := loadJSON(t, filepath.Join(root, "specs", "vercel", "api.json"))
	paths, ok := doc["paths"].(map[string]any)
	if !ok {
		t.Fatal("specs/vercel/api.json has no paths object")
	}
	ops := 0
	for _, item := range paths {
		for method := range item.(map[string]any) {
			switch strings.ToLower(method) {
			case "get", "put", "post", "delete", "head", "patch", "options":
				ops++
			}
		}
	}
	if len(paths) != 297 || ops != 417 {
		t.Fatalf("vercel document = %d paths / %d operations, want 297 / 417; the vendored bytes moved, so re-audit the ledger", len(paths), ops)
	}

	model := vercelLoadGzippedJSON(t, filepath.Join(root, "internal", "generated", "vercel", "api", "model.json.gz"))
	narrowed := len(model["Operations"].([]any))
	if narrowed != 50 {
		t.Fatalf("narrowed vercel model = %d operations, want 50; the mirror.set narrowing moved", narrowed)
	}

	kv := loadJSON(t, filepath.Join(root, "specs", "vercel", "kv.json"))
	kvPaths := kv["paths"].(map[string]any)
	if len(kvPaths) != 1 {
		t.Fatalf("authored kv document = %d paths, want 1 (POST /)", len(kvPaths))
	}

	// The vendor-authored oracle: vercel-labs/emulate, pinned in
	// specs/vercel/emulate-inventory.json. Its Vercel service is the behavior
	// denominator the way Azurite's test suite is Azure's.
	inv := loadJSON(t, filepath.Join(root, "specs", "vercel", "emulate-inventory.json"))
	if got := int(inv["routeCount"].(float64)); got != 52 {
		t.Fatalf("emulate-inventory routeCount = %d, want 52", got)
	}
	if got := int(inv["testCount"].(float64)); got != 26 {
		t.Fatalf("emulate-inventory testCount = %d, want 26", got)
	}
	routes, ok := inv["routes"].([]any)
	if !ok || len(routes) != 52 {
		t.Fatalf("emulate-inventory routes = %d, want 52", len(routes))
	}

	parity, err := os.ReadFile(filepath.Join(root, "docs", "PARITY.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc2 := string(parity)
	for _, needle := range []string{
		"| Vercel REST document operations (vendored) | 417 |",
		"| Vercel REST document paths (vendored) | 297 |",
		"| Narrowed `vercel.api` model operations | 50 |",
		"| emulate Vercel routes (vendor-authored oracle) | 52 |",
		"| emulate Vercel test functions | 26 |",
		"| emulate Vercel routes served by mirror | 52 / 52 |",
	} {
		if !strings.Contains(doc2, needle) {
			t.Fatalf("PARITY.md is missing the Vercel denominator row %q", needle)
		}
	}
}

func vercelLoadGzippedJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	var d map[string]any
	if err := json.NewDecoder(zr).Decode(&d); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return d
}
