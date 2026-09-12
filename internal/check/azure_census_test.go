package check

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Azure Storage parity is an S3/LocalStack-shaped ledger: unique swagger path
// keys (Blob/Queue x-ms-paths, Table paths method+path) plus 806 Azurite tests.
// YAML operation names are not the numerator. This dies if the pin's
// denominators drift or if PARITY.md drops a swagger key / scores 12 YAML names
// as 12/59.
func TestAzureCensusDenominators(t *testing.T) {
	root := findMod(t)
	inv := loadJSON(t, filepath.Join(root, "specs", "azure", "azurite-inventory.json"))
	if got := int(inv["totalTests"].(float64)); got != 806 {
		t.Fatalf("azurite-inventory totalTests = %d, want 806", got)
	}
	svcs := inv["services"].(map[string]any)
	for svc, want := range map[string]int{"blob": 521, "queue": 85, "table": 200} {
		got := int(svcs[svc].(map[string]any)["tests"].(float64))
		if got != want {
			t.Fatalf("%s tests = %d, want %d", svc, got, want)
		}
	}

	blob := loadJSON(t, filepath.Join(root, "specs", "azure", "blob-storage.json"))
	queue := loadJSON(t, filepath.Join(root, "specs", "azure", "queue-storage.json"))
	table := loadJSON(t, filepath.Join(root, "specs", "azure", "table", "table.json"))
	blobKeys := pathKeys(blob["x-ms-paths"])
	queueKeys := pathKeys(queue["x-ms-paths"])
	tableOps := methodPaths(table["paths"])
	if len(blobKeys) != 59 {
		t.Fatalf("blob x-ms-paths = %d, want 59", len(blobKeys))
	}
	if len(queueKeys) != 11 {
		t.Fatalf("queue x-ms-paths = %d, want 11", len(queueKeys))
	}
	if len(tableOps) != 12 {
		t.Fatalf("table paths method+path = %d, want 12", len(tableOps))
	}

	parity, err := os.ReadFile(filepath.Join(root, "docs", "PARITY.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(parity)
	if strings.Contains(doc, "12 / 59") || strings.Contains(doc, "12/59") {
		t.Fatal("PARITY.md still scores 12 YAML names as 12/59 swagger keys")
	}
	for _, needle := range []string{
		"| Blob swagger `x-ms-paths` keys | 59 |",
		"| Queue swagger `x-ms-paths` keys | 11 |",
		"| Table swagger `paths` method+path | 12 |",
		"| Azurite test functions | 806 |",
	} {
		if !strings.Contains(doc, needle) {
			t.Fatalf("PARITY.md missing denominator row %q", needle)
		}
	}
	for _, k := range blobKeys {
		if !strings.Contains(doc, "`"+k+"`") {
			t.Errorf("PARITY.md missing blob path key %s", k)
		}
	}
	for _, k := range queueKeys {
		if !strings.Contains(doc, "`"+k+"`") {
			t.Errorf("PARITY.md missing queue path key %s", k)
		}
	}
	for _, mp := range tableOps {
		if !strings.Contains(doc, "`"+mp+"`") {
			t.Errorf("PARITY.md missing table method+path %s", mp)
		}
	}
}

func loadJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return out
}

func pathKeys(v any) []string {
	m, _ := v.(map[string]any)
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func methodPaths(v any) []string {
	m, _ := v.(map[string]any)
	var out []string
	for path, item := range m {
		obj, _ := item.(map[string]any)
		for method := range obj {
			switch strings.ToLower(method) {
			case "get", "put", "post", "delete", "head", "patch", "options":
				out = append(out, strings.ToUpper(method)+" "+path)
			}
		}
	}
	return out
}
