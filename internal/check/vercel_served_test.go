package check

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The served surface of vercel.api, counted from the documents rather than typed
// into prose: of the narrowed model's operations, those the vendored document lists,
// those only a supplement (specs/vercel/api-extra.json) does, and mirror's own
// test-control routes. docs/VERCEL.md states the first as the figure it measures
// the unserved remainder against, and specs/urls.tsv names every operation of the
// supplement, so neither can drift from the bytes they describe.

// operationNames reads the operation ids of an OpenAPI document as the model
// spells them (the first letter upper-case).
func operationNames(t *testing.T, doc map[string]any) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	paths, _ := doc["paths"].(map[string]any)
	for _, item := range paths {
		for _, op := range item.(map[string]any) {
			if m, ok := op.(map[string]any); ok {
				if id, _ := m["operationId"].(string); id != "" {
					out[strings.ToUpper(id[:1])+id[1:]] = true
				}
			}
		}
	}
	return out
}

func TestVercelServedSurfaceIsCountedFromTheDocuments(t *testing.T) {
	root := findMod(t)
	document := operationNames(t, loadJSON(t, filepath.Join(root, "specs", "vercel", "api.json")))
	supplement := operationNames(t, loadJSON(t, filepath.Join(root, "specs", "vercel", "api-extra.json")))
	control := operationNames(t, loadJSON(t, filepath.Join(root, "specs", "vercel", "control.json")))
	model := vercelLoadGzippedJSON(t, filepath.Join(root, "internal", "generated", "vercel", "api", "model.json.gz"))

	served, authored, controls := 0, []string{}, 0
	for _, o := range model["Operations"].([]any) {
		name := o.(map[string]any)["Name"].(string)
		switch {
		case control[name]:
			controls++
		case document[name]:
			served++
		default:
			authored = append(authored, name)
		}
	}
	sort.Strings(authored)
	if served != 103 || controls != 3 || len(authored) != 17 {
		t.Fatalf("the model serves %d of the document's operations, %d authored (%v) and %d test-control; want 103, 17 and 3, which docs/VERCEL.md and the census state",
			served, len(authored), authored, controls)
	}
	for _, name := range authored {
		if !supplement[name] {
			t.Errorf("%s is authored but is in no supplement document", name)
		}
	}

	vercelDoc, err := os.ReadFile(filepath.Join(root, "docs", "VERCEL.md"))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("**%d of the document's %d operations are not served** (%d are)", len(document)-served, len(document), served)
	if !strings.Contains(string(vercelDoc), want) {
		t.Errorf("docs/VERCEL.md does not say %q", want)
	}

	urls, err := os.ReadFile(filepath.Join(root, "specs", "urls.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for name := range supplement {
		if !strings.Contains(string(urls), name) {
			t.Errorf("specs/urls.tsv describes api-extra.json and does not name its operation %s", name)
		}
	}
}
