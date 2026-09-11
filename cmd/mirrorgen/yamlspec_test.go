package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver/openapi"
)

const yamlSpec = `openapi: "3.0.0"
info:
  title: Demo API
servers:
  - url: "https://api.demo.test"
paths:
  /v2/widgets:
    get:
      operationId: list-widgets
      responses:
        200:
          description: ok
          content:
            application/json:
              schema:
                type: object
                properties:
                  widgets:
                    type: array
                    items:
                      type: string
`

// TestYAMLSpecIngestsLikeItsJSONTwin is the property that makes the conversion
// a serialization concern rather than a receiver concern: the same document,
// written either way, produces the same model.
func TestYAMLSpecIngestsLikeItsJSONTwin(t *testing.T) {
	converted, _, err := yamlToJSON([]byte(yamlSpec))
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	writeSpec(t, filepath.Join(dir, "demoyaml", "v2.yaml"), []byte(yamlSpec))
	writeSpec(t, filepath.Join(dir, "demojson", "v2.json"), converted)

	groups, n, err := ingestSpecs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("ingested %d service(s), want 2 -- the YAML document was skipped", n)
	}
	// Everything derived from where the file sits rather than from what it
	// says is blanked: the ID and endpoint prefix come from the directory, and
	// the provenance names the file and its hash. What is left is the model,
	// and the model is the thing that must not depend on the spelling.
	var models []string
	for _, g := range groups {
		for _, svc := range g {
			svc.ID = ""
			svc.EndpointPrefix = ""
			svc.Source = model.SourceRef{}
			for i := range svc.Operations {
				svc.Operations[i].Source = model.SourceRef{}
			}
			blob, err := json.Marshal(svc)
			if err != nil {
				t.Fatal(err)
			}
			models = append(models, string(blob))
		}
	}
	if len(models) != 2 {
		t.Fatalf("want two models, got %d", len(models))
	}
	if models[0] != models[1] {
		t.Fatalf("the YAML and JSON spellings produced different models:\n%s\n%s", models[0], models[1])
	}
}

// TestYAMLSpecIsHashedBeforeConversion is the rule the lock depends on. A
// document we re-serialized is not the vendor's document, and pinning our
// serialization would make an unannounced upstream change invisible -- which is
// the single thing the lock exists to prevent.
func TestYAMLSpecIsHashedBeforeConversion(t *testing.T) {
	dir := t.TempDir()
	writeSpec(t, filepath.Join(dir, "demo", "v2.yaml"), []byte(yamlSpec))

	groups, _, err := ingestSpecs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || len(groups[0]) != 1 {
		t.Fatalf("want one service, got %v", groups)
	}
	sum := sha256.Sum256([]byte(yamlSpec))
	want := hex.EncodeToString(sum[:])
	if got := groups[0][0].Source.SHA256; got != want {
		t.Fatalf("Source.SHA256 = %s, want the hash of the original YAML %s", got, want)
	}
	if got := groups[0][0].Source.Path; !strings.HasSuffix(got, "v2.yaml") {
		t.Fatalf("Source.Path = %q, want the vendor's own file", got)
	}
}

// TestYAMLConversionIsDeterministic keeps `make generate` reproducible, which
// the generated-models CI job asserts byte-for-byte.
func TestYAMLConversionIsDeterministic(t *testing.T) {
	first, _, err := yamlToJSON([]byte(yamlSpec))
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		again, _, err := yamlToJSON([]byte(yamlSpec))
		if err != nil {
			t.Fatal(err)
		}
		if string(again) != string(first) {
			t.Fatal("yamlToJSON is not deterministic")
		}
	}
}

// TestYAMLNonStringKeysBecomeStrings covers the shape that would otherwise make
// json.Marshal refuse a valid document: an unquoted response code is an integer
// key in YAML and there are no integer keys in JSON.
func TestYAMLNonStringKeysBecomeStrings(t *testing.T) {
	out, _, err := yamlToJSON([]byte("responses:\n  200:\n    description: ok\n  true:\n    description: odd\n"))
	if err != nil {
		t.Fatal(err)
	}
	var round map[string]map[string]map[string]string
	if err := json.Unmarshal(out, &round); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if round["responses"]["200"]["description"] != "ok" {
		t.Fatalf("integer key lost: %s", out)
	}
	if round["responses"]["true"]["description"] != "odd" {
		t.Fatalf("boolean key lost: %s", out)
	}
}

// TestMalformedYAMLIsSkippedNotFatal keeps one unreadable vendor file from
// taking the whole generation down, the way a malformed JSON one does not.
func TestMalformedYAMLIsSkippedNotFatal(t *testing.T) {
	dir := t.TempDir()
	writeSpec(t, filepath.Join(dir, "broken", "v2.yaml"), []byte("openapi: \"3.0.0\"\n\tbad: [unclosed\n"))

	groups, n, err := ingestSpecs(context.Background(), dir)
	if err != nil {
		t.Fatalf("a malformed document must not fail the walk: %v", err)
	}
	if n != 0 || len(groups) != 0 {
		t.Fatalf("ingested %d service(s) from a malformed document", n)
	}
}

func writeSpec(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestYAMLDocumentIsDetectedWhateverTheKeySort is the bug this nearly shipped
// with, and the reason the probe exists.
//
// `Detect` is shown the head of a file. json.Marshal sorts a map's keys, so a
// document that leads with `openapi` in the vendor's YAML leads with
// `components` once re-serialized -- and in DigitalOcean's real 3 MB document
// `"openapi"` lands at byte 1,122,354, a megabyte past any sane head window.
// Every receiver declined it and the walk skipped it in silence, which is
// exactly the failure C42 is about, reintroduced by the fix for it.
func TestYAMLDocumentIsDetectedWhateverTheKeySort(t *testing.T) {
	// `components` sorts before `openapi` and is padded past any head window.
	var big strings.Builder
	big.WriteString("openapi: \"3.0.0\"\ninfo:\n  title: Bulky API\ncomponents:\n  schemas:\n")
	for i := range 400 {
		fmt.Fprintf(&big, "    Filler%03d:\n      type: string\n      description: %s\n", i, strings.Repeat("x", 64))
	}
	big.WriteString("paths:\n  /v2/things:\n    get:\n      operationId: list-things\n      responses:\n        200:\n          description: ok\n")

	converted, probe, err := yamlToJSON([]byte(big.String()))
	if err != nil {
		t.Fatal(err)
	}
	if i := strings.Index(string(converted), `"openapi"`); i < 4096 {
		t.Skipf("document too small to exercise the sort: openapi at %d", i)
	}
	if len(probe) > 4096 {
		t.Fatalf("the probe is %d bytes; it must stay small whatever the document weighs", len(probe))
	}
	if !(openapi.Receiver{}).Detect("bulky/v1.yaml", probe) {
		t.Fatalf("the probe does not identify the document: %s", probe)
	}

	dir := t.TempDir()
	writeSpec(t, filepath.Join(dir, "bulky", "v1.yaml"), []byte(big.String()))
	_, n, err := ingestSpecs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("ingested %d service(s); the document was skipped for want of a detectable head", n)
	}
}

// TestYAMLProbeCarriesOnlyTopLevelScalars pins what the probe is. Growing it
// into "the first N bytes of something" would put the size back in play.
func TestYAMLProbeCarriesOnlyTopLevelScalars(t *testing.T) {
	_, probe, err := yamlToJSON([]byte("openapi: \"3.0.0\"\nswagger: 2\ninfo:\n  title: X\npaths: {}\ntags:\n  - a\n"))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(probe, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["openapi"] != "3.0.0" {
		t.Fatalf("probe = %s, want the two top-level scalars only", probe)
	}
}
