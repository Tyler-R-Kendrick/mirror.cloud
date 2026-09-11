package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// platform is a miniature of the shape this exists for: one document carrying
// two unrelated product surfaces, where only one is wanted. `shared` is
// reachable from both, `orphan` from neither, and `deep` only through a list
// inside a structure -- so a walk that stops at the first level keeps it and
// fails here.
func platform() model.Service {
	return model.Service{
		ID: "vendor.api",
		Operations: []model.Operation{
			{Name: "KvGet", HTTP: model.HTTPBinding{Method: "GET", URI: "/v4/kv/values/{k}"},
				Input: "KvGetIn", Output: "KvGetOut", Errors: []string{"KvFault"}},
			{Name: "KvPut", HTTP: model.HTTPBinding{Method: "PUT", URI: "/v4/kv/values/{k}"},
				Input: "KvPutIn", Output: "Shared"},
			{Name: "DnsList", HTTP: model.HTTPBinding{Method: "GET", URI: "/v4/dns/records"},
				Input: "DnsIn", Output: "DnsOut"},
		},
		Shapes: map[string]model.Shape{
			"KvGetIn":  {ID: "KvGetIn", Kind: model.KindStructure, Members: map[string]model.Member{"k": {Shape: "Str"}}},
			"KvGetOut": {ID: "KvGetOut", Kind: model.KindStructure, Members: map[string]model.Member{"items": {Shape: "DeepList"}, "s": {Shape: "Shared"}}},
			"DeepList": {ID: "DeepList", Kind: model.KindList, Member: "Deep"},
			"Deep":     {ID: "Deep", Kind: model.KindStructure, Members: map[string]model.Member{"v": {Shape: "Str"}}},
			"KvPutIn":  {ID: "KvPutIn", Kind: model.KindStructure, Members: map[string]model.Member{"body": {Shape: "Str"}}},
			"KvFault":  {ID: "KvFault", Kind: model.KindStructure},
			"Shared":   {ID: "Shared", Kind: model.KindStructure, Members: map[string]model.Member{"m": {Shape: "StrMap"}}},
			"StrMap":   {ID: "StrMap", Kind: model.KindMap, Key: "Str", Member: "Str"},
			"Str":      {ID: "Str", Kind: model.KindString},
			"DnsIn":    {ID: "DnsIn", Kind: model.KindStructure},
			"DnsOut":   {ID: "DnsOut", Kind: model.KindStructure, Members: map[string]model.Member{"only": {Shape: "DnsOnly"}}},
			"DnsOnly":  {ID: "DnsOnly", Kind: model.KindStructure},
			"orphan":   {ID: "orphan", Kind: model.KindStructure},
		},
	}
}

// TestNarrowKeepsOnlyWhatTheSelectedOperationsReach is the whole point: a
// vendor's one-document-per-platform publication costs what its selected
// surface costs, not what the platform costs.
func TestNarrowKeepsOnlyWhatTheSelectedOperationsReach(t *testing.T) {
	got, err := narrow(platform(), []string{"/v4/kv/"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, op := range got.Operations {
		names = append(names, op.Name)
	}
	if strings.Join(names, ",") != "KvGet,KvPut" {
		t.Fatalf("operations = %v, want KvGet,KvPut", names)
	}
	for _, want := range []string{"KvGetIn", "KvGetOut", "KvPutIn", "KvFault", "Shared", "Str", "StrMap", "DeepList", "Deep"} {
		if _, ok := got.Shapes[want]; !ok {
			t.Errorf("%s is reachable from a kept operation and was dropped", want)
		}
	}
	for _, gone := range []string{"DnsIn", "DnsOut", "DnsOnly", "orphan"} {
		if _, ok := got.Shapes[gone]; ok {
			t.Errorf("%s is reachable from nothing kept and was retained", gone)
		}
	}
	// Every shape a survivor names must still be present, or the model
	// describes a response the codec cannot serialize -- the dangling
	// reference the receiver refuses at ingest, reintroduced by pruning.
	for _, op := range got.Operations {
		for _, id := range []string{op.Input, op.Output} {
			if id == "" {
				continue
			}
			if _, ok := got.Shapes[id]; !ok {
				t.Errorf("%s names %s, which pruning removed", op.Name, id)
			}
		}
	}
	for id, shape := range got.Shapes {
		for name, m := range shape.Members {
			if _, ok := got.Shapes[m.Shape]; !ok {
				t.Errorf("%s.%s names %s, which pruning removed", id, name, m.Shape)
			}
		}
	}
}

// TestNarrowRefusesASelectorThatMatchesNothing is C42's lesson applied before
// the fact. A selector nothing answers would otherwise emit a model with no
// operations, which is indistinguishable from a service nobody has written --
// the same silence as a document ingested and dropped, or a model generated
// and not embedded.
func TestNarrowRefusesASelectorThatMatchesNothing(t *testing.T) {
	_, err := narrow(platform(), []string{"/v4/spectrum/"})
	if err == nil {
		t.Fatal("a selector matching no operation produced a model instead of an error")
	}
	for _, want := range []string{"vendor.api", "/v4/spectrum/", "3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q: %v", want, err)
		}
	}
}

// TestNarrowWithoutASelectorChangesNothing is what keeps all 152 existing
// models byte-identical: narrowing is opt-in per service.
func TestNarrowWithoutASelectorChangesNothing(t *testing.T) {
	before := platform()
	got, err := narrow(before, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Operations) != len(before.Operations) || len(got.Shapes) != len(before.Shapes) {
		t.Fatalf("a service declaring no selector was narrowed: %d ops, %d shapes",
			len(got.Operations), len(got.Shapes))
	}
	svcs, err := narrowAll([]model.Service{before}, []setEntry{{ID: "vendor.api"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(svcs[0].Shapes) != len(before.Shapes) {
		t.Fatalf("narrowAll narrowed a service with no Paths")
	}
}

// TestParseSelector pins the spelling, because a set line is the only place a
// selector is written and a typo there must not read as "no selector".
func TestParseSelector(t *testing.T) {
	got, err := parseSelector([]string{"paths=/b/,/a/"})
	if err != nil {
		t.Fatal(err)
	}
	// Sorted, so the same line always narrows the same way.
	if strings.Join(got, ",") != "/a/,/b/" {
		t.Fatalf("prefixes = %v", got)
	}
	if _, err := parseSelector([]string{"tags=kv"}); err == nil {
		t.Error("an unknown field was accepted as a selector")
	}
	if _, err := parseSelector([]string{"paths=kv"}); err == nil {
		t.Error("a prefix that is not a URI path was accepted")
	}
}

// TestLoadSetReadsASelector covers the line as it is actually written, and
// that a malformed one fails the load rather than silently generating
// everything.
func TestLoadSetReadsASelector(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mirror.set")
	if err := os.WriteFile(path, []byte(
		"# comment\naws.s3 emulate\nvendor.api emulate paths=/v4/kv/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := loadSet(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].Paths) != 0 {
		t.Fatalf("entries = %+v", got)
	}
	if len(got[1].Paths) != 1 || got[1].Paths[0] != "/v4/kv/" {
		t.Fatalf("selector = %+v", got[1])
	}

	bad := filepath.Join(dir, "bad.set")
	if err := os.WriteFile(bad, []byte("vendor.api emulate paths=nope\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSet(bad); err == nil {
		t.Fatal("a malformed selector loaded as though there were none")
	}
}

// TestCatalogModeIgnoresAPathSelector pins the reason applySet takes a flag.
//
// A `paths=` selector is a statement about the vendor's document. The
// bootstrap catalog is a hand-written stand-in whose bindings are
// approximations -- most of its operations sit at POST / because nothing there
// ever needed a URI -- so a selector written against the vendor's paths
// matches none of them. Since narrow treats a selector that matches nothing as
// fatal, and rightly so, applying one to the catalog does not narrow the
// catalog: it takes `mirrorgen --catalog` down for that service and every
// service after it.
//
// The stand-in here is deliberately a service whose operation URI is "/", the
// catalog's own shape, against a selector that is perfectly good for the real
// document. Out of catalog mode the same call must still fail, because a
// selector that matches nothing in the vendor's document is a real defect and
// this must not become a licence to ignore selectors generally.
func TestCatalogModeIgnoresAPathSelector(t *testing.T) {
	stub := model.Service{
		ID:         "vendor.api",
		Operations: []model.Operation{{Name: "KvGet", HTTP: model.HTTPBinding{Method: "POST", URI: "/"}, Input: "In", Output: "Out"}},
		Shapes: map[string]model.Shape{
			"In":  {ID: "In", Kind: model.KindStructure},
			"Out": {ID: "Out", Kind: model.KindStructure},
		},
	}
	want := []setEntry{{ID: "vendor.api", Tier: model.TierMock, Paths: []string{"/v4/kv/"}}}

	got, err := applySet([]model.Service{stub}, want, true)
	if err != nil {
		t.Fatalf("catalog mode: %v", err)
	}
	if len(got) != 1 || len(got[0].Operations) != 1 {
		t.Fatalf("catalog mode: got %d service(s) with %d operation(s), want the stand-in untouched",
			len(got), len(got[0].Operations))
	}

	if _, err := applySet([]model.Service{stub}, want, false); err == nil {
		t.Error("spec mode: a selector that matches no operation must still be fatal")
	}
}
