package main

import (
	"os"
	"path/filepath"
	"sort"
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
	got, err := narrow(platform(), selector{Paths: []string{"/v4/kv/"}})
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
	_, err := narrow(platform(), selector{Paths: []string{"/v4/spectrum/"}})
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
	got, err := narrow(before, selector{})
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
	if strings.Join(got.Paths, ",") != "/a/,/b/" {
		t.Fatalf("prefixes = %v", got.Paths)
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
	if len(got) != 2 || !got[0].Select.empty() {
		t.Fatalf("entries = %+v", got)
	}
	if len(got[1].Select.Paths) != 1 || got[1].Select.Paths[0] != "/v4/kv/" {
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

// oneEndpoint is the shape a GraphQL document ingests into: every operation
// answers at the same URI, so a `paths=` selector is all-or-nothing on it and
// cannot express "these three".
func oneEndpoint() model.Service {
	svc := model.Service{
		ID:     "vendor.api",
		Shapes: map[string]model.Shape{},
	}
	for _, name := range []string{"project", "projects", "projectCreate", "deployment", "deploymentCreate"} {
		in, out := name+".input", name+".output"
		svc.Operations = append(svc.Operations, model.Operation{
			Name:   name,
			HTTP:   model.HTTPBinding{Method: "POST", URI: "/graphql", Code: 200},
			Input:  in,
			Output: out,
		})
		svc.Shapes[in] = model.Shape{ID: in, Kind: model.KindStructure}
		svc.Shapes[out] = model.Shape{ID: out, Kind: model.KindStructure}
	}
	return svc
}

// TestNarrowByFieldSelectsFromOneEndpoint is why `fields=` exists. Narrowing by
// URI keeps all five here or none; naming the operations keeps three, and drops
// the shapes only the other two reach.
func TestNarrowByFieldSelectsFromOneEndpoint(t *testing.T) {
	got, err := narrow(oneEndpoint(), selector{Fields: []string{"project", "projects", "projectCreate"}})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, op := range got.Operations {
		names = append(names, op.Name)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "project,projectCreate,projects" {
		t.Fatalf("kept %v", names)
	}
	if _, ok := got.Shapes["deployment.output"]; ok {
		t.Error("a shape only the dropped operations reach survived")
	}
	if _, ok := got.Shapes["project.output"]; !ok {
		t.Error("a shape a kept operation reaches was dropped")
	}
	// The prefix every operation shares would have kept all five, which is the
	// all-or-nothing this selector exists to escape.
	all, err := narrow(oneEndpoint(), selector{Paths: []string{"/graphql"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Operations) != 5 {
		t.Fatalf("paths= on one endpoint kept %d, want all 5", len(all.Operations))
	}
}

// A name is matched whole. `project` must not take `projectCreate` with it --
// the prefix-matching that C34 and the railway routing bug are both about.
func TestNarrowByFieldMatchesWholeNames(t *testing.T) {
	got, err := narrow(oneEndpoint(), selector{Fields: []string{"project"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Operations) != 1 || got.Operations[0].Name != "project" {
		var names []string
		for _, op := range got.Operations {
			names = append(names, op.Name)
		}
		t.Fatalf("kept %v, want only project", names)
	}
}

// The two kinds are alternatives, not a filter pair: a line declaring both asks
// for the union of two ways of pointing at operations.
func TestNarrowUnionsTheTwoSelectorKinds(t *testing.T) {
	svc := oneEndpoint()
	svc.Operations = append(svc.Operations, model.Operation{
		Name:   "health",
		HTTP:   model.HTTPBinding{Method: "GET", URI: "/healthz", Code: 200},
		Input:  "project.input",
		Output: "project.output",
	})
	got, err := narrow(svc, selector{Paths: []string{"/healthz"}, Fields: []string{"project"}})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, op := range got.Operations {
		names = append(names, op.Name)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "health,project" {
		t.Fatalf("kept %v, want the union", names)
	}
}

// A selector matching nothing is fatal whichever kind it is, and the error says
// which kind was tried -- the report is the only way to tell a typo from a
// document that genuinely moved.
func TestNarrowRefusesAFieldSelectorThatMatchesNothing(t *testing.T) {
	_, err := narrow(oneEndpoint(), selector{Fields: []string{"noSuchField"}})
	if err == nil {
		t.Fatal("a field selector matching no operation produced a model instead of an error")
	}
	for _, want := range []string{"vendor.api", "fields=noSuchField", "5"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q: %v", want, err)
		}
	}
}

// Each kind refuses the other's spelling, because a value under the wrong key
// is a typo that would otherwise read as a selector matching nothing.
func TestParseSelectorKeepsTheTwoKindsApart(t *testing.T) {
	got, err := parseSelector([]string{"fields=b,a"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.Fields, ",") != "a,b" {
		t.Fatalf("fields = %v, want them sorted", got.Fields)
	}
	if len(got.Paths) != 0 {
		t.Errorf("fields= populated Paths: %+v", got)
	}
	both, err := parseSelector([]string{"paths=/v4/", "fields=kv"})
	if err != nil {
		t.Fatal(err)
	}
	if len(both.Paths) != 1 || len(both.Fields) != 1 {
		t.Fatalf("one line declaring both = %+v", both)
	}
	if _, err := parseSelector([]string{"fields=/v4/kv"}); err == nil {
		t.Error("a URI prefix was accepted as an operation name")
	}
	if _, err := parseSelector([]string{"paths=kv"}); err == nil {
		t.Error("an operation name was accepted as a URI prefix")
	}
}
