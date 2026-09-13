package graphql

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// fixture is one schema small enough to read and wide enough to exercise every
// mapping: a required argument, an argument with a default, a list, an enum, a
// union, an input object, a custom scalar, a reference to a type the document
// never defines, and a cycle (Project has services, a Service has its project).
const fixture = `{"data":{"__schema":{
 "queryType":{"name":"Query"},
 "mutationType":{"name":"Mutation"},
 "types":[
  {"kind":"OBJECT","name":"Query","fields":[
    {"name":"project","description":"one project","args":[
      {"name":"id","type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"String"}}}],
     "type":{"kind":"OBJECT","name":"Project"}},
    {"name":"projects","args":[
      {"name":"first","type":{"kind":"SCALAR","name":"Int"},"defaultValue":"10"},
      {"name":"orderBy","type":{"kind":"ENUM","name":"Order"}}],
     "type":{"kind":"NON_NULL","ofType":{"kind":"LIST","ofType":{"kind":"OBJECT","name":"Project"}}}},
    {"name":"ping","args":[],"type":{"kind":"SCALAR","name":"Boolean"}}]},
  {"kind":"OBJECT","name":"Mutation","fields":[
    {"name":"projectCreate","args":[
      {"name":"input","type":{"kind":"NON_NULL","ofType":{"kind":"INPUT_OBJECT","name":"ProjectInput"}}}],
     "type":{"kind":"OBJECT","name":"Project"}}]},
  {"kind":"OBJECT","name":"Project","description":"a project","fields":[
    {"name":"id","type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"String"}}},
    {"name":"createdAt","type":{"kind":"SCALAR","name":"DateTime"}},
    {"name":"owner","type":{"kind":"UNION","name":"Owner"}},
    {"name":"missing","type":{"kind":"OBJECT","name":"NeverDefined"}},
    {"name":"services","args":[{"name":"first","type":{"kind":"SCALAR","name":"Int"}}],
     "type":{"kind":"LIST","ofType":{"kind":"OBJECT","name":"Service"}}}]},
  {"kind":"OBJECT","name":"Service","fields":[
    {"name":"id","type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"String"}}},
    {"name":"project","type":{"kind":"OBJECT","name":"Project"}}]},
  {"kind":"OBJECT","name":"User","fields":[{"name":"id","type":{"kind":"SCALAR","name":"String"}}]},
  {"kind":"OBJECT","name":"Team","fields":[{"name":"id","type":{"kind":"SCALAR","name":"String"}}]},
  {"kind":"UNION","name":"Owner","possibleTypes":[
    {"kind":"OBJECT","name":"User"},{"kind":"OBJECT","name":"Team"}]},
  {"kind":"INPUT_OBJECT","name":"ProjectInput","inputFields":[
    {"name":"name","type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"String"}}},
    {"name":"public","type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"Boolean"}},"defaultValue":"false"}]},
  {"kind":"ENUM","name":"Order","enumValues":[{"name":"NAME"},{"name":"CREATED_AT"}]},
  {"kind":"SCALAR","name":"String"},{"kind":"SCALAR","name":"Int"},
  {"kind":"SCALAR","name":"Boolean"},{"kind":"SCALAR","name":"DateTime"}]}}}`

func ingest(t *testing.T) model.Service {
	t.Helper()
	svcs, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "vendor/api.json"}, []byte(fixture))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if len(svcs) != 1 {
		t.Fatalf("want one service, got %d", len(svcs))
	}
	return svcs[0]
}

func TestDetect(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
		want bool
	}{
		{"response envelope", fixture, true},
		{"bare schema", `{"__schema":{"queryType":{"name":"Query"},"types":[]}}`, true},
		{"truncated mid-document", `{"data":{"__schema":{"queryType":{"name":"Que`, true},
		{"openapi", `{"openapi":"3.0.0","paths":{}}`, false},
		{"smithy", `{"smithy":"2.0","shapes":{}}`, false},
		{"discovery", `{"discoveryVersion":"v1","resources":{}}`, false},
		{"not json", `# a readme`, false},
		{"a schema with no query type", `{"__schema":{"types":[]}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (Receiver{}).Detect("x.json", []byte(tc.doc)); got != tc.want {
				t.Fatalf("Detect = %v, want %v", got, tc.want)
			}
		})
	}
}

// A root field is an operation. The query type's fields are readonly and the
// mutation type's are not, which is the only thing GraphQL says about effect.
func TestRootFieldsBecomeOperations(t *testing.T) {
	svc := ingest(t)
	if svc.ID != "vendor.api" {
		t.Fatalf("id = %q", svc.ID)
	}
	got := map[string]model.Operation{}
	for _, o := range svc.Operations {
		got[o.Name] = o
	}
	for _, tc := range []struct {
		name     string
		output   string
		readonly bool
	}{
		{"project", "Project", true},
		{"projects", "Project.list", true},
		{"ping", "Boolean", true},
		{"projectCreate", "Project", false},
	} {
		o, ok := got[tc.name]
		if !ok {
			t.Fatalf("no operation %q", tc.name)
		}
		if o.Output != tc.output {
			t.Errorf("%s output = %q, want %q", tc.name, o.Output, tc.output)
		}
		if o.Readonly != tc.readonly {
			t.Errorf("%s readonly = %v, want %v", tc.name, o.Readonly, tc.readonly)
		}
		if o.HTTP.Method != "POST" || o.HTTP.URI != defaultEndpoint || o.HTTP.Code != 200 {
			t.Errorf("%s binding = %+v", tc.name, o.HTTP)
		}
		if o.Confidence != model.ConfDeclared {
			t.Errorf("%s confidence = %q", tc.name, o.Confidence)
		}
	}
	if len(got) != 4 {
		t.Errorf("operations = %d, want 4", len(got))
	}
}

// NON_NULL is not a type here, it is `required` on the member that carries it.
// A default contradicts it: the server supplies the value the caller omits, so
// requiring it of the caller would refuse a request the schema accepts.
func TestArgumentsBecomeAnInputShape(t *testing.T) {
	svc := ingest(t)
	for _, o := range svc.Operations {
		switch o.Name {
		case "project":
			in := svc.Shapes[o.Input]
			if in.Kind != model.KindStructure {
				t.Fatalf("project.input kind = %q", in.Kind)
			}
			if m := in.Members["id"]; !m.Required || m.Shape != "String" {
				t.Errorf("project.input.id = %+v, want required String", m)
			}
		case "projects":
			in := svc.Shapes[o.Input]
			if m := in.Members["first"]; m.Required || m.Default != "10" {
				t.Errorf("projects.input.first = %+v, want optional with default 10", m)
			}
			if m := in.Members["orderBy"]; m.Shape != "Order" {
				t.Errorf("projects.input.orderBy = %+v", m)
			}
			// NON_NULL with a default is the case the rule is actually about:
			// the argument cannot be null, and the caller may still omit it.
			pi := svc.Shapes["ProjectInput"]
			if m := pi.Members["public"]; m.Required || m.Default != "false" {
				t.Errorf("ProjectInput.public = %+v, want optional despite NON_NULL", m)
			}
			if m := pi.Members["name"]; !m.Required {
				t.Errorf("ProjectInput.name = %+v, want required", m)
			}
		case "ping":
			// A field with no arguments still gets a shape, because an
			// operation the engine validates must have one to validate against.
			in, ok := svc.Shapes[o.Input]
			if !ok {
				t.Fatal("ping has no input shape")
			}
			if in.Kind != model.KindStructure || len(in.Members) != 0 {
				t.Errorf("ping.input = %+v, want an empty structure", in)
			}
		}
	}
}

func TestTypeMapping(t *testing.T) {
	svc := ingest(t)
	for _, tc := range []struct {
		id   string
		kind model.ShapeKind
	}{
		{"Project", model.KindStructure},
		{"ProjectInput", model.KindStructure},
		{"Owner", model.KindUnion},
		{"Order", model.KindEnum},
		{"Project.list", model.KindList},
		{"String", model.KindString},
		{"Int", model.KindInteger},
		{"Boolean", model.KindBoolean},
		// A custom scalar serializes however its server likes and the
		// specification says nothing about which, so it is "any JSON" rather
		// than a guess that would reject what the real service accepts.
		{"DateTime", model.KindDocument},
		// A reference the document never defines still gets a shape, because a
		// dangling reference is the one thing consumers may assume cannot
		// happen. Recording it as opaque says honestly that nothing is known.
		{"NeverDefined", model.KindDocument},
	} {
		sh, ok := svc.Shapes[tc.id]
		if !ok {
			t.Errorf("no shape %q", tc.id)
			continue
		}
		if sh.Kind != tc.kind {
			t.Errorf("%s kind = %q, want %q", tc.id, sh.Kind, tc.kind)
		}
	}
	if sh := svc.Shapes["Project.list"]; sh.Member != "Project" {
		t.Errorf("list member = %q", sh.Member)
	}
	if sh := svc.Shapes["Order"]; strings.Join(sh.EnumValues, ",") != "CREATED_AT,NAME" {
		t.Errorf("enum values = %v, want them sorted", sh.EnumValues)
	}
	owner := svc.Shapes["Owner"]
	names := make([]string, 0, len(owner.Members))
	for n := range owner.Members {
		names = append(names, n)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "Team,User" {
		t.Errorf("union members = %v", names)
	}
	// A field's own arguments belong to a selection, which this model does not
	// carry, so `services(first:)` contributes a member and not an argument.
	if m := svc.Shapes["Project"].Members["services"]; m.Shape != "Service.list" {
		t.Errorf("Project.services = %+v", m)
	}
}

// The schema is a cyclic graph. Walking it must terminate and must still close.
func TestCyclesTerminateAndTheGraphIsClosed(t *testing.T) {
	svc := ingest(t)
	if m := svc.Shapes["Service"].Members["project"]; m.Shape != "Project" {
		t.Fatalf("Service.project = %+v, want the cycle back to Project", m)
	}
	for id, sh := range svc.Shapes {
		for name, m := range sh.Members {
			if _, ok := svc.Shapes[m.Shape]; !ok {
				t.Errorf("dangling member %s.%s -> %q", id, name, m.Shape)
			}
		}
		if sh.Member != "" {
			if _, ok := svc.Shapes[sh.Member]; !ok {
				t.Errorf("dangling list member %s -> %q", id, sh.Member)
			}
		}
	}
	for _, o := range svc.Operations {
		if _, ok := svc.Shapes[o.Input]; !ok {
			t.Errorf("%s: input %q is not a shape", o.Name, o.Input)
		}
		if _, ok := svc.Shapes[o.Output]; !ok {
			t.Errorf("%s: output %q is not a shape", o.Name, o.Output)
		}
	}
}

func TestRefusesADocumentWithNothingToServe(t *testing.T) {
	for _, tc := range []struct{ name, doc string }{
		{"no schema at all", `{"paths":{}}`},
		{"no query type", `{"__schema":{"types":[]}}`},
		{"a query type that is not in the type list", `{"__schema":{"queryType":{"name":"Query"},"types":[]}}`},
		{"a query type with no fields", `{"__schema":{"queryType":{"name":"Query"},"types":[{"kind":"OBJECT","name":"Query","fields":[]}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "v/a.json"}, []byte(tc.doc)); err == nil {
				t.Fatal("want an error, got none")
			}
		})
	}
}

// Ingestion is deterministic: the generated models are committed and CI asserts
// they reproduce byte for byte, so a map iteration leaking into operation order
// would be a rebuild that differs from its own checkout.
func TestOperationOrderIsStable(t *testing.T) {
	first := ingest(t)
	for i := 0; i < 8; i++ {
		again := ingest(t)
		if len(again.Operations) != len(first.Operations) {
			t.Fatalf("operation count moved: %d then %d", len(first.Operations), len(again.Operations))
		}
		for j := range again.Operations {
			if again.Operations[j].Name != first.Operations[j].Name {
				t.Fatalf("operation %d moved: %q then %q", j, first.Operations[j].Name, again.Operations[j].Name)
			}
		}
	}
}

// The endpoint is the one thing the schema cannot say, so it comes from where
// the document was fetched: the path of that URL is the endpoint the schema
// describes, not an inference about it.
func TestEndpointComesFromProvenance(t *testing.T) {
	for _, tc := range []struct{ name, repo, want string }{
		{"a fetched document", "https://backboard.railway.com/graphql/v2", "/graphql/v2"},
		{"a trailing slash is not part of the path", "https://api.example.com/graphql/", "/graphql"},
		{"a query string is not part of the path", "https://api.example.com/gql?foo=1", "/gql"},
		{"no provenance at all", "", defaultEndpoint},
		{"a host with no path says nothing more than the default", "https://api.example.com", defaultEndpoint},
		{"nor does a bare root", "https://api.example.com/", defaultEndpoint},
		{"an unparseable URL degrades to the default", "://not a url", defaultEndpoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svcs, err := (Receiver{}).Ingest(context.Background(),
				model.SourceRef{Repo: tc.repo, Path: "vendor/api.json"}, []byte(fixture))
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range svcs[0].Operations {
				if o.HTTP.URI != tc.want {
					t.Fatalf("%s binds to %q, want %q", o.Name, o.HTTP.URI, tc.want)
				}
				if o.HTTP.Method != "POST" {
					t.Errorf("%s method = %q", o.Name, o.HTTP.Method)
				}
			}
		})
	}
}
