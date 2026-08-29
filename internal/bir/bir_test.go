package bir

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// demoModel is a generated-model stand-in: one operation, one output shape
// with one member. Validation is only meaningful against a model, so every
// test here supplies one.
func demoModel() *model.Service {
	return &model.Service{
		ID:       "aws.demo",
		Protocol: model.ProtoAWSJSON10,
		Operations: []model.Operation{
			{Name: "MakeThing", Input: "com.demo#MakeThingRequest", Output: "com.demo#MakeThingResponse"},
			{Name: "GetThing", Input: "com.demo#GetThingRequest", Output: "com.demo#GetThingResponse"},
		},
		Shapes: map[string]model.Shape{
			"com.demo#MakeThingRequest": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Name": {Shape: "smithy.api#String", Required: true},
			}},
			"com.demo#MakeThingResponse": {Kind: model.KindStructure, Members: map[string]model.Member{
				"ThingId": {Shape: "smithy.api#String"},
			}},
			"com.demo#GetThingRequest": {Kind: model.KindStructure, Members: map[string]model.Member{
				"ThingId": {Shape: "smithy.api#String"},
			}},
			"com.demo#GetThingResponse": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Thing": {Shape: "com.demo#Thing"},
			}},
			"com.demo#Thing": {Kind: model.KindStructure},
		},
	}
}

const demoBundle = `schema: bir/1
service: aws.demo
provenance: authored
resources:
  thing:
    collection: things
    id:
      generate: { kind: hex, bytes: 8 }
      input_members: [ThingId]
    record:
      Id: id
      Name: input.Name
errors:
  NotFound:
    code: ResourceNotFoundException
    http: 400
    fault: client
    provenance: authored
operations:
  MakeThing:
    effects:
      - create: { resource: thing }
    output:
      ThingId: id
  GetThing:
    reads:
      rec: { resource: thing }
    require:
      - { cond: rec_found, error: NotFound }
    output:
      Thing: rec
`

func loadDemo(t *testing.T, body string) (*Service, error) {
	t.Helper()
	fsys := fstest.MapFS{"aws/demo/service.yaml": &fstest.MapFile{Data: []byte(body)}}
	return Load(fsys, "aws/demo", demoModel())
}

func TestLoadValidBundle(t *testing.T) {
	svc, err := loadDemo(t, demoBundle)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if svc.ServiceID != "aws.demo" || len(svc.Operations) != 2 || len(svc.Resources) != 1 {
		t.Fatalf("unexpected bundle: %+v", svc)
	}
	if svc.Compiled == nil || len(svc.Compiled.Programs) == 0 {
		t.Fatal("expressions were not compiled at load time")
	}
	// Compiled programs are keyed by their path so a failure names its source.
	if _, ok := svc.Compiled.Programs["operations.GetThing.output.Thing"]; !ok {
		t.Fatalf("missing compiled program; have %v", keysOf(svc.Compiled.Programs))
	}
}

// TestValidationRejects is the substance of this package: each case is a
// mistake that would otherwise reach a request. The loader must refuse it and
// say which path is wrong.
func TestValidationRejects(t *testing.T) {
	cases := []struct {
		name string
		edit func(string) string
		want string
	}{
		{
			name: "output member not in the model",
			edit: func(b string) string { return strings.Replace(b, "      ThingId: id", "      Nonsense: id", 1) },
			want: "is not a member of",
		},
		{
			name: "operation not in the model",
			edit: func(b string) string { return b + "  Imaginary:\n    output: {}\n" },
			want: "no such operation",
		},
		{
			name: "unknown error reference",
			edit: func(b string) string { return strings.Replace(b, "error: NotFound", "error: Missing", 1) },
			want: `unknown error "Missing"`,
		},
		{
			name: "unknown resource in a read",
			edit: func(b string) string {
				return strings.Replace(b, "rec: { resource: thing }", "rec: { resource: ghost }", 1)
			},
			want: `unknown resource "ghost"`,
		},
		{
			name: "expression references an unbound name",
			edit: func(b string) string { return strings.Replace(b, "cond: rec_found", "cond: typo_found", 1) },
			want: "undeclared reference",
		},
		{
			name: "expression does not parse",
			edit: func(b string) string { return strings.Replace(b, "Name: input.Name", "Name: 'input.'", 1) },
			want: "resources.thing.record.Name",
		},
		{
			name: "error row missing a fault class",
			edit: func(b string) string { return strings.Replace(b, "    fault: client\n", "", 1) },
			want: "fault must be client or server",
		},
		{
			name: "error row with an impossible status",
			edit: func(b string) string { return strings.Replace(b, "http: 400", "http: 9000", 1) },
			want: "out of range",
		},
		{
			name: "unknown provenance",
			edit: func(b string) string {
				return strings.Replace(b, "provenance: authored\nresources", "provenance: vibes\nresources", 1)
			},
			want: "unknown provenance",
		},
		{
			name: "unknown id generator",
			edit: func(b string) string { return strings.Replace(b, "kind: hex", "kind: sparkles", 1) },
			want: "unknown kind",
		},
		{
			name: "unknown key is a typo, not an extension",
			edit: func(b string) string {
				return strings.Replace(b, "    collection: things", "    collektion: things", 1)
			},
			want: "field collektion not found",
		},
		{
			name: "wrong schema version",
			edit: func(b string) string { return strings.Replace(b, "schema: bir/1", "schema: bir/99", 1) },
			want: "declares schema",
		},
		{
			name: "service id does not match the model",
			edit: func(b string) string { return strings.Replace(b, "service: aws.demo", "service: aws.other", 1) },
			want: "was loaded against model",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadDemo(t, tc.edit(demoBundle))
			if err == nil {
				t.Fatal("bundle was accepted; it should have been rejected")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error did not mention %q:\n%v", tc.want, err)
			}
		})
	}
}

// TestValidateNeedsShapes guards the specific regression the empty-shape
// catalog caused: without shapes there is nothing to validate against, so
// loading must refuse rather than silently accept anything.
func TestValidateNeedsShapes(t *testing.T) {
	fsys := fstest.MapFS{"aws/demo/service.yaml": &fstest.MapFile{Data: []byte(demoBundle)}}
	bare := demoModel()
	bare.Shapes = map[string]model.Shape{}
	_, err := Load(fsys, "aws/demo", bare)
	if err == nil || !strings.Contains(err.Error(), "carries no shapes") {
		t.Fatalf("want a no-shapes refusal, got %v", err)
	}
	if _, err := Load(fsys, "aws/demo", nil); err == nil {
		t.Fatal("loading without a model was accepted")
	}
}

// TestDuplicateDefinitionsAcrossFiles: splitting a service across files is
// supported, but defining the same thing twice is ambiguity, and letting the
// last file win is how drift starts.
func TestDuplicateDefinitionsAcrossFiles(t *testing.T) {
	fsys := fstest.MapFS{
		"aws/demo/service.yaml": &fstest.MapFile{Data: []byte(demoBundle)},
		"aws/demo/ops/more.yaml": &fstest.MapFile{Data: []byte(
			"schema: bir/1\nservice: aws.demo\noperations:\n  MakeThing:\n    output: {}\n")},
	}
	_, err := Load(fsys, "aws/demo", demoModel())
	if err == nil || !strings.Contains(err.Error(), "defined twice") {
		t.Fatalf("want a duplicate-definition error, got %v", err)
	}
}

// TestMultiFileBundle: the common case of splitting operations into ops/.
func TestMultiFileBundle(t *testing.T) {
	trimmed := strings.Replace(demoBundle, `  GetThing:
    reads:
      rec: { resource: thing }
    require:
      - { cond: rec_found, error: NotFound }
    output:
      Thing: rec
`, "", 1)
	fsys := fstest.MapFS{
		"aws/demo/service.yaml": &fstest.MapFile{Data: []byte(trimmed)},
		"aws/demo/ops/get.yaml": &fstest.MapFile{Data: []byte(`schema: bir/1
service: aws.demo
operations:
  GetThing:
    reads:
      rec: { resource: thing }
    require:
      - { cond: rec_found, error: NotFound }
    output:
      Thing: rec
`)},
	}
	svc, err := Load(fsys, "aws/demo", demoModel())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(svc.Operations) != 2 {
		t.Fatalf("want both operations, got %v", keysOf(svc.Operations))
	}
}

func TestProvenanceRanking(t *testing.T) {
	if ProvProbed.Rank() <= ProvDeclared.Rank() || ProvDeclared.Rank() <= ProvAuthored.Rank() {
		t.Fatal("provenance must rank probed > declared > authored")
	}
	if Provenance("guess").Valid() {
		t.Fatal("unknown provenance reported valid")
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// listModel adds a listing whose item shape declares one member, so a record
// with anything else in it is a listing no SDK can read in full.
func listModel() *model.Service {
	return &model.Service{
		ID:       "aws.demo",
		Protocol: model.ProtoAWSJSON10,
		Operations: []model.Operation{
			{Name: "ListThings", Output: "com.demo#ListThingsResponse"},
		},
		Shapes: map[string]model.Shape{
			"com.demo#ListThingsResponse": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Things": {Shape: "com.demo#ThingSummaryList"},
			}},
			"com.demo#ThingSummaryList": {Kind: model.KindList, Member: "com.demo#ThingSummary"},
			"com.demo#ThingSummary": {Kind: model.KindStructure, Members: map[string]model.Member{
				"ThingId": {Shape: "smithy.api#String"},
			}},
		},
	}
}

const listBundle = `schema: bir/1
service: aws.demo
provenance: authored
resources:
  thing:
    collection: things
    id:
      generate: { kind: hex, bytes: 8 }
    record:
      ThingId: id
operations:
  ListThings:
    list: { resource: thing, member: Things }
`

func loadList(t *testing.T, body string) (*Service, error) {
	t.Helper()
	fsys := fstest.MapFS{"aws/demo/service.yaml": &fstest.MapFile{Data: []byte(body)}}
	return Load(fsys, "aws/demo", listModel())
}

// TestListItemMembersMustBeDeclared covers the half of the output contract that
// checkOutputMember does not: naming a member every SDK reads and then filling
// it with items every SDK ignores.
func TestListItemMembersMustBeDeclared(t *testing.T) {
	if _, err := loadList(t, listBundle); err != nil {
		t.Fatalf("the declared listing must load: %v", err)
	}

	t.Run("undeclared record member", func(t *testing.T) {
		body := strings.Replace(listBundle, "      ThingId: id\n",
			"      ThingId: id\n      Colour: \"'red'\"\n", 1)
		_, err := loadList(t, body)
		if err == nil {
			t.Fatal("a record member the item shape does not declare must be rejected")
		}
		if !strings.Contains(err.Error(), `record member "Colour"`) ||
			!strings.Contains(err.Error(), "no SDK can read") {
			t.Fatalf("the error must name the member and why it matters: %v", err)
		}
	})

	t.Run("undeclared view", func(t *testing.T) {
		body := strings.Replace(listBundle, "      ThingId: id\n",
			"      ThingId: id\n    views:\n      loud: \"true\"\n", 1)
		_, err := loadList(t, body)
		if err == nil {
			t.Fatal("a view is merged into the record, so it must be checked too")
		}
		if !strings.Contains(err.Error(), `view "loud"`) {
			t.Fatalf("the error must name the view: %v", err)
		}
	})

	// A projection builds the items from an expression rather than from the
	// record, so the record is no longer what is listed and the check does not
	// apply. This is the escape hatch the three bundles this check caught use.
	t.Run("a projection lifts the check", func(t *testing.T) {
		body := strings.Replace(listBundle, "      ThingId: id\n",
			"      ThingId: id\n      Colour: \"'red'\"\n", 1) +
			"    output:\n      Things: \"items.map(t, {'ThingId': t.ThingId})\"\n"
		if _, err := loadList(t, body); err != nil {
			t.Fatalf("a projected listing must load: %v", err)
		}
	})
}
