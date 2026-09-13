package model

import (
	"os"
	"strings"
	"testing"
)

func TestNoServiceLiterals(t *testing.T) {
	b, err := os.ReadFile("model.go")
	if err != nil {
		t.Fatal(err)
	}
	src := stripComments(string(b))
	for _, n := range []string{`"s3"`, `"dynamodb"`, `"sqs"`, `"sns"`, `"iam"`, `"sts"`} {
		if strings.Contains(src, n) {
			t.Errorf("service literal %s in model.go", n)
		}
	}
}

func TestLookups(t *testing.T) {
	op := Operation{Name: "Get"}
	service := Service{ID: "example", Operations: []Operation{op}}
	bundle := Bundle{Services: []Service{service}}
	if got := bundle.ServiceByID("example"); got == nil || got.OperationByName("Get") == nil {
		t.Fatal("lookup missed existing service or operation")
	}
	if bundle.ServiceByID("missing") != nil || (*Bundle)(nil).ServiceByID("example") != nil || service.OperationByName("missing") != nil || (*Service)(nil).OperationByName("Get") != nil {
		t.Fatal("lookup found missing service or operation")
	}
}

func stripComments(s string) string {
	var out strings.Builder
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '/' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			i += 2
			continue
		}
		out.WriteByte(s[i])
		i++
	}
	return out.String()
}

// TestBodyMembersLooksThroughAUnion covers the rule that makes a union output
// projectable at all. DigitalOcean's droplet create is the only operation in
// the tree whose response is one -- `oneOf` of {droplet, links} and
// {droplets, links}, with no discriminator anywhere on the wire -- so what a
// reader may find in the body is what either arm declares. Asking the union
// shape itself yields option0 and option1, which appear in no response, and a
// bundle projecting `droplet` would be rejected as naming an unknown member.
//
// The scalar union is the case that must NOT gain members: Cloudflare's KV
// value is `oneOf` a string and a blob, which is an opaque body with nothing
// to project, and ScalarBody answers it instead.
func TestBodyMembersLooksThroughAUnion(t *testing.T) {
	svc := &Service{Shapes: map[string]Shape{
		"Structure": {Kind: KindStructure, Members: map[string]Member{"plain": {Shape: "S"}}},
		"Either": {Kind: KindUnion, Members: map[string]Member{
			"option0": {Shape: "One"},
			"option1": {Shape: "Many"},
		}},
		"One":  {Kind: KindStructure, Members: map[string]Member{"droplet": {Shape: "S"}, "links": {Shape: "S"}}},
		"Many": {Kind: KindStructure, Members: map[string]Member{"droplets": {Shape: "S"}, "links": {Shape: "S"}}},
		"Bytes": {Kind: KindUnion, Members: map[string]Member{
			"option0": {Shape: "S"},
			"option1": {Shape: "B"},
		}},
		"S": {Kind: KindString},
		"B": {Kind: KindBlob},
		// A document may reference itself in a circle, and a receiver is not
		// obliged to have noticed; the walk must end rather than recurse.
		"Loop": {Kind: KindUnion, Members: map[string]Member{"option0": {Shape: "Loop"}}},
	}}
	for _, test := range []struct {
		shape string
		want  []string
	}{
		{"Structure", []string{"plain"}},
		{"Either", []string{"droplet", "droplets", "links"}},
		{"Bytes", nil},
		{"Loop", nil},
		{"Absent", nil},
	} {
		got := svc.BodyMembers(test.shape)
		if len(got) != len(test.want) {
			t.Fatalf("%s: %v, want %v", test.shape, got, test.want)
		}
		for _, name := range test.want {
			if _, ok := got[name]; !ok {
				t.Fatalf("%s: %v is missing %q", test.shape, got, name)
			}
		}
	}
}

// TestScalarBodyAdmitsEveryScalar pins what may claim to BE a body: every
// scalar kind and a union of nothing but those, and nothing that has members.
//
// The reasoning for the set is on scalarKindBody. What this test is for is the
// boundary, which is the part a later edit can move without noticing: the
// admitted kinds are enumerated one by one so widening the set silently is not
// possible, and the refused ones are enumerated beside them so narrowing it
// back is caught in the same place. The union arm is checked in both
// directions because "every arm is a body" and "some arm is a body" differ
// only on the mixed case.
func TestScalarBodyAdmitsEveryScalar(t *testing.T) {
	svc := &Service{Shapes: map[string]Shape{
		"S": {Kind: KindString}, "B": {Kind: KindBlob},
		"Bool": {Kind: KindBoolean}, "Int": {Kind: KindInteger},
		"Long": {Kind: KindLong}, "Float": {Kind: KindFloat}, "Double": {Kind: KindDouble},
		"Struct": {Kind: KindStructure, Members: map[string]Member{"a": {Shape: "S"}}},
		"List":   {Kind: KindList, Member: "S"},
		"Map":    {Kind: KindMap, Key: "S", Member: "S"},
		"Enum":   {Kind: KindEnum, EnumValues: []string{"A"}},
		"Doc":    {Kind: KindDocument},
		// A union is a body only when every arm is one.
		"TextOrBytes": {Kind: KindUnion, Members: map[string]Member{"option0": {Shape: "S"}, "option1": {Shape: "B"}}},
		"BoolOrInt":   {Kind: KindUnion, Members: map[string]Member{"option0": {Shape: "Bool"}, "option1": {Shape: "Int"}}},
		"ScalarOrNot": {Kind: KindUnion, Members: map[string]Member{"option0": {Shape: "Bool"}, "option1": {Shape: "Struct"}}},
		"EmptyUnion":  {Kind: KindUnion},
		"UnknownArm":  {Kind: KindUnion, Members: map[string]Member{"option0": {Shape: "Nowhere"}}},
	}}
	for _, test := range []struct {
		shape string
		want  bool
	}{
		{"S", true}, {"B", true},
		// The widening: a scalar body is a body whatever its kind.
		{"Bool", true}, {"Int", true}, {"Long", true}, {"Float", true}, {"Double", true},
		// Shapes with members, which a reader finds by name, are not bodies.
		{"Struct", false}, {"List", false}, {"Map", false},
		// An enum is a closed string set and a document is "any JSON"; neither
		// is claimed here, because neither appeared as an operation's whole
		// response and guessing is what this predicate exists to refuse.
		{"Enum", false}, {"Doc", false},
		{"TextOrBytes", true}, {"BoolOrInt", true},
		{"ScalarOrNot", false}, {"EmptyUnion", false}, {"UnknownArm", false},
		{"Absent", false},
	} {
		if got := svc.ScalarBody(test.shape); got != test.want {
			t.Errorf("ScalarBody(%s) = %v, want %v", test.shape, got, test.want)
		}
	}
	if (*Service)(nil).ScalarBody("S") {
		t.Error("a nil service claimed a scalar body")
	}
}
