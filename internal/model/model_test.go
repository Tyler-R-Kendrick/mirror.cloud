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
