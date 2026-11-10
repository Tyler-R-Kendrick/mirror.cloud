package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// malformedDemo is one operation whose output converts a string member with
// string() and two integer members with int(), as bundles do, behind a rule
// that would refuse every request: so what answers shows what was asked first.
func malformedDemo(t *testing.T, option string) *engine.Engine {
	t.Helper()
	svc := &model.Service{
		ID:         "aws.demo",
		Protocol:   model.ProtoAWSJSON10,
		Operations: []model.Operation{{Name: "Echo", Input: "d#In", Output: "d#Out"}},
		Shapes: map[string]model.Shape{
			"d#In": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Name": {Shape: "d#S"}, "Kind": {Shape: "d#E"}, "Count": {Shape: "d#I"}, "Size": {Shape: "d#L"},
				"Flag": {Shape: "d#B"}, "Detail": {Shape: "d#T"}, "Must": {Shape: "d#S", Required: true},
			}},
			"d#Out": {Kind: model.KindStructure, Members: map[string]model.Member{"Text": {Shape: "d#S"}}},
			"d#S":   {Kind: model.KindString}, "d#E": {Kind: model.KindEnum}, "d#I": {Kind: model.KindInteger},
			"d#L": {Kind: model.KindLong}, "d#B": {Kind: model.KindBoolean}, "d#T": {Kind: model.KindStructure},
		},
	}
	doc := "schema: bir/1\nservice: aws.demo\nprovenance: authored\n" + option + `errors:
  Bad:
    code: MalformedThing
    http: 422
    fault: client
  Refused:
    code: RuleSaidNo
    http: 409
    fault: client
operations:
  Echo:
    require:
      - { cond: "'Pass' in input", error: Refused }
    output:
      Text: "string('Name' in input ? input.Name : '') + string('Kind' in input ? input.Kind : '') + string(int('Count' in input ? input.Count : 0) + int('Size' in input ? input.Size : 0))"
`
	ir, err := bir.Load(fstest.MapFS{"svc/service.yaml": &fstest.MapFile{Data: []byte(doc)}}, "svc", svc)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	e, err := engine.New(spitest.Deps(t), ir, svc)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func malformedAnswer(e *engine.Engine, in map[string]any) string {
	_, err := e.Invoke(context.Background(), &spi.Request{ServiceID: "aws.demo", Operation: "Echo", Input: in,
		Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"}})
	var f *spi.Fault
	if errors.As(err, &f) {
		return fmt.Sprint(f.HTTPStatus, " ", f.Code, ": ", f.Message)
	}
	return fmt.Sprint(err)
}

// A bundle that names malformed_input_error is answered for, with that row of
// its error table, when a member the model declares a string or an integer
// could be neither -- before the operation's rules, which here refuse every
// request, and before a required member is missed. What could be the member
// goes on to the rules: a number or a Boolean for a string; for an integer a
// whole number of any Go kind, written as text, or with a fraction of zero.
// Null is absent, so it is no mistake. Members of other kinds are not judged.
func TestAMalformedMemberIsAnsweredWithTheBundlesError(t *testing.T) {
	e := malformedDemo(t, "malformed_input_error: Bad\n")
	bad := func(member, kind string) string { return "422 MalformedThing: " + member + " must be " + kind }
	const rule = "409 RuleSaidNo: "
	for _, row := range []struct {
		member string
		value  any
		want   string
	}{
		{"Name", map[string]any{}, bad("Name", "a string")}, {"Name", []any{"a"}, bad("Name", "a string")},
		{"Kind", map[string]any{"a": nil}, bad("Kind", "a string")}, {"Kind", []any{}, bad("Kind", "a string")},
		{"Name", nil, rule}, {"Kind", nil, rule}, {"Count", nil, rule}, {"Size", nil, rule}, // null is absent, and absent is no mistake
		{"Name", "a", rule}, {"Name", "", rule}, {"Name", 7.0, rule}, {"Name", true, rule},
		{"Count", map[string]any{}, bad("Count", "an integer")}, {"Count", []any{}, bad("Count", "an integer")},
		{"Count", true, bad("Count", "an integer")}, {"Count", "x", bad("Count", "an integer")}, {"Count", "", bad("Count", "an integer")},
		{"Count", 1.5, bad("Count", "an integer")}, {"Count", 1e30, bad("Count", "an integer")}, {"Count", -1e30, bad("Count", "an integer")}, {"Count", "1.5", bad("Count", "an integer")},
		{"Size", 2.5, bad("Size", "an integer")}, {"Size", uint64(1) << 63, bad("Size", "an integer")}, {"Size", 9223372036854775807.0, bad("Size", "an integer")},
		{"Count", 3.0, rule}, {"Count", 0, rule}, {"Count", int64(-4), rule}, {"Count", "12", rule}, {"Size", 9e15, rule}, {"Size", "9223372036854775807", rule},
		{"Count", int32(5), rule}, {"Count", int8(-5), rule}, {"Count", uint(5), rule}, {"Count", uint16(5), rule}, {"Size", uint64(1)<<63 - 1, rule}, {"Count", float32(2), rule},
		{"Flag", "x", rule}, {"Flag", nil, rule}, {"Detail", "x", rule}, {"Detail", nil, rule}, {"Unknown", nil, rule},
	} {
		if got := malformedAnswer(e, map[string]any{row.member: row.value}); !strings.HasPrefix(got, row.want) {
			t.Errorf("%s = %#v answered %q; want %q", row.member, row.value, got, row.want)
		}
	}
	// The first malformed member by name is the one named, whatever the order they came in.
	if got := malformedAnswer(e, map[string]any{"Size": "x", "Count": "x", "Name": []any{}}); got != bad("Count", "an integer") {
		t.Errorf("three malformed members answered %q; want Count named, the first of them", got)
	}
	// A request that is sound is the rules', then the model's, as it was.
	if got := malformedAnswer(e, map[string]any{"Pass": true}); got != "400 ValidationException: Must is required" {
		t.Errorf("a sound request that the rule passes answered %q; want the missing required member", got)
	}
	if out, err := e.Invoke(context.Background(), &spi.Request{ServiceID: "aws.demo", Operation: "Echo", Input: map[string]any{"Pass": true, "Must": "m", "Name": "n", "Count": "2", "Size": 3.0},
		Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"}}); err != nil || out.Output["Text"] != "n5" {
		t.Errorf("a sound request answered %v, %v; want Text n5", out, err)
	}
}

// A member that is null is absent, for a bundle that names the option: "a
// server ignores null structure values" is what Smithy's awsJson1.1 compliance
// test AwsJson11ServersDontDeserializeNullStructureValues asks, sending
// {"string": null} and expecting no member. So a sound request with any
// member also given as null answers as without it; a required member that is
// null is missing, and is said to be after the rules as any missing member
// is; the expressions see no null; and the caller's own map is left as it
// was.
func TestANullMemberIsAbsentForABundleThatNamesTheOption(t *testing.T) {
	e := malformedDemo(t, "malformed_input_error: Bad\n")
	text := func(in map[string]any) string {
		out, err := e.Invoke(context.Background(), &spi.Request{ServiceID: "aws.demo", Operation: "Echo", Input: in, Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"}})
		if err != nil {
			return malformedAnswer(e, in)
		}
		return fmt.Sprint(out.Output["Text"])
	}
	want := text(map[string]any{"Pass": true, "Must": "m", "Count": 4})
	if want != "4" {
		t.Fatalf("the sound request answered %q", want)
	}
	for _, member := range []string{"Name", "Kind", "Size", "Flag", "Detail", "Unknown"} {
		in := map[string]any{"Pass": true, "Must": "m", "Count": 4, member: nil}
		if got := text(in); got != want {
			t.Errorf("with %s null the request answered %q; want %q, as without it", member, got, want)
		}
		if _, kept := in[member]; !kept || len(in) != 4 {
			t.Errorf("the caller's map was edited: %v", in)
		}
	}
	if got := text(map[string]any{"Pass": true, "Must": nil}); got != "400 ValidationException: Must is required" {
		t.Errorf("a required member that is null answered %q; want it missing", got)
	}
	if got := text(map[string]any{"Must": nil}); !strings.HasPrefix(got, "409 RuleSaidNo") {
		t.Errorf("a required member that is null, in a request the rule refuses, answered %q; want the rule first, as for one left out", got)
	}
	if got := text(map[string]any{"Pass": nil, "Must": "m"}); !strings.HasPrefix(got, "409 RuleSaidNo") {
		t.Errorf("the rule saw a null member as present: %q", got)
	}
}

// A bundle that names none is not judged: the value reaches its expressions
// as it came, and a service with its own answer for it gives that. Here the
// rule answers, and where the rule passes the conversion fails as it did.
func TestABundleThatNamesNoMalformedInputErrorIsNotJudged(t *testing.T) {
	e := malformedDemo(t, "")
	for _, in := range []map[string]any{{"Name": nil}, {"Count": "x"}, {"Size": 1.5}} {
		if got := malformedAnswer(e, in); !strings.HasPrefix(got, "409 RuleSaidNo") {
			t.Errorf("%v answered %q; want the operation's own rule", in, got)
		}
	}
	if got := malformedAnswer(e, map[string]any{"Pass": true, "Must": "m", "Count": "x"}); strings.Contains(got, "must be") || strings.HasPrefix(got, "4") {
		t.Errorf("a count that is no number, with no option named, answered %q; want the expression's own failure", got)
	}
	// Nor is a null dropped: the rule sees the member, and the expression its null.
	if got := malformedAnswer(e, map[string]any{"Pass": nil, "Must": "m", "Name": nil}); strings.HasPrefix(got, "409") || strings.HasPrefix(got, "<nil>") {
		t.Errorf("a null member, with no option named, answered %q; want it to reach the rule as present and the expression as null", got)
	}
	if got := malformedAnswer(e, map[string]any{"Pass": true, "Must": nil}); strings.Contains(got, "is required") {
		t.Errorf("a required member that is null, with no option named, answered %q; want it present, as it was", got)
	}
}
