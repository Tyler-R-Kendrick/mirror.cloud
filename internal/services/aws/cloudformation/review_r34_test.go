package cloudformation

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
)

// rv34Stream is the stream's capacity mode and open shards, as DescribeStreamSummary answers them.
func rv34Stream(h *stacks, id string) string {
	summary := asMap(rv31Get(h, "aws.kinesis", "DescribeStreamSummary", map[string]any{"StreamName": id})["StreamDescriptionSummary"])
	return fmt.Sprint(asMap(summary["StreamModeDetails"])["StreamMode"], " ", summary["OpenShardCount"])
}

func rv34Mode(mode string) map[string]any { return map[string]any{"StreamMode": mode} }

// A stream has the capacity mode its template gives. StreamModeDetails "Specifies the capacity mode to
// which you want to set your data stream" and its "Update requires: No interruption" (Template Reference,
// AWS::Kinesis::Stream); the engine passed it to neither the create nor an update, so every stream it
// made was provisioned. It is sent to CreateStream, and an update in place calls UpdateStreamMode, before
// the shard count: UpdateShardCount "is only supported for the data streams with the provisioned
// capacity mode" (Kinesis API reference), so a stream that becomes provisioned takes its count after, and
// one whose template makes it on-demand is sent no count. A rollback, and a continued one, put the
// previous mode back. Only what the previous template gave is cleared: a stream whose template gives no
// mode keeps one set outside it.
func TestCloudFormationAStreamHasTheModeItsTemplateGives(t *testing.T) {
	t.Parallel()
	stream := func(properties map[string]any) map[string]any {
		return map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": properties}
	}
	for _, continued := range []bool{false, true} {
		h := newStacks(t)
		if err := rveCreate(h, "sm", map[string]any{"R": stream(map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND")}), "P": stream(map[string]any{"ShardCount": 2})}); err != nil {
			t.Fatal(err)
		}
		ids := rv31IDs(h, "sm")
		state := func() string { return rv34Stream(h, ids["R"]) + ", " + rv34Stream(h, ids["P"]) }
		if got := state(); got != "ON_DEMAND 4, PROVISIONED 2" {
			t.Fatalf("made: %s", got)
		}
		step := func(what string, r, p map[string]any, want string) {
			t.Helper()
			if err := rv28Update(h, "sm", map[string]any{"R": stream(r), "P": stream(p)}); err != nil || state() != want || rv31IDs(h, "sm")["R"] != ids["R"] || rv31IDs(h, "sm")["P"] != ids["P"] {
				t.Fatalf("continued=%v, %s: answered %v and left %s, the ids %v; want %s and the same streams", continued, what, err, state(), rv31IDs(h, "sm"), want)
			}
		}
		step("each takes the other's mode, the one that becomes provisioned with three shards", map[string]any{"StreamModeDetails": rv34Mode("PROVISIONED"), "ShardCount": 3}, map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND"), "ShardCount": 2}, "PROVISIONED 3, ON_DEMAND 2")
		step("another property of an on-demand stream whose template gives a count", map[string]any{"StreamModeDetails": rv34Mode("PROVISIONED"), "ShardCount": 3}, map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND"), "ShardCount": 2, "RetentionPeriodHours": 48}, "PROVISIONED 3, ON_DEMAND 2")
		// A failed update that changes both modes, and its rollback: each has the mode it had.
		rveFailed(t, h, "sm", stream(map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND")}), continued)
		if continued {
			if state() != "ON_DEMAND 3, ON_DEMAND 2" {
				t.Fatalf("the failed update whose restore failed left %s", state())
			}
			rveContinue(t, h, "sm")
		}
		if got := state(); got != "PROVISIONED 3, ON_DEMAND 2" {
			t.Errorf("continued=%v: rolled back, the streams are %s; want the modes they had", continued, got)
		}
		// The template stops giving the mode: provisioned, as a stream made without one is.
		step("the mode taken out of both templates", map[string]any{"ShardCount": 3}, map[string]any{"ShardCount": 2}, "PROVISIONED 3, PROVISIONED 2")
		// A mode set outside a template that gives none stays, through an update of another property.
		if _, err := h.service("aws.kinesis", "UpdateStreamMode", map[string]any{"StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/" + ids["P"], "StreamModeDetails": rv34Mode("ON_DEMAND")}); err != nil {
			t.Fatal(err)
		}
		step("another property, with a mode set outside the template", map[string]any{"ShardCount": 3, "RetentionPeriodHours": 48}, map[string]any{"ShardCount": 2}, "PROVISIONED 3, ON_DEMAND 2")
	}
}

// rv34Sent is, for a call spelled as the kinds table spells one, the member each property is sent as
// with the type its operation's model declares: nothing for a property sent as text ($, %) or not at all.
func rv34Sent(t *testing.T, spec string) (operation string, members map[string]string) {
	t.Helper()
	f := strings.Fields(spec)
	svc := specboot.Bundle().ServiceByID(f[0])
	op := svc.OperationByName(f[1])
	if op == nil {
		t.Fatalf("%s %s is no operation of the model", f[0], f[1])
	}
	members = map[string]string{}
	for _, arg := range f[2:] {
		member, value, set := strings.Cut(arg, "=")
		if arg == "*" { // every property, under its own name
			for name := range svc.Shapes[op.Input].Members {
				members[name] = rv34Typing(svc, svc.Shapes[op.Input].Members[name].Shape, 0)
			}
		} else if !set || strings.HasPrefix(value, "@") { // a property as it is given: typed by the model
			members[cmp.Or(strings.TrimPrefix(value, "@"), member)] = rv34Typing(svc, svc.Shapes[op.Input].Members[member].Shape, 0)
		}
	}
	return f[0] + " " + f[1], members
}

// rv34Typing is how typed reads a value of the shape: the kinds it coerces a scalar to, through
// structures, lists and maps.
func rv34Typing(svc *model.Service, shape string, depth int) string {
	sh := svc.Shapes[shape]
	switch {
	case depth > 6:
		return "..."
	case sh.Kind == model.KindMap || sh.Member != "":
		return fmt.Sprint(sh.Kind, "<", rv34Typing(svc, sh.Member, depth+1), ">")
	case len(sh.Members) > 0:
		parts := []string{}
		for _, name := range slices.Sorted(maps.Keys(sh.Members)) {
			parts = append(parts, name+":"+rv34Typing(svc, sh.Members[name].Shape, depth+1))
		}
		return "{" + strings.Join(parts, " ") + "}"
	}
	return map[model.ShapeKind]string{model.KindString: "String", model.KindEnum: "String", model.KindInteger: "Integer", model.KindLong: "Integer", model.KindBoolean: "Boolean"}[sh.Kind]
}

// An update compares two templates' properties typed by the create operation's model (change), and
// sends them typed by the update operation's (call). The two agree only while a property both operations
// take is typed alike by both: an integer for one and a string for the other would make a value judged
// unchanged, or changed, against what is sent. Every kind's create is walked against each call its update
// makes, the ones provision spells itself included.
func TestCloudFormationCreateAndUpdateTypeAPropertyAlike(t *testing.T) {
	updates := map[string][]string{ // the calls an update makes beside the kind's own
		"AWS::Kinesis::Stream": {"aws.kinesis UpdateShardCount StreamName={id} ScalingType=UNIFORM_SCALING TargetShardCount=@ShardCount"},
		"AWS::Events::Rule":    {"aws.events PutTargets Rule={n} Targets"},
	}
	compared := 0
	for _, typ := range slices.Sorted(maps.Keys(kinds)) {
		create, made := rv34Sent(t, kinds[typ].create)
		for _, spec := range append(updates[typ], kinds[typ].update) {
			if spec == "" {
				continue
			}
			update, sent := rv34Sent(t, spec)
			for _, property := range slices.Sorted(maps.Keys(sent)) {
				if was, both := made[property]; both && was != sent[property] {
					t.Errorf("%s: %s is typed %s by %s and %s by %s", typ, property, was, create, sent[property], update)
				} else if both {
					compared++
				}
			}
		}
	}
	if compared < 10 { // a walk that compares nothing proves nothing
		t.Errorf("only %d properties are taken by both a create and an update", compared)
	}
	// The walk tells a difference when there is one: a count sent as a stream's name would be a string.
	_, count := rv34Sent(t, "aws.kinesis CreateStream ShardCount")
	_, named := rv34Sent(t, "aws.kinesis CreateStream StreamName=@ShardCount")
	if count["ShardCount"] != "Integer" || named["ShardCount"] != "String" {
		t.Errorf("the walk reads ShardCount as %q, and as %q where it is sent as the stream's name; want Integer and String", count["ShardCount"], named["ShardCount"])
	}
}
