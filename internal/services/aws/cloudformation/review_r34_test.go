package cloudformation

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// rv34Stream is the stream's capacity mode and open shards, as DescribeStreamSummary answers them.
func rv34Stream(h *stacks, id string) string {
	summary := asMap(rv31Get(h, "aws.kinesis", "DescribeStreamSummary", map[string]any{"StreamName": id})["StreamDescriptionSummary"])
	return fmt.Sprint(asMap(summary["StreamModeDetails"])["StreamMode"], " ", summary["OpenShardCount"])
}

func rv34Mode(mode string) map[string]any { return map[string]any{"StreamMode": mode} }

// rv34Day moves the clock on 25 hours: past the window in which a stream changes mode twice.
func rv34Day(t *testing.T, h *stacks) {
	t.Helper()
	if err := h.deps.Clock.Advance(25 * time.Hour); err != nil {
		t.Fatal(err)
	}
}

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
		// A failed update that changes both modes, and its rollback: each has the mode it had. (A day on:
		// a stream changes mode twice in 24 hours, and each has changed once.)
		rv34Day(t, h)
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
	updates := map[string][]string{ // the calls an update makes beside the kind's own, as provision spells them
		"AWS::Kinesis::Stream": {shardCountCall},
		"AWS::Events::Rule":    {putTargetsCall},
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

// A stream whose mode was set to on-demand outside its template is updated, and rolled back, as any other.
// The engine sent the template's shard count on every update in place, which Kinesis refuses for an
// on-demand stream, so such a stream failed every update, its rollback and ContinueUpdateRollback, and
// the stack stayed UPDATE_ROLLBACK_FAILED. The count is sent only when it differs from what the previous
// template gave. An update that does change it fails, as Kinesis refuses it, and rolls back to
// UPDATE_ROLLBACK_COMPLETE: the restore's count, refused the same way, is nothing left to put back.
func TestCloudFormationAStreamMadeOnDemandOutsideItsTemplateIsUpdated(t *testing.T) {
	t.Parallel()
	stream := func(properties map[string]any) map[string]any {
		return map[string]any{"R": map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": properties}}
	}
	for _, continued := range []bool{false, true} {
		h := newStacks(t)
		if err := rveCreate(h, "a1", stream(map[string]any{"ShardCount": 2})); err != nil {
			t.Fatal(err)
		}
		id := rv31IDs(h, "a1")["R"]
		if _, err := h.service("aws.kinesis", "UpdateStreamMode", map[string]any{"StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/" + id, "StreamModeDetails": rv34Mode("ON_DEMAND")}); err != nil {
			t.Fatal(err)
		}
		state := func() string {
			st, _, _ := h.status("a1")
			return st + ", " + rv34Stream(h, id)
		}
		if err := rv28Update(h, "a1", stream(map[string]any{"ShardCount": 2, "RetentionPeriodHours": 48})); err != nil || state() != "UPDATE_COMPLETE, ON_DEMAND 2" {
			t.Fatalf("continued=%v: an update of another property answered %v and left %s", continued, err, state())
		}
		// The same change with a failure after it: rolled back.
		failing := stream(map[string]any{"ShardCount": 2, "RetentionPeriodHours": 72})
		failing["Z"] = rv31Dep(rv31Bad, "R")
		if err := rv28Update(h, "a1", failing); err == nil || state() != "UPDATE_ROLLBACK_COMPLETE, ON_DEMAND 2" {
			t.Errorf("continued=%v: the failed update answered %v and left %s; want it rolled back", continued, err, state())
		}
		if continued { // and as a rollback that could not put back a count the update gave leaves it, for ContinueUpdateRollback
			rec := h.record("a1")
			rec["Status"], asMap(spi.Slice(rec["Resources"])[0])["Status"], asMap(spi.Slice(rec["Resources"])[0])["Gave"] = "UPDATE_ROLLBACK_FAILED", "UPDATE_FAILED", []any{"ShardCount"}
			if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "a1", []byte(spi.JSONText(rec))); err != nil {
				t.Fatal(err)
			}
			if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "a1"}); err != nil || state() != "UPDATE_ROLLBACK_COMPLETE, ON_DEMAND 2" {
				t.Errorf("ContinueUpdateRollback of the stream answered %v and left %s", err, state())
			}
		}
		// A count the template changes is Kinesis's to refuse: the update fails, and rolls back clean.
		err := rv28Update(h, "a1", stream(map[string]any{"ShardCount": 3, "RetentionPeriodHours": 48}))
		if err == nil || !strings.Contains(err.Error(), "provisioned capacity mode") || state() != "UPDATE_ROLLBACK_COMPLETE, ON_DEMAND 2" {
			t.Errorf("continued=%v: an update of the count answered %v and left %s; want Kinesis's refusal and a clean rollback", continued, err, state())
		}
		if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "a1"}); err == nil {
			t.Errorf("continued=%v: a stack rolled back clean took a ContinueUpdateRollback", continued)
		}
	}
}

// A change of a stream's mode in place leaves its shards as they are, as Kinesis itself does on a change
// of mode, and a stream that is on-demand by its template is never sent a count. For a while a stream
// made on-demand in place was first given the count a created one has: a rollback from a failed switch
// then left a stream that had one shard with four, and a switch beside 499 provisioned shards was refused
// at the shard limit.
//
// What follows from it, and is pinned here as readings: a stream made on-demand in place keeps the count
// it had, where one created from the same template has the template's or four; and a rollback to
// on-demand after an update that made the stream provisioned with five leaves five, an on-demand
// stream's count being the service's and not the template's.
func TestCloudFormationAModeSwitchLeavesAStreamsShards(t *testing.T) {
	t.Parallel()
	stream := func(properties map[string]any) map[string]any {
		return map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": properties}
	}
	onDemand := map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND")}
	// A failed switch to on-demand, rolled back and continued, from each template that makes one shard.
	for name, first := range map[string]map[string]any{"none": nil, "empty": {}, "provisioned": {"StreamModeDetails": rv34Mode("PROVISIONED")}, "retention": {"RetentionPeriodHours": 24}} {
		for _, continued := range []bool{false, true} {
			h := newStacks(t)
			resource := stream(first)
			if first == nil {
				delete(resource, "Properties")
			}
			if err := rveCreate(h, "b1", map[string]any{"R": resource}); err != nil {
				t.Fatal(err)
			}
			id := rv31IDs(h, "b1")["R"]
			rveFailed(t, h, "b1", stream(onDemand), continued)
			if continued {
				if got := rv34Stream(h, id); got != "ON_DEMAND 1" {
					t.Fatalf("%s: the failed switch whose restore fails left %s", name, got)
				}
				rveContinue(t, h, "b1")
			}
			if got := rv34Stream(h, id); got != "PROVISIONED 1" {
				t.Errorf("%s, continued=%v: rolled back from a failed switch to on-demand the stream is %s; want the provisioned stream of one shard it was", name, continued, got)
			}
		}
	}
	// A switch at the shard limit (498 shards beside the stack's two): the stream's one shard is not counted again, nor raised.
	h := newStacks(t)
	if _, err := h.service("aws.kinesis", "CreateStream", map[string]any{"StreamName": "beside", "ShardCount": 498}); err != nil {
		t.Fatal(err)
	}
	if err := rveCreate(h, "b2", map[string]any{"R": stream(map[string]any{}), "S": stream(map[string]any{}), "N": stream(map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND"), "ShardCount": 7})}); err != nil {
		t.Fatal(err)
	}
	ids := rv31IDs(h, "b2")
	state := func() string {
		st, _, _ := h.status("b2")
		return st + ": " + rv34Stream(h, ids["R"]) + ", " + rv34Stream(h, ids["S"]) + ", " + rv34Stream(h, ids["N"])
	}
	step := func(what string, r, s, n map[string]any, want string) {
		t.Helper()
		if err := rv28Update(h, "b2", map[string]any{"R": stream(r), "S": stream(s), "N": stream(n)}); err != nil || state() != want {
			t.Fatalf("%s: answered %v and left %s; want %s", what, err, state(), want)
		}
	}
	seven := map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND"), "ShardCount": 7}
	step("R to on-demand at the shard limit, S to on-demand with a templated count of seven", onDemand, seven, seven,
		"UPDATE_COMPLETE: ON_DEMAND 1, ON_DEMAND 1, ON_DEMAND 7") // S keeps its one shard, where N, created so, has seven
	// A count in the template of a stream that is on-demand by its template is not sent: taken out, set
	// to what the stream has, or changed, the update succeeds and the stream's shards stay.
	step("N's count taken out, S's set to the one it has", onDemand, map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND"), "ShardCount": 1}, onDemand,
		"UPDATE_COMPLETE: ON_DEMAND 1, ON_DEMAND 1, ON_DEMAND 7")
	step("N's count given again, as nine", onDemand, onDemand, map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND"), "ShardCount": 9},
		"UPDATE_COMPLETE: ON_DEMAND 1, ON_DEMAND 1, ON_DEMAND 7")
	// R is made provisioned with five and the update fails after it: back on-demand, with the five.
	if _, err := h.service("aws.kinesis", "DeleteStream", map[string]any{"StreamName": "beside"}); err != nil {
		t.Fatal(err)
	}
	for _, continued := range []bool{false, true} {
		rv34Day(t, h) // the stream changes mode twice in each pass, which is a day's allowance
		rveFailed(t, h, "b2", stream(map[string]any{"StreamModeDetails": rv34Mode("PROVISIONED"), "ShardCount": 5}), continued)
		if continued {
			rveContinue(t, h, "b2")
		}
		if got := rv34Stream(h, ids["R"]); got != "ON_DEMAND 5" {
			t.Errorf("continued=%v: rolled back from provisioned with five, R is %s; want on-demand again, with the five shards the update gave it", continued, got)
		}
	}
}

// A restore that fails is a failed rollback, whatever it is. A rollback and a continued one passed any
// ValidationException from a stream's restore, to let through the one case that is nothing to put back:
// a count Kinesis refuses an on-demand stream. A stored template whose mode Kinesis refuses, here
// "on_demand", was then reported rolled back with the stream left as the failed update made it. Only the
// count call's refusal is passed.
func TestCloudFormationAStreamsRestoreThatFailsIsAFailedRollback(t *testing.T) {
	t.Parallel()
	stream := func(properties map[string]any) map[string]any {
		return map[string]any{"R": map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": properties}}
	}
	for _, continued := range []bool{false, true} {
		h := newStacks(t)
		if err := rveCreate(h, "bm", stream(map[string]any{"ShardCount": 2})); err != nil {
			t.Fatal(err)
		}
		id := rv31IDs(h, "bm")["R"]
		// The stored template, as an earlier build could have taken it: a mode Kinesis does not know.
		rec := h.record("bm")
		rec["Template"] = rv19Body(map[string]any{"Resources": stream(map[string]any{"ShardCount": 2, "StreamModeDetails": rv34Mode("on_demand")})})
		if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "bm", []byte(spi.JSONText(rec))); err != nil {
			t.Fatal(err)
		}
		failing := stream(map[string]any{"ShardCount": 2, "StreamModeDetails": rv34Mode("ON_DEMAND")})
		failing["Z"] = rv31Dep(rv31Bad, "R")
		err := rv28Update(h, "bm", failing)
		st, reason, _ := h.status("bm")
		if err == nil || st != "UPDATE_ROLLBACK_FAILED" || rv34Stream(h, id) != "ON_DEMAND 2" {
			t.Fatalf("the update whose restore Kinesis refuses answered %v and left the stack %s (%s), the stream %s; want UPDATE_ROLLBACK_FAILED", err, st, reason, rv34Stream(h, id))
		}
		if !strings.Contains(spi.JSONText(h.record("bm")["Resources"]), "enum value set") {
			t.Errorf("the stream's record does not say why its restore failed: %s", spi.JSONText(h.record("bm")["Resources"]))
		}
		if continued {
			rveSettled(t, h, "bm")
			if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "bm"}); err == nil {
				t.Errorf("ContinueUpdateRollback, whose restore Kinesis refuses again, answered nothing")
			}
			if st, _, _ := h.status("bm"); st != "UPDATE_ROLLBACK_FAILED" {
				t.Errorf("after ContinueUpdateRollback the stack is %s; want UPDATE_ROLLBACK_FAILED still", st)
			}
		}
	}
}

// A change of mode that Kinesis refuses fails as any refused call does, in a rollback too. A stream
// changes mode twice in 24 hours ("you can switch between the on-demand and provisioned capacity modes
// twice within 24 hours", Kinesis Developer Guide, Quotas and limits), and the engine does nothing
// special about it: a rollback whose restore is a third change inside the window leaves the stack
// UPDATE_ROLLBACK_FAILED with Kinesis's reason, a ContinueUpdateRollback inside the window fails the same
// way, and one after it has passed puts the previous mode back.
func TestCloudFormationARollbackWaitsForAStreamsNextChangeOfMode(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	stream := func(mode string) map[string]any {
		return map[string]any{"R": map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": map[string]any{"StreamModeDetails": rv34Mode(mode)}}}
	}
	if err := rveCreate(h, "tw", stream("PROVISIONED")); err != nil {
		t.Fatal(err)
	}
	id := rv31IDs(h, "tw")["R"]
	state := func() string {
		st, reason, _ := h.status("tw")
		return st + ", " + rv34Stream(h, id) + ", " + reason
	}
	const refused = "has changed its capacity mode twice in the last 24 hours"
	if err := rv28Update(h, "tw", stream("ON_DEMAND")); err != nil { // the first change
		t.Fatal(err)
	}
	failing := stream("PROVISIONED") // the second, with a failure after it: its rollback is a third
	failing["Z"] = rv31Dep(rv31Bad, "R")
	if err := rv28Update(h, "tw", failing); err == nil || !strings.HasPrefix(state(), "UPDATE_ROLLBACK_FAILED, PROVISIONED 1, ") || !strings.Contains(spi.JSONText(h.record("tw")["Resources"]), refused) {
		t.Fatalf("the update whose rollback is a third change of mode answered %v and left %s, the record %s; want UPDATE_ROLLBACK_FAILED and Kinesis's reason on the stream", err, state(), spi.JSONText(h.record("tw")["Resources"]))
	}
	if err := h.deps.Clock.Advance(23 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "tw"}); err == nil || !strings.Contains(err.Error(), refused) || !strings.HasPrefix(state(), "UPDATE_ROLLBACK_FAILED, PROVISIONED 1, ") {
		t.Errorf("ContinueUpdateRollback inside the window answered %v and left %s; want Kinesis's refusal again", err, state())
	}
	if err := h.deps.Clock.Advance(2 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "tw"}); err != nil || state() != "UPDATE_ROLLBACK_COMPLETE, ON_DEMAND 1, " {
		t.Errorf("ContinueUpdateRollback after the window answered %v and left %s; want the stream on-demand again", err, state())
	}
}
