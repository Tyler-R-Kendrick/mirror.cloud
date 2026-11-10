package cloudformation

import (
	"fmt"
	"testing"
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
