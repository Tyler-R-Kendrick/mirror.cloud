package cloudformation

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Round 5 review (rv454e), part C: the takeover path (a rollback that panics in the engine), concurrency,
// and what stays registered.

// The engine panics as the rollback restores the bucket A; the secret S and the rule R after it are not put
// back, and are marked by the operation's own ending (cfn.go, the deferred marking). S's description alone
// changed: a rotation since must outlive the continued rollback; R's target put outside must stay.
func TestCloudFormationReview5CTakeoverMarksWhatChanged(t *testing.T) {
	live0, kept0, counter0 := rv30Ops() // the registry is the process's: a run before this one may have left something
	h := newStacks(t)
	rule := func(id string, dep ...any) map[string]any {
		m := map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)",
			"Targets": []any{map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}}
		m["DependsOn"] = dep
		return m
	}
	if err := rvMake(t, h, "tk", map[string]any{"Resources": map[string]any{"A": rv30CorsBucket("one"), "R": rule("a", "A"), "S": rv30Secret("one", "R")}}); err != nil {
		t.Fatal(err)
	}
	ids := rv31IDs(h, "tk")
	n := spi.LastSlash(ids["R"])
	if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": n, "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}}}); err != nil {
		t.Fatal(err)
	}
	var w atomic.Int32
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" && w.Add(1) == 2 })
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "tk", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"A": rv30CorsBucket("two"), "R": rule("b", "A"), "S": rv30Secret("two", "R"), "Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"S"}}}})})
	off()
	t.Logf("update: %v; record %v", err, h.record("tk")["Status"])
	_, uerr := h.invoke("UpdateStack", map[string]any{"StackName": "tk", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"A": rv30CorsBucket("one")}})})
	st, reason, _ := h.status("tk")
	t.Logf("taken over (%v): %s %q %v rec=%s", uerr, st, reason, h.statuses("tk"), spi.JSONText(h.record("tk")["Resources"]))
	if st != "UPDATE_ROLLBACK_FAILED" {
		t.Fatalf("want UPDATE_ROLLBACK_FAILED")
	}
	rec := spi.JSONText(h.record("tk")["Resources"])
	if strings.Contains(rec, `"SecretString"`) {
		t.Errorf("the takeover lists SecretString, which the failed update gave as the template does: %s", rec)
	}
	if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": ids["S"], "SecretString": "ROT"}); err != nil {
		t.Fatal(err)
	}
	rveContinue(t, h, "tk")
	var tids []string
	for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
		tids = append(tids, str(asMap(tg)["Id"]))
	}
	slices.Sort(tids)
	desc := rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]})["Description"]
	cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, ids["A"]+"/cors")
	t.Logf("after: value=%q desc=%v targets=%v cors=%s", rveValue(h, ids["S"]), desc, tids, cors)
	if rveValue(h, ids["S"]) != "ROT" || desc != "one" || fmt.Sprint(tids) != "[a outside]" || !strings.Contains(string(cors), `"one"`) {
		t.Errorf("want the rotated value, the description one, the targets a and outside, the CORS origin one")
	}
	if live, records, counter := rv30Ops(); live != live0 || records != kept0 || counter != counter0 {
		t.Errorf("registered after: %d live, %d kept, counter %d; before, %d, %d and %d", live, records, counter, live0, kept0, counter0)
	}
}

// Many ContinueUpdateRollback calls at once on a stack whose secret is UPDATE_FAILED: one settles it, none
// leaves it in progress, the value is the template's.
func TestCloudFormationReview5CConcurrentContinues(t *testing.T) {
	live0, kept0, counter0 := rv30Ops() // the registry is the process's: a run before this one may have left something
	for round := 0; round < 8; round++ {
		h := newStacks(t)
		if err := rveCreate(h, "cc", map[string]any{"R": rveSecret(map[string]any{"SecretString": "A", "Description": "one"})}); err != nil {
			t.Fatal(err)
		}
		id := rv31IDs(h, "cc")["R"]
		rveFailed(t, h, "cc", rveSecret(map[string]any{"SecretString": "B", "Description": "two"}), true)
		var wg sync.WaitGroup
		var ok, refused atomic.Int32
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "cc"}); err == nil {
					ok.Add(1)
				} else {
					refused.Add(1)
				}
			}()
		}
		wg.Wait()
		st, _, _ := h.status("cc")
		rec := h.record("cc")
		if st != "UPDATE_ROLLBACK_COMPLETE" || rveValue(h, id) != "A" || str(rec["Op"]) != "" || strings.Contains(spi.JSONText(rec["Resources"]), "Gave") {
			t.Errorf("round %d: %s value=%q ok=%d refused=%d rec=%s", round, st, rveValue(h, id), ok.Load(), refused.Load(), spi.JSONText(rec))
		}
		if err := rv28Update(h, "cc", map[string]any{"R": rveSecret(map[string]any{"SecretString": "C", "Description": "three"})}); err != nil || rveValue(h, id) != "C" {
			t.Errorf("round %d: the update after: %v %q", round, err, rveValue(h, id))
		}
	}
	if live, records, counter := rv30Ops(); live != live0 || records != kept0 || counter != counter0 {
		t.Errorf("registered after: %d live, %d kept, counter %d; before, %d, %d and %d", live, records, counter, live0, kept0, counter0)
	}
}

// A continued rollback that fails again keeps the names, and a third succeeds; nothing grows.
func TestCloudFormationReview5CContinueFailsThenSucceeds(t *testing.T) {
	h := newStacks(t)
	rule := func(d string, ids ...string) map[string]any {
		var tg []any
		for _, id := range ids {
			tg = append(tg, map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"})
		}
		return rveProps("AWS::Events::Rule", map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": d, "Targets": tg})
	}
	if err := rveCreate(h, "cf", map[string]any{"R": rule("one", "a")}); err != nil {
		t.Fatal(err)
	}
	n := spi.LastSlash(rv31IDs(h, "cf")["R"])
	rveFailed(t, h, "cf", rule("two", "b", "c"), true)
	first := spi.JSONText(h.record("cf")["Resources"])
	for i := 0; i < 3; i++ {
		off := faultStore(h, func(op, c, k string) bool { return c != "cfn" && c != "cfn-cs" && (op == "put" || op == "txput") })
		_, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "cf"})
		off()
		st, reason, _ := h.status("cf")
		now := spi.JSONText(h.record("cf")["Resources"])
		t.Logf("continue %d: %v; %s %q rec=%s", i, err, st, reason, now)
		if err == nil || st != "UPDATE_ROLLBACK_FAILED" || len(now) > len(first)+200 {
			t.Errorf("a continued rollback that fails: %v, %s", err, st)
		}
		rveSettled(t, h, "cf")
		if evs, err := h.invoke("DescribeStackEvents", map[string]any{"StackName": "cf"}); err != nil || len(spi.Slice(evs["StackEvents"])) != 2 {
			t.Errorf("continue %d: the events are %v (%v); want the rule's and the trigger's, and no more with each retry", i, evs["StackEvents"], err)
		}
	}
	rveContinue(t, h, "cf")
	var tids []string
	for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
		tids = append(tids, str(asMap(tg)["Id"]))
	}
	slices.Sort(tids)
	if d := rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": n})["Description"]; d != "one" || fmt.Sprint(tids) != "[a]" {
		t.Errorf("after: desc=%v targets=%v", d, tids)
	}
	// DeleteStack from UPDATE_ROLLBACK_FAILED with names recorded
	h2 := newStacks(t)
	if err := rveCreate(h2, "df", map[string]any{"R": rule("one", "a")}); err != nil {
		t.Fatal(err)
	}
	rveFailed(t, h2, "df", rule("two", "b"), true)
	if _, err := h2.invoke("DeleteStack", map[string]any{"StackName": "df"}); err != nil {
		t.Errorf("DeleteStack: %v", err)
	}
	if _, ok := h2.stored("df"); ok {
		rec := h2.record("df")
		t.Logf("after delete: %v %s", rec["Status"], spi.JSONText(rec["Resources"]))
		if rec["Status"] != "DELETE_COMPLETE" {
			t.Errorf("delete left %v", rec["Status"])
		}
	}
}
