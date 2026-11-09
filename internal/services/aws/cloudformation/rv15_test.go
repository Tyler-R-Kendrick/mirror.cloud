package cloudformation

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func rv15Body(tpl map[string]any) string { b, _ := json.Marshal(tpl); return string(b) }

// A Fn::Sub over a resource not made yet AND AWS::NoValue: sound sees only
// `unknown`; at provision the value is inValue. Does the mark reach the provider?
func TestRV15SubUnknownAndNoValueLeaks(t *testing.T) {
	h := newStacks(t)
	tpl := map[string]any{"Resources": map[string]any{
		"T": map[string]any{"Type": "AWS::SNS::Topic"},
		"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{
			"Name": "leak", "SecretString": "v", "Description": map[string]any{"Fn::Sub": "${T}-${AWS::NoValue}"}}},
	}}
	_, err := h.invoke("CreateStack", map[string]any{"StackName": "leak", "TemplateBody": rv15Body(tpl)})
	got, derr := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": "leak"})
	t.Logf("create answered %v; secret %v %v", err, got["Description"], derr)
	if code(err) != "ValidationError" {
		t.Errorf("BUG: Sub of an unmade resource and AWS::NoValue answered %v, want ValidationError; the secret's Description %q", err, got["Description"])
	}
	// same through a bucket's stored configuration
	tpl2 := map[string]any{"Resources": map[string]any{
		"T": map[string]any{"Type": "AWS::SNS::Topic"},
		"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "leakbkt",
			"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedMethods": []any{"GET"},
				"AllowedOrigins": []any{map[string]any{"Fn::Join": []any{"", []any{map[string]any{"Ref": "T"}, map[string]any{"Fn::Sub": "${AWS::NoValue}"}}}}}}}}}},
	}}
	_, err = h.invoke("CreateStack", map[string]any{"StackName": "leak2", "TemplateBody": rv15Body(tpl2)})
	raw, ok, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, "leakbkt/cors")
	t.Logf("create2 answered %v; stored cors %v %s", err, ok, raw)
	if strings.Contains(string(raw), "AWS::NoValue") || strings.Contains(string(raw), "unknown") {
		t.Errorf("BUG: a mark was stored: %s", raw)
	}
}

// A Fn::Sub over a resource not made yet and an out-of-range Fn::Select:
// refused before anything is made, as a Select alone is?
func TestRV15SubUnknownAndUnselected(t *testing.T) {
	h := newStacks(t)
	tpl := map[string]any{"Resources": map[string]any{
		"T": map[string]any{"Type": "AWS::SNS::Topic"},
		"S": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String",
			"Value": map[string]any{"Fn::Sub": []any{"${T}${X}", map[string]any{"X": map[string]any{"Fn::Select": []any{5, []any{"a"}}}}}}}},
	}}
	_, err := h.invoke("CreateStack", map[string]any{"StackName": "us", "TemplateBody": rv15Body(tpl)})
	rec, ok := h.live("us")
	t.Logf("create answered %v; stack %v %v", err, ok, rec["Status"])
	if ok {
		t.Errorf("BUG: refused only after making (stack %v), want refused before anything is made", rec["Status"])
	}
}

// Marks in an output's list: Fn::Split of something holding a mark inside a list output.
func TestRV15OutputMarks(t *testing.T) {
	h := newStacks(t)
	for name, v := range map[string]any{
		"o1": []any{"a", map[string]any{"Fn::Sub": "${AWS::NoValue}x"}},
		"o2": map[string]any{"Fn::GetAZs": map[string]any{"Ref": "AWS::NoValue"}},
		"o3": map[string]any{"Fn::Join": []any{",", []any{"a", map[string]any{"Fn::Select": []any{0, []any{map[string]any{"Ref": "AWS::NoValue"}}}}}}},
		"o4": map[string]any{"Fn::Split": []any{",", map[string]any{"Fn::Sub": "a,${AWS::NoValue}"}}},
		"o5": map[string]any{"Fn::Select": []any{0, map[string]any{"Fn::Split": []any{",", map[string]any{"Fn::Sub": "a,${AWS::NoValue}"}}}}},
	} {
		tpl := map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
			"Outputs": map[string]any{"O": map[string]any{"Value": v}}}
		_, err := h.invoke("CreateStack", map[string]any{"StackName": name, "TemplateBody": rv15Body(tpl)})
		_, verr := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": rv15Body(tpl)})
		var o any
		if err == nil {
			o = h.outputs(name)["O"]
		}
		t.Logf("%s: create %v validate %v output %q", name, err, verr, o)
		if s := fmt.Sprint(o); strings.Contains(s, "NoValue") || strings.Contains(s, "unknown") || strings.Contains(s, "Fn::Select") {
			t.Errorf("BUG %s: a mark answered as an output: %q", name, s)
		}
	}
}

// The refusal of an execution whose change set was made again under its name
// is written on the new change set.
func TestRV15RefusalStampsRecreatedChangeSet(t *testing.T) {
	h := newStacks(t)
	g := h.getGate(func(col, key string) bool { return col == "cfn" && key == "s" })
	changeSets(t, h, map[string]string{"a": topicTpl("OLD")})
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
		done <- err
	}()
	within(t, g.reached, "execute's load")
	if _, err := h.invoke("DeleteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a", "TemplateBody": topicTpl("NEW")}); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := within(t, done, "execute")
	d, _ := h.describe("a")
	t.Logf("execute answered %v; new a %v reason %q", err, d["ExecutionStatus"], d["StatusReason"])
	if d["StatusReason"] != nil {
		t.Errorf("BUG: the new change set a, never executed, carries the old execution's refusal: %q", d["StatusReason"])
	}
}

// An UPDATE change set's execution is in progress; DeleteStack, CreateStack of
// the same name, a new change set of the same name, whose execution starts.
// The first execution's failure must not be written on the second.
func TestRV15ExecuteVsRecreatedExecuting(t *testing.T) {
	h, g := gated(t)
	changeSets(t, h, map[string]string{"a": rv15Body(map[string]any{"Resources": paused(0)})})
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
		done <- err
	}()
	within(t, g.reached, "a's execution at the bucket")
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
		t.Fatal(err)
	}
	st, _, serr := h.status("s")
	t.Logf("after DeleteStack: %v %v", st, serr)
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": topicTpl()}); err != nil {
		t.Fatalf("CreateStack again: %v", err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a", "TemplateBody": topicTpl("A2")}); err != nil {
		t.Fatal(err)
	}
	// claim a2 by hand, as a second ExecuteChangeSet's claim would, mid-execution
	col := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn-cs")
	raw, _, _ := col.Get(h.ctx, "s:a")
	rec := spi.Decode[map[string]any](raw)
	rec["ExecutionStatus"] = "EXECUTE_IN_PROGRESS"
	_ = col.Put(h.ctx, "s:a", []byte(spi.JSONText(rec)))
	close(g.release)
	err := within(t, done, "a's execution")
	d, _ := h.describe("a")
	t.Logf("a answered %v; a2 now %v %q", err, d["ExecutionStatus"], d["StatusReason"])
	if d["ExecutionStatus"] != "EXECUTE_IN_PROGRESS" {
		t.Errorf("BUG: the first execution's end rewrote the second change set (executing) as %v, reason %q", d["ExecutionStatus"], d["StatusReason"])
	}
}

// Stack names: existing ARN forms, and a stored name outside the rule.
func TestRV15Names(t *testing.T) {
	h := newStacks(t)
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "ok", "TemplateBody": topicTpl()}); err != nil {
		t.Fatal(err)
	}
	out, _ := h.invoke("DescribeStacks", map[string]any{"StackName": "ok"})
	arn := str(out["Stacks"].([]any)[0].(map[string]any)["StackId"])
	for _, op := range []string{"DescribeStacks", "UpdateStack", "DeleteStack"} {
		in := map[string]any{"StackName": arn}
		if op == "UpdateStack" {
			in["TemplateBody"] = topicTpl("U")
		}
		_, err := h.invoke(op, in)
		t.Logf("%s by ARN: %v", op, err)
	}
	for _, n := range []string{"a:b", "1a", "a_b", strings.Repeat("a", 129), "", "A", strings.Repeat("a", 128)} {
		_, err := h.invoke("CreateStack", map[string]any{"StackName": n, "TemplateBody": topicTpl()})
		_, err2 := h.invoke("CreateChangeSet", map[string]any{"StackName": n + "x", "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateBody": topicTpl()})
		t.Logf("%q: create %v; cs %v", n, err, err2)
		if want := map[int]string{0: "length greater than or equal to 1", 129: "length less than or equal to 128"}[len(n)]; want != "" && !strings.Contains(fmt.Sprint(err), want) {
			t.Errorf("BUG: CreateStack of a %d-character name answered %v, want %q", len(n), err, want)
		}
	}
	// a record stored under an old name
	old := stack{Name: "a:b", ID: "arn:aws:cloudformation:us-east-1:000000000000:stack/a:b/1", Status: "CREATE_COMPLETE", Resources: []res{}, Outputs: []any{}, NotificationARNs: []any{}, Tags: []any{}}
	_ = h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "a:b", []byte(spi.JSONText(old)))
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "a", "TemplateBody": topicTpl()}); err != nil {
		t.Fatal(err)
	}
	_, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "a:b", "ChangeSetName": "c", "TemplateBody": topicTpl("Z")})
	t.Logf("UPDATE change set on stored a:b: %v", err)
	_, err = h.invoke("UpdateStack", map[string]any{"StackName": "a:b", "TemplateBody": topicTpl("Z")})
	t.Logf("UpdateStack on stored a:b: %v", err)
	ls, _ := h.invoke("ListChangeSets", map[string]any{"StackName": "a"})
	t.Logf("a's change sets: %v", ls)
	_, err = h.invoke("DeleteStack", map[string]any{"StackName": "a:b"})
	t.Logf("DeleteStack a:b: %v", err)
}

// Join over a string holding AWS::NoValue, no unmade resource.
func TestRV15JoinOverInValue(t *testing.T) {
	h := newStacks(t)
	for name, v := range map[string]any{
		"j1": map[string]any{"Fn::Join": []any{"", []any{"a", map[string]any{"Fn::Sub": "b${AWS::NoValue}"}}}},
		"j2": map[string]any{"Fn::Select": []any{0, []any{map[string]any{"Fn::Sub": "b${AWS::NoValue}"}}}},
		"j3": map[string]any{"Fn::Sub": []any{"${X}", map[string]any{"X": map[string]any{"Fn::Sub": "b${AWS::NoValue}"}}}},
	} {
		tpl := map[string]any{"Resources": map[string]any{"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{
			"Name": name, "SecretString": "v", "Description": v}}}}
		_, err := h.invoke("CreateStack", map[string]any{"StackName": name, "TemplateBody": rv15Body(tpl)})
		got, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": name})
		t.Logf("%s: create %v; Description %q", name, err, got["Description"])
		if code(err) != "ValidationError" {
			t.Errorf("BUG %s: AWS::NoValue inside a value answered %v, Description %q", name, err, got["Description"])
		}
	}
}

// Execute A (UPDATE, in progress) vs DeleteStack, CreateStack s, CreateChangeSet a, ExecuteChangeSet a (in progress, really).
func TestRV15TwoExecutionsSameName(t *testing.T) {
	h, g := gated(t)
	changeSets(t, h, map[string]string{"a": rv15Body(map[string]any{"Resources": paused(1)})})
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
		done <- err
	}()
	within(t, g.reached, "a's execution at the bucket")
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.live("s"); ok {
		t.Fatal("s still stored")
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": topicTpl()}); err != nil {
		t.Fatal(err)
	}
	web := map[string]any{"B2": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"WebsiteConfiguration": map[string]any{"IndexDocument": "i.html"}}}}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a", "TemplateBody": rv15Body(map[string]any{"Resources": web})}); err != nil {
		t.Fatal(err)
	}
	g2 := h.gateOn(func(c, k string, _ []byte) bool { return c == "bktcfg" && strings.HasSuffix(k, "/website") })
	done2 := make(chan error, 1)
	go func() {
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
		done2 <- err
	}()
	within(t, g2.reached, "a2's execution at its bucket")
	close(g.release)
	err := within(t, done, "a's execution")
	d, _ := h.describe("a")
	t.Logf("a answered %v; a2 mid-execution: %v %q", err, d["ExecutionStatus"], d["StatusReason"])
	if d["ExecutionStatus"] != "EXECUTE_IN_PROGRESS" {
		t.Errorf("BUG: a2, executing, now reads %v (%q), written by a's end", d["ExecutionStatus"], d["StatusReason"])
	}
	if _, err := h.invoke("DeleteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"}); err == nil {
		t.Errorf("BUG: DeleteChangeSet of a2 mid-execution succeeded")
		if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a", "TemplateBody": topicTpl("A3")}); err != nil {
			t.Fatal(err)
		}
	}
	close(g2.release)
	err2 := within(t, done2, "a2's execution")
	d, _ = h.describe("a")
	t.Logf("a2 answered %v; change set a now %v %q", err2, d["ExecutionStatus"], d["StatusReason"])
	if strings.Contains(fmt.Sprint(d["ChangeSetId"]), "") && d["ExecutionStatus"] == "EXECUTE_COMPLETE" {
		out, _ := h.invoke("DescribeStackResources", map[string]any{"StackName": "s"})
		t.Logf("resources: %s", spi.JSONText(out))
	}
	rec, _ := h.live("s")
	t.Logf("stack: %v", rec["Status"])
}

// UPDATE change set execution vs DeleteStack: no resurrection, nothing left.
func TestRV15UpdateExecVsDelete(t *testing.T) {
	h, g := gated(t)
	changeSets(t, h, map[string]string{"a": rv15Body(map[string]any{"Resources": paused(2)})})
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
		done <- err
	}()
	within(t, g.reached, "a at the bucket")
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := within(t, done, "a")
	rec, ok := h.live("s")
	t.Logf("a answered %v; stack %v %v; params %v", err, ok, rec["Status"], h.params("s-"))
	if ok || len(h.params("s-")) > 0 {
		t.Errorf("BUG: stack %v %v, params left %v", ok, rec["Status"], h.params("s-"))
	}
	kvs, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn-cs").List(h.ctx, "s:", "", 0)
	t.Logf("change sets left: %d", len(kvs))
	// UpdateStack in flight vs DeleteStack
	h2, g2 := gated(t)
	if _, err := h2.invoke("CreateStack", map[string]any{"StackName": "u", "TemplateBody": topicTpl()}); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := h2.invoke("UpdateStack", map[string]any{"StackName": "u", "TemplateBody": rv15Body(map[string]any{"Resources": paused(2)})})
		done <- err
	}()
	within(t, g2.reached, "update at the bucket")
	if _, err := h2.invoke("DeleteStack", map[string]any{"StackName": "u"}); err != nil {
		t.Fatal(err)
	}
	close(g2.release)
	err = within(t, done, "update")
	rec, ok = h2.live("u")
	t.Logf("update answered %v; stack %v %v; params %v", err, ok, rec["Status"], h2.params("u-"))
	if ok || len(h2.params("u-")) > 0 {
		t.Errorf("BUG: stack %v, params left %v", ok, h2.params("u-"))
	}
}

// B1 with DeleteStack then CreateStack (and a recreated change set) between the read and the claim.
func TestRV15ExecVsDeleteCreate(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		h := newStacks(t)
		g := h.getGate(func(col, key string) bool { return col == "cfn" && key == "s" })
		changeSets(t, h, map[string]string{"a": topicTpl("OLD")})
		g.armed.Store(true)
		done := make(chan error, 1)
		go func() {
			_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
			done <- err
		}()
		within(t, g.reached, "load")
		if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
			t.Fatal(err)
		}
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": topicTpl("NEWSTACK")}); err != nil {
			t.Fatal(err)
		}
		if recreate {
			if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a", "TemplateBody": topicTpl("NEWCS")}); err != nil {
				t.Fatal(err)
			}
		}
		close(g.release)
		err := within(t, done, "execute")
		out, _ := h.invoke("DescribeStackResources", map[string]any{"StackName": "s"})
		got := spi.JSONText(out)
		t.Logf("recreate=%v: execute %v; OLD made %v", recreate, err, strings.Contains(got, "OLD"))
		if code(err) != "ChangeSetNotFound" || strings.Contains(got, "OLD") {
			t.Errorf("BUG recreate=%v: %v %s", recreate, err, got)
		}
	}
}

// ValidateTemplate: a resource or output whose condition is unsettled (a parameter with no value).
func TestRV15ValidateUnsettledCondition(t *testing.T) {
	h := newStacks(t)
	base := func(res, out bool) map[string]any {
		tpl := map[string]any{"Parameters": map[string]any{"P": map[string]any{"Type": "String"}},
			"Conditions": map[string]any{"C": map[string]any{"Fn::Equals": []any{map[string]any{"Ref": "P"}, "x"}}},
			"Resources":  map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}}
		bad := map[string]any{"Fn::Sub": "a${AWS::NoValue}"}
		if res {
			tpl["Resources"].(map[string]any)["R"] = map[string]any{"Type": "AWS::SSM::Parameter", "Condition": "C", "Properties": map[string]any{"Type": "String", "Value": bad}}
		}
		if out {
			tpl["Outputs"] = map[string]any{"O": map[string]any{"Condition": "C", "Value": bad}}
		}
		return tpl
	}
	for name, tpl := range map[string]map[string]any{"res": base(true, false), "out": base(false, true)} {
		_, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": rv15Body(tpl)})
		t.Logf("%s: ValidateTemplate %v", name, err)
		if code(err) != "ValidationError" {
			t.Errorf("BUG %s: a value its unsettled condition may make answered %v, want ValidationError", name, err)
		}
	}
}
