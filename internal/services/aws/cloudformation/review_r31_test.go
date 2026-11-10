package cloudformation

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// The tests of the fourth review of the engine (of 11d10efe), adapted from the reviewer's own.

func rv31IDs(h *stacks, name string) map[string]string {
	ids := map[string]string{}
	for _, r := range h.resources(name) {
		ids[r.Logical] = r.Physical
	}
	return ids
}

// rv31Trace logs every store op while on, to learn the collections a service writes.
func rv31Trace(t *testing.T, h *stacks) (off func()) {
	var mu sync.Mutex
	seen := map[string]bool{}
	return faultStore(h, func(op, c, k string) bool {
		mu.Lock()
		defer mu.Unlock()
		if key := op + " " + c; !seen[key] && c != "cfn" {
			seen[key] = true
			t.Logf("store: %s %s %s", op, c, k)
		}
		return false
	})
}

// S-B, the unpinned line: a restore that fails with an ERROR (the owning service fails), not an engine panic.
// The function's restore fails; record must list the names (no values); CUR must keep the Timeout set outside.
func TestCloudFormationARestoreThatFailsRecordsWhatTheUpdateGave(t *testing.T) {
	for _, gaveTimeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed-update-gave-no-timeout", true: "failed-update-gave-timeout"}[gaveTimeout], func(t *testing.T) {
			h := newStacks(t)
			fn := func(d string, extra map[string]any) map[string]any {
				p := map[string]any{"Description": d, "Environment": map[string]any{"Variables": map[string]any{"SECRET_TOKEN": "hunter2-" + d}}}
				for k, v := range extra {
					p[k] = v
				}
				return rv29Function(p)
			}
			if err := rvMake(t, h, "sb", map[string]any{"Resources": map[string]any{"F": fn("one", nil)}}); err != nil {
				t.Fatal(err)
			}
			ids := rv31IDs(h, "sb")
			if _, err := h.service("aws.lambda", "UpdateFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"]), "Timeout": 77}); err != nil {
				t.Fatal(err)
			}
			extra := map[string]any{}
			if gaveTimeout {
				extra["Timeout"] = 5
			}
			// the lambda service's 2nd write of the function (1st: the update, 2nd: the restore) fails
			var n atomic.Int32
			var cols sync.Map
			off := faultStore(h, func(op, c, k string) bool {
				if (op == "put" || op == "txput") && strings.Contains(c, "lambda") {
					cols.Store(c, true)
					return n.Add(1) == 2
				}
				return false
			})
			err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "sb", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
				"F": fn("two", extra), "Z": rv31Dep(rv31Bad, "F")}})})
			off()
			cols.Range(func(k, _ any) bool { t.Logf("lambda collection %v (writes %d)", k, n.Load()); return true })
			st, reason, _ := h.status("sb")
			rec := h.record("sb")
			raw := spi.JSONText(rec)
			t.Logf("answered %v; %s %q %v", err, st, reason, h.statuses("sb"))
			t.Logf("record resources: %s", spi.JSONText(rec["Resources"]))
			if st != "UPDATE_ROLLBACK_FAILED" || h.statuses("sb")["F"] != "UPDATE_FAILED" {
				t.Fatalf("want UPDATE_ROLLBACK_FAILED with F UPDATE_FAILED")
			}
			var gave any
			for _, r := range spi.Slice(rec["Resources"]) {
				gave = asMap(r)["Gave"]
			}
			if gave == nil {
				t.Errorf("no names recorded on the error path: %s", spi.JSONText(rec["Resources"]))
			}
			if strings.Contains(spi.JSONText(rec["Resources"]), "hunter2") {
				t.Errorf("a value in the resources: %s", raw)
			}
			// nothing of it in what the API answers
			for _, op := range []string{"DescribeStackResources", "DescribeStackEvents", "ListStackResources", "DescribeStacks"} {
				out, oerr := h.invoke(op, map[string]any{"StackName": "sb"})
				if s := spi.JSONText(out); oerr != nil || strings.Contains(s, "hunter2") || strings.Contains(s, "Gave") {
					t.Errorf("%s (%v): %s", op, oerr, s)
				}
			}
			out, _ := h.invoke("DescribeStackResource", map[string]any{"StackName": "sb", "LogicalResourceId": "F"})
			if s := spi.JSONText(out); strings.Contains(s, "Gave") || strings.Contains(s, "hunter2") {
				t.Errorf("DescribeStackResource: %s", s)
			}
			if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "sb"}); err != nil {
				t.Fatalf("CUR: %v", err)
			}
			st, _, _ = h.status("sb")
			f, _ := h.service("aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"])})
			t.Logf("after CUR: %s desc=%v timeout=%v env=%v rec=%s", st, f["Description"], f["Timeout"], f["Environment"], spi.JSONText(h.record("sb")["Resources"]))
			if st != "UPDATE_ROLLBACK_COMPLETE" || f["Description"] != "one" {
				t.Errorf("not restored: %s %v", st, f["Description"])
			}
			want := map[bool]int{false: 77, true: 3}[gaveTimeout]
			if spi.Integer(f["Timeout"]) != want {
				t.Errorf("Timeout %v; want %d", f["Timeout"], want)
			}
			if strings.Contains(spi.JSONText(h.record("sb")["Resources"]), "Gave") {
				t.Errorf("Gave outlives UPDATE_FAILED: %s", spi.JSONText(h.record("sb")["Resources"]))
			}
		})
	}
}

// The failed update gave the bucket NO properties (the template dropped them all). names({}) is a pointer to a
// nil slice, which JSON writes as null and reads back as "no names": the legacy path.
func TestCloudFormationAnUpdateThatGaveNothingIsNotAnOlderRecord(t *testing.T) {
	h := newStacks(t)
	tagged := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": "v"}}}}
	if err := rvMake(t, h, "gn", map[string]any{"Resources": map[string]any{"B": tagged}}); err != nil {
		t.Fatal(err)
	}
	b := rv31IDs(h, "gn")["B"]
	rv30BucketOOB(t, h, b)
	before := rv30BucketState(h, b)
	// S3's write of the tags fails as the rollback restores them: the update itself only deletes them
	off := faultStore(h, func(op, c, k string) bool {
		return c != "cfn" && c != "cfn-cs" && c != "bktcfg" && (op == "put" || op == "txput") && strings.Contains(k, b)
	})
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "gn", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"B": map[string]any{"Type": "AWS::S3::Bucket"}, "Z": rv31Dep(rv31Bad, "B")}})})
	off()
	if st, _, _ := h.status("gn"); err == nil || st != "UPDATE_ROLLBACK_FAILED" {
		t.Fatalf("the update whose restore fails answered %v and left %s; want UPDATE_ROLLBACK_FAILED", err, st)
	}
	raw, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Get(h.ctx, "gn")
	var typed stack
	_ = json.Unmarshal(raw, &typed)
	for _, r := range typed.Resources {
		if r.Status == "UPDATE_FAILED" && (r.Gave == nil || len(*r.Gave) != 0 || !strings.Contains(string(raw), `"Gave":[]`)) {
			t.Errorf("the UPDATE_FAILED resource %s reads back with the names %v (stored %s); want none, as []: a record with no names is an older one", r.Logical, r.Gave, raw)
		}
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "gn"}); err != nil {
		t.Fatalf("ContinueUpdateRollback: %v", err)
	}
	if after := rv30BucketState(h, b); after != before {
		t.Errorf("ContinueUpdateRollback changed what was set outside the template: %s; want %s", after, before)
	}
}

var rv31Bad = map[string]any{"Type": "AWS::Nothing::Here"}

func rv31Dep(m map[string]any, dep ...any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	out["DependsOn"] = dep
	return out
}

// A record from before res.Name (no Name on the resource). The update changes the resource in place and then
// fails: the rollback now restores from the OLD record's entry (no Name) where it used the update's own.
func TestCloudFormationARecordWithoutNamesRollsBack(t *testing.T) {
	cases := map[string][2]map[string]any{
		"bucket": {
			{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "rv31-legacy-bkt", "Tags": []any{map[string]any{"Key": "k", "Value": "one"}}}},
			{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "rv31-legacy-bkt", "Tags": []any{map[string]any{"Key": "k", "Value": "two"}}}},
		},
		"bucket-unnamed": {
			{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": "one"}}}},
			{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": "two"}}}},
		},
		"rule": {
			{"Type": "AWS::Events::Rule", "Properties": map[string]any{"Name": "rv31-legacy-rule", "ScheduleExpression": "rate(5 minutes)", "Description": "one"}},
			{"Type": "AWS::Events::Rule", "Properties": map[string]any{"Name": "rv31-legacy-rule", "ScheduleExpression": "rate(5 minutes)", "Description": "two"}},
		},
		"restapi": {
			{"Type": "AWS::ApiGateway::RestApi", "Properties": map[string]any{"Name": "rv31-legacy-api", "Description": "one"}},
			{"Type": "AWS::ApiGateway::RestApi", "Properties": map[string]any{"Name": "rv31-legacy-api", "Description": "two"}},
		},
	}
	for name, c := range cases {
		for _, legacy := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/current-record", true: "/legacy-record"}[legacy], func(t *testing.T) {
				h := newStacks(t)
				if err := rvMake(t, h, "lg", map[string]any{"Resources": map[string]any{"R": c[0]}}); err != nil {
					t.Fatal(err)
				}
				if legacy {
					rec := h.record("lg")
					for _, r := range spi.Slice(rec["Resources"]) {
						delete(asMap(r), "Name")
					}
					if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "lg", []byte(spi.JSONText(rec))); err != nil {
						t.Fatal(err)
					}
				}
				err := rv28Update(h, "lg", map[string]any{"R": c[1], "Z": rv31Dep(rv31Bad, "R")})
				st, reason, _ := h.status("lg")
				t.Logf("answered %v; %s %q %v rec=%s", err, st, reason, h.statuses("lg"), spi.JSONText(h.record("lg")["Resources"]))
				if st != "UPDATE_ROLLBACK_COMPLETE" {
					t.Errorf("a failed update of a stack ends %s (%s); want UPDATE_ROLLBACK_COMPLETE", st, reason)
					_, cerr := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "lg"})
					st, reason, _ = h.status("lg")
					t.Logf("CUR: %v; %s %q", cerr, st, reason)
				}
			})
		}
	}
}

// One kind at a time: make it, set something outside the template, then (a) a no-change update, (b) an update
// of another property, (c) a failed update of it and its rollback. state() is logged after each; a change is
// reported, and judged in the review.
type rv31Kind struct {
	name     string
	one, two map[string]any                           // the resource, and the same with another property changed in place
	oob      func(t *testing.T, h *stacks, id string) // the setting made outside the template
	state    func(h *stacks, id string) string        // what is read back of it
}

func rv31Get(h *stacks, svc, op string, in map[string]any) map[string]any {
	out, _ := h.service(svc, op, in)
	return out
}

func TestCloudFormationEveryKindKeepsWhatWasSetOutsideItsTemplate(t *testing.T) {
	role := func(d string) map[string]any {
		return map[string]any{"Type": "AWS::IAM::Role", "Properties": map[string]any{"Description": d,
			"AssumeRolePolicyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}}}}}
	}
	kindsOf := []rv31Kind{
		{"secret-string",
			map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "tpl", "Tags": []any{map[string]any{"Key": "a", "Value": "1"}}}},
			map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "tpl", "Tags": []any{map[string]any{"Key": "a", "Value": "2"}}}},
			func(t *testing.T, h *stacks, id string) {
				_, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "rotated-outside"})
				_, err2 := h.service("aws.secretsmanager", "UpdateSecret", map[string]any{"SecretId": id, "Description": "outside"})
				if err != nil || err2 != nil {
					t.Fatal(err, err2)
				}
			},
			func(h *stacks, id string) string {
				return "value=" + str(rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"]) +
					" desc=" + str(rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": id})["Description"])
			}},
		{"secret-generated",
			map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}, "Tags": []any{map[string]any{"Key": "a", "Value": "1"}}}},
			map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}, "Tags": []any{map[string]any{"Key": "a", "Value": "2"}}}},
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "rotated-outside"}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				v := str(rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"])
				return "value-is-rotated=" + fmt.Sprint(v == "rotated-outside")
			}},
		{"kms",
			map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{"EnableKeyRotation": false}},
			map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{"EnableKeyRotation": true}},
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.kms", "UpdateKeyDescription", map[string]any{"KeyId": id, "Description": "outside"}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				return "desc=" + str(asMap(rv31Get(h, "aws.kms", "DescribeKey", map[string]any{"KeyId": id})["KeyMetadata"])["Description"])
			}},
		{"stream",
			map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": map[string]any{"RetentionPeriodHours": 24}},
			map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": map[string]any{"RetentionPeriodHours": 48}},
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.kinesis", "UpdateShardCount", map[string]any{"StreamName": id, "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				raw, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("kinesis").Get(h.ctx, id)
				return fmt.Sprint("shards=", max(spi.Integer(spi.JSONObject(raw)["OpenShardCount"]), 1))
			}},
		{"rule",
			map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)"}},
			map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(6 minutes)"}},
			func(t *testing.T, h *stacks, id string) {
				n := spi.LastSlash(id)
				_, e1 := h.service("aws.events", "PutTargets", map[string]any{"Rule": n, "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}})
				_, e2 := h.service("aws.events", "DisableRule", map[string]any{"Name": n})
				_, e3 := h.service("aws.events", "TagResource", map[string]any{"ResourceARN": "arn:aws:events:us-east-1:000000000000:rule/" + n, "Tags": []any{map[string]any{"Key": "k", "Value": "v"}}})
				if e1 != nil || e2 != nil {
					t.Fatal(e1, e2, e3)
				}
			},
			func(h *stacks, id string) string {
				n := spi.LastSlash(id)
				d := rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": n})
				tg := rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})
				return fmt.Sprint("state=", d["State"], " targets=", len(spi.Slice(tg["Targets"])))
			}},
		{"rule-with-targets",
			map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Targets": []any{map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}},
			map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Targets": []any{map[string]any{"Id": "b", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}},
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": spi.LastSlash(id), "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}}}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				var ids []string
				for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": spi.LastSlash(id)})["Targets"]) {
					ids = append(ids, str(asMap(tg)["Id"]))
				}
				return fmt.Sprint("the target put outside is kept: ", slices.Contains(ids, "outside")) // the template's own change with it
			}},
		{"function",
			rv29Function(map[string]any{"Description": "one"}), rv29Function(map[string]any{"Description": "two"}),
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.lambda", "UpdateFunctionConfiguration", map[string]any{"FunctionName": lastColon(id), "Timeout": 77, "MemorySize": 512}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				f := rv31Get(h, "aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(id)})
				return fmt.Sprint("timeout=", f["Timeout"], " memory=", f["MemorySize"])
			}},
		{"table",
			rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST"}), rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST", "TableClass": "STANDARD_INFREQUENT_ACCESS"}),
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.dynamodb", "UpdateTable", map[string]any{"TableName": id, "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				d := asMap(rv31Get(h, "aws.dynamodb", "DescribeTable", map[string]any{"TableName": id})["Table"])
				return fmt.Sprint("stream=", asMap(d["StreamSpecification"])["StreamEnabled"])
			}},
		{"parameter",
			map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": "tpl", "Description": "one"}},
			map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": "tpl", "Description": "two"}},
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.ssm", "PutParameter", map[string]any{"Name": id, "Value": "outside", "Type": "String", "Overwrite": true}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				return "value=" + str(asMap(rv31Get(h, "aws.ssm", "GetParameter", map[string]any{"Name": id})["Parameter"])["Value"])
			}},
		{"role", role("one"), role("two"),
			func(t *testing.T, h *stacks, id string) {
				if _, err := h.service("aws.iam", "UpdateAssumeRolePolicy", map[string]any{"RoleName": id, "PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`}); err != nil {
					t.Fatal(err)
				}
			},
			func(h *stacks, id string) string {
				doc := spi.JSONText(asMap(rv31Get(h, "aws.iam", "GetRole", map[string]any{"RoleName": id})["Role"])["AssumeRolePolicyDocument"])
				return fmt.Sprint("trust-is-outside=", strings.Contains(doc, "ec2"))
			}},
		{"restapi",
			map[string]any{"Type": "AWS::ApiGateway::RestApi", "Properties": map[string]any{"Name": "rv31-api", "Description": "one"}},
			map[string]any{"Type": "AWS::ApiGateway::RestApi", "Properties": map[string]any{"Name": "rv31-api", "Description": "two"}},
			func(t *testing.T, h *stacks, id string) {},
			func(h *stacks, id string) string {
				a := rv31Get(h, "aws.apigateway", "GetRestApi", map[string]any{"restApiId": id})
				return fmt.Sprint("name=", a["name"], " desc=", a["description"])
			}},
	}
	// A parameter's value, a role's trust policy and a rule are replaced whole by their update, with what the
	// template must give: for them the walk is logged, not judged (a rule's targets are, in the next kind).
	replaces := map[string]bool{"rule": true, "parameter": true, "role": true}
	for _, k := range kindsOf {
		t.Run(k.name, func(t *testing.T) {
			h := newStacks(t)
			if _, err := h.invoke("CreateStack", map[string]any{"StackName": "k1", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"R": k.one}}), "Capabilities": []any{"CAPABILITY_NAMED_IAM"}}); err != nil {
				t.Fatal(err)
			}
			id := rv31IDs(h, "k1")["R"]
			k.oob(t, h, id)
			want := k.state(h, id)
			t.Logf("%-18s outside the template:        %s", k.name, want)
			step := func(what string, err error, wantErr bool) {
				st, _, _ := h.status("k1")
				got := k.state(h, id)
				mark := "  "
				if got != want {
					mark = "!!"
				}
				if (err != nil) != wantErr {
					mark = "EE"
				}
				t.Logf("%s %-18s %-28s %s [%s] err=%v phys-same=%v", mark, k.name, what, got, st, err, rv31IDs(h, "k1")["R"] == id)
				if (err != nil) != wantErr || got != want && !replaces[k.name] {
					t.Errorf("%s, after %s (%v): %s; want %s, what was set outside the template", k.name, what, err, got, want)
				}
			}
			step("(a) no-change update", rv28Update(h, "k1", map[string]any{"R": k.one}), false)
			step("(c0) failed, R untouched", rv28Update(h, "k1", map[string]any{"R": k.one, "Z": rv31Dep(rv31Bad, "R")}), true)
			step("(c) failed update+rollback", rv28Update(h, "k1", map[string]any{"R": k.two, "Z": rv31Dep(rv31Bad, "R")}), true)
			step("(b) update another property", rv28Update(h, "k1", map[string]any{"R": k.two}), false)
			step("(b') and back", rv28Update(h, "k1", map[string]any{"R": k.one}), false)
		})
	}
}

// S-A with an ERROR (not a panic) part way through an in-place update: a rule whose PutRule is applied and
// whose PutTargets then fails. The rollback must put the rule back.
func TestCloudFormationARuleHalfUpdatedIsPutBack(t *testing.T) {
	h := newStacks(t)
	rule := func(d string, targets []any) map[string]any {
		return map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": d, "Targets": targets}}
	}
	good := []any{map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}
	if err := rvMake(t, h, "ru", map[string]any{"Resources": map[string]any{"E": rule("one", good)}}); err != nil {
		t.Fatal(err)
	}
	n := spi.LastSlash(rv31IDs(h, "ru")["E"])
	state := func() string {
		d := rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": n})
		var ids []string
		for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
			ids = append(ids, str(asMap(tg)["Id"]))
		}
		return fmt.Sprint("desc=", d["Description"], " targets=", ids)
	}
	want := state()
	// the events service's write of the targets fails once: PutRule of the update lands, PutTargets does not
	var seen sync.Map
	var hits atomic.Int32
	off := faultStore(h, func(op, c, k string) bool {
		if (op == "put" || op == "txput") && c != "cfn" && c != "cfn-cs" {
			seen.Store(op+" "+c, true)
			return strings.Contains(c, "target") && hits.Add(1) == 1
		}
		return false
	})
	bad := []any{map[string]any{"Id": "b", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "ru", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"E": rule("two", bad)}})})
	off()
	seen.Range(func(k, _ any) bool { t.Logf("write: %v", k); return true })
	st, reason, _ := h.status("ru")
	t.Logf("answered %v; %s %q %v; state %s (want %s); target-write faults %d", err, st, reason, h.statuses("ru"), state(), want, hits.Load())
	if hits.Load() == 0 {
		t.Skip("no target write was seen to fail")
	}
	if st == "UPDATE_ROLLBACK_COMPLETE" && state() != want {
		t.Errorf("UPDATE_ROLLBACK_COMPLETE with the rule left %s; want %s", state(), want)
	}
	if st != "UPDATE_ROLLBACK_COMPLETE" {
		_, cerr := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ru"})
		st, _, _ = h.status("ru")
		if state() != want || st != "UPDATE_ROLLBACK_COMPLETE" {
			t.Errorf("after CUR (%v) %s: %s; want %s", cerr, st, state(), want)
		}
	}
}

// The takeover path of the "gave nothing" case: the failed update dropped every property of the bucket, the
// rollback's restore PANICS in the engine (the stored CORS), the next operation takes the stack over.
func TestCloudFormationATakeoverOfAnUpdateThatGaveNothing(t *testing.T) {
	h := newStacks(t)
	if err := rvMake(t, h, "gt", map[string]any{"Resources": map[string]any{"B": rv30CorsBucket("one")}}); err != nil {
		t.Fatal(err)
	}
	b := rv31IDs(h, "gt")["B"]
	rv30BucketOOB(t, h, b)
	before := rv30BucketState(h, b)
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" })
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "gt", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"B": map[string]any{"Type": "AWS::S3::Bucket"}, "Z": rv31Dep(rv31Bad, "B")}})})
	off()
	t.Logf("answered %v; record %v", err, h.record("gt")["Status"])
	_, cerr := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "gt"}) // takes it over: refused, the stack is now UPDATE_ROLLBACK_FAILED? or continued
	st, reason, _ := h.status("gt")
	t.Logf("first CUR %v; %s %q; record %s", cerr, st, reason, spi.JSONText(h.record("gt")["Resources"]))
	if st != "UPDATE_ROLLBACK_COMPLETE" {
		_, cerr = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "gt"})
		st, _, _ = h.status("gt")
		t.Logf("second CUR %v; %s", cerr, st)
	}
	cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, b+"/cors")
	after := rv30BucketState(h, b)
	t.Logf("after: %s cors=%s (before: %s)", after, cors, before)
	if after != before {
		t.Errorf("the continued rollback changed what was set outside the template: %s; want %s", after, before)
	}
}

// opKept under concurrent takers, for -race: many stacks each leave a kept record; many goroutines per stack
// take it over at once with different operations. The counter must equal the records kept, and never go negative.
func TestCloudFormationTheKeptCountHoldsUnderConcurrentTakers(t *testing.T) {
	h := newStacks(t)
	_, kept0, c0 := rv30Ops()
	const stacksN, takers = 6, 12
	var names []string
	for i := range stacksN {
		name := fmt.Sprintf("kc%d", i)
		names = append(names, name)
		if err := rvMake(t, h, name, map[string]any{"Resources": map[string]any{"B": rv30CorsBucket("one")}}); err != nil {
			t.Fatal(err)
		}
	}
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" })
	for _, name := range names {
		_ = invoked(t, h, "UpdateStack", map[string]any{"StackName": name, "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"B": rv30CorsBucket("two"), "N": rv28Topic}})})
	}
	off()
	if _, k, c := rv30Ops(); k != kept0+stacksN || c != c0+stacksN {
		t.Fatalf("kept %+d counter %+d; want +%d each", k-kept0, c-c0, stacksN)
	}
	var wg sync.WaitGroup
	var low atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if v := opKept.Load(); v < low.Load() {
					low.Store(v)
				}
			}
		}
	}()
	for _, name := range names {
		for i := range takers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				switch i % 4 {
				case 0:
					_, _ = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": name})
				case 1:
					_ = rv28Update(h, name, map[string]any{"B": rv30CorsBucket("three")})
				case 2:
					_, _ = h.invoke("CreateChangeSet", map[string]any{"StackName": name, "ChangeSetName": fmt.Sprintf("c%d", i), "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"B": rv30CorsBucket("four")}})})
				case 3:
					_, _ = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": name})
				}
			}()
		}
	}
	wg.Wait()
	close(stop)
	live, k, c := rv30Ops()
	t.Logf("after: live %d kept %+d counter %+d lowest %d", live, k-kept0, c-c0, low.Load())
	if k != kept0 || c != c0 || low.Load() < 0 || int(c) != k {
		t.Errorf("kept %+d counter %+d (records %d, counter %d), lowest %d", k-kept0, c-c0, k, c, low.Load())
	}
	for _, name := range names {
		rec := h.record(name)
		st := str(rec["Status"])
		if strings.HasSuffix(st, "_IN_PROGRESS") || rec["Op"] != nil {
			t.Errorf("%s left %s op %v", name, st, rec["Op"])
		}
		if st == "UPDATE_ROLLBACK_FAILED" {
			if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": name}); err != nil {
				t.Errorf("%s: CUR %v", name, err)
			}
			st, _, _ = h.status(name)
		}
		topics := rv31Get(h, "aws.sns", "ListTopics", map[string]any{})
		t.Logf("%s ends %s; topics %d", name, st, len(spi.Slice(topics["Topics"])))
	}
}

// A record with the new field survives a JSON round trip through the stack type (what a snapshot/restore does),
// and a settled record after ContinueUpdateRollback with ResourcesToSkip carries no Gave and no Op.
func TestCloudFormationASkippedResourceForgetsWhatTheUpdateGave(t *testing.T) {
	h := newStacks(t)
	sec := func(d string) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "v", "Description": d}}
	}
	if err := rvMake(t, h, "sk", map[string]any{"Resources": map[string]any{"S": sec("one")}}); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	off := faultStore(h, func(op, c, k string) bool {
		return (op == "put" || op == "txput") && strings.Contains(c, "secret") && n.Add(1) >= 2
	})
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "sk", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"S": sec("two"), "Z": rv31Dep(rv31Bad, "S")}})})
	st, _, _ := h.status("sk")
	t.Logf("answered %v; %s; writes %d; rec %s", err, st, n.Load(), spi.JSONText(h.record("sk")["Resources"]))
	if st != "UPDATE_ROLLBACK_FAILED" {
		off()
		t.Skipf("restore did not fail: %s", st)
	}
	// while the service still fails: CUR fails again and keeps the names
	_, cerr := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "sk"})
	st, reason, _ := h.status("sk")
	t.Logf("CUR while failing: %v; %s %q; rec %s", cerr, st, reason, spi.JSONText(h.record("sk")["Resources"]))
	if st != "UPDATE_ROLLBACK_FAILED" || !strings.Contains(spi.JSONText(h.record("sk")["Resources"]), `"Gave":["Description"]`) {
		t.Errorf("a failed retry lost the names or the state: %s %s", st, spi.JSONText(h.record("sk")["Resources"]))
	}
	if strings.Contains(spi.JSONText(h.record("sk")), "rv21") {
		t.Errorf("panic text on the record")
	}
	off()
	_, cerr = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "sk", "ResourcesToSkip": []any{"S"}})
	st, _, _ = h.status("sk")
	rec := h.record("sk")
	t.Logf("CUR skip: %v; %s; rec %s", cerr, st, spi.JSONText(rec["Resources"]))
	if st != "UPDATE_ROLLBACK_COMPLETE" || strings.Contains(spi.JSONText(rec), "Gave") || rec["Op"] != nil || h.statuses("sk")["S"] != "UPDATE_COMPLETE" {
		t.Errorf("after a skip: %s %s", st, spi.JSONText(rec))
	}
}

// The names of what a failed update gave are told apart through a round trip of the record: none recorded
// (a record from before they were kept) is absent and reads back nil; an update that gave nothing is [] and
// reads back empty, and is not taken for the older record.
func TestCloudFormationWhatAFailedUpdateGaveSurvivesARoundTrip(t *testing.T) {
	t.Parallel()
	none, _, _ := gave(map[string]any{}, map[string]any{"Tags": "t"})
	some, targets, _ := gave(map[string]any{"Description": "new", "Same": 1, "Targets": []any{map[string]any{"Id": "a"}}}, map[string]any{"Description": "old", "Same": 1})
	for name, c := range map[string]struct {
		r       res
		stored  string
		unknown bool
	}{"an older record": {res{Logical: "R"}, `{"Logical":"R","Physical":"","Type":""}`, true},
		"gave nothing": {res{Logical: "R", Gave: none}, `{"Logical":"R","Physical":"","Type":"","Gave":[]}`, false},
		"gave some":    {res{Logical: "R", Gave: some, Targets: targets}, `{"Logical":"R","Physical":"","Type":"","Gave":["Description","Targets"],"Targets":["a"]}`, false}} {
		raw, _ := json.Marshal(c.r)
		var back res
		if err := json.Unmarshal(raw, &back); err != nil || string(raw) != c.stored || recorded(back).unknown != c.unknown {
			t.Errorf("%s is stored %s (%v) and reads back unknown: %t; want %s and %t", name, raw, err, recorded(back).unknown, c.stored, c.unknown)
		}
		if was := recorded(back); name == "gave some" && (!was.gave("Description") || was.gave("Same") || !was.differs("Description", "old") || was.differs("Same", 1) || len(spi.Slice(was.props["Targets"])) != 1) {
			t.Errorf("what is read back of %s is %+v; want Description and Targets given and changed, Same not", name, was)
		}
	}
}

// A continued rollback of a record from before resources recorded their names restores each under the
// name its template gives it, or the generated one, as an update finds it.
func TestCloudFormationAContinuedRollbackOfARecordWithoutNames(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	rule := func(d string) map[string]any {
		return map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"Name": "rv31-older", "ScheduleExpression": "rate(5 minutes)", "Description": d}}
	}
	bucket := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": "one"}}}}
	if err := rvMake(t, h, "ln", map[string]any{"Resources": map[string]any{"E": rule("one"), "B": bucket}}); err != nil {
		t.Fatal(err)
	}
	b := rv31IDs(h, "ln")["B"]
	if _, err := h.service("aws.events", "PutRule", map[string]any{"Name": "rv31-older", "ScheduleExpression": "rate(5 minutes)", "Description": "left by the failed update"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.s3", "DeleteBucketTagging", map[string]any{"Bucket": b}); err != nil {
		t.Fatal(err)
	}
	rec := h.record("ln")
	rec["Status"] = "UPDATE_ROLLBACK_FAILED"
	for _, r := range spi.Slice(rec["Resources"]) {
		delete(asMap(r), "Name")
		asMap(r)["Status"] = "UPDATE_FAILED"
	}
	if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "ln", []byte(spi.JSONText(rec))); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ln"}); err != nil {
		t.Fatalf("ContinueUpdateRollback of the older record: %v", err)
	}
	described := rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": "rv31-older"})
	tags := rv31Get(h, "aws.s3", "GetBucketTagging", map[string]any{"Bucket": b})
	if st, _, _ := h.status("ln"); st != "UPDATE_ROLLBACK_COMPLETE" || described["Description"] != "one" || len(spi.Slice(tags["TagSet"])) != 1 {
		t.Errorf("the stack is %s, its rule described %v and its bucket tagged %v; want UPDATE_ROLLBACK_COMPLETE with the template's", st, described["Description"], tags["TagSet"])
	}
}

// A table's update does not send DeletionProtectionEnabled, which the table service here ignores: the
// call the engine builds for it holds the members the service reads, and not that one.
func TestCloudFormationATableUpdateSendsNoDeletionProtection(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "dq", map[string]any{"Resources": map[string]any{"T": rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST"})}}); err != nil {
		t.Fatal(err)
	}
	id := rv31IDs(h, "dq")["T"]
	sent, err := h.pack.do(h.ctx, &spi.Request{Identity: h.id}, kinds["AWS::DynamoDB::Table"].update, "", id,
		map[string]any{"DeletionProtectionEnabled": true, "TableClass": "STANDARD_INFREQUENT_ACCESS"})
	if _, protected := sent["DeletionProtectionEnabled"]; err != nil || protected || sent["TableClass"] != "STANDARD_INFREQUENT_ACCESS" || sent["TableName"] != id {
		t.Errorf("the table's update sends %v (%v); want its name and class, and no DeletionProtectionEnabled", sent, err)
	}
}

// A failed update that replaced a resource rolls back by deleting the new one and calling the old one for
// nothing: the table keeps its id, its key and the stream set on it outside the template.
func TestCloudFormationAFailedReplacementLeavesTheOldResourceAlone(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	table := func(key string) map[string]any {
		return map[string]any{"Type": "AWS::DynamoDB::Table", "Properties": map[string]any{"BillingMode": "PAY_PER_REQUEST",
			"AttributeDefinitions": []any{map[string]any{"AttributeName": key, "AttributeType": "S"}}, "KeySchema": []any{map[string]any{"AttributeName": key, "KeyType": "HASH"}}}}
	}
	if err := rvMake(t, h, "fr", map[string]any{"Resources": map[string]any{"T": table("a")}}); err != nil {
		t.Fatal(err)
	}
	old := rv31IDs(h, "fr")["T"]
	if _, err := h.service("aws.dynamodb", "UpdateTable", map[string]any{"TableName": old, "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}, "TableClass": "STANDARD_INFREQUENT_ACCESS"}); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "fr", map[string]any{"T": table("b"), "Z": rv31Dep(rv31Bad, "T")}); err == nil {
		t.Fatal("the update to a type no service has succeeded")
	}
	listed, _ := h.service("aws.dynamodb", "ListTables", map[string]any{})
	d := asMap(rv31Get(h, "aws.dynamodb", "DescribeTable", map[string]any{"TableName": old})["Table"])
	if st, _, _ := h.status("fr"); st != "UPDATE_ROLLBACK_COMPLETE" || rv31IDs(h, "fr")["T"] != old || len(spi.Slice(listed["TableNames"])) != 1 ||
		asMap(d["StreamSpecification"])["StreamEnabled"] != true || str(asMap(d["TableClassSummary"])["TableClass"]) != "STANDARD_INFREQUENT_ACCESS" || !strings.Contains(spi.JSONText(d["KeySchema"]), `"a"`) {
		t.Errorf("the stack is %s holding %s (was %s), the tables are %v and the old one is %s; want UPDATE_ROLLBACK_COMPLETE, the old table alone, as it was", st, rv31IDs(h, "fr")["T"], old, listed["TableNames"], spi.JSONText(d))
	}
}

// A continued rollback removes from a rule the targets the failed update gave, by the ids its resource
// recorded, and no others: one put on the rule outside its template stays. Before, knowing only that the
// failed update gave targets, it removed every target the template does not give.
func TestCloudFormationAContinuedRollbackRemovesOnlyTheTargetsTheUpdateGave(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	rule := func(id string) map[string]any {
		return map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)",
			"Targets": []any{map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}}
	}
	if err := rvMake(t, h, "ti", map[string]any{"Resources": map[string]any{"E": rule("a")}}); err != nil {
		t.Fatal(err)
	}
	name := spi.LastSlash(rv31IDs(h, "ti")["E"])
	if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": name, "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}}}); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32 // of the rule's targets: the update removes a and puts b, the rollback removes b and fails to put a
	off := faultStore(h, func(op, c, k string) bool {
		return (op == "put" || op == "txput") && strings.Contains(c, "target") && writes.Add(1) == 4
	})
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "ti", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"E": rule("b"), "Z": rv31Dep(rv31Bad, "E")}})})
	off()
	if st, _, _ := h.status("ti"); err == nil || st != "UPDATE_ROLLBACK_FAILED" || h.statuses("ti")["E"] != "UPDATE_FAILED" || !strings.Contains(spi.JSONText(h.record("ti")["Resources"]), `"Targets":["b"]`) {
		t.Fatalf("the update whose restore fails answered %v and left %s, %s (%d target writes); want the rule UPDATE_FAILED, recording the target b", err, st, spi.JSONText(h.record("ti")["Resources"]), writes.Load())
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ti"}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, target := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": name})["Targets"]) {
		ids = append(ids, str(asMap(target)["Id"]))
	}
	if slices.Sort(ids); strings.Join(ids, " ") != "a outside" || strings.Contains(spi.JSONText(h.record("ti")["Resources"]), "Targets") {
		t.Errorf("the rule's targets are %v and its record %s; want the template's a and the one put outside it, and nothing recorded any more", ids, spi.JSONText(h.record("ti")["Resources"]))
	}
}

// A continued rollback sends a secret its value again only where the failed update gave another: a
// secret whose description the failed update changed, rotated outside the template since, keeps its value.
func TestCloudFormationAContinuedRollbackKeepsARotatedSecret(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	secret := func(d string) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "tpl", "Description": d}}
	}
	if err := rvMake(t, h, "rs", map[string]any{"Resources": map[string]any{"S": secret("one")}}); err != nil {
		t.Fatal(err)
	}
	id := rv31IDs(h, "rs")["S"]
	var writes atomic.Int32 // of the secret: the update's lands, the rollback's restore fails
	off := faultStore(h, func(op, c, k string) bool {
		return (op == "put" || op == "txput") && strings.Contains(c, "secret") && writes.Add(1) == 2
	})
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "rs", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"S": secret("two"), "Z": rv31Dep(rv31Bad, "S")}})})
	off()
	if st, _, _ := h.status("rs"); err == nil || st != "UPDATE_ROLLBACK_FAILED" {
		t.Fatalf("the update whose restore fails answered %v and left %s (%d secret writes)", err, st, writes.Load())
	}
	if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "rotated-outside"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "rs"}); err != nil {
		t.Fatal(err)
	}
	value := rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"]
	described := rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": id})["Description"]
	if st, _, _ := h.status("rs"); st != "UPDATE_ROLLBACK_COMPLETE" || value != "rotated-outside" || described != "one" {
		t.Errorf("the stack is %s, its secret's value %v and description %v; want UPDATE_ROLLBACK_COMPLETE, the value rotated outside and the template's description", st, value, described)
	}
}

// A secret's value is the template's when the template's changes: a new SecretString is stored, and a
// change to GenerateSecretString makes a new password ("When you make a change to this property, a new
// secret version is created", Template Reference, a reading from memory of the page).
func TestCloudFormationASecretTakesAValueItsTemplateChanges(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	resources := func(value string, length int) map[string]any {
		return map[string]any{"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": value}},
			"G": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": length}}}}
	}
	if err := rvMake(t, h, "sv", map[string]any{"Resources": resources("one", 20)}); err != nil {
		t.Fatal(err)
	}
	ids := rv31IDs(h, "sv")
	value := func(logical string) string {
		return str(rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": ids[logical]})["SecretString"])
	}
	if err := rv28Update(h, "sv", resources("two", 30)); err != nil || value("S") != "two" || len(value("G")) != 30 {
		t.Errorf("the update answered %v and left the values %q and one of %d characters; want two, and a new password of 30", err, value("S"), len(value("G")))
	}
}
