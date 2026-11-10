package cloudformation

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Round 5 review (rv454e), part B: the secret's value, what a failed update removed, rule targets.

func rveSecret(p map[string]any) map[string]any { return rveProps("AWS::SecretsManager::Secret", p) }

func rveValue(h *stacks, id string) string {
	return str(rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"])
}

// rveFailed updates the stack's R to two, failing after; with failRestore the restore fails too. It answers the status.
func rveFailed(t *testing.T, h *stacks, name string, two map[string]any, failRestore bool) string {
	t.Helper()
	var off func()
	if failRestore {
		off, _ = rveFailRestore(h)
	}
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": name, "Capabilities": []any{"CAPABILITY_NAMED_IAM"},
		"TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"R": two, "T": rveTrigger, "Z": rv31Dep(rv31Bad, "T")}})})
	if off != nil {
		off()
	}
	st, reason, _ := h.status(name)
	t.Logf("failed update: %v; %s %q rec=%s", err, st, reason, spi.JSONText(h.record(name)["Resources"]))
	if err == nil {
		t.Fatalf("the update did not fail")
	}
	if failRestore && st != "UPDATE_ROLLBACK_FAILED" {
		t.Fatalf("the restore did not fail: %s", st)
	}
	rveSettled(t, h, name)
	return st
}

func rveContinue(t *testing.T, h *stacks, name string, skip ...any) {
	t.Helper()
	in := map[string]any{"StackName": name}
	if len(skip) > 0 {
		in["ResourcesToSkip"] = skip
	}
	_, err := h.invoke("ContinueUpdateRollback", in)
	st, reason, _ := h.status(name)
	t.Logf("continued: %v; %s %q rec=%s", err, st, reason, spi.JSONText(h.record(name)["Resources"]))
	if err != nil || st != "UPDATE_ROLLBACK_COMPLETE" {
		t.Errorf("ContinueUpdateRollback: %v, %s", err, st)
	}
	rveSettled(t, h, name)
}

func TestCloudFormationReview5BSecret(t *testing.T) {
	type sc struct {
		name     string
		one, two map[string]any
		rotate   string // "" none, "before" the failed update, "between" the failed rollback and its continuation
		want     func(created, mid, got string) string
	}
	same := func(want string) func(string, string, string) string {
		return func(_, _, got string) string {
			if got != want {
				return "want " + want
			}
			return ""
		}
	}
	asCreated := func(created, _, got string) string {
		if got != created {
			return "want the password it was created with, " + created
		}
		return ""
	}
	cases := []sc{
		{"string-changed", rveSecret(map[string]any{"SecretString": "A"}), rveSecret(map[string]any{"SecretString": "B"}), "", same("A")},
		{"string-changed/rotated-before", rveSecret(map[string]any{"SecretString": "A"}), rveSecret(map[string]any{"SecretString": "B"}), "before", same("A")},
		{"string-changed/rotated-between", rveSecret(map[string]any{"SecretString": "A"}), rveSecret(map[string]any{"SecretString": "B"}), "between", same("A")},
		{"only-description-changed/rotated-before", rveSecret(map[string]any{"SecretString": "A", "Description": "one"}), rveSecret(map[string]any{"SecretString": "A", "Description": "two"}), "before", same("ROT")},
		{"only-description-changed/rotated-between", rveSecret(map[string]any{"SecretString": "A", "Description": "one"}), rveSecret(map[string]any{"SecretString": "A", "Description": "two"}), "between", same("ROT")},
		{"string-removed", rveSecret(map[string]any{"SecretString": "A", "Description": "one"}), rveSecret(map[string]any{"Description": "two"}), "", same("A")},
		{"string-removed/rotated-before", rveSecret(map[string]any{"SecretString": "A", "Description": "one"}), rveSecret(map[string]any{"Description": "two"}), "before", same("A")},
		{"string-added", rveSecret(map[string]any{"Description": "one"}), rveSecret(map[string]any{"SecretString": "B", "Description": "two"}), "", nil},
		{"generated/params-changed", rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}}), rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 30}}), "", asCreated},
		{"generated/params-changed/rotated-before", rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}}), rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 30}}), "before", asCreated},
		{"generated/only-description/rotated-before", rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}, "Description": "one"}), rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}, "Description": "two"}), "before", same("ROT")},
		{"generated-to-string", rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}}), rveSecret(map[string]any{"SecretString": "B"}), "", asCreated},
		{"string-to-generated", rveSecret(map[string]any{"SecretString": "A"}), rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}}), "", same("A")},
		{"number-to-string", rveSecret(map[string]any{"SecretString": 3}), rveSecret(map[string]any{"SecretString": "3", "Description": "two"}), "before", nil},
		{"generated/nested-subfield", rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20, "ExcludeCharacters": "ab"}}), rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20, "ExcludeCharacters": "abc"}}), "", asCreated},
	}
	for _, c := range cases {
		for _, continued := range []bool{false, true} {
			if c.rotate == "between" && !continued {
				continue
			}
			t.Run(fmt.Sprintf("%s/continued=%v", c.name, continued), func(t *testing.T) {
				h := newStacks(t)
				if err := rveCreate(h, "se", map[string]any{"R": c.one}); err != nil {
					t.Fatal(err)
				}
				id := rv31IDs(h, "se")["R"]
				created := rveValue(h, id)
				rot := func() {
					if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "ROT"}); err != nil {
						t.Fatal(err)
					}
				}
				if c.rotate == "before" {
					rot()
				}
				rveFailed(t, h, "se", c.two, continued)
				mid := rveValue(h, id)
				if c.rotate == "between" {
					rot()
				}
				if continued {
					rveContinue(t, h, "se")
				}
				got := rveValue(h, id)
				desc := rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": id})["Description"]
				t.Logf("RESULT %-45s continued=%-5v created=%q mid=%q final=%q desc=%v", c.name, continued, created, mid, got, desc)
				if c.want != nil {
					if msg := c.want(created, mid, got); msg != "" {
						t.Errorf("the secret's value is %q after the rollback; %s", got, msg)
					}
				}
				if want := asMap(c.one["Properties"])["Description"]; str(desc) != str(want) {
					t.Errorf("the description is %v; want %v", desc, want)
				}
				if st, _, _ := h.status("se"); st != "UPDATE_ROLLBACK_COMPLETE" {
					t.Errorf("ends %s", st)
				}
			})
		}
	}
}

// Forward updates of a secret: the template wins when it changes the value; otherwise the rotated one stays.
func TestCloudFormationReview5BSecretForward(t *testing.T) {
	h := newStacks(t)
	if err := rveCreate(h, "sf", map[string]any{"R": rveSecret(map[string]any{"SecretString": "A"}), "G": rveSecret(map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}})}); err != nil {
		t.Fatal(err)
	}
	ids := rv31IDs(h, "sf")
	g0 := rveValue(h, ids["G"])
	if len(g0) != 20 || rveValue(h, ids["R"]) != "A" {
		t.Fatalf("create: %q %q", rveValue(h, ids["R"]), g0)
	}
	for _, l := range []string{"R", "G"} {
		if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": ids[l], "SecretString": "ROT"}); err != nil {
			t.Fatal(err)
		}
	}
	up := func(r, g map[string]any) {
		t.Helper()
		if err := rv28Update(h, "sf", map[string]any{"R": rveSecret(r), "G": rveSecret(g)}); err != nil {
			t.Fatal(err)
		}
		t.Logf("R=%q G=%q", rveValue(h, ids["R"]), rveValue(h, ids["G"]))
	}
	up(map[string]any{"SecretString": "A", "Description": "d"}, map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}, "Description": "d"})
	if rveValue(h, ids["R"]) != "ROT" || rveValue(h, ids["G"]) != "ROT" {
		t.Errorf("an update of the description changed a rotated value")
	}
	up(map[string]any{"SecretString": "B", "Description": "d"}, map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 25}, "Description": "d"})
	if rveValue(h, ids["R"]) != "B" || len(rveValue(h, ids["G"])) != 25 {
		t.Errorf("the template's change did not win")
	}
	g1 := rveValue(h, ids["G"])
	// back to the first: same password as created (deterministic), logged
	up(map[string]any{"SecretString": "A"}, map[string]any{"GenerateSecretString": map[string]any{"PasswordLength": 20}})
	t.Logf("G created %q, at 25 %q, back at 20 %q (same as created: %v)", g0, g1, rveValue(h, ids["G"]), rveValue(h, ids["G"]) == g0)
	// a value given by a Ref to a parameter: the parameter's change is a change of the value
	h2 := newStacks(t)
	tpl := func() string {
		return rv19Body(map[string]any{"Parameters": map[string]any{"V": map[string]any{"Type": "String", "NoEcho": true}},
			"Resources": map[string]any{"R": rveSecret(map[string]any{"SecretString": map[string]any{"Ref": "V"}})}})
	}
	if _, err := h2.invoke("CreateStack", map[string]any{"StackName": "sp", "TemplateBody": tpl(), "Parameters": []any{map[string]any{"ParameterKey": "V", "ParameterValue": "p1"}}}); err != nil {
		t.Fatal(err)
	}
	id := rv31IDs(h2, "sp")["R"]
	if _, err := h2.invoke("UpdateStack", map[string]any{"StackName": "sp", "TemplateBody": tpl(), "Parameters": []any{map[string]any{"ParameterKey": "V", "ParameterValue": "p2"}}}); err != nil || rveValue(h2, id) != "p2" {
		t.Errorf("an update of the parameter the value is: %v, %q; want p2", err, rveValue(h2, id))
	}
	if strings.Contains(spi.JSONText(h2.record("sp")["Resources"]), "p2") {
		t.Errorf("the value is in the resources")
	}
}

// A continued rollback with the secret skipped, then updates: logged.
func TestCloudFormationReview5BSecretSkipped(t *testing.T) {
	h := newStacks(t)
	if err := rveCreate(h, "sk", map[string]any{"R": rveSecret(map[string]any{"SecretString": "A"})}); err != nil {
		t.Fatal(err)
	}
	id := rv31IDs(h, "sk")["R"]
	rveFailed(t, h, "sk", rveSecret(map[string]any{"SecretString": "B"}), true)
	rveContinue(t, h, "sk", "R")
	t.Logf("skipped: value %q (the template says A)", rveValue(h, id))
	err := rv28Update(h, "sk", map[string]any{"R": rveSecret(map[string]any{"SecretString": "A", "Description": "d"})})
	t.Logf("update of the description only: %v, value %q", err, rveValue(h, id))
}

// 3b: what the failed update REMOVED is put back by the rollback and by a continued one.
func TestCloudFormationReview5BRemovedIsPutBack(t *testing.T) {
	tgt := func(ids ...string) []any {
		var out []any
		for _, id := range ids {
			out = append(out, map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"})
		}
		return out
	}
	cases := []rveKind{
		{"function", rv29Function(map[string]any{"Description": "one", "Timeout": 9, "MemorySize": 256, "Environment": map[string]any{"Variables": map[string]any{"A": "1", "B": "2"}}}),
			rv29Function(map[string]any{"Description": "two", "Environment": map[string]any{"Variables": map[string]any{"A": "1", "B": "3"}}}),
			func(h *stacks, id string) string {
				f := rv31Get(h, "aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(id)})
				return fmt.Sprint("desc=", f["Description"], " timeout=", f["Timeout"], " mem=", f["MemorySize"], " env=", spi.JSONText(asMap(f["Environment"])["Variables"]))
			}},
		{"bucket", rveProps("AWS::S3::Bucket", map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": "one"}}, "VersioningConfiguration": map[string]any{"Status": "Enabled"},
			"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedOrigins": []any{"a"}}}}}),
			rveProps("AWS::S3::Bucket", map[string]any{"PublicAccessBlockConfiguration": map[string]any{"BlockPublicAcls": true}}),
			func(h *stacks, id string) string {
				cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, id+"/cors")
				return rv29Bucket(h, id) + " cors=" + string(cors)
			}},
		{"rule", rveProps("AWS::Events::Rule", map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": "one", "Targets": tgt("a", "b")}),
			rveProps("AWS::Events::Rule", map[string]any{"ScheduleExpression": "rate(5 minutes)"}),
			func(h *stacks, id string) string {
				n := spi.LastSlash(id)
				var ids []string
				for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
					ids = append(ids, str(asMap(tg)["Id"]))
				}
				slices.Sort(ids)
				return fmt.Sprint("desc=", rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": n})["Description"], " targets=", ids)
			}},
		{"rule-targets-reordered-and-one-swapped", rveProps("AWS::Events::Rule", map[string]any{"ScheduleExpression": "rate(5 minutes)", "Targets": tgt("a", "b")}),
			rveProps("AWS::Events::Rule", map[string]any{"ScheduleExpression": "rate(5 minutes)", "Targets": tgt("c", "a")}),
			func(h *stacks, id string) string {
				n := spi.LastSlash(id)
				var ids []string
				for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
					ids = append(ids, str(asMap(tg)["Id"]))
				}
				slices.Sort(ids)
				return fmt.Sprint("targets=", ids)
			}},
		{"table", rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST", "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}}),
			rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST", "TableClass": "STANDARD_INFREQUENT_ACCESS"}),
			func(h *stacks, id string) string {
				d := asMap(rv31Get(h, "aws.dynamodb", "DescribeTable", map[string]any{"TableName": id})["Table"])
				return fmt.Sprint("stream=", spi.JSONText(d["StreamSpecification"]), " billing=", asMap(d["BillingModeSummary"])["BillingMode"])
			}},
		{"secret", rveSecret(map[string]any{"SecretString": "A", "Description": "one"}), rveSecret(map[string]any{"SecretString": "A"}),
			func(h *stacks, id string) string {
				return "value=" + rveValue(h, id) + " desc=" + str(rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": id})["Description"])
			}},
		{"kms", rveProps("AWS::KMS::Key", map[string]any{"Description": "one"}), rveProps("AWS::KMS::Key", map[string]any{"EnableKeyRotation": true}),
			func(h *stacks, id string) string {
				return "desc=" + str(asMap(rv31Get(h, "aws.kms", "DescribeKey", map[string]any{"KeyId": id})["KeyMetadata"])["Description"])
			}},
		{"stream", rveProps("AWS::Kinesis::Stream", map[string]any{"ShardCount": 3, "RetentionPeriodHours": 24}), rveProps("AWS::Kinesis::Stream", map[string]any{"RetentionPeriodHours": 48}),
			func(h *stacks, id string) string {
				raw, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("kinesis").Get(h.ctx, id)
				return fmt.Sprint("shards=", max(spi.Integer(spi.JSONObject(raw)["OpenShardCount"]), 1))
			}},
	}
	for _, k := range cases {
		for _, continued := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continued=%v", k.name, continued), func(t *testing.T) {
				h := newStacks(t)
				if err := rveCreate(h, "rm", map[string]any{"R": k.one}); err != nil {
					t.Fatal(err)
				}
				id := rv31IDs(h, "rm")["R"]
				want := k.state(h, id)
				if k.name == "stream" && want != "shards=3" { // the stream is made with the count its template gives
					t.Fatalf("the stream was made with %s", want)
				}
				rveFailed(t, h, "rm", k.two, continued)
				mid := k.state(h, id)
				if continued {
					rveContinue(t, h, "rm")
				}
				got := k.state(h, id)
				t.Logf("RESULT %-10s continued=%-5v\n  before=%s\n  mid=   %s\n  after= %s", k.name, continued, want, mid, got)
				if got != want {
					t.Errorf("after the rollback: %s; want %s", got, want)
				}
			})
		}
	}
}

// Rule targets: the record a failed restore leaves, and what a continued rollback removes, with a target
// put outside the template; the legacy record (no names) keeps the old behaviour.
func TestCloudFormationReview5BTargets(t *testing.T) {
	tgt := func(ids ...string) []any {
		out := []any{}
		for _, id := range ids {
			out = append(out, map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"})
		}
		return out
	}
	rule := func(p map[string]any) map[string]any {
		p["ScheduleExpression"] = "rate(5 minutes)"
		return rveProps("AWS::Events::Rule", p)
	}
	for _, c := range []struct {
		name     string
		one, two map[string]any
		legacy   bool
		want     string
	}{
		{"swapped", rule(map[string]any{"Targets": tgt("a")}), rule(map[string]any{"Targets": tgt("b")}), false, "[a outside]"},
		{"unchanged-targets-other-change", rule(map[string]any{"Targets": tgt("a"), "Description": "one"}), rule(map[string]any{"Targets": tgt("a"), "Description": "two"}), false, "[a outside]"},
		{"update-gave-none", rule(map[string]any{"Targets": tgt("a"), "Description": "one"}), rule(map[string]any{"Description": "two"}), false, "[a outside]"},
		{"update-gave-empty-list", rule(map[string]any{"Targets": tgt("a"), "Description": "one"}), rule(map[string]any{"Description": "two", "Targets": []any{}}), false, "[a outside]"},
		{"template-gave-none-update-gave", rule(map[string]any{"Description": "one"}), rule(map[string]any{"Description": "two", "Targets": tgt("b")}), false, "[outside]"},
		{"update-gave-the-outside-id", rule(map[string]any{"Targets": tgt("a")}), rule(map[string]any{"Targets": tgt("a", "outside")}), false, ""},
		{"swapped-legacy", rule(map[string]any{"Targets": tgt("a")}), rule(map[string]any{"Targets": tgt("b")}), true, "[a]"},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newStacks(t)
			if err := rveCreate(h, "tg", map[string]any{"R": c.one}); err != nil {
				t.Fatal(err)
			}
			n := spi.LastSlash(rv31IDs(h, "tg")["R"])
			if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": n, "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}}}); err != nil {
				t.Fatal(err)
			}
			ids := func() string {
				var out []string
				for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
					out = append(out, str(asMap(tg)["Id"])+map[bool]string{true: "(o)"}[strings.HasSuffix(str(asMap(tg)["Arn"]), ":o") && str(asMap(tg)["Id"]) != "outside"])
				}
				slices.Sort(out)
				return fmt.Sprint(out)
			}
			// EventBridge refuses an empty list ("Targets must contain between 1 and 10 entries"), so that update
			// fails at the rule itself, before anything whose restore could fail is made: its rollback is the whole case.
			refused := c.name == "update-gave-empty-list"
			rveFailed(t, h, "tg", c.two, !refused)
			mid := ids()
			if c.legacy {
				rveStrip(t, h, "tg", true)
			}
			if !refused {
				rveContinue(t, h, "tg")
			}
			t.Logf("RESULT %-32s mid=%s after=%s", c.name, mid, ids())
			if c.want != "" && ids() != c.want {
				t.Errorf("targets %s; want %s", ids(), c.want)
			}
		})
	}
}

// What the record holds and what a round trip of it keeps: [] is not absent; nothing but names and ids.
func TestCloudFormationReview5BRecordRoundTrip(t *testing.T) {
	for _, in := range []string{`{"Logical":"a","Physical":"b","Type":"t"}`, `{"Gave":[],"Logical":"a","Physical":"b","Type":"t"}`,
		`{"Gave":[],"Logical":"a","Physical":"b","Targets":[],"Type":"t"}`, `{"Gave":["x"],"Logical":"a","Physical":"b","Targets":["i"],"Type":"t"}`} {
		r := spi.Decode[res]([]byte(in))
		if out := spi.JSONText(spi.JSONObject([]byte(spi.JSONText(r)))); out != in { // through a map: the members in name order, as in is written
			t.Errorf("%s reads back %s", in, out)
		}
		w := recorded(r)
		t.Logf("%s -> unknown=%v changed=%v props=%v gave(Targets)=%v", in, w.unknown, w.changed, w.props, w.gave("Targets"))
	}
	// a null written by an older build is the absent list
	r := spi.Decode[res]([]byte(`{"Gave":null,"Logical":"a"}`))
	if r.Gave != nil || !recorded(r).unknown {
		t.Errorf("a null list is not read as the absent one")
	}
	// gave: on values. A number and a Boolean are their text (A, E: a Number parameter's Ref is a String), a
	// list is its members in order (C), an object its members in any order (D), and nothing given is nothing (F).
	names, targets, _ := gave(map[string]any{"A": 3, "B": "3", "C": []any{1, 2}, "D": map[string]any{"x": 1, "y": 2}, "E": true, "F": nil, "G": "same"},
		map[string]any{"A": "3", "B": "3", "C": []any{2, 1}, "D": map[string]any{"y": 2, "x": 1}, "E": "true", "G": "same", "H": "only-now"})
	t.Logf("gave: %v %v", *names, targets)
	if fmt.Sprint(*names) != "[C]" || targets != nil {
		t.Errorf("gave answers %v", *names)
	}
}
