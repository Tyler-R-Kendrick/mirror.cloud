package cloudformation

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// The tests of the third review of the engine (of c45f02e7), adapted from the reviewer's own.

// rv30BucketOOB sets versioning, public access block and ownership controls on the bucket outside any template.
func rv30BucketOOB(t *testing.T, h *stacks, b string) {
	t.Helper()
	for op, in := range map[string]map[string]any{"PutBucketVersioning": {"Status": "Enabled", "VersioningConfiguration": map[string]any{"Status": "Enabled"}},
		"PutPublicAccessBlock":       {"PublicAccessBlockConfiguration": map[string]any{"BlockPublicAcls": true}},
		"PutBucketOwnershipControls": {"OwnershipControls": map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "BucketOwnerEnforced"}}}}} {
		in["Bucket"] = b
		if _, err := h.service("aws.s3", op, in); err != nil {
			t.Fatal(err)
		}
	}
}

func rv30BucketState(h *stacks, b string) string {
	v, _ := h.service("aws.s3", "GetBucketVersioning", map[string]any{"Bucket": b})
	_, block := h.service("aws.s3", "GetPublicAccessBlock", map[string]any{"Bucket": b})
	_, own := h.service("aws.s3", "GetBucketOwnershipControls", map[string]any{"Bucket": b})
	tags, _ := h.service("aws.s3", "GetBucketTagging", map[string]any{"Bucket": b})
	return "versioning=" + str(v["Status"]) + " block=" + map[bool]string{true: "set", false: "none"}[block == nil] + " ownership=" + map[bool]string{true: "set", false: "none"}[own == nil] +
		" tags=" + map[bool]string{true: "set", false: "none"}[len(spi.Slice(tags["TagSet"])) > 0]
}

const rv30Kept = "versioning=Enabled block=set ownership=set tags=none"

// B-1 variants: every way an update or a rollback touches a bucket whose template gave it nothing.
func TestCloudFormationABucketGivenNothingKeepsWhatWasSetOutsideIt(t *testing.T) {
	t.Parallel()
	bad := map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"R"}}
	cors := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"CorsConfiguration": map[string]any{"CorsRules": []any{}}}}
	for name, base := range map[string]map[string]any{"absent": {"Type": "AWS::S3::Bucket"}, "empty": {"Type": "AWS::S3::Bucket", "Properties": map[string]any{}}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if err := rvMake(t, h, "va", map[string]any{"Resources": map[string]any{"R": base, "K": rv28Topic}}); err != nil {
				t.Fatal(err)
			}
			var b string
			for _, r := range h.resources("va") {
				if r.Logical == "R" {
					b = r.Physical
				}
			}
			rv30BucketOOB(t, h, b)
			check := func(what string, err error, wantErr bool) {
				t.Helper()
				if got := rv30BucketState(h, b); got != rv30Kept || (err != nil) != wantErr {
					t.Errorf("after %s (%v): %s; want %s", what, err, got, rv30Kept)
				}
			}
			check("cors added (bktcfg only)", rv28Update(h, "va", map[string]any{"R": cors, "K": rv28Topic}), false)
			check("cors dropped", rv28Update(h, "va", map[string]any{"R": base, "K": rv28Topic}), false)
			check("rollback of cors add", rv28Update(h, "va", map[string]any{"R": cors, "K": rv28Topic, "Z": bad}), true)
			check("other resource dropped", rv28Update(h, "va", map[string]any{"R": base}), false)
			// the bucket's own update fails half way: a bad tag set after nothing else
			badTags := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": []any{map[string]any{"Key": "aws:reserved", "Value": strings.Repeat("x", 400)}}}}
			err := rv28Update(h, "va", map[string]any{"R": badTags})
			t.Logf("bad tags update: %v; state %s", err, rv30BucketState(h, b))
			if err != nil {
				check("rollback of the bucket's own failed update", err, true)
			}
			// via a change set
			if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "va", "ChangeSetName": "c1", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"R": cors}})}); err != nil {
				t.Fatal(err)
			}
			_, err = h.invoke("ExecuteChangeSet", map[string]any{"StackName": "va", "ChangeSetName": "c1"})
			check("change set execution", err, false)
			st, _, _ := h.status("va")
			t.Logf("final %s", st)
		})
	}
}

// A property the old template gave through Fn::If and the new parameters take away is cleared; one it never
// gave is not. And the other way around on the way back.
func TestCloudFormationAPropertyAConditionTakesAwayIsCleared(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	tpl := map[string]any{
		"Parameters": map[string]any{"On": map[string]any{"Type": "String", "Default": "no"}},
		"Conditions": map[string]any{"C": map[string]any{"Fn::Equals": []any{map[string]any{"Ref": "On"}, "yes"}}},
		"Resources": map[string]any{"R": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{
			"Tags": map[string]any{"Fn::If": []any{"C", []any{map[string]any{"Key": "a", "Value": "b"}}, map[string]any{"Ref": "AWS::NoValue"}}}}}},
	}
	if err := rvMake(t, h, "cf", tpl); err != nil {
		t.Fatal(err)
	}
	b := h.resources("cf")[0].Physical
	rv30BucketOOB(t, h, b)
	upd := func(on string, extra map[string]any) error {
		body := map[string]any{"Parameters": tpl["Parameters"], "Conditions": tpl["Conditions"], "Resources": map[string]any{"R": asMap(tpl["Resources"])["R"]}}
		for k, v := range extra {
			asMap(body["Resources"])[k] = v
		}
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "cf", "TemplateBody": rv19Body(body), "Parameters": []any{map[string]any{"ParameterKey": "On", "ParameterValue": on}}})
		return err
	}
	if err := upd("yes", nil); err != nil {
		t.Fatal(err)
	}
	if got, want := rv30BucketState(h, b), "versioning=Enabled block=set ownership=set tags=set"; got != want {
		t.Errorf("flip on: %s want %s", got, want)
	}
	// a failing update that flips it off: rollback must restore tags and keep the rest
	err := upd("no", map[string]any{"Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": "R"}})
	if got, want := rv30BucketState(h, b), "versioning=Enabled block=set ownership=set tags=set"; got != want || err == nil {
		t.Errorf("rollback of flip off (%v): %s want %s", err, got, want)
	}
	if err := upd("no", nil); err != nil {
		t.Fatal(err)
	}
	if got := rv30BucketState(h, b); got != rv30Kept {
		t.Errorf("flip off: %s want %s", got, rv30Kept)
	}
}

// The same rule for the other types an update changes in place: what the template never gave and was set
// outside it stays through an update of another property and through a rollback.
func TestCloudFormationEveryTypeKeepsWhatWasSetOutsideItsTemplate(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	secret := func(v string) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": v}}
	}
	rule := func(d string) map[string]any {
		return map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": d}}
	}
	param := func(v string) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": v}}
	}
	fn := func(extra map[string]any) map[string]any { return rv29Function(extra) }
	tbl := func(extra map[string]any) map[string]any { return rv29Table(extra) }
	res := func(v string) map[string]any {
		return map[string]any{"S": secret(v), "E": rule(v), "F": fn(map[string]any{"Description": v}), "T": tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST", "TableClass": map[string]string{"one": "STANDARD", "two": "STANDARD_INFREQUENT_ACCESS"}[v]}), "P": param(v)}
	}
	if err := rvMake(t, h, "ot2", map[string]any{"Resources": res("one")}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, r := range h.resources("ot2") {
		ids[r.Logical] = r.Physical
	}
	// outside the template
	if _, err := h.service("aws.secretsmanager", "UpdateSecret", map[string]any{"SecretId": ids["S"], "Description": "outside"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": spi.LastSlash(ids["E"]), "Targets": []any{map[string]any{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.lambda", "UpdateFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"]), "Timeout": 77, "MemorySize": 512, "Environment": map[string]any{"Variables": map[string]any{"A": "B"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.dynamodb", "UpdateTable", map[string]any{"TableName": ids["T"], "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}, "DeletionProtectionEnabled": true}); err != nil {
		t.Fatal(err)
	}
	state := func() string {
		d, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]})
		tg, _ := h.service("aws.events", "ListTargetsByRule", map[string]any{"Rule": spi.LastSlash(ids["E"])})
		f, _ := h.service("aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"])})
		tb, _ := h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": ids["T"]})
		table := asMap(tb["Table"])
		return "secretDesc=" + str(d["Description"]) + " targets=" + str(len(spi.Slice(tg["Targets"]))) + " timeout=" + str(f["Timeout"]) + " mem=" + str(f["MemorySize"]) +
			" env=" + spi.JSONText(asMap(f["Environment"])["Variables"]) + " stream=" + str(asMap(table["StreamSpecification"])["StreamEnabled"]) + " delprot=" + str(table["DeletionProtectionEnabled"])
	}
	want := state()
	t.Logf("before: %s", want)
	if err := rv28Update(h, "ot2", res("two")); err != nil {
		t.Fatal(err)
	}
	if got := state(); got != want {
		t.Errorf("after an update of another property:\n got  %s\n want %s", got, want)
	}
	failing := res("one")
	failing["Z"] = map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"S", "E", "F", "T", "P"}}
	if err := rv28Update(h, "ot2", failing); err == nil {
		t.Fatal("want failure")
	}
	if got := state(); got != want {
		t.Errorf("after a rollback:\n got  %s\n want %s", got, want)
	}
	st, _, _ := h.status("ot2")
	t.Logf("status %s; after: %s", st, state())
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "ot2"}); err != nil {
		t.Logf("DeleteStack: %v", err)
	}
	st, reason, _ := h.status(h.record("ot2")["ID"].(string))
	t.Logf("after delete: %s %s", st, reason)
}

// A table the template gave DeletionProtectionEnabled and gives it no longer: the claim is the table
// service keeps it false, so there is nothing to clear.
func TestCloudFormationATablesDeletionProtectionIsNotSent(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "dp", map[string]any{"Resources": map[string]any{"T": rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST", "DeletionProtectionEnabled": true})}}); err != nil {
		t.Fatal(err)
	}
	id := h.resources("dp")[0].Physical
	tb, _ := h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": id})
	t.Logf("created: delprot=%v", asMap(tb["Table"])["DeletionProtectionEnabled"])
	if err := rv28Update(h, "dp", map[string]any{"T": rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST", "TableClass": "STANDARD_INFREQUENT_ACCESS", "DeletionProtectionEnabled": true})}); err != nil {
		t.Fatal(err)
	}
	tb, _ = h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": id})
	t.Logf("updated with it: delprot=%v", asMap(tb["Table"])["DeletionProtectionEnabled"])
	if err := rv28Update(h, "dp", map[string]any{"T": rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST"})}); err != nil {
		t.Fatal(err)
	}
	tb, _ = h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": id})
	if v := asMap(tb["Table"])["DeletionProtectionEnabled"]; v == true {
		t.Errorf("the update that drops DeletionProtectionEnabled left it %v", v)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "dp"}); err != nil {
		t.Errorf("DeleteStack: %v", err)
	}
}

// rv30Ops counts live operations, kept records, and the counter of kept records.
func rv30Ops() (live, records int, counter int64) {
	ops.Range(func(_, v any) bool {
		if _, isKept := v.(kept); isKept {
			records++
		} else {
			live++
		}
		return true
	})
	return live, records, opKept.Load()
}

func rv30CorsBucket(origin string, dep ...any) map[string]any {
	m := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedOrigins": []any{origin}}}}}}
	if len(dep) > 0 {
		m["DependsOn"] = dep
	}
	return m
}

func rv30Secret(d string, dep ...any) map[string]any {
	m := map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "v", "Description": d}}
	if len(dep) > 0 {
		m["DependsOn"] = dep
	}
	return m
}

// S-2, my own shape: secret S restored first, then the bucket's restore panics, with the function F still to
// come. S is put back and must not be marked; B and F must be UPDATE_FAILED after a takeover, and
// ContinueUpdateRollback must put them back. Also shows the documented exception on F (Timeout set outside).
func TestCloudFormationAContinuedRollbackClearsOnlyWhatTheFailedUpdateGave(t *testing.T) {
	h := newStacks(t)
	live0, kept0, _ := rv30Ops()
	fn := func(d string) map[string]any {
		f := rv29Function(map[string]any{"Description": d})
		f["DependsOn"] = []any{"B"}
		return f
	}
	if err := rvMake(t, h, "s2", map[string]any{"Resources": map[string]any{"S": rv30Secret("one"), "B": rv30CorsBucket("one", "S"), "F": fn("one")}}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, r := range h.resources("s2") {
		ids[r.Logical] = r.Physical
	}
	if _, err := h.service("aws.lambda", "UpdateFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"]), "Timeout": 77}); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" && n.Add(1) == 2 })
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "s2", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"S": rv30Secret("two"), "B": rv30CorsBucket("two", "S"), "F": fn("two"), "N": rv28Topic, "Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"F", "N"}}}})})
	off()
	if !isInternalFailure(err) {
		t.Fatalf("answered %v", err)
	}
	if f, ok := err.(*spi.Fault); ok && strings.Contains(f.Message, "rv21") {
		t.Errorf("panic text in the answer: %v", f.Message)
	}
	rec := h.record("s2")
	t.Logf("left: status=%v op=%v", rec["Status"], rec["Op"])
	if l, k, _ := rv30Ops(); l != live0 || k != kept0+1 {
		t.Errorf("after the panic: live %+d kept %+d; want +0 and +1", l-live0, k-kept0)
	}
	// take it over straight through ContinueUpdateRollback
	_, err = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "s2"})
	st, reason, _ := h.status("s2")
	t.Logf("CUR: %v; status %s reason %q statuses %v", err, st, reason, h.statuses("s2"))
	if strings.Contains(reason, "rv21") || strings.Contains(spi.JSONText(h.record("s2")), "rv21") {
		t.Errorf("panic text on the record: %s", spi.JSONText(h.record("s2")))
	}
	described, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]})
	cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, ids["B"]+"/cors")
	f, _ := h.service("aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"])})
	topics, _ := h.service("aws.sns", "ListTopics", map[string]any{})
	if st != "UPDATE_ROLLBACK_COMPLETE" || described["Description"] != "one" || !strings.Contains(string(cors), `"one"`) || f["Description"] != "one" || len(spi.Slice(topics["Topics"])) != 0 {
		t.Errorf("status %s secret %v cors %s fn %v topics %v", st, described["Description"], cors, f["Description"], topics["Topics"])
	}
	if spi.Integer(f["Timeout"]) != 77 { // the failed update gave no Timeout, so the continued rollback clears none
		t.Errorf("the function's Timeout, set outside the template, was 77 and is %v", f["Timeout"])
	}
	if l, k, c := rv30Ops(); l != live0 || k != kept0 || int(c) != k {
		t.Errorf("at the end: live %+d kept %+d counter %d records %d", l-live0, k-kept0, c, k)
	}
	if h.record("s2")["Op"] != nil {
		t.Errorf("settled record carries Op %v", h.record("s2")["Op"])
	}
}

// The same panic, and what the takeover's record lists before ContinueUpdateRollback: S was put back.
func TestCloudFormationATakeoverMarksOnlyWhatWasNotPutBack(t *testing.T) {
	h := newStacks(t)
	if err := rvMake(t, h, "s3", map[string]any{"Resources": map[string]any{"S": rv30Secret("one"), "B": rv30CorsBucket("one", "S"), "T": rv30Secret("one", "B")}}); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" && n.Add(1) == 2 })
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "s3", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"S": rv30Secret("two"), "B": rv30CorsBucket("two", "S"), "T": rv30Secret("two", "B"), "N": rv28Topic, "Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"T", "N"}}}})})
	off()
	if !isInternalFailure(err) {
		t.Fatalf("answered %v", err)
	}
	_, derr := h.invoke("DeleteChangeSet", map[string]any{"StackName": "s3", "ChangeSetName": "none"}) // not a taker
	_ = derr
	if rec := h.record("s3"); rec["Status"] != "UPDATE_IN_PROGRESS" {
		t.Logf("record before takeover: %v", rec["Status"])
	}
	_, uerr := h.invoke("UpdateStack", map[string]any{"StackName": "s3", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"S": rv30Secret("one")}})})
	st, reason, _ := h.status("s3")
	got := h.statuses("s3")
	t.Logf("taken over by a refused UpdateStack (%v): %s %q %v", uerr, st, reason, got)
	if st != "UPDATE_ROLLBACK_FAILED" || got["B"] != "UPDATE_FAILED" || got["T"] != "UPDATE_FAILED" || got["S"] == "UPDATE_FAILED" || got["N"] != "DELETE_FAILED" {
		t.Errorf("want S restored (not UPDATE_FAILED), B and T UPDATE_FAILED, N DELETE_FAILED; got %s %v", st, got)
	}
	// DeleteStack from here
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s3"}); err != nil {
		t.Errorf("DeleteStack: %v", err)
	}
	topics, _ := h.service("aws.sns", "ListTopics", map[string]any{})
	if len(spi.Slice(topics["Topics"])) != 0 {
		t.Errorf("topics left: %v", topics["Topics"])
	}
}

// A panic of the engine's own in the update itself, and then again in the rollback it starts.
func TestCloudFormationAnUpdateThatPanicsTwiceIsContinued(t *testing.T) {
	h := newStacks(t)
	live0, kept0, _ := rv30Ops()
	if err := rvMake(t, h, "s4", map[string]any{"Resources": map[string]any{"S": rv30Secret("one"), "B": rv30CorsBucket("one", "S")}}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, r := range h.resources("s4") {
		ids[r.Logical] = r.Physical
	}
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" })
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "s4", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"S": rv30Secret("two"), "B": rv30CorsBucket("two", "S"), "N": rv28Topic}})})
	off()
	t.Logf("double panic answered %v; record %v op %v", err, h.record("s4")["Status"], h.record("s4")["Op"])
	if !isInternalFailure(err) {
		t.Errorf("answered %v", err)
	}
	if l, k, _ := rv30Ops(); l != live0 || k != kept0+1 {
		t.Errorf("after the panics: live %+d kept %+d; want +0 and +1", l-live0, k-kept0)
	}
	_, err = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "s4"})
	st, _, _ := h.status("s4")
	described, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]})
	cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, ids["B"]+"/cors")
	t.Logf("CUR %v: %s %v secret=%v cors=%s", err, st, h.statuses("s4"), described["Description"], cors)
	if st != "UPDATE_ROLLBACK_COMPLETE" || described["Description"] != "one" || !strings.Contains(string(cors), `"one"`) {
		t.Errorf("not restored")
	}
	if l, k, c := rv30Ops(); l != live0 || k != kept0 || int(c) != k {
		t.Errorf("at the end: live %+d kept %+d counter %d", l-live0, k-kept0, c)
	}
}

// S-1: a kept record is pruned only by its own stack: not by one of the same name in another account, nor
// by one whose name it is a prefix of; and it is gone once its stack is taken over.
func TestCloudFormationAKeptRecordIsReleasedOnlyByItsOwnStack(t *testing.T) {
	h := newStacks(t)
	_, kept0, _ := rv30Ops()
	bad := map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "N", "Properties": map[string]any{"QueueName": "rv30-nofifo", "FifoQueue": true}}
	if err := rvMake(t, h, "kp", map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	off := faultStore(h, once(func(op, c, k string) bool { return op == "list" && c == "cfn" }))
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "kp", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "N": rv28Topic, "Z": bad}})})
	off()
	if !isInternalFailure(err) {
		t.Fatal(err)
	}
	was := h.id
	for _, other := range []struct {
		what string
		id   spi.Identity
		name string
	}{{"another account", spi.Identity{Account: "111111111111", Region: was.Region}, "kp"}, {"another region", spi.Identity{Account: was.Account, Region: "eu-west-1"}, "kp"},
		{"a longer name", was, "kp-2"}, {"a name it extends", was, "k"}, {"a stack id of another region's stack of its name", was, "arn:aws:cloudformation:eu-west-1:" + was.Account + ":stack/kp2/abc"}} {
		h.id = other.id
		_, _ = h.invoke("CreateStack", map[string]any{"StackName": other.name, "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic}})})
		_, _ = h.invoke("DeleteStack", map[string]any{"StackName": other.name})
		h.id = was
		if _, k, _ := rv30Ops(); k != kept0+1 {
			t.Errorf("after an operation on %s the kept records are %+d; want +1", other.what, k-kept0)
		}
	}
	// by its stack id, in its own region
	id := str(h.record("kp")["ID"])
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": id}); err != nil {
		t.Errorf("CUR by id: %v", err)
	}
	out, _ := h.service("aws.sns", "ListTopics", map[string]any{})
	if st, _, _ := h.status("kp"); st != "UPDATE_ROLLBACK_COMPLETE" || strings.Contains(spi.JSONText(out["Topics"]), ":kp-N-") {
		t.Errorf("%s %s", st, spi.JSONText(out["Topics"]))
	}
	if _, k, c := rv30Ops(); k != kept0 || int(c) != k {
		t.Errorf("at the end kept %+d counter %d", k-kept0, c)
	}
}

// A stack deleted and made again under its name while a record of its earlier generation is kept: the new
// one is not given the old one's record.
func TestCloudFormationAnotherGenerationIsNotGivenAKeptRecord(t *testing.T) {
	h := newStacks(t)
	_, kept0, _ := rv30Ops()
	bad := map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "N", "Properties": map[string]any{"QueueName": "rv30-nofifo2", "FifoQueue": true}}
	if err := rvMake(t, h, "rg", map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	off := faultStore(h, once(func(op, c, k string) bool { return op == "list" && c == "cfn" }))
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "rg", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "N": rv28Topic, "Z": bad}})})
	off()
	if !isInternalFailure(err) {
		t.Fatal(err)
	}
	// the record is replaced under the engine, as a restored store would: a new generation, in progress under an op unknown here
	col := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn")
	rec := h.record("rg")
	rec["ID"] = strings.Replace(str(rec["ID"]), "stack/rg/", "stack/rg/ffff", 1)
	rec["Op"] = "/someone-else-9"
	_ = col.Put(h.ctx, "rg", []byte(spi.JSONText(rec)))
	_, err = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "rg"})
	st, reason, _ := h.status("rg")
	t.Logf("CUR on the other generation: %v; %s %q %v", err, st, reason, h.statuses("rg"))
	if _, k, c := rv30Ops(); k != kept0 || int(c) != k {
		t.Errorf("at the end kept %+d counter %d records %d", k-kept0, c, k)
	}
}

// N-1: nothing stays registered after any refusal or ending.
func TestCloudFormationNothingStaysRegistered(t *testing.T) {
	h := newStacks(t)
	live0, kept0, c0 := rv30Ops()
	same := func(after string) {
		t.Helper()
		if l, k, c := rv30Ops(); l != live0 || k != kept0 || c != c0 {
			t.Errorf("after %s: live %+d kept %+d counter %+d", after, l-live0, k-kept0, c-c0)
		}
	}
	topic := rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic}})
	inv := func(op string, in map[string]any) error { _, err := h.invoke(op, in); return err }
	_ = inv("CreateStack", map[string]any{"StackName": "n1", "TemplateBody": topic})
	same("a create")
	t.Log(inv("CreateStack", map[string]any{"StackName": "n1", "TemplateBody": topic}))
	same("a refused create")
	t.Log(inv("UpdateStack", map[string]any{"StackName": "nope", "TemplateBody": topic}))
	same("an update of no stack")
	t.Log(inv("UpdateStack", map[string]any{"StackName": "n1", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": map[string]any{"Type": "AWS::SQS::Queue"}}})}))
	same("a retyping update")
	t.Log(inv("UpdateStack", map[string]any{"StackName": "n1", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "Z": map[string]any{"Type": "AWS::Nothing::Here"}}})}))
	same("a rolled back update")
	t.Log(inv("CreateChangeSet", map[string]any{"StackName": "n1", "ChangeSetName": "c", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "M": rv28Topic}})}))
	t.Log(inv("ExecuteChangeSet", map[string]any{"StackName": "n1", "ChangeSetName": "c"}))
	same("a change set executed")
	t.Log(inv("ExecuteChangeSet", map[string]any{"StackName": "n1", "ChangeSetName": "c"}))
	same("a change set executed twice")
	t.Log(inv("ContinueUpdateRollback", map[string]any{"StackName": "n1"}))
	same("a refused ContinueUpdateRollback")
	// stale rereads to Throttling
	inner := h.deps.Store
	s := &staleStore{Store: inner}
	s.n.Store(10)
	h.deps.Store = s
	h.pack, h.cfn = New(h.deps), bundled.Handler("aws.cloudformation", h.deps)
	t.Log(inv("UpdateStack", map[string]any{"StackName": "n1", "TemplateBody": topic}))
	same("an update that gives up on a stale stack")
	h.deps.Store = inner
	h.pack, h.cfn = New(h.deps), bundled.Handler("aws.cloudformation", h.deps)
	// termination protection refuses the delete
	_ = inv("UpdateTerminationProtection", map[string]any{"StackName": "n1", "EnableTerminationProtection": true})
	t.Log(inv("DeleteStack", map[string]any{"StackName": "n1"}))
	same("a refused delete")
	_ = inv("UpdateTerminationProtection", map[string]any{"StackName": "n1", "EnableTerminationProtection": false})
	t.Log(inv("DeleteStack", map[string]any{"StackName": "n1"}))
	t.Log(inv("DeleteStack", map[string]any{"StackName": "n1"}))
	same("deletes")
	// a failed create
	t.Log(inv("CreateStack", map[string]any{"StackName": "n2", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "Z": map[string]any{"Type": "AWS::Nothing::Here"}}})}))
	same("a failed create")
	st, _, _ := h.status("n2")
	t.Logf("n2 %s", st)
}

// N-4 and its edges.
func TestCloudFormationAChangeSetIsCheckedForAChangeOfType(t *testing.T) {
	h := newStacks(t)
	if err := rvMake(t, h, "c4", map[string]any{"Resources": map[string]any{"N": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	queue := rv19Body(map[string]any{"Resources": map[string]any{"N": map[string]any{"Type": "AWS::SQS::Queue"}}})
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "c4", "ChangeSetName": "c", "TemplateBody": queue}); err != nil {
		t.Fatal(err)
	}
	cs, _ := h.invoke("DescribeChangeSet", map[string]any{"StackName": "c4", "ChangeSetName": "c"})
	t.Logf("retyping change set: Status=%v ExecutionStatus=%v Reason=%v", cs["Status"], cs["ExecutionStatus"], cs["StatusReason"])
	_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "c4", "ChangeSetName": "c"})
	st, _, _ := h.status("c4")
	if cs["Status"] != "FAILED" || cs["ExecutionStatus"] != "UNAVAILABLE" || code(err) != "InvalidChangeSetStatus" || st != "CREATE_COMPLETE" {
		t.Errorf("%v %v %v %s", cs["Status"], cs["ExecutionStatus"], err, st)
	}
	// a CREATE change set on a new name with any type is fine, and a second CREATE change set giving another type too
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "c5", "ChangeSetName": "a", "ChangeSetType": "CREATE", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"N": rv28Topic}})}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "c5", "ChangeSetName": "b", "ChangeSetType": "CREATE", "TemplateBody": queue}); err != nil {
		t.Fatal(err)
	}
	b, _ := h.invoke("DescribeChangeSet", map[string]any{"StackName": "c5", "ChangeSetName": "b"})
	if b["Status"] != "CREATE_COMPLETE" {
		t.Errorf("a CREATE change set of another type than its sibling is %v (%v)", b["Status"], b["StatusReason"])
	}
	// an available change set made stale: the stack retyped under it by... a type cannot change; so by a resource added of the other type
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "c4", "ChangeSetName": "d", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"N": rv28Topic, "Q": map[string]any{"Type": "AWS::SQS::Queue"}}})}); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "c4", map[string]any{"N": rv28Topic, "Q": rv28Topic}); err != nil {
		t.Fatal(err)
	}
	_, err = h.invoke("ExecuteChangeSet", map[string]any{"StackName": "c4", "ChangeSetName": "d"})
	d, derr := h.invoke("DescribeChangeSet", map[string]any{"StackName": "c4", "ChangeSetName": "d"})
	st, _, _ = h.status("c4")
	t.Logf("executing a change set the stack moved under: %v; change set %v/%v (%v); stack %s", err, d["Status"], d["ExecutionStatus"], derr, st)
}

// Many takers at once of a stack an update left in progress: one settles it, the registry ends balanced.
func TestCloudFormationManyTakersSettleOneStack(t *testing.T) {
	bad := map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "N", "Properties": map[string]any{"QueueName": "rv30-race-nofifo", "FifoQueue": true}}
	for i := 0; i < 6; i++ {
		h := newStacks(t)
		live0, kept0, c0 := rv30Ops()
		if err := rvMake(t, h, "rc", map[string]any{"Resources": map[string]any{"K": rv28Topic, "S": rv30Secret("one")}}); err != nil {
			t.Fatal(err)
		}
		off := faultStore(h, once(func(op, c, k string) bool { return op == "list" && c == "cfn" }))
		err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "rc", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "S": rv30Secret("two"), "N": rv28Topic, "Z": bad}})})
		off()
		if !isInternalFailure(err) {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for g := 0; g < 12; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				switch g % 4 {
				case 0, 1:
					_, _ = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "rc"})
				case 2:
					_, _ = h.invoke("UpdateStack", map[string]any{"StackName": "rc", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "S": rv30Secret("one"), "M": rv28Topic}})})
				case 3:
					_, _ = h.invoke("CreateChangeSet", map[string]any{"StackName": "rc", "ChangeSetName": "c" + string(rune('a'+g)), "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic}})})
				}
			}(g)
		}
		wg.Wait()
		rec := h.record("rc")
		st := str(rec["Status"])
		if strings.HasSuffix(st, "_IN_PROGRESS") || rec["Op"] != nil {
			t.Errorf("round %d: left %s op %v", i, st, rec["Op"])
		}
		if l, k, c := rv30Ops(); l != live0 || k != kept0 || c != c0 {
			t.Errorf("round %d (%s): live %+d kept %+d counter %+d", i, st, l-live0, k-kept0, c-c0)
		}
	}
}

// An engine panic inside the in-place update of a resource, after part of it was applied: the bucket's tags
// are put through S3, then the store write of its CORS panics. The rollback must put the tags back, or the
// stack must not say UPDATE_ROLLBACK_COMPLETE.
func TestCloudFormationAHalfMadeChangeIsPutBack(t *testing.T) {
	h := newStacks(t)
	bucket := func(v string) map[string]any {
		return map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": v}},
			"VersioningConfiguration": map[string]any{"Status": map[string]string{"one": "Suspended", "two": "Enabled"}[v]},
			"CorsConfiguration":       map[string]any{"CorsRules": []any{map[string]any{"AllowedOrigins": []any{v}}}}}}
	}
	if err := rvMake(t, h, "pi", map[string]any{"Resources": map[string]any{"B": bucket("one")}}); err != nil {
		t.Fatal(err)
	}
	b := h.resources("pi")[0].Physical
	state := func() string {
		tags, _ := h.service("aws.s3", "GetBucketTagging", map[string]any{"Bucket": b})
		v, _ := h.service("aws.s3", "GetBucketVersioning", map[string]any{"Bucket": b})
		cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, b+"/cors")
		return "tags=" + spi.JSONText(tags["TagSet"]) + " versioning=" + str(v["Status"]) + " cors=" + string(cors)
	}
	want := state()
	off := faultStore(h, once(func(op, c, k string) bool { return op == "put" && c == "bktcfg" }))
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "pi", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"B": bucket("two")}})})
	off()
	st, reason, _ := h.status("pi")
	t.Logf("answered %v; %s %q %v", err, st, reason, h.statuses("pi"))
	if got := state(); st == "UPDATE_ROLLBACK_COMPLETE" && got != want {
		t.Errorf("the stack is UPDATE_ROLLBACK_COMPLETE under its previous template, but the bucket has\n  %s\nwant\n  %s", got, want)
	}
	if st != "UPDATE_ROLLBACK_COMPLETE" {
		_, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "pi"})
		st, _, _ = h.status("pi")
		if got := state(); got != want {
			t.Errorf("after ContinueUpdateRollback (%v, %s) the bucket has %s; want %s", err, st, got, want)
		}
	}
}

// A key whose template gives no Description, described outside it, updated in place.
func TestCloudFormationAKeyKeepsADescriptionSetOutsideItsTemplate(t *testing.T) {
	h := newStacks(t)
	key := func(rot bool) map[string]any {
		return map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{"EnableKeyRotation": rot}}
	}
	if err := rvMake(t, h, "ky", map[string]any{"Resources": map[string]any{"K": key(false)}}); err != nil {
		t.Fatal(err)
	}
	id := h.resources("ky")[0].Physical
	if _, err := h.service("aws.kms", "UpdateKeyDescription", map[string]any{"KeyId": id, "Description": "outside"}); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "ky", map[string]any{"K": key(true)}); err != nil {
		t.Fatal(err)
	}
	described := func() string {
		d, _ := h.service("aws.kms", "DescribeKey", map[string]any{"KeyId": id})
		return str(asMap(d["KeyMetadata"])["Description"])
	}
	if got := described(); got != "outside" || h.resources("ky")[0].Physical != id {
		t.Errorf("after an update of another property the key (%s, was %s) is described %q; want outside", h.resources("ky")[0].Physical, id, got)
	}
	if err := rv28Update(h, "ky", map[string]any{"K": key(false), "Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": "K"}}); err == nil {
		t.Fatal("the update to a type no service has succeeded")
	}
	if got := described(); got != "outside" {
		t.Errorf("after a rollback the key is described %q; want outside", got)
	}
	// One the template gave and gives no longer is cleared.
	given := map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{"EnableKeyRotation": true, "Description": "the template's"}}
	if err := rv28Update(h, "ky", map[string]any{"K": given}); err != nil || described() != "the template's" {
		t.Fatalf("the update that gives a description answered %v and left %q", err, described())
	}
	if err := rv28Update(h, "ky", map[string]any{"K": key(true)}); err != nil || described() != "" {
		t.Errorf("the update that drops the description answered %v and left %q; want none", err, described())
	}
}

// The same for a secret and a rule alone, to keep the repro small.
func TestCloudFormationASecretAndARuleKeepWhatWasSetOutsideTheirTemplates(t *testing.T) {
	h := newStacks(t)
	res := func(v string) map[string]any {
		return map[string]any{"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": v}},
			"E": map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": v}}}
	}
	if err := rvMake(t, h, "sr", map[string]any{"Resources": res("one")}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, r := range h.resources("sr") {
		ids[r.Logical] = r.Physical
	}
	if _, err := h.service("aws.secretsmanager", "UpdateSecret", map[string]any{"SecretId": ids["S"], "Description": "outside"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": spi.LastSlash(ids["E"]), "Targets": []any{map[string]any{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "sr", res("two")); err != nil {
		t.Fatal(err)
	}
	d, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]})
	tg, _ := h.service("aws.events", "ListTargetsByRule", map[string]any{"Rule": spi.LastSlash(ids["E"])})
	if d["Description"] != "outside" {
		t.Errorf("secret: the template never gave a Description; the one set outside it is now %q", str(d["Description"]))
	}
	if len(spi.Slice(tg["Targets"])) != 1 {
		t.Errorf("rule: the template never gave Targets; the one put outside it is gone: %v", tg["Targets"])
	}
}

// A stream's shard count is the template's only where the template gives one: a stream it gives none,
// resharded outside it, keeps its shards through an update of another property, and one whose template
// drops its ShardCount has one shard again.
func TestCloudFormationAStreamKeepsShardsSetOutsideItsTemplate(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	stream := func(extra map[string]any) map[string]any {
		return map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": extra}
	}
	if err := rvMake(t, h, "sh", map[string]any{"Resources": map[string]any{"S": stream(map[string]any{"RetentionPeriodHours": 24})}}); err != nil {
		t.Fatal(err)
	}
	name := h.resources("sh")[0].Physical
	shards := func() int { // as the stream's record holds it: DescribeStreamSummary here answers 1 whatever it is
		raw, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("kinesis").Get(h.ctx, name)
		return max(spi.Integer(spi.JSONObject(raw)["OpenShardCount"]), 1)
	}
	if _, err := h.service("aws.kinesis", "UpdateShardCount", map[string]any{"StreamName": name, "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"}); err != nil || shards() != 2 {
		t.Fatalf("resharding outside the template answered %v and left %d shards", err, shards())
	}
	if err := rv28Update(h, "sh", map[string]any{"S": stream(map[string]any{"RetentionPeriodHours": 48})}); err != nil || shards() != 2 {
		t.Errorf("an update of another property answered %v and left %d shards; want the 2 set outside the template", err, shards())
	}
	if err := rv28Update(h, "sh", map[string]any{"S": stream(map[string]any{"RetentionPeriodHours": 48, "ShardCount": 3})}); err != nil || shards() != 3 {
		t.Fatalf("the update that gives a ShardCount answered %v and left %d shards", err, shards())
	}
	if err := rv28Update(h, "sh", map[string]any{"S": stream(map[string]any{"RetentionPeriodHours": 48})}); err != nil || shards() != 1 {
		t.Errorf("the update that drops the ShardCount answered %v and left %d shards; want 1", err, shards())
	}
}

// A rule's targets that the template gave and gives no longer are removed, and no others: one put on the
// rule outside its template stays through an update that changes the template's.
func TestCloudFormationARuleKeepsATargetPutOutsideItsTemplate(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	rule := func(id string) map[string]any {
		return map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)",
			"Targets": []any{map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}}
	}
	if err := rvMake(t, h, "rt", map[string]any{"Resources": map[string]any{"E": rule("a")}}); err != nil {
		t.Fatal(err)
	}
	name := spi.LastSlash(h.resources("rt")[0].Physical)
	if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": name, "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}}}); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "rt", map[string]any{"E": rule("b")}); err != nil {
		t.Fatal(err)
	}
	out, _ := h.service("aws.events", "ListTargetsByRule", map[string]any{"Rule": name})
	var ids []string
	for _, target := range spi.Slice(out["Targets"]) {
		ids = append(ids, str(asMap(target)["Id"]))
	}
	if got := strings.Join(ids, " "); len(ids) != 2 || !strings.Contains(got, "b") || !strings.Contains(got, "outside") {
		t.Errorf("the rule's targets are %v; want the template's b and the one put outside it, and not a", ids)
	}
}

// A kept record is counted once as it is released, however many operations come to release it. Not
// parallel: the count is the process's.
func TestCloudFormationAKeptRecordIsReleasedOnce(t *testing.T) {
	before := opKept.Load()
	ops.Store("rv30-kept", kept{})
	opKept.Add(1)
	unkeep("rv30-kept")
	unkeep("rv30-kept")
	if got := opKept.Load(); got != before {
		t.Errorf("after two releases of one kept record the count is %d; want %d", got, before)
	}
}
