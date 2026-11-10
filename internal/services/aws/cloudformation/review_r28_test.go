package cloudformation

import (
	"bytes"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

var rv28Topic = map[string]any{"Type": "AWS::SNS::Topic"}

func rv28Update(h *stacks, name string, resources map[string]any) error {
	_, err := h.invoke("UpdateStack", map[string]any{"StackName": name, "TemplateBody": rv19Body(map[string]any{"Resources": resources}), "Capabilities": []any{"CAPABILITY_NAMED_IAM"}})
	return err
}

// events answers the ResourceStatus of each event DescribeStackEvents lists for the logical id.
func (h *stacks) events(stack, logical string) []string {
	h.t.Helper()
	out, err := h.invoke("DescribeStackEvents", map[string]any{"StackName": stack})
	if err != nil {
		h.t.Fatalf("DescribeStackEvents %s: %v", stack, err)
	}
	var statuses []string
	for _, e := range spi.Slice(out["StackEvents"]) {
		if str(asMap(e)["LogicalResourceId"]) == logical {
			statuses = append(statuses, str(asMap(e)["ResourceStatus"]))
		}
	}
	return statuses
}

// A finished update's cleanup runs once the update is written: "the stack has been updated and is usable,
// but CloudFormation is still deleting the old resources" (User Guide, UPDATE_COMPLETE_CLEANUP_IN_PROGRESS).
// So a panic of the engine's own in the cleanup rolls nothing back: the stack stays UPDATE_COMPLETE with
// the new template, what it lists exists as the template gives it, and what the cleanup had not deleted is
// abandoned, a DELETE_FAILED event. Before, the panic ran the update's rollback over the written record:
// it deleted what the update made and restored the old properties under an UPDATE_COMPLETE stack, and with
// a replacement both generations were gone.
func TestCloudFormationACleanupThatPanicsRollsNothingBack(t *testing.T) {
	t.Parallel()
	secret := func(d string) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "v", "Description": d}}
	}
	for name, c := range map[string]struct {
		pick      func() func(op, c, k string) bool
		abandoned bool // whether the old topic is left, abandoned, or was deleted before the panic
	}{
		"its last write": {func() func(op, c, k string) bool {
			var n atomic.Int32 // the stack's third write of the update: the reservation, the update, the cleanup
			return func(op, c, k string) bool { return op == "txput" && c == "cfn" && k == "cp" && n.Add(1) == 3 }
		}, false},
		"its first delete": {func() func(op, c, k string) bool {
			return once(func(op, c, k string) bool { return op == "list" && c == "cfn" })
		}, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if err := rvMake(t, h, "cp", map[string]any{"Resources": map[string]any{"K": rv28Topic, "S": secret("one")}}); err != nil {
				t.Fatal(err)
			}
			old := h.resources("cp")[slices.IndexFunc(h.resources("cp"), func(r res) bool { return r.Logical == "K" })]
			off := faultStore(h, c.pick())
			err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "cp", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"N": rv28Topic, "S": secret("two")}})})
			off()
			if status, _, _ := h.status("cp"); err != nil || status != "UPDATE_COMPLETE" {
				t.Fatalf("the update answered %v and the stack is %s; want it done, UPDATE_COMPLETE", err, status)
			}
			listed := h.resources("cp")
			for _, r := range listed {
				if _, err := h.lookup(r); err != nil {
					t.Errorf("the UPDATE_COMPLETE stack lists %s (%s), which is gone: %v", r.Logical, r.Physical, err)
				}
			}
			out, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": h.record("cp")["Refs"].(map[string]any)["S"]})
			if len(listed) != 2 || out["Description"] != "two" {
				t.Errorf("the stack lists %v and its secret is described %v; want N and S, as the new template gives them", listed, out["Description"])
			}
			_, gone := h.lookup(old)
			if abandoned := slices.Contains(h.events("cp", "K"), "DELETE_FAILED"); (gone == nil) != c.abandoned || abandoned != c.abandoned {
				t.Errorf("the old topic exists: %t, and is recorded DELETE_FAILED: %t; want both %t", gone == nil, abandoned, c.abandoned)
			}
			if _, reason, _ := h.status("cp"); strings.Contains(reason, "InternalFailure") || c.abandoned != strings.Contains(reason, "cleanup ended before it deleted") {
				t.Errorf("the stack's reason is %q; want the cleanup's own wording where it abandoned the topic, and nothing of the panic", reason)
			}
			if err := rv28Update(h, "cp", map[string]any{"N": rv28Topic, "S": secret("two"), "X": rv28Topic}); err != nil {
				t.Errorf("the next update answered %v", err)
			}
		})
	}
	t.Run("a replacement", func(t *testing.T) {
		t.Parallel()
		h := newStacks(t)
		policy := func(stream string) map[string]any {
			return map[string]any{"Type": "AWS::Kinesis::ResourcePolicy", "Properties": map[string]any{
				"ResourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/" + stream, "ResourcePolicy": map[string]any{"Version": "2012-10-17", "Statement": []any{}}}}
		}
		if err := rvMake(t, h, "cr", map[string]any{"Resources": map[string]any{"P": policy("one")}}); err != nil {
			t.Fatal(err)
		}
		var n atomic.Int32
		off := faultStore(h, func(op, c, k string) bool { return op == "txput" && c == "cfn" && k == "cr" && n.Add(1) == 3 })
		err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "cr", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"P": policy("two")}})})
		off()
		out, _ := h.service("aws.kinesis", "GetResourcePolicy", map[string]any{"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/two"})
		if status, _, _ := h.status("cr"); err != nil || status != "UPDATE_COMPLETE" || str(out["Policy"]) == "" {
			t.Errorf("the update answered %v, the stack is %s and the new policy is %q; want the replacement in place", err, status, str(out["Policy"]))
		}
	})
}

// Two types may share a name, a bucket and a parameter, so a resource is told by its type and its physical
// id together. A dropped bucket whose name a kept parameter has is deleted. Before, the cleanup matched the
// physical id alone, read the bucket as still the stack's, and left it behind, listed by no one.
func TestCloudFormationADroppedResourceGoesThoughAnotherTypeHasItsName(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	bucket := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "rv28same"}}
	param := map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "rv28same", "Type": "String", "Value": "v"}}
	if err := rvMake(t, h, "sn", map[string]any{"Resources": map[string]any{"B": bucket, "P": param}}); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "sn", map[string]any{"P": param, "K": rv28Topic}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv28same"}); err == nil {
		t.Error("the bucket the update dropped is still there")
	}
	if out, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "rv28same"}); err != nil || len(h.resources("sn")) != 2 {
		t.Errorf("the kept parameter answers %v (%v) and the stack lists %v; want it kept, with the topic", out, err, h.resources("sn"))
	}
}

// A logical id keeps its type: CloudFormation refuses an update that gives one another, "Update of resource
// type is not permitted. The new template modifies resource type of the following resources: [X]" (the
// service's wording as reported; the guides do not print it, so a reading). Nothing is made and nothing
// goes. Before, the update made the new type and its cleanup deleted the old resource as a replaced one,
// by the new template's UpdateReplacePolicy: a DeletionPolicy: Retain bucket was deleted.
func TestCloudFormationRefusesAChangeOfType(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "tc", map[string]any{"Resources": map[string]any{"X": map[string]any{"Type": "AWS::S3::Bucket", "DeletionPolicy": "Retain"}, "K": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	before := h.resources("tc")
	err := rv28Update(h, "tc", map[string]any{"X": map[string]any{"Type": "AWS::SNS::Topic", "DeletionPolicy": "Retain"}, "K": rv28Topic})
	if code(err) != "ValidationError" || !strings.Contains(err.Error(), "Update of resource type is not permitted") || !strings.Contains(err.Error(), "[X]") {
		t.Errorf("the update answered %v; want ValidationError naming [X]", err)
	}
	if status, _, _ := h.status("tc"); status != "CREATE_COMPLETE" || !slices.Equal(h.resources("tc"), before) {
		t.Errorf("the stack is %s listing %v; want it as it was, %v", status, h.resources("tc"), before)
	}
	for _, r := range before {
		if _, err := h.lookup(r); err != nil {
			t.Errorf("%s is gone after the refused update: %v", r.Logical, err)
		}
	}
}

// ListStacks "Returns the summary information for stacks whose status matches the specified
// StackStatusFilter ... If no StackStatusFilter is specified, summary information for all stacks is returned
// (including existing stacks and stacks that have been deleted)". Before, the filter was not read.
func TestCloudFormationListStacksFiltersByStatus(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	for _, name := range []string{"ls1", "ls2"} {
		if err := rvMake(t, h, name, map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "ls1"}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		filter []any
		want   []string
	}{{[]any{"CREATE_COMPLETE"}, []string{"ls2"}}, {[]any{"DELETE_COMPLETE"}, []string{"ls1"}}, {[]any{"DELETE_COMPLETE", "CREATE_COMPLETE"}, []string{"ls1", "ls2"}},
		{[]any{"UPDATE_COMPLETE"}, nil}, {nil, []string{"ls1", "ls2"}}, {[]any{}, []string{"ls1", "ls2"}}} {
		in := map[string]any{}
		if c.filter != nil {
			in["StackStatusFilter"] = c.filter
		}
		out, err := h.invoke("ListStacks", in)
		var names []string
		for _, s := range spi.Slice(out["StackSummaries"]) {
			names = append(names, str(asMap(s)["StackName"]))
		}
		if slices.Sort(names); err != nil || !slices.Equal(names, c.want) {
			t.Errorf("ListStacks with the filter %v lists %v (%v); want %v", c.filter, names, err, c.want)
		}
	}
}

// What a failed update made is deleted by its rollback but what DeletionPolicy: Retain keeps, and a
// rollback that did not settle, taken over by the next operation, keeps the same: the retained topic
// leaves the stack, DELETE_SKIPPED, and RetainExceptOnCreate's, made by the operation rolled back, is
// deleted by ContinueUpdateRollback. Before, the takeover listed both DELETE_FAILED and
// ContinueUpdateRollback deleted both.
func TestCloudFormationATakeoverKeepsWhatRetainKeeps(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	topic := func(policy string) map[string]any {
		return map[string]any{"Type": "AWS::SNS::Topic", "DeletionPolicy": policy}
	}
	bad := map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": []any{"N", "E"}, "Properties": map[string]any{"QueueName": "rv28-nofifo", "FifoQueue": true}}
	if err := rvMake(t, h, "tr", map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	off := faultStore(h, once(func(op, c, k string) bool { return op == "list" && c == "cfn" })) // the rollback's first delete
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "tr", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "N": topic("Retain"), "E": topic("RetainExceptOnCreate"), "Z": bad}})})
	off()
	if !isInternalFailure(err) {
		t.Fatalf("the update whose rollback panics answered %v; want InternalFailure", err)
	}
	topics := func() string {
		out, _ := h.service("aws.sns", "ListTopics", map[string]any{})
		return spi.JSONText(out["Topics"])
	}
	if err := invoked(t, h, "ContinueUpdateRollback", map[string]any{"StackName": "tr"}); err != nil {
		t.Fatalf("ContinueUpdateRollback of the stack taken over answered %v", err)
	}
	status, _, _ := h.status("tr")
	if left := topics(); status != "UPDATE_ROLLBACK_COMPLETE" || len(h.resources("tr")) != 1 || !strings.Contains(left, ":tr-N-") || strings.Contains(left, ":tr-E-") {
		t.Errorf("the stack is %s listing %v, with the topics %s; want UPDATE_ROLLBACK_COMPLETE, K alone, N kept and E deleted", status, h.resources("tr"), left)
	}
	if n, e := h.events("tr", "N"), h.events("tr", "E"); !slices.Contains(n, "DELETE_SKIPPED") || slices.Contains(e, "DELETE_SKIPPED") {
		t.Errorf("N's events are %v and E's %v; want N DELETE_SKIPPED and E not", n, e)
	}
}

// A deleted stack changes no more: DeleteStack by its stack id answers as a delete does and writes
// nothing. And a delete that ends cleanly has no StackStatusReason: the reason is "associated with the
// stack status" (the model), and the earlier failure's is not DELETE_COMPLETE's. Before, a DeleteStack by
// the id of a deleted stack reserved the record and settled it again, and a deleted stack kept the reason
// its create failed with.
func TestCloudFormationADeletedStackIsNotDeletedAgain(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "dd", map[string]any{"Resources": map[string]any{"K": rv28Topic, "Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": "K"}}}); err == nil {
		t.Fatal("the create of a type no service has succeeded")
	}
	id := str(h.record("dd")["ID"])
	if status, reason, _ := h.status("dd"); status != "ROLLBACK_COMPLETE" || reason == "" {
		t.Fatalf("the failed create is %s (%q); want ROLLBACK_COMPLETE, with why", status, reason)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "dd"}); err != nil {
		t.Fatal(err)
	}
	if status, reason, _ := h.status(id); status != "DELETE_COMPLETE" || reason != "" {
		t.Errorf("the deleted stack is %s (%q); want DELETE_COMPLETE with no reason", status, reason)
	}
	col := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn")
	before, _, _ := col.Get(h.ctx, "dd")
	writes := 0
	off := faultStore(h, func(op, c, k string) bool {
		writes += map[bool]int{true: 1}[strings.HasSuffix(op, "put") && c == "cfn"]
		return false
	})
	_, err := h.invoke("DeleteStack", map[string]any{"StackName": id})
	off()
	if after, _, _ := col.Get(h.ctx, "dd"); err != nil || writes != 0 || !bytes.Equal(before, after) {
		t.Errorf("DeleteStack of the deleted stack, by its id, answered %v and wrote %d times; want nothing written", err, writes)
	}
}

// An update, and a rollback's restore, leave a resource with exactly its template's properties: a property
// the template no longer gives is cleared through its service's own call, since each service's update
// keeps what a call does not name. A table's stream is disabled, a function's description, environment and
// timeout are its API's defaults ("The default is 3 seconds"), and a bucket's tags, public access block and
// stored CORS are deleted and its versioning suspended ("it can never return to an unversioned state. You
// can, however, suspend versioning"). ContinueUpdateRollback, which does not know what the failed update
// gave, clears every such property its template lacks. Before, each update sent only what the template
// gave, so what a failed update added stayed after its rollback.
func TestCloudFormationAnUpdateClearsWhatItsTemplateNoLongerGives(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	with := func(typ string, base, extra map[string]any) map[string]any {
		p := map[string]any{}
		for _, m := range []map[string]any{base, extra} {
			for k, v := range m {
				p[k] = v
			}
		}
		return map[string]any{"Type": typ, "Properties": p}
	}
	table := map[string]any{"AttributeDefinitions": []any{map[string]any{"AttributeName": "k", "AttributeType": "S"}},
		"KeySchema": []any{map[string]any{"AttributeName": "k", "KeyType": "HASH"}}, "BillingMode": "PAY_PER_REQUEST"}
	function := map[string]any{"Runtime": "python3.12", "Handler": "i.h", "Role": "arn:aws:iam::000000000000:role/r", "Code": map[string]any{"ZipFile": "def h(e,c): pass"}}
	resources := func(full bool) map[string]any {
		t, f, b := map[string]any{}, map[string]any{}, map[string]any{}
		if full {
			t = map[string]any{"StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}}
			f = map[string]any{"Description": "added", "Environment": map[string]any{"Variables": map[string]any{"A": "b"}}, "Timeout": 77}
			b = map[string]any{"VersioningConfiguration": map[string]any{"Status": "Enabled"}, "Tags": []any{map[string]any{"Key": "a", "Value": "b"}},
				"PublicAccessBlockConfiguration": map[string]any{"BlockPublicAcls": true}, "CorsConfiguration": map[string]any{"CorsRules": []any{}}}
		}
		return map[string]any{"T": with("AWS::DynamoDB::Table", table, t), "F": with("AWS::Lambda::Function", function, f), "B": with("AWS::S3::Bucket", nil, b)}
	}
	if err := rvMake(t, h, "ex", map[string]any{"Resources": resources(false)}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, r := range h.resources("ex") {
		ids[r.Logical] = r.Physical
	}
	bare := func(when string) {
		t.Helper()
		tbl, _ := h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": ids["T"]})
		if spec := asMap(asMap(tbl["Table"])["StreamSpecification"]); spec["StreamEnabled"] != false {
			t.Errorf("%s: the table's stream is %v; want it disabled", when, spec)
		}
		fn, _ := h.service("aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"])})
		if str(fn["Description"]) != "" || len(asMap(asMap(fn["Environment"])["Variables"])) != 0 || spi.Integer(fn["Timeout"]) != 3 {
			t.Errorf("%s: the function has Description %v, Environment %v, Timeout %v; want none, none and 3", when, fn["Description"], fn["Environment"], fn["Timeout"])
		}
		versioning, _ := h.service("aws.s3", "GetBucketVersioning", map[string]any{"Bucket": ids["B"]})
		tags, _ := h.service("aws.s3", "GetBucketTagging", map[string]any{"Bucket": ids["B"]})
		block := rv32Bucket(h, ids["B"]) // what S3 gives a new bucket, not the one setting the template gave
		_, cors, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, ids["B"]+"/cors")
		if versioning["Status"] == "Enabled" || len(spi.Slice(tags["TagSet"])) != 0 || !strings.Contains(block, `"RestrictPublicBuckets":true`) || cors {
			t.Errorf("%s: the bucket has versioning %v, tags %v, %s, CORS: %t; want suspended, none, a new bucket's block and none", when, versioning["Status"], tags["TagSet"], block, cors)
		}
	}
	full := resources(true)
	full["Z"] = map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"T", "F", "B"}}
	if err := rv28Update(h, "ex", full); err == nil {
		t.Fatal("the update to a type no service has succeeded")
	}
	if status, _, _ := h.status("ex"); status != "UPDATE_ROLLBACK_COMPLETE" {
		t.Fatalf("the failed update left the stack %s", status)
	}
	bare("after the rollback")

	if err := rv28Update(h, "ex", resources(true)); err != nil {
		t.Fatal(err)
	}
	if fn, _ := h.service("aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"])}); spi.Integer(fn["Timeout"]) != 77 {
		t.Fatalf("the update did not set the function's timeout: %v", fn["Timeout"])
	}
	if err := rv28Update(h, "ex", resources(false)); err != nil {
		t.Fatal(err)
	}
	bare("after an update that drops them")

	// ContinueUpdateRollback restores a resource the rollback could not: it holds what the failed update gave it.
	if _, err := h.service("aws.lambda", "UpdateFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["F"]), "Description": "left", "Timeout": 9}); err != nil {
		t.Fatal(err)
	}
	col := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn")
	rec := h.record("ex")
	rec["Status"] = "UPDATE_ROLLBACK_FAILED"
	for _, r := range spi.Slice(rec["Resources"]) {
		if asMap(r)["Logical"] == "F" {
			asMap(r)["Status"] = "UPDATE_FAILED"
		}
	}
	if err := col.Put(h.ctx, "ex", []byte(spi.JSONText(rec))); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ex"}); err != nil {
		t.Fatal(err)
	}
	bare("after ContinueUpdateRollback")
}

// rv28Failed is a stack named name left UPDATE_ROLLBACK_FAILED by an update whose rollback could not
// delete the bucket it made, bucket, which holds an object; emptied is the bucket's object deleted.
func rv28Failed(t *testing.T, h *stacks, name, bucket string) (emptied func()) {
	t.Helper()
	if err := rvMake(t, h, name, map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	objects := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("objects")
	if err := objects.Put(h.ctx, bucket+"/k", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	err := rv28Update(h, name, map[string]any{"K": rv28Topic, "B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": bucket}},
		"Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": "B"}})
	if status, _, _ := h.status(name); err == nil || status != "UPDATE_ROLLBACK_FAILED" {
		t.Fatalf("the failing update answered %v and left %s; want UPDATE_ROLLBACK_FAILED", err, status)
	}
	return func() {
		if err := objects.Delete(h.ctx, bucket+"/k"); err != nil {
			t.Fatal(err)
		}
	}
}

// ContinueUpdateRollback reserves its stack as every operation does: UPDATE_ROLLBACK_IN_PROGRESS under its
// own id, so while it runs a DeleteStack, an UpdateStack and a second ContinueUpdateRollback are each
// refused and change nothing, and it settles the stack itself. Before, it took no reservation: the stack
// read UPDATE_ROLLBACK_FAILED while it ran, and a DeleteStack ran beside it.
func TestCloudFormationContinueUpdateRollbackHoldsItsStack(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	rv28Failed(t, h, "cu", "rv28held")()
	reached, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	off := faultStore(h, func(op, col, k string) bool {
		if op == "delete" && strings.Contains(k, "rv28held") && first.CompareAndSwap(false, true) { // the bucket's delete, held once
			close(reached)
			<-release
		}
		return false
	})
	defer off()
	done := make(chan error, 1)
	go func() { _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "cu"}); done <- err }()
	within(t, reached, "ContinueUpdateRollback reaching the bucket")
	held := h.record("cu")
	if held["Status"] != "UPDATE_ROLLBACK_IN_PROGRESS" || held["Op"] == nil {
		t.Errorf("while it runs the record is %v, held by %v; want UPDATE_ROLLBACK_IN_PROGRESS under its operation", held["Status"], held["Op"])
	}
	for op, want := range map[string]string{"DeleteStack": "cannot be deleted while in status UPDATE_ROLLBACK_IN_PROGRESS", "UpdateStack": "is in UPDATE_ROLLBACK_IN_PROGRESS state",
		"ContinueUpdateRollback": "cannot be called from current stack status"} {
		_, err := h.invoke(op, map[string]any{"StackName": "cu", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic}})})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s during the ContinueUpdateRollback answered %v; want %q", op, err, want)
		}
	}
	if rec := h.record("cu"); rec["Status"] != "UPDATE_ROLLBACK_IN_PROGRESS" || rec["Op"] != held["Op"] {
		t.Errorf("after the refusals the record is %v, held by %v; want it unchanged", rec["Status"], rec["Op"])
	}
	close(release)
	if err := within(t, done, "ContinueUpdateRollback"); err != nil {
		t.Fatalf("ContinueUpdateRollback answered %v", err)
	}
	if rec := h.record("cu"); rec["Status"] != "UPDATE_ROLLBACK_COMPLETE" || rec["Op"] != nil || len(h.resources("cu")) != 1 {
		t.Errorf("it ended %v, held by %v, listing %v; want UPDATE_ROLLBACK_COMPLETE, held by none, the topic alone", rec["Status"], rec["Op"], h.resources("cu"))
	}
}

// A ContinueUpdateRollback that ended without settling its stack is taken over as any operation is: the
// next operation finds UPDATE_ROLLBACK_IN_PROGRESS under an operation that is not running, settles it
// UPDATE_ROLLBACK_FAILED, and goes on from there. One whose process ended (an id this process never gave)
// and one that panicked here are both taken over, and a second ContinueUpdateRollback then finishes.
func TestCloudFormationAContinueUpdateRollbackLeftIsTakenOver(t *testing.T) {
	t.Parallel()
	for name, leave := range map[string]func(t *testing.T, h *stacks){
		"another process's": func(t *testing.T, h *stacks) {
			rec := h.record("to")
			rec["Status"], rec["Op"] = "UPDATE_ROLLBACK_IN_PROGRESS", "/another-process-1"
			if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "to", []byte(spi.JSONText(rec))); err != nil {
				t.Fatal(err)
			}
		},
		"one that panicked": func(t *testing.T, h *stacks) {
			off := faultStore(h, once(func(op, c, k string) bool { return op == "list" && c == "cfn" })) // its first delete
			defer off()
			if err := invoked(t, h, "ContinueUpdateRollback", map[string]any{"StackName": "to"}); !isInternalFailure(err) {
				t.Fatalf("the ContinueUpdateRollback that panics answered %v; want InternalFailure", err)
			}
			if rec := h.record("to"); rec["Status"] != "UPDATE_ROLLBACK_IN_PROGRESS" {
				t.Fatalf("the panic left the record %v; want UPDATE_ROLLBACK_IN_PROGRESS, for the next operation", rec["Status"])
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			rv28Failed(t, h, "to", "rv28left")()
			leave(t, h)
			_, err := h.invoke("UpdateStack", map[string]any{"StackName": "to", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic}})})
			status, reason, _ := h.status("to")
			if err == nil || !strings.Contains(err.Error(), "is in UPDATE_ROLLBACK_FAILED state") || status != "UPDATE_ROLLBACK_FAILED" || h.record("to")["Op"] != nil {
				t.Fatalf("the next operation answered %v and the stack is %s; want it taken over, UPDATE_ROLLBACK_FAILED, and refused", err, status)
			}
			if kept := strings.Contains(reason, "BucketNotEmpty"); kept != (name == "one that panicked") { // this process's is settled as it found the stack
				t.Errorf("the stack taken over has the reason %q; want the stack's own only where its operation ended in this process", reason)
			}
			if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "to"}); err != nil {
				t.Fatalf("ContinueUpdateRollback of the stack taken over answered %v", err)
			}
			_, bucket := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv28left"})
			if status, _, _ := h.status("to"); status != "UPDATE_ROLLBACK_COMPLETE" || bucket == nil {
				t.Errorf("the stack is %s and the bucket exists: %t; want UPDATE_ROLLBACK_COMPLETE and the bucket deleted", status, bucket == nil)
			}
		})
	}
}

// A rollback puts back what the failed update changed and calls nothing else: a secret the update kept
// holds the value given it outside the stack. What the failed update made goes but what DeletionPolicy:
// Retain keeps, DELETE_SKIPPED; RetainExceptOnCreate's goes, as the operation that made it is the one
// rolled back. A finished update that drops a RetainExceptOnCreate resource keeps it, as Retain would.
func TestCloudFormationARollbackCallsWhatChangedAndKeepsWhatRetainKeeps(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	secret := map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "v", "Description": "d"}}
	param := func(v string) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "rv28-p", "Type": "String", "Value": v}}
	}
	topic := func(policy string) map[string]any {
		return map[string]any{"Type": "AWS::SNS::Topic", "DeletionPolicy": policy}
	}
	if err := rvMake(t, h, "rb", map[string]any{"Resources": map[string]any{"S": secret, "P": param("one")}}); err != nil {
		t.Fatal(err)
	}
	id := asMap(h.record("rb")["Refs"])["S"]
	if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "outside"}); err != nil {
		t.Fatal(err)
	}
	topics := func() string {
		out, _ := h.service("aws.sns", "ListTopics", map[string]any{})
		return spi.JSONText(out["Topics"])
	}
	if err := rv28Update(h, "rb", map[string]any{"S": secret, "P": param("two"), "A": topic("Retain"), "B": topic("RetainExceptOnCreate"),
		"Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"P", "A", "B"}}}); err == nil {
		t.Fatal("the update to a type no service has succeeded")
	}
	value, _ := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})
	p, _ := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "rv28-p"})
	if status, _, _ := h.status("rb"); status != "UPDATE_ROLLBACK_COMPLETE" || asMap(p["Parameter"])["Value"] != "one" || value["SecretString"] != "outside" {
		t.Errorf("the stack is %s, its parameter %v and its secret %v; want UPDATE_ROLLBACK_COMPLETE, the parameter put back and the kept secret not called", status, asMap(p["Parameter"])["Value"], value["SecretString"])
	}
	if left := topics(); !strings.Contains(left, ":rb-A-") || strings.Contains(left, ":rb-B-") || !slices.Contains(h.events("rb", "A"), "DELETE_SKIPPED") || slices.Contains(h.events("rb", "B"), "DELETE_SKIPPED") {
		t.Errorf("the topics left are %s, A's events %v and B's %v; want Retain's kept, DELETE_SKIPPED, and RetainExceptOnCreate's deleted", left, h.events("rb", "A"), h.events("rb", "B"))
	}
	if err := rv28Update(h, "rb", map[string]any{"S": secret, "P": param("one"), "B": topic("RetainExceptOnCreate")}); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "rb", map[string]any{"S": secret, "P": param("one")}); err != nil {
		t.Fatal(err)
	}
	if left := topics(); !strings.Contains(left, ":rb-B-") || !slices.Contains(h.events("rb", "B"), "DELETE_SKIPPED") {
		t.Errorf("after an update dropped the RetainExceptOnCreate topic the topics are %s and its events %v; want it kept, DELETE_SKIPPED", left, h.events("rb", "B"))
	}
}

// An update compares what the stack holds with the template: a property the template gives no longer is
// a change (the secret's description is cleared), and a resource the stack lists only for a delete that
// failed (DELETE_FAILED, a record of an earlier engine's) is not the stack's to keep, so its logical id is
// made anew. ContinueUpdateRollback's ResourcesToSkip may name only "resources that are in the
// UPDATE_FAILED state because a rollback failed".
func TestCloudFormationAnUpdateComparesWhatTheStackHolds(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	secret := func(p map[string]any) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": p}
	}
	if err := rvMake(t, h, "cm", map[string]any{"Resources": map[string]any{"S": secret(map[string]any{"SecretString": "v", "Description": "d"})}}); err != nil {
		t.Fatal(err)
	}
	col := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn")
	rec := h.record("cm")
	rec["Resources"] = append(spi.Slice(rec["Resources"]), map[string]any{"Logical": "N", "Physical": "arn:aws:sns:us-east-1:000000000000:rv28-gone", "Type": "AWS::SNS::Topic", "Status": "DELETE_FAILED"},
		map[string]any{"Logical": "M", "Physical": "http://127.0.0.1:4566/000000000000/rv28-gone", "Type": "AWS::SQS::Queue", "Status": "DELETE_FAILED"}) // and one of another type
	if err := col.Put(h.ctx, "cm", []byte(spi.JSONText(rec))); err != nil {
		t.Fatal(err)
	}
	if err := rv28Update(h, "cm", map[string]any{"S": secret(map[string]any{"SecretString": "v"}), "N": rv28Topic, "M": rv28Topic}); err != nil {
		t.Fatal(err)
	}
	out, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": asMap(h.record("cm")["Refs"])["S"]})
	n := str(asMap(h.record("cm")["Refs"])["N"])
	if _, err := h.service("aws.sns", "GetTopicAttributes", map[string]any{"TopicArn": n}); out["Description"] != nil || err != nil || strings.HasSuffix(n, "rv28-gone") {
		t.Errorf("the secret is described %v and N is %s (%v); want no description, and a topic made anew", out["Description"], n, err)
	}

	rec = h.record("cm")
	rec["Status"] = "UPDATE_ROLLBACK_FAILED"
	asMap(spi.Slice(rec["Resources"])[0])["Status"] = "UPDATE_FAILED"
	if err := col.Put(h.ctx, "cm", []byte(spi.JSONText(rec))); err != nil {
		t.Fatal(err)
	}
	other := str(asMap(spi.Slice(rec["Resources"])[1])["Logical"])
	_, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "cm", "ResourcesToSkip": []any{other}})
	if status, _, _ := h.status("cm"); code(err) != "ValidationError" || !strings.Contains(err.Error(), "ResourcesToSkip") || status != "UPDATE_ROLLBACK_FAILED" {
		t.Errorf("ResourcesToSkip naming %s, which is not UPDATE_FAILED, answered %v and left %s; want ValidationError and nothing changed", other, err, status)
	}
}

// A table's KeySchema and LocalSecondaryIndexes are set when it is made: DynamoDB's UpdateTable takes
// neither, so a change to one replaces the table (the Template Reference prints "Some interruptions" and
// "No interruption" for them, which no API can carry out: a reading). Before, the update called
// UpdateTable without them and reported the table updated.
func TestCloudFormationAKeySchemaChangeReplacesTheTable(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	table := func(key string) map[string]any {
		return map[string]any{"Type": "AWS::DynamoDB::Table", "Properties": map[string]any{"BillingMode": "PAY_PER_REQUEST",
			"AttributeDefinitions": []any{map[string]any{"AttributeName": key, "AttributeType": "S"}}, "KeySchema": []any{map[string]any{"AttributeName": key, "KeyType": "HASH"}}}}
	}
	if err := rvMake(t, h, "ks", map[string]any{"Resources": map[string]any{"T": table("a")}}); err != nil {
		t.Fatal(err)
	}
	old := h.resources("ks")[0]
	if err := rv28Update(h, "ks", map[string]any{"T": table("b")}); err != nil {
		t.Fatal(err)
	}
	now := h.resources("ks")[0]
	out, _ := h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": now.Physical})
	if _, err := h.lookup(old); err == nil || now.Physical == old.Physical || !strings.Contains(spi.JSONText(asMap(out["Table"])["KeySchema"]), `"b"`) {
		t.Errorf("the table was %s and is %s, keyed %s, the old one deleted: %t; want a new table keyed by b", old.Physical, now.Physical, spi.JSONText(asMap(out["Table"])["KeySchema"]), err != nil)
	}
	indexed := table("b")
	asMap(indexed["Properties"])["AttributeDefinitions"] = []any{map[string]any{"AttributeName": "b", "AttributeType": "S"}, map[string]any{"AttributeName": "r", "AttributeType": "S"}}
	asMap(indexed["Properties"])["KeySchema"] = []any{map[string]any{"AttributeName": "b", "KeyType": "HASH"}, map[string]any{"AttributeName": "r", "KeyType": "RANGE"}}
	if err := rv28Update(h, "ks", map[string]any{"T": indexed}); err != nil {
		t.Fatal(err)
	}
	ranged := h.resources("ks")[0]
	asMap(indexed["Properties"])["LocalSecondaryIndexes"] = []any{map[string]any{"IndexName": "i", "Projection": map[string]any{"ProjectionType": "ALL"},
		"KeySchema": []any{map[string]any{"AttributeName": "b", "KeyType": "HASH"}, map[string]any{"AttributeName": "r", "KeyType": "RANGE"}}}}
	if err := rv28Update(h, "ks", map[string]any{"T": indexed}); err != nil {
		t.Fatal(err)
	}
	if again := h.resources("ks")[0]; again.Physical == ranged.Physical {
		t.Errorf("the table given a local secondary index is still %s; want it replaced", again.Physical)
	}
}

// What a stack's own template read is the stack's: AWS::NotificationARNs in the previous template answers
// the ARNs the stack had, not the request's, so an update that changes them changes the resource built on
// them. Before, the previous properties were read with the new request's ARNs, compared equal to the new
// ones, and the resource was left as it was.
func TestCloudFormationAnUpdateReadsThePreviousNotificationARNs(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	a, b := "arn:aws:sns:us-east-1:000000000000:a", "arn:aws:sns:us-east-1:000000000000:b"
	body := rv19Body(map[string]any{"Resources": map[string]any{"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{
		"Name": "rv28-arns", "Type": "String", "Value": map[string]any{"Fn::Join": []any{",", map[string]any{"Ref": "AWS::NotificationARNs"}}}}}}})
	for op, arns := range map[string][]any{"CreateStack": {a}, "UpdateStack": {a, b}} {
		if _, err := h.invoke(op, map[string]any{"StackName": "na", "TemplateBody": body, "NotificationARNs": arns}); op == "CreateStack" && err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "na", "TemplateBody": body, "NotificationARNs": []any{a, b}}); err != nil {
		t.Fatal(err)
	}
	if out, _ := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "rv28-arns"}); asMap(out["Parameter"])["Value"] != a+","+b {
		t.Errorf("the parameter built on AWS::NotificationARNs holds %v; want both ARNs", asMap(out["Parameter"])["Value"])
	}
}

// EventBridge's DeleteRule takes the rule's targets record with it, empty as it must be by then.
func TestCloudFormationADeletedRuleLeavesNoTargetsRecord(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	for _, call := range []struct {
		op string
		in map[string]any
	}{{"PutRule", map[string]any{"Name": "rv28", "ScheduleExpression": "rate(1 hour)"}},
		{"PutTargets", map[string]any{"Rule": "rv28", "Targets": []any{map[string]any{"Id": "1", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}},
		{"RemoveTargets", map[string]any{"Rule": "rv28", "Ids": []any{"1"}}}, {"DeleteRule", map[string]any{"Name": "rv28"}}} {
		if _, err := h.service("aws.events", call.op, call.in); err != nil {
			t.Fatalf("%s: %v", call.op, err)
		}
	}
	if kvs, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("targets").List(h.ctx, "", "", 0); len(kvs) != 0 {
		t.Errorf("the deleted rule left %d targets records", len(kvs))
	}
}

// A record kept for a stack is released once the stack is seen held by no such operation: here its record
// is written settled by other means. Not parallel: the count is the process's.
func TestCloudFormationAKeptRecordIsReleasedOnceItsStackIsSettled(t *testing.T) {
	h := newStacks(t)
	if err := rvMake(t, h, "kr", map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	settled := h.record("kr")
	before := opKept.Load()
	var writes atomic.Int32
	off := faultStore(h, func(o, col, k string) bool { return o == "txput" && col == "cfn" && k == "kr" && writes.Add(1) >= 2 })
	_ = invoked(t, h, "UpdateStack", map[string]any{"StackName": "kr", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "N": rv28Topic}})})
	off()
	if got := opKept.Load(); got != before+1 {
		t.Fatalf("after an update that could not settle the count is %d; want %d", got, before+1)
	}
	if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "kr", []byte(spi.JSONText(settled))); err != nil {
		t.Fatal(err)
	}
	_, _ = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "kr"}) // refused: the stack is settled
	if got := opKept.Load(); got != before {
		t.Errorf("once the stack was seen settled the count is %d; want %d, the kept record released", got, before)
	}
}
