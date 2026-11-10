package cloudformation

import (
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func rv29Table(extra map[string]any) map[string]any {
	p := map[string]any{"AttributeDefinitions": []any{map[string]any{"AttributeName": "k", "AttributeType": "S"}}, "KeySchema": []any{map[string]any{"AttributeName": "k", "KeyType": "HASH"}}}
	for k, v := range extra {
		p[k] = v
	}
	return map[string]any{"Type": "AWS::DynamoDB::Table", "Properties": p}
}

func rv29Function(extra map[string]any) map[string]any {
	p := map[string]any{"Runtime": "python3.12", "Handler": "i.h", "Role": "arn:aws:iam::000000000000:role/r", "Code": map[string]any{"ZipFile": "def h(e,c): pass"}}
	for k, v := range extra {
		p[k] = v
	}
	return map[string]any{"Type": "AWS::Lambda::Function", "Properties": p}
}

// rv29Bucket is what S3 holds of a bucket's versioning, public access block, ownership controls and tags.
func rv29Bucket(h *stacks, b string) string {
	v, _ := h.service("aws.s3", "GetBucketVersioning", map[string]any{"Bucket": b})
	_, block := h.service("aws.s3", "GetPublicAccessBlock", map[string]any{"Bucket": b})
	_, own := h.service("aws.s3", "GetBucketOwnershipControls", map[string]any{"Bucket": b})
	tags, _ := h.service("aws.s3", "GetBucketTagging", map[string]any{"Bucket": b})
	return "versioning=" + str(v["Status"]) + " block=" + map[bool]string{true: "set", false: "none"}[block == nil] + " ownership=" + map[bool]string{true: "set", false: "none"}[own == nil] +
		" tags=" + map[bool]string{true: "set", false: "none"}[len(spi.Slice(tags["TagSet"])) > 0]
}

// rv29Unrestored makes the stack's record the one a rollback that could not restore the resources leaves:
// UPDATE_ROLLBACK_FAILED, each of them UPDATE_FAILED.
func rv29Unrestored(t *testing.T, h *stacks, name string, logical ...string) {
	t.Helper()
	rec := h.record(name)
	rec["Status"] = "UPDATE_ROLLBACK_FAILED"
	for _, r := range spi.Slice(rec["Resources"]) {
		if slices.Contains(logical, str(asMap(r)["Logical"])) {
			asMap(r)["Status"] = "UPDATE_FAILED"
		}
	}
	if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, name, []byte(spi.JSONText(rec))); err != nil {
		t.Fatal(err)
	}
}

// An update clears what the template it is updated from gave and the new one gives no longer, and nothing
// else: what was set on a resource outside its template stays, through an update of another property, an
// update that changes nothing and a rollback. A bucket declared with no Properties was given nothing, as
// one declared with none in them was. Before, a resource with no Properties read as one whose previous
// properties were not known, and an update that added a bucket's tags suspended its versioning and
// deleted its public access block and ownership controls.
func TestCloudFormationAnUpdateClearsOnlyWhatItsTemplateGave(t *testing.T) {
	t.Parallel()
	bad := map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"R"}}
	for name, base := range map[string]map[string]any{"a bucket with no Properties": {"Type": "AWS::S3::Bucket"}, "a bucket with none in them": {"Type": "AWS::S3::Bucket", "Properties": map[string]any{}}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if err := rvMake(t, h, "oc", map[string]any{"Resources": map[string]any{"R": base}}); err != nil {
				t.Fatal(err)
			}
			b := h.resources("oc")[0].Physical
			for op, in := range map[string]map[string]any{"PutBucketVersioning": {"Status": "Enabled", "VersioningConfiguration": map[string]any{"Status": "Enabled"}},
				"PutPublicAccessBlock":       {"PublicAccessBlockConfiguration": map[string]any{"BlockPublicAcls": true}},
				"PutBucketOwnershipControls": {"OwnershipControls": map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "BucketOwnerEnforced"}}}}} {
				in["Bucket"] = b
				if _, err := h.service("aws.s3", op, in); err != nil {
					t.Fatal(err)
				}
			}
			tagged := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": []any{map[string]any{"Key": "a", "Value": "b"}}}}
			for _, step := range []struct {
				what      string
				resources map[string]any
				tags      string
			}{{"an update that changes nothing", map[string]any{"R": base}, "none"}, {"an update that adds its tags", map[string]any{"R": tagged}, "set"},
				{"an update that drops its tags", map[string]any{"R": base}, "none"}, {"a rollback", map[string]any{"R": tagged, "Z": bad}, "none"}} {
				err := rv28Update(h, "oc", step.resources)
				if got, want := rv29Bucket(h, b), "versioning=Enabled block=set ownership=set tags="+step.tags; (err != nil) != (step.what == "a rollback") || got != want {
					t.Errorf("after %s (%v) the bucket has %s; want %s", step.what, err, got, want)
				}
			}
		})
	}
	t.Run("a table and a function", func(t *testing.T) {
		t.Parallel()
		h := newStacks(t)
		resources := func(class any, timeout any) map[string]any {
			table, function := map[string]any{"BillingMode": "PAY_PER_REQUEST"}, map[string]any{}
			if class != nil {
				table["TableClass"], function["Timeout"] = class, timeout
			}
			return map[string]any{"T": rv29Table(table), "R": rv29Function(function)}
		}
		if err := rvMake(t, h, "ot", map[string]any{"Resources": resources(nil, nil)}); err != nil {
			t.Fatal(err)
		}
		ids := map[string]string{}
		for _, r := range h.resources("ot") {
			ids[r.Logical] = r.Physical
		}
		if _, err := h.service("aws.lambda", "UpdateFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["R"]), "Description": "outside", "MemorySize": 512}); err != nil {
			t.Fatal(err)
		}
		if _, err := h.service("aws.dynamodb", "UpdateTable", map[string]any{"TableName": ids["T"], "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}}); err != nil {
			t.Fatal(err)
		}
		failing := resources("STANDARD", 31)
		failing["Z"] = bad
		for _, step := range []struct {
			what      string
			resources map[string]any
		}{{"an update that changes nothing", resources(nil, nil)}, {"an update of another property", resources("STANDARD_INFREQUENT_ACCESS", 30)}, {"a rollback", failing}} {
			err := rv28Update(h, "ot", step.resources)
			fn, _ := h.service("aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(ids["R"])})
			tbl, _ := h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": ids["T"]})
			if stream := asMap(asMap(tbl["Table"])["StreamSpecification"]); (err != nil) != (step.what == "a rollback") || fn["Description"] != "outside" || spi.Integer(fn["MemorySize"]) != 512 || stream["StreamEnabled"] != true {
				t.Errorf("after %s (%v) the function has Description %v and MemorySize %v, and the table's stream is %v; want what was set outside the template", step.what, err, fn["Description"], fn["MemorySize"], stream)
			}
		}
	})
}

// ContinueUpdateRollback does not know what the failed update gave a resource, so it clears every property
// the template lacks: a bucket's tags and public access block go. Its versioning is suspended only where it
// is on: a bucket never versioned stays so, since suspended is not unversioned.
func TestCloudFormationAContinuedRollbackClearsABucket(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "cb", map[string]any{"Resources": map[string]any{"B": map[string]any{"Type": "AWS::S3::Bucket"}}}); err != nil {
		t.Fatal(err)
	}
	b := h.resources("cb")[0].Physical
	for _, versioned := range []bool{false, true} {
		calls := map[string]map[string]any{"PutPublicAccessBlock": {"PublicAccessBlockConfiguration": map[string]any{"BlockPublicAcls": true}},
			"PutBucketTagging": {"Tagging": map[string]any{"TagSet": []any{map[string]any{"Key": "a", "Value": "b"}}}, "TagSet": []any{map[string]any{"Key": "a", "Value": "b"}}}}
		if versioned {
			calls["PutBucketVersioning"] = map[string]any{"Status": "Enabled", "VersioningConfiguration": map[string]any{"Status": "Enabled"}}
		}
		for op, in := range calls {
			in["Bucket"] = b
			if _, err := h.service("aws.s3", op, in); err != nil {
				t.Fatal(err)
			}
		}
		rv29Unrestored(t, h, "cb", "B")
		if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "cb"}); err != nil {
			t.Fatal(err)
		}
		if got, want := rv29Bucket(h, b), "versioning="+map[bool]string{true: "Suspended"}[versioned]+" block=none ownership=none tags=none"; got != want {
			t.Errorf("after ContinueUpdateRollback (versioned: %t) the bucket has %s; want %s", versioned, got, want)
		}
	}
}

// Each property a table's and a function's update clears is cleared: a table given no BillingMode and no
// TableClass again is PROVISIONED, with the throughput its template gives (on demand it has none), and
// STANDARD, and a function has the settings one made without them has.
func TestCloudFormationAnUpdateClearsEachProperty(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	bare := map[string]any{"T": rv29Table(map[string]any{"ProvisionedThroughput": map[string]any{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}), "F": rv29Function(nil)}
	full := map[string]any{"T": rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST", "TableClass": "STANDARD_INFREQUENT_ACCESS"}),
		"F": rv29Function(map[string]any{"MemorySize": 512, "Layers": []any{"arn:aws:lambda:us-east-1:000000000000:layer:l:1"}, "KMSKeyArn": "arn:aws:kms:us-east-1:000000000000:key/k",
			"DeadLetterConfig": map[string]any{"TargetArn": "arn:aws:sqs:us-east-1:000000000000:q"}, "TracingConfig": map[string]any{"Mode": "Active"}, "EphemeralStorage": map[string]any{"Size": 1024}})}
	if err := rvMake(t, h, "ep", map[string]any{"Resources": bare}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, r := range h.resources("ep") {
		ids[r.Logical] = r.Physical
	}
	state := func() string {
		tbl, _ := h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": ids["T"]})
		got, _ := h.service("aws.lambda", "GetFunction", map[string]any{"FunctionName": lastColon(ids["F"])})
		fn := asMap(got["Configuration"]) // every setting the function holds
		return strings.Join([]string{str(asMap(asMap(tbl["Table"])["ProvisionedThroughput"])["ReadCapacityUnits"]), str(asMap(asMap(tbl["Table"])["TableClassSummary"])["TableClass"]), str(fn["MemorySize"]),
			spi.JSONText(fn["Layers"]), str(fn["KMSKeyArn"]), str(asMap(fn["DeadLetterConfig"])["TargetArn"]), str(asMap(fn["TracingConfig"])["Mode"]), str(asMap(fn["EphemeralStorage"])["Size"])}, " ")
	}
	if err := rv28Update(h, "ep", full); err != nil {
		t.Fatal(err)
	}
	if got := state(); !strings.HasPrefix(got, "0 STANDARD_INFREQUENT_ACCESS 512 ") || !strings.HasSuffix(got, " Active 1024") {
		t.Fatalf("the update left %s; want what the template gave", got)
	}
	if err := rv28Update(h, "ep", bare); err != nil {
		t.Fatal(err)
	}
	if got, want := state(), "5 STANDARD 128 []  PassThrough 512"; strings.Join(strings.Fields(got), " ") != strings.Join(strings.Fields(want), " ") {
		t.Errorf("the update that drops them left %s; want %s", got, want)
	}
}

// A bucket's ObjectLockEnabled is set when it is made ("Update requires: Replacement"), so a change to it
// replaces the bucket.
func TestCloudFormationObjectLockReplacesTheBucket(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	bucket := func(lock bool) map[string]any {
		return map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"ObjectLockEnabled": lock}}
	}
	if err := rvMake(t, h, "ol", map[string]any{"Resources": map[string]any{"B": bucket(false)}}); err != nil {
		t.Fatal(err)
	}
	old := h.resources("ol")[0]
	if err := rv28Update(h, "ol", map[string]any{"B": bucket(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.lookup(old); err == nil || h.resources("ol")[0].Physical == old.Physical {
		t.Errorf("the bucket was %s and is %s, the old one deleted: %t; want it replaced", old.Physical, h.resources("ol")[0].Physical, err != nil)
	}
}

// A record kept for a stack is that stack's: the one of its name in its account and Region, in its store.
// An operation on a stack of the same name in another Region, or in another store, leaves it kept, and the
// takeover then lists what the failed update made, which ContinueUpdateRollback deletes. Before, the other
// Region's operation released the record, and the topic the failed update made was left behind.
func TestCloudFormationAKeptRecordIsItsOwnStacks(t *testing.T) {
	bad := map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "N", "Properties": map[string]any{"QueueName": "rv29-nofifo", "FifoQueue": true}}
	for name, elsewhere := range map[string]func(t *testing.T, h *stacks){
		"another Region": func(t *testing.T, h *stacks) {
			was := h.id
			defer func() { h.id = was }()
			h.id.Region = "eu-west-1"
			if err := rvMake(t, h, "ko", map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
				t.Fatal(err)
			}
		},
		"another store": func(t *testing.T, _ *stacks) {
			if err := rvMake(t, newStacks(t), "ko", map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newStacks(t)
			if err := rvMake(t, h, "ko", map[string]any{"Resources": map[string]any{"K": rv28Topic}}); err != nil {
				t.Fatal(err)
			}
			off := faultStore(h, once(func(op, c, k string) bool { return op == "list" && c == "cfn" })) // the rollback's first delete
			err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "ko", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic, "N": rv28Topic, "Z": bad}})})
			off()
			if !isInternalFailure(err) {
				t.Fatalf("the update whose rollback panics answered %v", err)
			}
			elsewhere(t, h)
			if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ko"}); err != nil {
				t.Fatal(err)
			}
			out, _ := h.service("aws.sns", "ListTopics", map[string]any{})
			if status, _, _ := h.status("ko"); status != "UPDATE_ROLLBACK_COMPLETE" || strings.Contains(spi.JSONText(out["Topics"]), ":ko-N-") {
				t.Errorf("the stack is %s and its Region's topics are %s; want UPDATE_ROLLBACK_COMPLETE and the topic the failed update made deleted", status, spi.JSONText(out["Topics"]))
			}
		})
	}
}

// What a failed update changed in place and its rollback did not put back is UPDATE_FAILED on the record
// a takeover writes, so ContinueUpdateRollback puts it back: here the rollback panics at its first
// restore, the bucket's, with the secret's still to come. Before, the takeover left both as they were
// listed, ContinueUpdateRollback answered UPDATE_ROLLBACK_COMPLETE, and both kept the failed update's values.
func TestCloudFormationATakeoverMarksWhatWasNotPutBack(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	bucket := func(origin string) map[string]any {
		return map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedOrigins": []any{origin}}}}}}
	}
	secret := func(d string) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "DependsOn": "B", "Properties": map[string]any{"SecretString": "v", "Description": d}}
	}
	if err := rvMake(t, h, "gp", map[string]any{"Resources": map[string]any{"B": bucket("one"), "S": secret("one")}}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, r := range h.resources("gp") {
		ids[r.Logical] = r.Physical
	}
	var n atomic.Int32 // the bucket's CORS is stored twice: by the update, and by the rollback, which panics
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" && n.Add(1) == 2 })
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "gp", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"B": bucket("two"), "S": secret("two"), "N": rv28Topic, "Z": map[string]any{"Type": "AWS::Nothing::Here", "DependsOn": []any{"S", "N"}}}})})
	off()
	if !isInternalFailure(err) {
		t.Fatalf("the update whose rollback panics answered %v", err)
	}
	_, _ = h.invoke("UpdateStack", map[string]any{"StackName": "gp", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"B": bucket("one")}})}) // refused: it takes the stack over
	if status, _, _ := h.status("gp"); status != "UPDATE_ROLLBACK_FAILED" || h.statuses("gp")["B"] != "UPDATE_FAILED" || h.statuses("gp")["S"] != "UPDATE_FAILED" {
		t.Fatalf("the stack taken over is %s listing %v; want UPDATE_ROLLBACK_FAILED with the bucket and the secret UPDATE_FAILED", status, h.statuses("gp"))
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "gp"}); err != nil {
		t.Fatal(err)
	}
	described, _ := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]})
	cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, ids["B"]+"/cors")
	topics, _ := h.service("aws.sns", "ListTopics", map[string]any{})
	if status, _, _ := h.status("gp"); status != "UPDATE_ROLLBACK_COMPLETE" || described["Description"] != "one" || !strings.Contains(string(cors), `"one"`) || len(spi.Slice(topics["Topics"])) != 0 {
		t.Errorf("the stack is %s, its secret described %v, its bucket's CORS %s, and the topics %v; want UPDATE_ROLLBACK_COMPLETE with the previous template's values and no topic", status, described["Description"], cors, topics["Topics"])
	}
}

// rv29Ops counts the operations registered as running, and the records kept.
func rv29Ops() (live, records int) {
	ops.Range(func(_, v any) bool {
		if _, isKept := v.(kept); isKept {
			records++
		} else {
			live++
		}
		return true
	})
	return live, records
}

// An operation is registered only while it runs, and keeps a record only when it could not settle its
// stack: a reservation that is refused, a ContinueUpdateRollback that fails again and one that completes
// each leave nothing registered and nothing kept. Before, every refused reservation left its operation
// registered for good. Not parallel: the registry is the process's.
func TestCloudFormationAnOperationThatEndsIsNoLongerRegistered(t *testing.T) {
	h := newStacks(t)
	emptied := rv28Failed(t, h, "nr", "rv29-reg")
	live, records := rv29Ops()
	same := func(after string) {
		t.Helper()
		if l, k := rv29Ops(); l != live || k != records {
			t.Errorf("after %s there are %+d operations registered and %+d records kept; want none more", after, l-live, k-records)
		}
	}
	_, err := h.invoke("CreateStack", map[string]any{"StackName": "nr", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv28Topic}})})
	if code(err) != "AlreadyExistsException" {
		t.Fatalf("CreateStack of a name taken answered %v", err)
	}
	same("a refused CreateStack")
	if err := rv28Update(h, "nr", map[string]any{"K": rv28Topic}); err == nil {
		t.Fatal("UpdateStack of an UPDATE_ROLLBACK_FAILED stack succeeded")
	}
	same("a refused UpdateStack")
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "nr"}); err == nil {
		t.Fatal("ContinueUpdateRollback succeeded while the bucket cannot be deleted")
	}
	same("a ContinueUpdateRollback that fails again")
	emptied()
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "nr"}); err != nil {
		t.Fatal(err)
	}
	same("a ContinueUpdateRollback that completes")
}

// A change set whose template gives a logical id another type is FAILED when it is made, as its
// execution would refuse it, and cannot be executed. Before, it was made AVAILABLE and refused only when
// it was executed.
func TestCloudFormationAChangeSetThatChangesATypeFails(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "ct", map[string]any{"Resources": map[string]any{"N": rv28Topic}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "ct", "ChangeSetName": "c", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"N": map[string]any{"Type": "AWS::SQS::Queue"}}})}); err != nil {
		t.Fatal(err)
	}
	cs, _ := h.invoke("DescribeChangeSet", map[string]any{"StackName": "ct", "ChangeSetName": "c"})
	_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "ct", "ChangeSetName": "c"})
	if cs["Status"] != "FAILED" || !strings.Contains(str(cs["StatusReason"]), "Update of resource type is not permitted") || code(err) != "InvalidChangeSetStatus" {
		t.Errorf("the change set is %v (%v) and its execution answered %v; want FAILED with the refusal, and InvalidChangeSetStatus", cs["Status"], cs["StatusReason"], err)
	}
}
