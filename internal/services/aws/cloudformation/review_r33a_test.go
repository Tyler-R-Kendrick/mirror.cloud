package cloudformation

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

type c458 struct {
	name     string
	one, two map[string]any
	outside  func(h *stacks, id string) error
	state    func(h *stacks, id string) string
}

func s458Secret(h *stacks, id string) string {
	return "value=" + fmt.Sprint(rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"]) +
		" desc=" + fmt.Sprint(rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": id})["Description"])
}
func s458Param(h *stacks, id string) string {
	p := asMap(rv31Get(h, "aws.ssm", "GetParameter", map[string]any{"Name": id})["Parameter"])
	return fmt.Sprint("type=", p["Type"], " value=", p["Value"])
}
func s458Fn(h *stacks, id string) string {
	f := rv31Get(h, "aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(id)})
	return fmt.Sprint("desc=", f["Description"], " timeout=", f["Timeout"], " mem=", f["MemorySize"], " env=", spi.JSONText(f["Environment"]), " handler=", f["Handler"], " layers=", spi.JSONText(f["Layers"]), " trace=", spi.JSONText(f["TracingConfig"]))
}
func s458Table(h *stacks, id string) string {
	d := asMap(rv31Get(h, "aws.dynamodb", "DescribeTable", map[string]any{"TableName": id})["Table"])
	return fmt.Sprint("billing=", spi.JSONText(d["BillingModeSummary"]), " rcu=", asMap(d["ProvisionedThroughput"])["ReadCapacityUnits"], " stream=", spi.JSONText(d["StreamSpecification"]), " class=", spi.JSONText(d["TableClassSummary"]))
}
func s458Stream(h *stacks, id string) string {
	return fmt.Sprint("open=", asMap(rv31Get(h, "aws.kinesis", "DescribeStreamSummary", map[string]any{"StreamName": id})["StreamDescriptionSummary"])["OpenShardCount"])
}
func s458Kms(h *stacks, id string) string {
	return "desc=" + fmt.Sprint(asMap(rv31Get(h, "aws.kms", "DescribeKey", map[string]any{"KeyId": id})["KeyMetadata"])["Description"])
}
func s458Rule(h *stacks, id string) string {
	n := spi.LastSlash(id[strings.Index(id, "|")+1:])
	var ids []string
	for _, tg := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
		ids = append(ids, str(asMap(tg)["Id"])+"@"+str(asMap(tg)["Arn"]))
	}
	slices.Sort(ids)
	r := rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": n})
	return fmt.Sprint("desc=", r["Description"], " state=", r["State"], " sched=", r["ScheduleExpression"], " pattern=", r["EventPattern"], " targets=", ids)
}

// rv32Bucket is the bucket's public access block and object ownership as S3 answers them.
func rv32Bucket(h *stacks, bucket string) string {
	block := asMap(rv31Get(h, "aws.s3", "GetPublicAccessBlock", map[string]any{"Bucket": bucket})["PublicAccessBlockConfiguration"])
	rules := spi.Slice(asMap(rv31Get(h, "aws.s3", "GetBucketOwnershipControls", map[string]any{"Bucket": bucket})["OwnershipControls"])["Rules"])
	owner := any(nil)
	if len(rules) > 0 {
		owner = asMap(rules[0])["ObjectOwnership"]
	}
	return fmt.Sprint("block=", spi.JSONText(block), " ownership=", owner)
}

func s458Bucket(h *stacks, id string) string {
	v, _ := h.service("aws.s3", "GetBucketVersioning", map[string]any{"Bucket": id})
	tg, _ := h.service("aws.s3", "GetBucketTagging", map[string]any{"Bucket": id})
	return rv32Bucket(h, id) + " ver=" + fmt.Sprint(v["Status"]) + " tags=" + spi.JSONText(tg["TagSet"])
}
func s458Role(h *stacks, id string) string {
	return spi.JSONText(asMap(rv31Get(h, "aws.iam", "GetRole", map[string]any{"RoleName": spi.LastSlash(id)})["Role"])["AssumeRolePolicyDocument"])
}

func cases458() []c458 {
	sec := func(p map[string]any) map[string]any { return rveProps("AWS::SecretsManager::Secret", p) }
	rot := func(h *stacks, id string) error {
		_, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "ROT"})
		return err
	}
	secDesc := func(h *stacks, id string) error {
		_, err := h.service("aws.secretsmanager", "UpdateSecret", map[string]any{"SecretId": id, "Description": "OUT"})
		return err
	}
	both := func(fs ...func(h *stacks, id string) error) func(h *stacks, id string) error {
		return func(h *stacks, id string) error {
			for _, f := range fs {
				if err := f(h, id); err != nil {
					return err
				}
			}
			return nil
		}
	}
	gen := map[string]any{"PasswordLength": 20}
	gen2 := map[string]any{"PasswordLength": 30}
	par := func(p map[string]any) map[string]any { return rveProps("AWS::SSM::Parameter", p) }
	parOut := func(h *stacks, id string) error {
		_, err := h.service("aws.ssm", "PutParameter", map[string]any{"Name": id, "Value": "OUT", "Type": "String", "Overwrite": true})
		return err
	}
	fnOut := func(in map[string]any) func(h *stacks, id string) error {
		return func(h *stacks, id string) error {
			m := map[string]any{"FunctionName": lastColon(id)}
			for k, v := range in {
				m[k] = v
			}
			_, err := h.service("aws.lambda", "UpdateFunctionConfiguration", m)
			return err
		}
	}
	env := func(v string) map[string]any { return map[string]any{"Variables": map[string]any{"K": v}} }
	tbl := rv29Table
	tblOut := func(in map[string]any) func(h *stacks, id string) error {
		return func(h *stacks, id string) error {
			m := map[string]any{"TableName": id}
			for k, v := range in {
				m[k] = v
			}
			_, err := h.service("aws.dynamodb", "UpdateTable", m)
			return err
		}
	}
	pt := func(n int) map[string]any { return map[string]any{"ReadCapacityUnits": n, "WriteCapacityUnits": n} }
	str2 := func(p map[string]any) map[string]any { return rveProps("AWS::Kinesis::Stream", p) }
	strOut := func(h *stacks, id string) error {
		_, err := h.service("aws.kinesis", "UpdateShardCount", map[string]any{"StreamName": id, "TargetShardCount": 7, "ScalingType": "UNIFORM_SCALING"})
		return err
	}
	kms := func(p map[string]any) map[string]any { return rveProps("AWS::KMS::Key", p) }
	kmsOut := func(h *stacks, id string) error {
		_, err := h.service("aws.kms", "UpdateKeyDescription", map[string]any{"KeyId": id, "Description": "OUT"})
		return err
	}
	tgt := func(ids ...string) []any {
		out := []any{}
		for _, id := range ids {
			out = append(out, map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:" + id})
		}
		return out
	}
	rule := func(p map[string]any) map[string]any {
		p["ScheduleExpression"] = "rate(5 minutes)"
		return rveProps("AWS::Events::Rule", p)
	}
	ruleOut := func(h *stacks, id string) error {
		n := spi.LastSlash(id[strings.Index(id, "|")+1:])
		_, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": n, "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}, map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:000000000000:CHANGED"}}})
		return err
	}
	bkt := func(p map[string]any) map[string]any { return rveProps("AWS::S3::Bucket", p) }
	pab := func(v bool) map[string]any { return map[string]any{"BlockPublicAcls": v, "IgnorePublicAcls": false} }
	own := func(v string) map[string]any {
		return map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": v}}}
	}
	bktOut := func(h *stacks, id string) error {
		_, err := h.service("aws.s3", "PutPublicAccessBlock", map[string]any{"Bucket": id, "PublicAccessBlockConfiguration": map[string]any{"BlockPublicPolicy": true}})
		return err
	}
	tags := func(v string) []any { return []any{map[string]any{"Key": "k", "Value": v}} }
	trust := func(svc string) map[string]any {
		return map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"Service": svc}, "Action": "sts:AssumeRole"}}}
	}
	return []c458{
		{"secret/value-removed", sec(map[string]any{"SecretString": "A", "Description": "one"}), sec(map[string]any{"Description": "two"}), rot, s458Secret},
		{"secret/value-removed-only", sec(map[string]any{"SecretString": "A", "Description": "one"}), sec(map[string]any{"Description": "one"}), rot, s458Secret},
		{"secret/value-null-explicit", sec(map[string]any{"SecretString": "A", "Description": "one"}), sec(map[string]any{"SecretString": nil, "Description": "two"}), rot, s458Secret},
		{"secret/value-changed", sec(map[string]any{"SecretString": "A", "Description": "one"}), sec(map[string]any{"SecretString": "B", "Description": "one"}), rot, s458Secret},
		{"secret/value-added", sec(map[string]any{"Description": "one"}), sec(map[string]any{"SecretString": "B", "Description": "one"}), rot, s458Secret},
		{"secret/value-same-desc-changed", sec(map[string]any{"SecretString": "A", "Description": "one"}), sec(map[string]any{"SecretString": "A", "Description": "two"}), both(rot, secDesc), s458Secret},
		{"secret/desc-removed", sec(map[string]any{"SecretString": "A", "Description": "one"}), sec(map[string]any{"SecretString": "A"}), both(rot, secDesc), s458Secret},
		{"secret/desc-added", sec(map[string]any{"SecretString": "A"}), sec(map[string]any{"SecretString": "A", "Description": "two"}), both(rot, secDesc), s458Secret},
		{"secret/gen-removed", sec(map[string]any{"GenerateSecretString": gen, "Description": "one"}), sec(map[string]any{"Description": "two"}), rot, s458Secret},
		{"secret/gen-changed", sec(map[string]any{"GenerateSecretString": gen, "Description": "one"}), sec(map[string]any{"GenerateSecretString": gen2, "Description": "one"}), rot, s458Secret},
		{"secret/gen-to-string", sec(map[string]any{"GenerateSecretString": gen}), sec(map[string]any{"SecretString": "B"}), rot, s458Secret},
		{"secret/string-to-gen", sec(map[string]any{"SecretString": "A"}), sec(map[string]any{"GenerateSecretString": gen}), rot, s458Secret},
		{"secret/value-null-text", sec(map[string]any{"SecretString": "null", "Description": "one"}), sec(map[string]any{"Description": "two"}), rot, s458Secret},
		{"secret/value-num-text", sec(map[string]any{"SecretString": "3", "Description": "one"}), sec(map[string]any{"SecretString": 3, "Description": "two"}), rot, s458Secret},
		{"param/value-changed", par(map[string]any{"Type": "String", "Value": "a"}), par(map[string]any{"Type": "String", "Value": "b"}), parOut, s458Param},
		{"param/type-removed", par(map[string]any{"Type": "StringList", "Value": "a,b"}), par(map[string]any{"Value": "c"}), parOut, s458Param},
		{"param/value-removed", par(map[string]any{"Type": "String", "Value": "a"}), par(map[string]any{"Type": "String"}), parOut, s458Param},
		{"fn/env-removed", rv29Function(map[string]any{"Environment": env("one"), "Timeout": 9}), rv29Function(map[string]any{"Timeout": 10}), fnOut(map[string]any{"MemorySize": 512}), s458Fn},
		{"fn/env-changed", rv29Function(map[string]any{"Environment": env("one")}), rv29Function(map[string]any{"Environment": env("two")}), fnOut(map[string]any{"Timeout": 77}), s458Fn},
		{"fn/env-added", rv29Function(map[string]any{"Timeout": 9}), rv29Function(map[string]any{"Environment": env("two"), "Timeout": 9}), fnOut(map[string]any{"Environment": env("OUT")}), s458Fn},
		{"fn/timeout-removed", rv29Function(map[string]any{"Timeout": 9, "Description": "one"}), rv29Function(map[string]any{"Description": "two"}), fnOut(map[string]any{"MemorySize": 512}), s458Fn},
		{"fn/timeout-added", rv29Function(map[string]any{"Description": "one"}), rv29Function(map[string]any{"Description": "one", "Timeout": 20, "MemorySize": 256}), fnOut(map[string]any{"Timeout": 77}), s458Fn},
		{"fn/handler-changed", rv29Function(map[string]any{"Description": "one"}), rv29Function(map[string]any{"Description": "one", "Handler": "j.k"}), fnOut(map[string]any{"Timeout": 77}), s458Fn},
		{"table/billing-removed", tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST"}), tbl(map[string]any{"ProvisionedThroughput": pt(3)}), nil, s458Table},
		{"table/billing-added", tbl(map[string]any{"ProvisionedThroughput": pt(3)}), tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST"}), tblOut(map[string]any{"ProvisionedThroughput": pt(9)}), s458Table},
		{"table/throughput-changed", tbl(map[string]any{"ProvisionedThroughput": pt(3)}), tbl(map[string]any{"ProvisionedThroughput": pt(4)}), tblOut(map[string]any{"ProvisionedThroughput": pt(9)}), s458Table},
		{"table/stream-removed", tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST", "StreamSpecification": map[string]any{"StreamViewType": "NEW_IMAGE"}}), tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST"}), nil, s458Table},
		{"table/stream-added", tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST"}), tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST", "StreamSpecification": map[string]any{"StreamViewType": "NEW_IMAGE"}}), nil, s458Table},
		{"table/class-removed", tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST", "TableClass": "STANDARD_INFREQUENT_ACCESS"}), tbl(map[string]any{"BillingMode": "PAY_PER_REQUEST"}), nil, s458Table},
		{"stream/count-removed", str2(map[string]any{"ShardCount": 3}), str2(map[string]any{"RetentionPeriodHours": 24}), strOut, s458Stream},
		{"stream/count-added", str2(map[string]any{"RetentionPeriodHours": 24}), str2(map[string]any{"RetentionPeriodHours": 24, "ShardCount": 3}), strOut, s458Stream},
		{"stream/count-changed", str2(map[string]any{"ShardCount": 3}), str2(map[string]any{"ShardCount": 4}), strOut, s458Stream},
		{"stream/mode-changed", str2(map[string]any{"ShardCount": 2}), str2(map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND")}), nil, rv34Stream},
		{"stream/mode-removed", str2(map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND")}), str2(map[string]any{"RetentionPeriodHours": 48}), nil, rv34Stream},
		{"stream/mode-and-count", str2(map[string]any{"StreamModeDetails": rv34Mode("ON_DEMAND")}), str2(map[string]any{"StreamModeDetails": rv34Mode("PROVISIONED"), "ShardCount": 5}), nil, rv34Stream},
		{"kms/desc-removed", kms(map[string]any{"Description": "one", "EnableKeyRotation": true}), kms(map[string]any{"EnableKeyRotation": false}), kmsOut, s458Kms},
		{"kms/desc-added", kms(map[string]any{"EnableKeyRotation": true}), kms(map[string]any{"Description": "two", "EnableKeyRotation": true}), kmsOut, s458Kms},
		{"kms/desc-changed", kms(map[string]any{"Description": "one"}), kms(map[string]any{"Description": "two"}), kmsOut, s458Kms},
		{"rule/targets-removed", rule(map[string]any{"Description": "one", "Targets": tgt("a", "b")}), rule(map[string]any{"Description": "two"}), ruleOut, s458Rule},
		{"rule/targets-added", rule(map[string]any{"Description": "one"}), rule(map[string]any{"Description": "one", "Targets": tgt("a", "c")}), ruleOut, s458Rule},
		{"rule/targets-changed", rule(map[string]any{"Description": "one", "Targets": tgt("a", "b")}), rule(map[string]any{"Description": "one", "Targets": tgt("b", "c")}), ruleOut, s458Rule},
		{"rule/targets-emptied", rule(map[string]any{"Description": "one", "Targets": tgt("a", "b")}), rule(map[string]any{"Description": "two", "Targets": nil}), ruleOut, s458Rule},
		{"rule/desc-removed", rule(map[string]any{"Description": "one", "State": "DISABLED", "Targets": tgt("a")}), rule(map[string]any{"Targets": tgt("a")}), ruleOut, s458Rule},
		{"bucket/pab-removed", bkt(map[string]any{"PublicAccessBlockConfiguration": pab(true), "Tags": tags("one")}), bkt(map[string]any{"Tags": tags("two")}), nil, s458Bucket},
		{"bucket/pab-added", bkt(map[string]any{"Tags": tags("one")}), bkt(map[string]any{"Tags": tags("one"), "PublicAccessBlockConfiguration": pab(true)}), bktOut, s458Bucket},
		{"bucket/pab-changed", bkt(map[string]any{"PublicAccessBlockConfiguration": pab(true)}), bkt(map[string]any{"PublicAccessBlockConfiguration": pab(false)}), bktOut, s458Bucket},
		{"bucket/own-removed", bkt(map[string]any{"OwnershipControls": own("ObjectWriter"), "Tags": tags("one")}), bkt(map[string]any{"Tags": tags("one")}), bktOut, s458Bucket},
		{"bucket/own-added", bkt(map[string]any{"Tags": tags("one")}), bkt(map[string]any{"OwnershipControls": own("ObjectWriter"), "Tags": tags("one")}), bktOut, s458Bucket},
		{"bucket/tags-removed", bkt(map[string]any{"Tags": tags("one"), "VersioningConfiguration": map[string]any{"Status": "Enabled"}}), bkt(map[string]any{}), bktOut, s458Bucket},
		{"bucket/tags-added-ver-added", bkt(map[string]any{}), bkt(map[string]any{"Tags": tags("one"), "VersioningConfiguration": map[string]any{"Status": "Enabled"}}), bktOut, s458Bucket},
		{"role/trust-changed", rveProps("AWS::IAM::Role", map[string]any{"AssumeRolePolicyDocument": trust("lambda.amazonaws.com")}), rveProps("AWS::IAM::Role", map[string]any{"AssumeRolePolicyDocument": trust("ec2.amazonaws.com")}), nil, s458Role},
	}
}

// Every case: path A (rollback restores) and path B (restore fails, ContinueUpdateRollback) end alike.
func TestCloudFormationReview6RollbackAndContinueAgree(t *testing.T) {
	for _, c := range cases458() {
		t.Run(c.name, func(t *testing.T) {
			var end [2]string
			var pre, mid [2]string
			for i, continued := range []bool{false, true} {
				h := newStacks(t)
				if err := rveCreate(h, "ag", map[string]any{"R": c.one}); err != nil {
					t.Fatalf("create: %v", err)
				}
				id := rv31IDs(h, "ag")["R"]
				made := c.state(h, id)
				if c.outside != nil {
					if err := c.outside(h, id); err != nil {
						t.Fatalf("outside: %v", err)
					}
				}
				pre[i] = c.state(h, id)
				// The harness fails a restore by failing store writes, but not the bucket configuration's, which the
				// engine itself writes: a bucket whose public access block alone changed has a restore it cannot
				// fail, so that case is a rollback on both legs.
				continued = continued && c.name != "bucket/pab-changed"
				st := rveFailed(t, h, "ag", c.two, continued)
				mid[i] = c.state(h, id)
				if continued {
					for _, op := range []string{"DescribeStackResources", "ListStackResources", "DescribeStackEvents", "DescribeStacks"} {
						out, err := h.invoke(op, map[string]any{"StackName": "ag"})
						if txt := spi.JSONText(out); err != nil || strings.Contains(txt, "Took") || strings.Contains(txt, "Gave") {
							t.Errorf("%s leaks or fails: %v %s", op, err, txt)
						}
					}
					rveContinue(t, h, "ag")
				}
				end[i] = c.state(h, id)
				if rec := spi.JSONText(h.record("ag")["Resources"]); strings.Contains(rec, "Took") || strings.Contains(rec, "Gave") || strings.Contains(rec, "UPDATE_FAILED") {
					t.Errorf("settled record keeps marks: %s", rec)
				}
				if rv31IDs(h, "ag")["R"] != id {
					t.Errorf("id changed")
				}
				t.Logf("continued=%v st=%s made=%s | pre=%s | mid=%s | end=%s", continued, st, made, pre[i], mid[i], end[i])
			}
			if end[0] != end[1] {
				t.Errorf("DISAGREE: rollback ends %s; continued ends %s", end[0], end[1])
			}
		})
	}
}
