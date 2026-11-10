package cloudformation

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Round 5 review (rv454e), part A: every kind, current and legacy (no Name) record, through a direct
// rollback and through a rollback whose restore fails and is then continued.

type rveKind struct {
	name     string
	one, two map[string]any
	state    func(h *stacks, id string) string
}

func rveProps(typ string, p map[string]any) map[string]any {
	return map[string]any{"Type": typ, "Properties": p}
}

func rveWith(m map[string]any, k string, v any) map[string]any {
	out := map[string]any{"Type": m["Type"]}
	p := map[string]any{}
	for a, b := range asMap(m["Properties"]) {
		p[a] = b
	}
	p[k] = v
	out["Properties"] = p
	return out
}

func rveCreate(h *stacks, name string, resources map[string]any) error {
	_, err := h.invoke("CreateStack", map[string]any{"StackName": name, "TemplateBody": rv19Body(map[string]any{"Resources": resources}), "Capabilities": []any{"CAPABILITY_NAMED_IAM"}})
	return err
}

func rveKinds() []rveKind {
	get := rv31Get
	trust := func(svc string) map[string]any {
		return map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"Service": svc}, "Action": "sts:AssumeRole"}}}
	}
	tgt := func(id string) []any {
		return []any{map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}
	}
	tags := func(v string) []any { return []any{map[string]any{"Key": "k", "Value": v}} }
	return []rveKind{
		{"bucket", rveProps("AWS::S3::Bucket", map[string]any{"Tags": tags("one")}), rveProps("AWS::S3::Bucket", map[string]any{"Tags": tags("two")}),
			func(h *stacks, id string) string {
				return spi.JSONText(get(h, "aws.s3", "GetBucketTagging", map[string]any{"Bucket": id})["TagSet"])
			}},
		{"rule", rveProps("AWS::Events::Rule", map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": "one", "Targets": tgt("a")}),
			rveProps("AWS::Events::Rule", map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": "two", "Targets": tgt("b")}),
			func(h *stacks, id string) string {
				n := spi.LastSlash(id[strings.Index(id, "|")+1:])
				var ids []string
				for _, tg := range spi.Slice(get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": n})["Targets"]) {
					ids = append(ids, str(asMap(tg)["Id"]))
				}
				slices.Sort(ids)
				return fmt.Sprint("desc=", get(h, "aws.events", "DescribeRule", map[string]any{"Name": n})["Description"], " targets=", ids)
			}},
		{"restapi", rveProps("AWS::ApiGateway::RestApi", map[string]any{"Description": "one"}), rveProps("AWS::ApiGateway::RestApi", map[string]any{"Description": "two"}),
			func(h *stacks, id string) string {
				return fmt.Sprint("name=", get(h, "aws.apigateway", "GetRestApi", map[string]any{"restApiId": id})["name"])
			}},
		{"table", rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST"}), rv29Table(map[string]any{"BillingMode": "PAY_PER_REQUEST", "TableClass": "STANDARD_INFREQUENT_ACCESS"}),
			func(h *stacks, id string) string {
				d := asMap(get(h, "aws.dynamodb", "DescribeTable", map[string]any{"TableName": id})["Table"])
				// A table never given a class has the default one, STANDARD, and no summary; a rollback that takes a
				// class away sets STANDARD, which writes the summary (the DynamoDB note says why that is left).
				return fmt.Sprint("class=", cmp.Or(str(asMap(d["TableClassSummary"])["TableClass"]), "STANDARD"), " billing=", spi.JSONText(asMap(d["BillingModeSummary"])["BillingMode"]))
			}},
		{"function", rv29Function(map[string]any{"Description": "one", "Timeout": 9}), rv29Function(map[string]any{"Description": "two"}),
			func(h *stacks, id string) string {
				f := get(h, "aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(id)})
				return fmt.Sprint("desc=", f["Description"], " timeout=", f["Timeout"])
			}},
		{"queue", rveProps("AWS::SQS::Queue", map[string]any{"VisibilityTimeout": 30}), rveProps("AWS::SQS::Queue", map[string]any{"VisibilityTimeout": 40}),
			func(h *stacks, id string) string { return "" }},
		{"topic", rveProps("AWS::SNS::Topic", map[string]any{"DisplayName": "one"}), rveProps("AWS::SNS::Topic", map[string]any{"DisplayName": "two"}),
			func(h *stacks, id string) string { return "" }},
		{"loggroup", rveProps("AWS::Logs::LogGroup", map[string]any{"RetentionInDays": 1}), rveProps("AWS::Logs::LogGroup", map[string]any{"RetentionInDays": 3}),
			func(h *stacks, id string) string { return "" }},
		{"role", rveProps("AWS::IAM::Role", map[string]any{"AssumeRolePolicyDocument": trust("lambda.amazonaws.com")}), rveProps("AWS::IAM::Role", map[string]any{"AssumeRolePolicyDocument": trust("ec2.amazonaws.com")}),
			func(h *stacks, id string) string {
				doc := spi.JSONText(asMap(get(h, "aws.iam", "GetRole", map[string]any{"RoleName": id})["Role"])["AssumeRolePolicyDocument"])
				return fmt.Sprint("trust-lambda=", strings.Contains(doc, "lambda"), " ec2=", strings.Contains(doc, "ec2"))
			}},
		{"parameter", rveProps("AWS::SSM::Parameter", map[string]any{"Type": "String", "Value": "one"}), rveProps("AWS::SSM::Parameter", map[string]any{"Type": "String", "Value": "two"}),
			func(h *stacks, id string) string {
				return "value=" + str(asMap(get(h, "aws.ssm", "GetParameter", map[string]any{"Name": id})["Parameter"])["Value"])
			}},
		{"secret", rveProps("AWS::SecretsManager::Secret", map[string]any{"SecretString": "one", "Description": "one"}), rveProps("AWS::SecretsManager::Secret", map[string]any{"SecretString": "two", "Description": "two"}),
			func(h *stacks, id string) string {
				return "value=" + str(get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"]) +
					" desc=" + str(get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": id})["Description"])
			}},
		{"kms", rveProps("AWS::KMS::Key", map[string]any{"Description": "one"}), rveProps("AWS::KMS::Key", map[string]any{"Description": "two"}),
			func(h *stacks, id string) string {
				return "desc=" + str(asMap(get(h, "aws.kms", "DescribeKey", map[string]any{"KeyId": id})["KeyMetadata"])["Description"])
			}},
		{"stream", rveProps("AWS::Kinesis::Stream", map[string]any{"RetentionPeriodHours": 24}), rveProps("AWS::Kinesis::Stream", map[string]any{"RetentionPeriodHours": 24, "ShardCount": 3}),
			func(h *stacks, id string) string {
				raw, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("kinesis").Get(h.ctx, id)
				return fmt.Sprint("shards=", max(spi.Integer(spi.JSONObject(raw)["OpenShardCount"]), 1))
			}},
	}
}

// rveStrip removes Name from every resource of the stack's record: a record from before res.Name.
func rveStrip(t *testing.T, h *stacks, name string, alsoGave bool) {
	rec := h.record(name)
	for _, r := range spi.Slice(rec["Resources"]) {
		delete(asMap(r), "Name")
		if alsoGave {
			delete(asMap(r), "Gave")
			delete(asMap(r), "Targets")
		}
	}
	if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, name, []byte(spi.JSONText(rec))); err != nil {
		t.Fatal(err)
	}
}

// rveFailRestore fails every write to a service once the trigger parameter (made after R by the update)
// is written: the update of R lands, and its restore fails.
func rveFailRestore(h *stacks) (off func(), fired *atomic.Bool) {
	fired = &atomic.Bool{}
	off = faultStore(h, func(op, c, k string) bool {
		if c == "cfn" || c == "cfn-cs" || c == "bktcfg" {
			return false
		}
		if strings.Contains(k, "rve-trigger") {
			if op == "put" || op == "txput" {
				fired.Store(true)
			}
			return false
		}
		return fired.Load() && (op == "put" || op == "txput")
	})
	return off, fired
}

var rveTrigger = map[string]any{"Type": "AWS::SSM::Parameter", "DependsOn": []any{"R"}, "Properties": map[string]any{"Name": "/rve-trigger", "Type": "String", "Value": "x"}}

func rveSettled(t *testing.T, h *stacks, name string) {
	t.Helper()
	rec := h.record(name)
	if str(rec["Op"]) != "" || strings.HasSuffix(str(rec["Status"]), "_IN_PROGRESS") {
		t.Errorf("the record is %s with Op %q", rec["Status"], rec["Op"])
	}
}

func TestCloudFormationReview5AEveryKindLegacyAndCurrent(t *testing.T) {
	names := map[string]string{"AWS::S3::Bucket": "rve-named-bkt", "AWS::Events::Rule": "rve-named-rule", "AWS::ApiGateway::RestApi": "rve-named-api",
		"AWS::DynamoDB::Table": "rve-named-table", "AWS::Lambda::Function": "rve-named-fn", "AWS::SQS::Queue": "rve-named-q", "AWS::SNS::Topic": "rve-named-t",
		"AWS::Logs::LogGroup": "rve-named-lg", "AWS::IAM::Role": "rve-named-role", "AWS::SSM::Parameter": "/rve/named", "AWS::SecretsManager::Secret": "rve-named-secret",
		"AWS::Kinesis::Stream": "rve-named-stream"}
	for _, k := range rveKinds() {
		for _, named := range []bool{false, true} {
			typ := str(k.one["Type"])
			if named && names[typ] == "" {
				continue
			}
			for _, legacy := range []bool{false, true} {
				for _, cur := range []bool{false, true} {
					label := fmt.Sprintf("%s/named=%v/legacy=%v/continued=%v", k.name, named, legacy, cur)
					t.Run(label, func(t *testing.T) {
						h := newStacks(t)
						one, two := k.one, k.two
						if named {
							one, two = rveWith(one, kinds[typ].member, names[typ]), rveWith(two, kinds[typ].member, names[typ])
						}
						if err := rveCreate(h, "lg", map[string]any{"R": one}); err != nil {
							t.Fatal(err)
						}
						var st stack
						raw, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Get(h.ctx, "lg")
						st = spi.Decode[stack](raw)
						r := st.Resources[0]
						id := r.Physical
						// the derivation: what under answers for the record without its name is the name it was made under
						bare := r
						bare.Name = ""
						props := h.pack.Props(stored(st.Template), "R", st.Refs, st.Name)
						if got := under(st, bare, props); got != r.Name {
							t.Errorf("under answers %q for the record without its name; it was made under %q", got, r.Name)
						}
						if legacy {
							rveStrip(t, h, "lg", false)
						}
						want := k.state(h, id)
						var off func()
						fired := &atomic.Bool{}
						if cur {
							off, fired = rveFailRestore(h)
						}
						err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "lg", "Capabilities": []any{"CAPABILITY_NAMED_IAM"},
							"TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"R": two, "T": rveTrigger, "Z": rv31Dep(rv31Bad, "T")}})})
						if off != nil {
							off()
						}
						status, reason, _ := h.status("lg")
						mid := k.state(h, id)
						t.Logf("update: %v; %s %q %v fired=%v state=%s rec=%s", err, status, reason, h.statuses("lg"), fired.Load(), mid, spi.JSONText(h.record("lg")["Resources"]))
						rveSettled(t, h, "lg")
						if err == nil {
							t.Fatalf("the update did not fail")
						}
						if cur && status == "UPDATE_ROLLBACK_FAILED" {
							for _, op := range []string{"DescribeStackResources", "DescribeStackEvents", "ListStackResources", "DescribeStacks"} {
								out, oerr := h.invoke(op, map[string]any{"StackName": "lg"})
								if s := spi.JSONText(out); oerr != nil || strings.Contains(s, `"Gave"`) || strings.Contains(s, `"Targets"`) {
									t.Errorf("%s (%v) exposes the record: %s", op, oerr, s)
								}
							}
							if legacy { // the record a legacy stack has when it is continued has no names at all
								rveStrip(t, h, "lg", false)
							}
							_, cerr := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "lg"})
							status, reason, _ = h.status("lg")
							t.Logf("continued: %v; %s %q rec=%s", cerr, status, reason, spi.JSONText(h.record("lg")["Resources"]))
							rveSettled(t, h, "lg")
						} else if cur {
							t.Logf("NOTE: the restore did not fail for this kind (status %s): nothing to continue", status)
						}
						got := k.state(h, id)
						if status != "UPDATE_ROLLBACK_COMPLETE" {
							t.Errorf("ends %s (%s); want UPDATE_ROLLBACK_COMPLETE", status, reason)
						}
						if got != want {
							t.Errorf("state after the rollback %q; want %q (as before the update)", got, want)
						}
						rec := spi.JSONText(h.record("lg")["Resources"])
						if strings.Contains(rec, `"Gave"`) || strings.Contains(rec, `"Targets"`) || strings.Contains(rec, "UPDATE_FAILED") {
							t.Errorf("the settled record still holds %s", rec)
						}
						if ids := rv31IDs(h, "lg"); ids["R"] != id || len(ids) != 1 {
							t.Errorf("resources after: %v; want only R=%s", ids, id)
						}
						// and the stack is usable: the update goes through now
						if st, _, _ := h.status("lg"); st == "UPDATE_ROLLBACK_COMPLETE" {
							if err := rv28Update(h, "lg", map[string]any{"R": two}); err != nil {
								t.Errorf("the update after: %v", err)
							}
							t.Logf("after a good update: %s", k.state(h, id))
						}
					})
				}
			}
		}
	}
}
