package cloudformation

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// upload puts body in the templates bucket under key and answers its TemplateURL.
func (h *stacks) upload(key, body string) string {
	h.t.Helper()
	s3Pack := s3.New(h.deps)
	if _, err := s3Pack.Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "templates"}}); err != nil && code(err) != "BucketAlreadyOwnedByYou" {
		h.t.Fatal(err)
	}
	in := map[string]any{"Bucket": "templates", "Key": key}
	if _, err := s3Pack.Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: "PutObject", Input: in, Body: io.NopCloser(strings.NewReader(body))}); err != nil {
		h.t.Fatal(err)
	}
	return "s3://templates/" + key
}

// padded is a queue's template, YAML or JSON, padded with a comment or a
// Description to exactly size bytes.
func padded(size int, asJSON bool) string {
	if asJSON {
		head, tail := `{"Description":"`, `","Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`
		return head + strings.Repeat("x", size-len(head)-len(tail)) + tail
	}
	head := "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n#"
	return head + strings.Repeat("x", size-len(head)-1) + "\n"
}

// CloudFormation bounds a template by its size, not by its shape: "Template
// body size in a request ... 51,200 bytes" and "Template body size in an
// Amazon S3 object ... 1 MB" (User Guide, Understand CloudFormation quotas);
// TemplateBody is "a maximum length of 51,200 bytes" and TemplateURL's
// template "max size: 1 MB" (API reference, CreateStack). A body over 51,200
// bytes is the member's constraint error, ValidationError, in YAML and JSON
// alike, on every operation that takes one; a body of exactly 51,200 bytes is
// a template. The quota page writes 204,800 bytes as "204.8 KB", so its 1 MB
// is 1,000,000 bytes: an S3 object of 1,000,000 bytes is a template and one
// of 1,000,001 is not. 200 mappings of 100 keys of two attributes and a
// queue, 642 KB, within the quotas' "200 mappings" and "200 mapping
// attributes", is a template through TemplateURL and too large as a
// TemplateBody. Before, that template was refused for its nodes wherever it
// came from, and a body of any size was read.
func TestCloudFormationBoundsATemplateBySize(t *testing.T) {
	h := newStacks(t)
	var b strings.Builder
	b.WriteString("Mappings:\n")
	for m := range 200 {
		fmt.Fprintf(&b, "  M%d:\n", m)
		for k := range 100 {
			fmt.Fprintf(&b, "    K%d: {A: 012345, B: 012345}\n", k)
		}
	}
	b.WriteString("Resources:\n  Q:\n    Type: AWS::SQS::Queue\n")
	mappings := b.String()
	if n := len(mappings); n < 600_000 || n > 700_000 {
		t.Fatalf("the mappings template is %d bytes, want about 642 KB", n)
	}
	url := h.upload("mappings.yaml", mappings)
	for _, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack"} {
		if _, err := h.invoke(op, map[string]any{"StackName": "big", "TemplateURL": url}); err != nil {
			t.Errorf("%s of the 642 KB template through TemplateURL answered %v", op, err)
		}
	}
	for j, c := range []struct {
		name, body string
		ok         bool
	}{
		{"the mappings", mappings, false},
		{"51,200 bytes of YAML", padded(51_200, false), true},
		{"51,201 bytes of YAML", padded(51_201, false), false},
		{"51,200 bytes of JSON", padded(51_200, true), true},
		{"51,201 bytes of JSON", padded(51_201, true), false},
	} {
		for i, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack", "UpdateStack", "CreateChangeSet"} {
			stack := map[string]string{"UpdateStack": "big"}[op] // an update of the stack made above
			in := map[string]any{"StackName": cmp.Or(stack, fmt.Sprintf("s%d-%d", j, i)), "ChangeSetName": "cs", "TemplateBody": c.body}
			_, err := h.invoke(op, in)
			if c.ok && err != nil {
				t.Errorf("%s of %s answered %v", op, c.name, err)
			} else if !c.ok && (code(err) != "ValidationError" || !strings.Contains(err.Error(), "Member must have length less than or equal to 51200")) {
				t.Errorf("%s of %s as a TemplateBody answered %v, want ValidationError: at most 51200", op, c.name, err)
			}
		}
	}
	for size, ok := range map[int]bool{1_000_000: true, 1_000_001: false} {
		url := h.upload(fmt.Sprintf("padded-%d.yaml", size), padded(size, false))
		_, err := h.invoke("ValidateTemplate", map[string]any{"TemplateURL": url})
		if ok && err != nil {
			t.Errorf("a template of %d bytes through TemplateURL answered %v", size, err)
		} else if !ok && (code(err) != "ValidationError" || !strings.Contains(err.Error(), "1 MB")) {
			t.Errorf("a template of %d bytes through TemplateURL answered %v, want ValidationError: 1 MB", size, err)
		}
	}
}

// yaml.v3 and encoding/json each refuse nesting deeper than 10,000 levels
// ("exceeded max depth"), so no walk of a template goes deeper than that, and
// Go's stack grows to a gigabyte: a template 9,990 levels deep is read and
// made, in YAML and JSON, and one 10,001 levels deep is ValidationError, the
// parser's. Before, any template deeper than 100 levels was refused.
func TestCloudFormationReadsADeepTemplate(t *testing.T) {
	h := newStacks(t)
	deep := func(levels int, asJSON bool) string {
		nest := strings.Repeat("[", levels) + strings.Repeat("]", levels)
		if asJSON {
			raw, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"Tags": "@"}}}})
			return strings.Replace(string(raw), `"@"`, nest, 1)
		}
		return "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      Tags: " + nest + "\n"
	}
	for _, asJSON := range []bool{false, true} {
		for i, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack"} {
			in := map[string]any{"StackName": fmt.Sprintf("deep%d%t", i, asJSON), "TemplateBody": deep(9_990, asJSON)}
			if _, err := h.invoke(op, in); err != nil {
				t.Errorf("%s of a template 9,990 levels deep (JSON %t) answered %v", op, asJSON, err)
			}
			in["TemplateBody"] = deep(10_001, asJSON)
			if _, err := h.invoke(op, in); code(err) != "ValidationError" || !strings.Contains(err.Error(), "depth") {
				t.Errorf("%s of a template 10,001 levels deep (JSON %t) answered %v, want ValidationError: depth", op, asJSON, err)
			}
		}
	}
}

// Resources are ordered once per operation, in time linear in the template:
// one walk of each resource's properties collects the names it refers to,
// and Kahn's algorithm orders them, the earliest logical id first among
// those ready. 500 SSM parameters, each naming the next by Ref (the order
// that took the most work before), are validated and made with exactly 500
// walks each, and in the order the chain gives. A 30 s timer catches only a
// hang. Before, ordering resolved every unordered resource's properties each
// time it placed one, twice per create: 125,250 walks to validate, 250,500 to
// create, 3.9 s and 8.1 s.
func TestCloudFormationOrdersInLinearTime(t *testing.T) {
	h := newStacks(t)
	resources := map[string]any{}
	for i := range 500 {
		props := map[string]any{"Type": "String", "Value": "end"}
		if i < 499 {
			props["Value"] = map[string]any{"Ref": fmt.Sprintf("P%03d", i+1)}
		}
		resources[fmt.Sprintf("P%03d", i)] = map[string]any{"Type": "AWS::SSM::Parameter", "Properties": props}
	}
	body, _ := json.Marshal(map[string]any{"Resources": resources})
	for _, op := range []string{"ValidateTemplate", "CreateStack"} {
		pack := New(h.deps)
		done := make(chan error, 1)
		go func() {
			_, err := pack.Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: op, Input: map[string]any{"StackName": "chain", "TemplateBody": string(body)}})
			done <- err
		}()
		if err := within(t, done, op); err != nil {
			t.Fatalf("%s answered %v", op, err)
		}
		if pack.walks != 500 {
			t.Errorf("%s walked %d resources' properties, want 500", op, pack.walks)
		}
	}
	made := h.resources("chain")
	if len(made) != 500 || made[0].Logical != "P499" || made[499].Logical != "P000" {
		t.Errorf("the chain was made %d resources long, from %v", len(made), made[:min(len(made), 2)])
	}
}

// A cycle's error names the resources in it, and no others: "Circular
// dependency between resources: [A, B]" for A and B naming each other, beside
// C, which names A and so cannot be made either, and D, which can. A
// resource naming itself is a cycle of one, and two cycles are listed
// together. Before, the error listed every resource left unordered, C
// included.
func TestCloudFormationACycleNamesOnlyItsMembers(t *testing.T) {
	h := newStacks(t)
	ref := func(names ...string) map[string]any {
		props := map[string]any{"Type": "String", "Value": "v"}
		if len(names) > 0 {
			var parts []any
			for _, n := range names {
				parts = append(parts, map[string]any{"Ref": n})
			}
			props["Value"] = map[string]any{"Fn::Join": []any{",", parts}}
		}
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": props}
	}
	for _, c := range []struct {
		resources map[string]any
		want      string
	}{
		{map[string]any{"A": ref("B"), "B": ref("A"), "C": ref("A"), "D": ref()}, "[A, B]"},
		{map[string]any{"A": ref("A"), "C": ref("A")}, "[A]"},
		{map[string]any{"A": ref("B"), "B": ref("A"), "C": ref("D"), "D": ref("C"), "E": ref("A", "C"), "F": ref("E")}, "[A, B, C, D]"},
	} {
		body, _ := json.Marshal(map[string]any{"Resources": c.resources})
		_, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": string(body)})
		if want := "Circular dependency between resources: " + c.want; code(err) != "ValidationError" || err.Error() != "ValidationError: "+want {
			t.Errorf("%v answered %v, want ValidationError: %s", c.resources, err, want)
		}
	}
}

// Fn::Sub's ${Name.Attribute} is that resource's Fn::GetAtt, a name its
// variable map defines is that variable's value, and ${!Literal} is the
// literal ${Literal} ("To write a dollar sign and curly braces (${}) literally,
// add an exclamation point (!) after the open curly brace, such as
// ${!Literal}. CloudFormation resolves this text as ${Literal}.", Template
// Reference, Fn::Sub). Each is ordered by what it names: a resource named by
// ${Name.Attribute} or by a variable's value is made first, and one a
// variable's name or ${!...} merely spells is not. Before, ${ZQueue.Arn} and
// ${X} were left as written and ordered nothing, and ${ZQueue} under a
// variable of that name was the queue's URL.
func TestCloudFormationSubstitutesEveryForm(t *testing.T) {
	h := newStacks(t)
	param := func(sub any) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": map[string]any{"Fn::Sub": sub}}}
	}
	h.mustCreate("sub", map[string]map[string]any{
		"ZQueue": {"Type": "AWS::SQS::Queue"},
		"ZTopic": {"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "sub-topic"}},
		"A":      param("${ZQueue.Arn}"),
		"B":      param([]any{"${X}-${AWS::Region}", map[string]any{"X": map[string]any{"Ref": "ZTopic"}}}),
		"C":      param("${!Literal}-${!ZQueue}"),
		"D":      param([]any{"${ZQueue}", map[string]any{"ZQueue": "mapped"}}),
	})
	made, at := map[string]res{}, map[string]int{}
	for i, r := range h.resources("sub") {
		made[r.Logical], at[r.Logical] = r, i
	}
	queue := "arn:aws:sqs:us-east-1:000000000000:" + spi.LastSlash(made["ZQueue"].Physical)
	for logical, want := range map[string]string{"A": queue, "B": "arn:aws:sns:us-east-1:000000000000:sub-topic-us-east-1", "C": "${Literal}-${ZQueue}", "D": "mapped"} {
		out, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": made[logical].Physical})
		if got := str(asMap(out["Parameter"])["Value"]); err != nil || got != want {
			t.Errorf("%s is %q (%v), want %q", logical, got, err, want)
		}
	}
	for _, before := range [][2]string{{"ZQueue", "A"}, {"ZTopic", "B"}, {"D", "ZQueue"}, {"C", "ZQueue"}} {
		if at[before[0]] > at[before[1]] {
			t.Errorf("%s was made after %s (order %v)", before[0], before[1], at)
		}
	}
}

// A Ref answers what each type's Template Reference page says its Ref
// returns, and Fn::GetAtt each attribute the page lists, from it: a role's
// and a function's Ref is its name, a rule's its "event rule ID", the name on
// the default bus and, on another bus, <bus>|<name> (a reading: the page's
// example is a default-bus rule); every other type's Ref was already its
// page's (a bucket's, table's, parameter's, log group's and stream's name, a
// queue's URL, a topic's and a secret's ARN, a key's and a REST API's id).
// The ARNs are Fn::GetAtt's Arn, which a DynamoDB table answered as a
// bucket's. DescribeStackResources' PhysicalResourceId is the Ref's id, and
// DeleteStack deletes each resource by it. Before, a role's, a function's and
// a rule's Ref answered its ARN, and GetAtt Table.Arn arn:aws:s3:::<table>.
func TestCloudFormationARefAnswersWhatItsTypeDocuments(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	named := map[string]string{"Bucket": "BucketName", "Queue": "QueueName", "Topic": "TopicName", "Table": "TableName", "Role": "RoleName",
		"Parameter": "Name", "Secret": "Name", "Log": "LogGroupName", "Rule": "Name", "Function": "FunctionName", "Stream": "Name"}
	resources := map[string]any{"Key": map[string]any{"Type": "AWS::KMS::Key"}}
	for logical, member := range named {
		all[logical]["Properties"].(map[string]any)[member] = "ref-" + strings.ToLower(logical)
		resources[logical] = all[logical]
	}
	resources["Api"] = all["Api"]
	resources["BusRule"] = map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"Name": "ref-busrule", "EventBusName": "ref-bus", "EventPattern": map[string]any{"source": []any{"app"}}}}
	outputs := map[string]any{}
	attributes := map[string][]string{"Bucket": {"Arn", "DomainName"}, "Queue": {"Arn", "QueueName", "QueueUrl"}, "Topic": {"TopicArn", "TopicName"},
		"Table": {"Arn"}, "Role": {"Arn"}, "Parameter": {"Arn"}, "Secret": {"Id"}, "Key": {"Arn", "KeyId"}, "Log": {"Arn"},
		"Rule": {"Arn", "RuleName"}, "BusRule": {"Arn", "RuleName"}, "Function": {"Arn"}, "Stream": {"Arn"}, "Api": {"RestApiId"}}
	for logical, attrs := range attributes {
		outputs["Ref"+logical] = map[string]any{"Value": map[string]any{"Ref": logical}}
		for _, a := range attrs {
			outputs[logical+a] = map[string]any{"Value": map[string]any{"Fn::GetAtt": []any{logical, a}}}
		}
	}
	body, _ := json.Marshal(map[string]any{"Resources": resources, "Outputs": outputs})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "refs", "TemplateBody": string(body)}); err != nil {
		t.Fatal(err)
	}
	physical := map[string]string{}
	for _, r := range h.resources("refs") {
		physical[r.Logical] = r.Physical
	}
	out, _ := h.invoke("DescribeStacks", map[string]any{"StackName": "refs"})
	got := map[string]string{}
	for _, o := range out["Stacks"].([]any)[0].(map[string]any)["Outputs"].([]any) {
		got[str(o.(map[string]any)["OutputKey"])] = str(o.(map[string]any)["OutputValue"])
	}
	region := "us-east-1:000000000000:"
	want := map[string]string{
		"RefBucket": "ref-bucket", "BucketArn": "arn:aws:s3:::ref-bucket", "BucketDomainName": "ref-bucket.s3.amazonaws.com",
		"QueueArn": "arn:aws:sqs:" + region + "ref-queue", "QueueQueueName": "ref-queue",
		"RefTopic": "arn:aws:sns:" + region + "ref-topic", "TopicTopicArn": "arn:aws:sns:" + region + "ref-topic", "TopicTopicName": "ref-topic",
		"RefTable": "ref-table", "TableArn": "arn:aws:dynamodb:" + region + "table/ref-table",
		"RefRole": "ref-role", "RoleArn": "arn:aws:iam::000000000000:role/ref-role",
		"RefParameter": "ref-parameter", "ParameterArn": "arn:aws:ssm:" + region + "parameter/ref-parameter",
		"KeyArn": "arn:aws:kms:" + region + "key/" + physical["Key"], "RefKey": physical["Key"], "KeyKeyId": physical["Key"],
		"RefLog": "ref-log", "LogArn": "arn:aws:logs:" + region + "log-group:ref-log:*",
		"RefRule": "ref-rule", "RuleArn": "arn:aws:events:" + region + "rule/ref-rule", "RuleRuleName": "ref-rule",
		"RefBusRule": "ref-bus|ref-busrule", "BusRuleArn": "arn:aws:events:" + region + "rule/ref-bus/ref-busrule", "BusRuleRuleName": "ref-busrule",
		"RefFunction": "ref-function", "FunctionArn": "arn:aws:lambda:" + region + "function:ref-function",
		"RefStream": "ref-stream", "StreamArn": "arn:aws:kinesis:" + region + "stream/ref-stream",
		"RefApi": physical["Api"], "ApiRestApiId": physical["Api"],
	}
	for k, v := range want {
		if got[k] != v || v == "" {
			t.Errorf("%s is %q, want %q", k, got[k], v)
		}
	}
	for logical, p := range physical {
		if logical != "Queue" && logical != "Secret" && got["Ref"+logical] != p {
			t.Errorf("%s's PhysicalResourceId is %q, its Ref %q", logical, p, got["Ref"+logical])
		}
	}
	if !strings.HasPrefix(got["RefSecret"], "arn:aws:secretsmanager:"+region+"secret:ref-secret") || got["SecretId"] != got["RefSecret"] {
		t.Errorf("the secret's Ref is %q and Id %q, want its ARN", got["RefSecret"], got["SecretId"])
	}
	if !strings.HasSuffix(got["RefQueue"], "/000000000000/ref-queue") || got["QueueQueueUrl"] != got["RefQueue"] {
		t.Errorf("the queue's Ref is %q and QueueUrl %q, want its URL", got["RefQueue"], got["QueueQueueUrl"])
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "refs"}); err != nil {
		t.Fatal(err)
	}
	for _, r := range []res{{Type: "AWS::IAM::Role", Physical: "ref-role"}, {Type: "AWS::Events::Rule", Physical: "ref-rule"}} {
		if _, err := h.lookup(r); err == nil {
			t.Errorf("DeleteStack left %s %s", r.Type, r.Physical)
		}
	}
	if _, err := h.service("aws.lambda", "GetFunction", map[string]any{"FunctionName": "ref-function"}); err == nil {
		t.Error("DeleteStack left the function")
	}
	if _, err := h.service("aws.events", "DescribeRule", map[string]any{"Name": "ref-busrule", "EventBusName": "ref-bus"}); err == nil {
		t.Error("DeleteStack left the rule on ref-bus")
	}
}
