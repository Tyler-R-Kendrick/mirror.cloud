package cloudformation

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
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

// Every property the engine passes to a service is read as the type the
// operation's model declares, by one helper keyed by that type. So a
// function's MemorySize from a Number parameter (a
// Ref's value "becomes a string", User Guide, Parameters) is 256, and its
// Timeout 30; a table's ReadCapacityUnits from a parameter is 5; a bucket's
// tag values, which are strings, written true and 123 unquoted, are "true"
// and "123". Before, the function was made with MemorySize "256" and Timeout
// a Go int, the table refused "5", and PutBucketTagging was MalformedXML.
func TestCloudFormationPassesEveryPropertyAsItsModelType(t *testing.T) {
	h := newStacks(t)
	parameters := "Parameters:\n  Memory: {Type: Number, Default: 256}\n  Read: {Type: Number, Default: 5}\nResources:\n"
	for name, resource := range map[string]string{
		"fn": `  Fn:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: typed-fn
      Runtime: provided.al2
      Handler: bootstrap
      Role: arn:aws:iam::000000000000:role/fn
      Code: {ZipFile: UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==}
      MemorySize: !Ref Memory
      Timeout: 30
`,
		"table": `  Table:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: typed-table
      KeySchema: [{AttributeName: id, KeyType: HASH}]
      AttributeDefinitions: [{AttributeName: id, AttributeType: S}]
      ProvisionedThroughput: {ReadCapacityUnits: !Ref Read, WriteCapacityUnits: 1}
`,
		"bucket": `  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: typed-bucket
      Tags: [{Key: flag, Value: true}, {Key: count, Value: 123}]
`,
	} {
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": "typed-" + name, "TemplateBody": parameters + resource}); err != nil {
			t.Errorf("the %s stack answered %v", name, err)
		}
	}
	fn, err := h.service("aws.lambda", "GetFunction", map[string]any{"FunctionName": "typed-fn"})
	if config := asMap(fn["Configuration"]); err != nil || config["MemorySize"] != 256.0 || config["Timeout"] != 30.0 {
		t.Errorf("the function's MemorySize is %#v and Timeout %#v (%v), want 256 and 30", config["MemorySize"], config["Timeout"], err)
	}
	table, err := h.service("aws.dynamodb", "DescribeTable", map[string]any{"TableName": "typed-table"})
	if read := asMap(asMap(table["Table"])["ProvisionedThroughput"])["ReadCapacityUnits"]; err != nil || read != 5.0 {
		t.Errorf("the table's ReadCapacityUnits is %#v (%v), want 5", read, err)
	}
	tags, err := h.service("aws.s3", "GetBucketTagging", map[string]any{"Bucket": "typed-bucket"})
	if text := spi.JSONText(tags); err != nil || !strings.Contains(text, `"Value":"true"`) || !strings.Contains(text, `"Value":"123"`) {
		t.Errorf("the bucket's tags are %s (%v), want the values \"true\" and \"123\"", text, err)
	}
}

// "CloudFormation doesn't support the following features: The binary, omap,
// pairs, set, and timestamp tags[,] Aliases[,] Hash merges" (User Guide,
// Template formats): it names aliases, not anchors, so an anchor nothing
// refers to is read as if it were not there (a reading), on a mapping and on
// a scalar, while an alias is still refused. Before, any anchor was
// ValidationError.
func TestCloudFormationReadsALoneAnchor(t *testing.T) {
	h := newStacks(t)
	body := "Resources:\n  Q: &queue\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: &name lone-anchor\n"
	for _, op := range []string{"ValidateTemplate", "CreateStack"} {
		if _, err := h.invoke(op, map[string]any{"StackName": "anchor", "TemplateBody": body}); err != nil {
			t.Errorf("%s of a template with a lone anchor answered %v", op, err)
		}
	}
	if _, err := h.service("aws.sqs", "GetQueueUrl", map[string]any{"QueueName": "lone-anchor"}); err != nil {
		t.Errorf("the anchored name made no queue: %v", err)
	}
	aliased := body + "  R:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: *name\n"
	if _, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": aliased}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "aliases") {
		t.Errorf("an alias of the anchor answered %v, want ValidationError: aliases", err)
	}
}

// The same sentence: "CloudFormation doesn't support ... The binary, omap,
// pairs, set, and timestamp tags". Each, written on a node, is
// ValidationError naming the tag. A date written plainly is not a tag, and
// stays the string it is (AWSTemplateFormatVersion: 2010-09-09, which the
// User Guide's own example writes so). Before, each tag was read: !!binary
// as its decoded bytes, !!timestamp as a time, !!set as a map of nulls.
func TestCloudFormationRefusesTheUnsupportedTags(t *testing.T) {
	h := newStacks(t)
	queue := "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n"
	for _, tagged := range []string{"!!binary aGVsbG8=", "!!omap [{a: 1}]", "!!pairs [{a: 1}]", "!!set {a, b}", "!!timestamp 2010-09-09"} {
		body := "AWSTemplateFormatVersion: 2010-09-09\nMetadata:\n  M: " + tagged + "\n" + queue
		_, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": body})
		if tag, _, _ := strings.Cut(tagged, " "); code(err) != "ValidationError" || !strings.Contains(err.Error(), tag) {
			t.Errorf("%s answered %v, want ValidationError naming %s", tagged, err, tag)
		}
	}
	if _, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": "AWSTemplateFormatVersion: 2010-09-09\n" + queue}); err != nil {
		t.Errorf("a plain date answered %v", err)
	}
}

// "CloudFormation supports the YAML Version 1.1 specification" (User Guide,
// Template formats), so a plain scalar is read as YAML 1.1's type repository
// reads it, where yaml.v3 reads YAML 1.2: yes, no, on and off are booleans
// (yaml.org/type/bool.html), and y and n are left strings, as PyYAML and
// SnakeYAML, the 1.1 parsers in use, leave them (round 8 read them as
// booleans); an integer may be binary, octal as
// 0777, hexadecimal, sexagesimal as 1:20, with underscores, but 0o17 is not
// one (int.html); a float has a point, so 1e3 is a string (float.html).
// FifoQueue is a Boolean (Template Reference, AWS::SQS::Queue): yes makes a
// FIFO queue, and a value that is no boolean is ValidationError before
// anything is made. Quoted, a scalar is a string, as in both versions.
// Before, yes was the string "yes" and made a standard queue, 1:20 was the
// string "1:20", 1e3 was 1000 and 0o17 was 15, and FifoQueue "maybe" made a
// standard queue.
func TestCloudFormationReadsYAMLAsVersion11(t *testing.T) {
	h := newStacks(t)
	values := map[string]string{"y": "y", "N": "N", "yes": "true", "No": "false", "0b101": "5", "0777": "511", "0x1F": "31", "1_000": "1000",
		"1:20": "80", "-1:30": "-90", "1e3": "1e3", "0o17": "0o17", "1.5": "1.5", "'yes'": "yes", "08": "08"}
	queues := "Resources:\n  F:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: v11-fifo.fifo\n      FifoQueue: yes\n" +
		"  S:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: v11-standard\n      FifoQueue: off\n"
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "v11-queues", "TemplateBody": queues}); err != nil {
		t.Errorf("FifoQueue: yes and off answered %v", err)
	} else if attrs, err := h.service("aws.sqs", "GetQueueAttributes", map[string]any{"QueueUrl": h.resources("v11-queues")[0].Physical, "AttributeNames": []any{"All"}}); !strings.Contains(spi.JSONText(attrs), `"FifoQueue":"true"`) || err != nil {
		t.Errorf("FifoQueue: yes made a queue with %v (%v), want a FIFO queue", attrs, err)
	}
	body, texts := "Resources:\n", slices.Sorted(maps.Keys(values))
	for i, text := range texts {
		body += fmt.Sprintf("  P%d:\n    Type: AWS::SSM::Parameter\n    Properties:\n      Name: v11-%d\n      Type: String\n      Value: %s\n", i, i, text)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "v11", "TemplateBody": body}); err != nil {
		t.Fatal(err)
	}
	for i, text := range texts {
		out, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": fmt.Sprintf("v11-%d", i)})
		if got := str(asMap(out["Parameter"])["Value"]); err != nil || got != values[text] {
			t.Errorf("Value: %s is %q (%v), want %q", text, got, err, values[text])
		}
	}
	for _, bad := range []string{"maybe", "1", "[true]"} {
		tpl := "Resources:\n  A:\n    Type: AWS::SSM::Parameter\n    Properties:\n      Name: v11-before\n      Type: String\n      Value: v\n" +
			"  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      FifoQueue: " + bad + "\n"
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": "v11-bad", "TemplateBody": tpl}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "FifoQueue") {
			t.Errorf("FifoQueue: %s answered %v, want ValidationError naming FifoQueue", bad, err)
		}
		if _, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "v11-before"}); err == nil {
			t.Errorf("FifoQueue: %s made the parameter before it", bad)
		}
		_, _ = h.invoke("DeleteStack", map[string]any{"StackName": "v11-bad"})
	}
}

// A Fn::If whose condition the parameters settle orders by the branch it
// takes, and only that one: Fn::Equals of literals and Refs to parameters,
// and Fn::Not, Fn::And, Fn::Or and Condition over such conditions (Template
// Reference, condition functions). So A, which takes "fixed" when Env is
// prod and names B otherwise, and B, which names A only when Env is not
// prod, are no cycle; E, which names ZZ only in prod, is made after it, and
// F, which names ZZ only outside prod, before it, by its logical id.
// A condition the parameters do not settle (one on AWS::Region, or one that
// names itself) orders by both branches, a reading, so C and D, each naming
// the other under it, are a cycle. Before, every Fn::If ordered by both
// branches, and A and B were "Circular dependency between resources: [A, B]".
func TestCloudFormationOrdersAnIfByTheBranchItTakes(t *testing.T) {
	h := newStacks(t)
	param := func(value any) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": value}}
	}
	when := func(cond string, then, otherwise any) any {
		return map[string]any{"Fn::If": []any{cond, then, otherwise}}
	}
	ref := func(n string) any { return map[string]any{"Ref": n} }
	tpl := map[string]any{
		"Parameters": map[string]any{"Env": map[string]any{"Type": "String", "Default": "prod"}},
		"Conditions": map[string]any{
			"IsProd":   map[string]any{"Fn::Equals": []any{ref("Env"), "prod"}},
			"IsDev":    map[string]any{"Fn::Not": []any{map[string]any{"Condition": "IsProd"}}},
			"Both":     map[string]any{"Fn::And": []any{map[string]any{"Condition": "IsProd"}, map[string]any{"Fn::Or": []any{map[string]any{"Condition": "IsDev"}, map[string]any{"Condition": "IsProd"}}}}},
			"InRegion": map[string]any{"Fn::Equals": []any{ref("AWS::Region"), "us-east-1"}},
			"Loop":     map[string]any{"Condition": "Loop"},
		},
		"Resources": map[string]any{
			"A": param(when("IsProd", "fixed", ref("B"))), "B": param(when("IsDev", ref("A"), "fixed")),
			"E": param(when("Both", ref("ZZ"), "x")), "F": param(when("IsDev", ref("ZZ"), "x")), "ZZ": param("z"),
		},
	}
	body, _ := json.Marshal(tpl)
	if _, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": string(body)}); err != nil {
		t.Errorf("A and B, settled by Env, answered %v", err)
	}
	if order, err := h.pack.order(tpl, map[string]string{"Env": "prod"}); err != nil || slices.Index(order, "E") < slices.Index(order, "ZZ") || slices.Index(order, "F") > slices.Index(order, "ZZ") {
		t.Errorf("the order is %v (%v), want E after ZZ and F, which names ZZ only outside prod, before it", order, err)
	}
	for _, cond := range []string{"InRegion", "Loop"} {
		tpl["Resources"] = map[string]any{"C": param(when(cond, ref("D"), "x")), "D": param(when(cond, ref("C"), "x"))}
		body, _ := json.Marshal(tpl)
		if _, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": string(body)}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "[C, D]") {
			t.Errorf("C and D under %s answered %v, want the cycle [C, D]", cond, err)
		}
	}
}

// A condition is evaluated once per operation, however many conditions name
// it: C0 is Fn::Equals [a, a] and each Ci is Fn::And of Condition Ci-1 twice,
// so read without a memo C30 asks for C0 2^30 times. With one, ValidateTemplate
// and GetTemplateSummary of a template whose one Fn::If names C30 answer at
// once (a 30 s timer catches only a hang), the Fn::If settled true, so A and
// B, which name each other only through its false branch, are no cycle.
// Before, each Condition was evaluated afresh wherever it was named: 24
// conditions took 19 s, and 30 did not answer.
func TestCloudFormationEvaluatesEachConditionOnce(t *testing.T) {
	h := newStacks(t)
	conds := map[string]any{"C0": map[string]any{"Fn::Equals": []any{"a", "a"}}}
	for i := 1; i <= 30; i++ {
		prev := map[string]any{"Condition": fmt.Sprintf("C%d", i-1)}
		conds[fmt.Sprintf("C%d", i)] = map[string]any{"Fn::And": []any{prev, prev}}
	}
	body, _ := json.Marshal(map[string]any{"Conditions": conds, "Resources": map[string]any{
		"A": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "once", "Type": "String",
			"Value": map[string]any{"Fn::If": []any{"C30", "taken", map[string]any{"Ref": "B"}}}}},
		"B": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": map[string]any{"Ref": "A"}}}}})
	for _, op := range []string{"ValidateTemplate", "GetTemplateSummary"} {
		done := make(chan error, 1)
		go func() {
			_, err := h.invoke(op, map[string]any{"TemplateBody": string(body)})
			done <- err
		}()
		if err := within(t, done, op); err != nil {
			t.Errorf("%s answered %v", op, err)
		}
	}
}

// A cycle's members are found in one pass over the template's references
// (Tarjan's strongly connected components), so the error costs time linear in
// the template: 498 chained parameters waiting on X and Y, which name each
// other, walk each resource once to order and once to find the cycle, 1,000
// walks in all, and the error names X and Y alone. Before, the search ran
// once per resource left unordered, each over everything after it: 125,250
// walks here, and 8,000 chained resources took 5.9 s.
func TestCloudFormationFindsACycleInLinearTime(t *testing.T) {
	param := func(ref string) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": map[string]any{"Ref": ref}}}
	}
	resources := map[string]any{"X": param("Y"), "Y": param("X"), "P000": param("X")}
	for i := 1; i < 498; i++ {
		resources[fmt.Sprintf("P%03d", i)] = param(fmt.Sprintf("P%03d", i-1))
	}
	body, _ := json.Marshal(map[string]any{"Resources": resources})
	h := newStacks(t)
	pack := New(h.deps)
	_, err := pack.Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: "ValidateTemplate", Input: map[string]any{"TemplateBody": string(body)}})
	if code(err) != "ValidationError" || err.Error() != "ValidationError: Circular dependency between resources: [X, Y]" {
		t.Errorf("the template answered %v, want the cycle [X, Y]", err)
	}
	if pack.walks > 1000 {
		t.Errorf("finding the cycle walked %d resources, want at most 1000", pack.walks)
	}
}

// "Resources: The maximum number of resources that you can declare in your
// CloudFormation template. 500 resources" (User Guide, Understand
// CloudFormation quotas): 500 are a template, 501 are ValidationError on every
// operation that reads one. Before, any number was read.
func TestCloudFormationHoldsATemplateTo500Resources(t *testing.T) {
	h := newStacks(t)
	for n, ok := range map[int]bool{500: true, 501: false} {
		resources := map[string]any{}
		for i := range n {
			resources[fmt.Sprintf("R%03d", i)] = map[string]any{"Type": "AWS::SQS::Queue"}
		}
		body, _ := json.Marshal(map[string]any{"Resources": resources})
		for _, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack"} {
			_, err := h.invoke(op, map[string]any{"StackName": fmt.Sprintf("quota%d", n), "TemplateBody": string(body)})
			if ok && err != nil && op != "CreateStack" {
				t.Errorf("%s of %d resources answered %v", op, n, err)
			} else if !ok && (code(err) != "ValidationError" || !strings.Contains(err.Error(), "maximum allowed, 500")) {
				t.Errorf("%s of %d resources answered %v, want ValidationError: maximum allowed, 500", op, n, err)
			}
		}
	}
}

// Fn::If resolves to the branch its condition takes, the conditions read
// from the parameters once; AWS::NoValue "Removes the corresponding resource
// property when specified as a return value in the Fn::If intrinsic function"
// and AWS::StackId "Returns the ID (ARN) of the stack" (User Guide, pseudo
// parameters). So in prod the parameter's Value is "p", the secret has no
// Description, its value is the stack's id, and an output takes its branch.
// Before, the Fn::If map itself reached the service (SSM refused it), and
// AWS::StackId was left as written.
func TestCloudFormationResolvesFnIf(t *testing.T) {
	h := newStacks(t)
	when := func(c string, a, b any) any { return map[string]any{"Fn::If": []any{c, a, b}} }
	body, _ := json.Marshal(map[string]any{
		"Parameters": map[string]any{"Env": map[string]any{"Type": "String", "Default": "prod"}},
		"Conditions": map[string]any{"IsProd": map[string]any{"Fn::Equals": []any{map[string]any{"Ref": "Env"}, "prod"}}},
		"Resources": map[string]any{
			"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "if-p", "Type": "String", "Value": when("IsProd", "p", "d")}},
			"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "if-s",
				"Description":  when("IsProd", map[string]any{"Ref": "AWS::NoValue"}, "dev"),
				"SecretString": map[string]any{"Fn::Sub": "${AWS::StackId}"}}}},
		"Outputs": map[string]any{"O": map[string]any{"Value": when("IsProd", "out-p", "out-d")}},
	})
	out, err := h.invoke("CreateStack", map[string]any{"StackName": "ifs", "TemplateBody": string(body)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "if-p"}); err != nil || str(asMap(got["Parameter"])["Value"]) != "p" {
		t.Errorf("the parameter is %v (%v), want p", got, err)
	}
	if got, err := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": "if-s"}); err != nil || got["Description"] != nil {
		t.Errorf("the secret is %v (%v), want no Description", got, err)
	}
	if got, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": "if-s"}); err != nil || got["SecretString"] != out["StackId"] {
		t.Errorf("the secret's value is %v (%v), want the stack id %v", got["SecretString"], err, out["StackId"])
	}
	if !strings.Contains(spi.JSONText(h.record("ifs")["Outputs"]), `"OutputValue":"out-p"`) {
		t.Errorf("the outputs are %v, want O out-p", h.record("ifs")["Outputs"])
	}
}

// A Fn::Sub variable "can be template parameter names, resource logical IDs,
// resource attributes, or a variable in a key-value map" (Template Reference,
// Fn::Sub), and pseudo parameters, which its examples use; ${Nope}, which is
// none, is "Template format error: Unresolved resource dependencies [Nope] in
// the Resources block of the template", ValidationError. Before, it was left
// in the string as written.
func TestCloudFormationRefusesAnUnresolvedSubVariable(t *testing.T) {
	h := newStacks(t)
	sub := func(s any) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": map[string]any{"Fn::Sub": s}}}
	}
	for text, ok := range map[string]bool{"${Nope}": false, "${Nope.Arn}": false, "${Env}-${AWS::Region}-${Q}-${Q.Arn}-${!Lit}": true} {
		body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Env": map[string]any{"Type": "String", "Default": "e"}},
			"Resources": map[string]any{"Q": map[string]any{"Type": "AWS::SQS::Queue"}, "P": sub(text), "V": sub([]any{"${X}", map[string]any{"X": "x"}})}})
		_, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": string(body)})
		if ok && err != nil {
			t.Errorf("%s answered %v", text, err)
		} else if !ok && (code(err) != "ValidationError" || !strings.Contains(err.Error(), "Unresolved resource dependencies [Nope]")) {
			t.Errorf("%s answered %v, want ValidationError: Unresolved resource dependencies [Nope]", text, err)
		}
	}
}

// An Integer may be negative: "-5" is -5, as a Number parameter's text can
// be, and "-", "+5", "- 5" and "5-" are still none. Before, "-5" was refused.
func TestCloudFormationReadsANegativeInteger(t *testing.T) {
	for text, want := range map[string]any{"-5": -5, "016": 16, "-": nil, "+5": nil, "- 5": nil, "5-": nil} {
		got, ok := coerce(text, "Integer")
		if want == nil && ok || want != nil && (!ok || got != want) {
			t.Errorf("coerce(%q, Integer) is %v, %t, want %v", text, got, ok, want)
		}
	}
}

// AWS::NoValue "Removes the corresponding resource property when specified
// as a return value in the Fn::If intrinsic function" (User Guide, pseudo
// parameters), so it is never a value itself. Each intrinsic that builds a
// value is walked: in a Fn::Join's list it removes its element, the list rule
// that removes it from any list (whether CloudFormation's Fn::Join does so the
// Template Reference does not say: a reading); as a whole property it removes
// the property. Where it would be part of a value, a Fn::Join's separator, a
// Fn::Sub string or variable, an output's value, it is ValidationError, and
// nothing is made. No stored value holds the sentinel that stands for it.
// Before, Fn::Join and Fn::Sub wrote the sentinel into the string: "\ufffe,b",
// "a,\ufffe,b", "a\ufffeb".
func TestCloudFormationNoValueIsNeverAValue(t *testing.T) {
	no := map[string]any{"Ref": "AWS::NoValue"}
	when := func(c string, a, b any) any { return map[string]any{"Fn::If": []any{c, a, b}} }
	join := func(sep any, parts ...any) any { return map[string]any{"Fn::Join": []any{sep, parts}} }
	param := func(value any) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": value}}
	}
	template := func(resources map[string]any, outputs map[string]any) string {
		body, _ := json.Marshal(map[string]any{"Conditions": map[string]any{"No": map[string]any{"Fn::Equals": []any{"a", "b"}}},
			"Resources": resources, "Outputs": outputs})
		return string(body)
	}
	h := newStacks(t)
	kept := map[string]any{
		"If":   param(join(",", when("No", "a", no), "b")),
		"Ref":  param(join(",", "a", no, "b")),
		"Both": param(join("-", when("No", no, "a"), no, when("No", "b", no), "c")),
		"S":    map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "nv-s", "SecretString": "v", "Description": no}},
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "nv", "TemplateBody": template(kept, nil)}); err != nil {
		t.Fatalf("CreateStack of every place AWS::NoValue removes something: %v", err)
	}
	want := map[string]string{"If": "b", "Ref": "a,b", "Both": "a-c"}
	for _, r := range h.resources("nv") {
		if r.Type != "AWS::SSM::Parameter" {
			continue
		}
		got, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": r.Physical})
		if v := str(asMap(got["Parameter"])["Value"]); err != nil || v != want[r.Logical] {
			t.Errorf("%s's value is %q (%v), want %q", r.Logical, v, err, want[r.Logical])
		}
	}
	if got, err := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": "nv-s"}); err != nil || got["Description"] != nil {
		t.Errorf("the secret is %v (%v), want no Description", got, err)
	}
	if strings.Contains(spi.JSONText(h.record("nv")), noValue) {
		t.Errorf("the stack record holds the sentinel: %v", h.record("nv"))
	}
	refused := map[string]string{
		"separator": template(map[string]any{"P": param(join(no, "a", "b"))}, nil),
		"sub":       template(map[string]any{"P": param(map[string]any{"Fn::Sub": "a${AWS::NoValue}b"})}, nil),
		"variable":  template(map[string]any{"P": param(map[string]any{"Fn::Sub": []any{"a${X}", map[string]any{"X": no}}})}, nil),
		"output":    template(map[string]any{"P": param("v")}, map[string]any{"O": map[string]any{"Value": when("No", "x", no)}}),
		"bare":      template(map[string]any{"P": param("v")}, map[string]any{"O": map[string]any{"Value": no}}),
	}
	for name, body := range refused {
		_, err := h.invoke("CreateStack", map[string]any{"StackName": "nv-" + name, "TemplateBody": body})
		if code(err) != "ValidationError" || !strings.Contains(err.Error(), "AWS::NoValue") {
			t.Errorf("AWS::NoValue as part of a value (%s) answered %v, want ValidationError naming it", name, err)
		}
		if strings.Contains(spi.JSONText(h.record("nv-"+name)), noValue) || len(h.params("nv-"+name)) != 0 {
			t.Errorf("AWS::NoValue as part of a value (%s) left %v", name, h.record("nv-"+name))
		}
	}
}
