package cloudformation

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// stacks drives the CloudFormation handler and the services it provisions
// through, in one account and region.
type stacks struct {
	t    *testing.T
	cfn  spi.Handler
	pack *Pack
	ctx  context.Context
	id   spi.Identity
}

func newStacks(t *testing.T) *stacks {
	deps := spitest.Deps(t)
	return &stacks{t: t, cfn: bundled.Handler("aws.cloudformation", deps), pack: New(deps), ctx: context.Background(),
		id: spi.Identity{Account: "000000000000", Region: "us-east-1"}}
}

func (h *stacks) invoke(op string, in map[string]any) (map[string]any, error) {
	out, err := h.cfn.Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: op, Input: in})
	if err != nil {
		return nil, err
	}
	return out.Output, nil
}

func (h *stacks) create(stack string, resources map[string]map[string]any) error {
	tpl, _ := json.Marshal(map[string]any{"Resources": resources})
	_, err := h.invoke("CreateStack", map[string]any{"StackName": stack, "TemplateBody": string(tpl)})
	return err
}

func (h *stacks) mustCreate(stack string, resources map[string]map[string]any) {
	h.t.Helper()
	if err := h.create(stack, resources); err != nil {
		h.t.Fatalf("CreateStack %s: %v", stack, err)
	}
}

func (h *stacks) service(svc, op string, in map[string]any) (map[string]any, error) {
	return h.pack.call(h.ctx, &spi.Request{Identity: h.id}, svc, op, in)
}

// resources answers what DescribeStackResources lists for the stack.
func (h *stacks) resources(stack string) []res {
	h.t.Helper()
	out, err := h.invoke("DescribeStackResources", map[string]any{"StackName": stack})
	if err != nil {
		h.t.Fatalf("DescribeStackResources %s: %v", stack, err)
	}
	var rs []res
	for _, raw := range out["StackResources"].([]any) {
		m := raw.(map[string]any)
		rs = append(rs, res{Logical: str(m["LogicalResourceId"]), Physical: str(m["PhysicalResourceId"]), Type: str(m["ResourceType"])})
	}
	return rs
}

// lookup is the read that finds a resource in its owning service by the
// physical id its stack recorded, and the name it has there.
func (h *stacks) lookup(r res) (name string, err error) {
	in := func(member, value string) map[string]any { return map[string]any{member: value} }
	switch r.Type {
	case "AWS::S3::Bucket":
		_, err = h.service("aws.s3", "HeadBucket", in("Bucket", r.Physical))
		return r.Physical, err
	case "AWS::SQS::Queue":
		_, err = h.service("aws.sqs", "GetQueueUrl", in("QueueName", lastSlash(r.Physical)))
		return lastSlash(r.Physical), err
	case "AWS::SNS::Topic":
		_, err = h.service("aws.sns", "GetTopicAttributes", in("TopicArn", r.Physical))
		return lastColon(r.Physical), err
	case "AWS::DynamoDB::Table":
		_, err = h.service("aws.dynamodb", "DescribeTable", in("TableName", r.Physical))
		return r.Physical, err
	case "AWS::IAM::Role":
		_, err = h.service("aws.iam", "GetRole", in("RoleName", lastSlash(r.Physical)))
		return lastSlash(r.Physical), err
	case "AWS::SSM::Parameter":
		_, err = h.service("aws.ssm", "GetParameter", in("Name", r.Physical))
		return r.Physical, err
	case "AWS::SecretsManager::Secret":
		_, err = h.service("aws.secretsmanager", "DescribeSecret", in("SecretId", r.Physical))
		return lastColon(r.Physical), err
	case "AWS::Logs::LogGroup":
		out, err := h.service("aws.logs", "DescribeLogGroups", in("logGroupNamePrefix", r.Physical))
		if err == nil && !strings.Contains(jsonText(out), `"logGroupName":"`+r.Physical+`"`) {
			err = &spi.Fault{Code: "ResourceNotFoundException"}
		}
		return r.Physical, err
	case "AWS::Events::Rule":
		_, err = h.service("aws.events", "DescribeRule", in("Name", lastSlash(r.Physical)))
		return lastSlash(r.Physical), err
	case "AWS::Lambda::Function":
		_, err = h.service("aws.lambda", "GetFunction", in("FunctionName", lastColon(r.Physical)))
		return lastColon(r.Physical), err
	case "AWS::Kinesis::Stream":
		_, err = h.service("aws.kinesis", "DescribeStreamSummary", in("StreamName", r.Physical))
		return r.Physical, err
	case "AWS::ApiGateway::RestApi":
		out, err := h.service("aws.apigateway", "GetRestApi", in("restApiId", r.Physical))
		return str(out["name"]), err
	}
	h.t.Fatalf("no lookup for %s", r.Type)
	return "", nil
}

// nameable is one resource of every type CloudFormation names, each with
// the least its service needs, none of them named.
func nameable() map[string]map[string]any {
	props := func(kv ...any) map[string]any {
		m := map[string]any{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i].(string)] = kv[i+1]
		}
		return m
	}
	return map[string]map[string]any{
		"Api":    {"Type": "AWS::ApiGateway::RestApi", "Properties": props()},
		"Bucket": {"Type": "AWS::S3::Bucket", "Properties": props()},
		"Function": {"Type": "AWS::Lambda::Function", "Properties": props("Runtime", "provided.al2", "Handler", "bootstrap",
			"Role", "arn:aws:iam::000000000000:role/fn", "Code", map[string]any{"ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="})},
		"Log":       {"Type": "AWS::Logs::LogGroup", "Properties": props()},
		"Parameter": {"Type": "AWS::SSM::Parameter", "Properties": props("Value", "v")},
		"Queue":     {"Type": "AWS::SQS::Queue", "Properties": props()},
		"Role":      {"Type": "AWS::IAM::Role", "Properties": props("AssumeRolePolicyDocument", map[string]any{"Version": "2012-10-17", "Statement": []any{}})},
		"Rule":      {"Type": "AWS::Events::Rule", "Properties": props("ScheduleExpression", "rate(1 hour)")},
		"Secret":    {"Type": "AWS::SecretsManager::Secret", "Properties": props("SecretString", "v")},
		"Stream":    {"Type": "AWS::Kinesis::Stream", "Properties": props()},
		"Table": {"Type": "AWS::DynamoDB::Table", "Properties": props("BillingMode", "PAY_PER_REQUEST",
			"KeySchema", []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}},
			"AttributeDefinitions", []any{map[string]any{"AttributeName": "id", "AttributeType": "S"}})},
		"Topic": {"Type": "AWS::SNS::Topic", "Properties": props()},
	}
}

var suffix = regexp.MustCompile(`^[0-9a-f]{12}(\.fifo)?$`)

// Every resource a template leaves unnamed is made under
// <stack>-<logical id>-<suffix>, so two stacks made from one template each
// get their own. Before, most types were named for the logical id alone: the
// second stack failed on its table, or took over the first stack's queue,
// topic and rule.
func TestCloudFormationNamesEveryUnnamedResourceForItsStack(t *testing.T) {
	h := newStacks(t)
	for _, stack := range []string{"dev", "staging"} {
		h.mustCreate(stack, nameable())
	}
	seen := map[string]string{}
	for _, stack := range []string{"dev", "staging"} {
		rs := h.resources(stack)
		if len(rs) != len(nameable()) {
			t.Fatalf("%s lists %d resources, want %d", stack, len(rs), len(nameable()))
		}
		for _, r := range rs {
			name, err := h.lookup(r)
			if err != nil {
				t.Errorf("%s's %s is not in its service: %v", stack, r.Logical, err)
			}
			want := stack + "-" + r.Logical + "-"
			if r.Type == "AWS::S3::Bucket" {
				want = strings.ToLower(want)
			}
			if !strings.HasPrefix(name, want) || !suffix.MatchString(name[len(want):]) {
				t.Errorf("%s's %s (%s) is named %q, want %s<12 hex digits>", stack, r.Logical, r.Type, name, want)
			}
			if other, ok := seen[name]; ok {
				t.Errorf("%s's %s and %s share the name %q", stack, r.Logical, other, name)
			}
			seen[name] = stack + "/" + r.Logical
		}
	}
}

// A long stack name and logical id are cut to fit the service's longest
// name; the suffix, and a FIFO queue's .fifo, are kept whole.
func TestCloudFormationCutsALongNameToFitItsService(t *testing.T) {
	h := newStacks(t)
	longest := map[string]int{
		"AWS::SQS::Queue": 80, "AWS::Events::Rule": 64, "AWS::IAM::Role": 64, "AWS::Lambda::Function": 64,
		"AWS::Kinesis::Stream": 128, "AWS::DynamoDB::Table": 255, "AWS::SNS::Topic": 256, "AWS::S3::Bucket": 63,
		"AWS::SecretsManager::Secret": 512, "AWS::Logs::LogGroup": 512, "AWS::SSM::Parameter": 2048, "AWS::ApiGateway::RestApi": 1024,
	}
	resources := map[string]map[string]any{}
	for logical, r := range nameable() {
		resources[logical+strings.Repeat("L", 250-len(logical))] = r
	}
	fifo := "Fifo" + strings.Repeat("L", 246)
	resources[fifo] = map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"FifoQueue": true}}
	stack := strings.Repeat("S", 128)
	h.mustCreate(stack, resources)
	for _, r := range h.resources(stack) {
		name, err := h.lookup(r)
		if err != nil {
			t.Errorf("%s is not in its service: %v", r.Type, err)
		}
		i := strings.LastIndex(name, "-")
		if len(name) > longest[r.Type] || i < 0 || !suffix.MatchString(name[i+1:]) || !strings.HasPrefix(strings.ToUpper(name), "SSS") {
			t.Errorf("%s is named %q (%d characters), want at most %d ending in -<12 hex digits>", r.Type, name, len(name), longest[r.Type])
		}
		if r.Logical == fifo {
			attrs, err := h.service("aws.sqs", "GetQueueAttributes", map[string]any{"QueueUrl": r.Physical, "AttributeNames": []any{"All"}})
			if !strings.HasSuffix(name, ".fifo") || err != nil || str(attrs["Attributes"].(map[string]any)["FifoQueue"]) != "true" {
				t.Errorf("the FIFO queue is %q (%v), want a FIFO queue ending in .fifo", name, err)
			}
		}
	}
}

// Two stacks made from one template are two sets of resources, so the
// second one's failure rolls back only what the second one made. Before,
// both named the queue, topic and rule for their logical ids: the second
// stack took the first one's over, and its rollback deleted them.
func TestCloudFormationARollbackLeavesAnotherStacksResources(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	shared := map[string]map[string]any{"Q": all["Queue"], "T": all["Topic"], "R": all["Rule"]}
	h.mustCreate("owner", shared)
	owner := h.resources("owner")
	shared["ZZZ"] = map[string]any{"Type": "AWS::Nope::Thing"}
	if err := h.create("intruder", shared); err == nil {
		t.Fatal("a template with an unsupported resource type was created")
	}
	for _, r := range owner {
		if _, err := h.lookup(r); err != nil {
			t.Errorf("the failed stack deleted owner's %s %s: %v", r.Type, r.Physical, err)
		}
	}
}
