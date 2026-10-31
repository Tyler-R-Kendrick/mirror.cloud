package cloudformation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// stacks drives the CloudFormation handler and the services it provisions
// through, in one account and region.
type stacks struct {
	t    *testing.T
	deps spi.Deps
	cfn  spi.Handler
	pack *Pack
	ctx  context.Context
	id   spi.Identity
}

func newStacks(t *testing.T) *stacks {
	deps := spitest.Deps(t)
	return &stacks{t: t, deps: deps, cfn: bundled.Handler("aws.cloudformation", deps), pack: New(deps), ctx: context.Background(),
		id: spi.Identity{Account: "000000000000", Region: "us-east-1"}}
}

// status answers DescribeStacks' StackStatus and StackStatusReason for the stack.
func (h *stacks) status(stack string) (status, reason string, err error) {
	out, err := h.invoke("DescribeStacks", map[string]any{"StackName": stack})
	if err != nil {
		return "", "", err
	}
	st := out["Stacks"].([]any)[0].(map[string]any)
	return str(st["StackStatus"]), str(st["StackStatusReason"]), nil
}

// statuses answers each listed resource's ResourceStatus, by logical id.
func (h *stacks) statuses(stack string) map[string]string {
	h.t.Helper()
	out, err := h.invoke("DescribeStackResources", map[string]any{"StackName": stack})
	if err != nil {
		h.t.Fatalf("DescribeStackResources %s: %v", stack, err)
	}
	m := map[string]string{}
	for _, raw := range out["StackResources"].([]any) {
		m[str(raw.(map[string]any)["LogicalResourceId"])] = str(raw.(map[string]any)["ResourceStatus"])
	}
	return m
}

// fill puts an object in the bucket, which S3 then refuses to delete.
func (h *stacks) fill(bucket string, put bool) {
	h.t.Helper()
	in := map[string]any{"Bucket": bucket, "Key": "k"}
	op, body := "DeleteObject", io.ReadCloser(nil)
	if put {
		op, body = "PutObject", io.NopCloser(strings.NewReader("x"))
	}
	if _, err := s3.New(h.deps).Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: op, Input: in, Body: body}); err != nil {
		h.t.Fatalf("%s %s: %v", op, bucket, err)
	}
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

// nameable is one resource of every type the template engine names, each with
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

// code is the fault code of err, or "" when it carries none.
func code(err error) string {
	var f *spi.Fault
	if errors.As(err, &f) {
		return f.Code
	}
	return ""
}

// A name the template gives that a resource the stack did not make already
// holds fails the stack, before anything is made, with the error that
// resource's service answers for a name taken (CloudFormation's own
// AlreadyExistsException, "The resource with the name requested already
// exists.", for a topic: CreateTopic "is idempotent" and has none). The
// resource is left as it was. Before, a create the service answers with
// what it holds (a queue, topic, rule, role, stream or log group) took the
// resource over, and the stack's rollback, or a DeleteStack, deleted it.
func TestCloudFormationRefusesANameItDidNotMake(t *testing.T) {
	h := newStacks(t)
	queues := func() int {
		t.Helper()
		out, err := h.service("aws.sqs", "ListQueues", map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		urls, _ := out["QueueUrls"].([]any)
		return len(urls)
	}
	if _, err := h.service("aws.sqs", "CreateQueue", map[string]any{"QueueName": "handmade"}); err != nil {
		t.Fatal(err)
	}
	all := nameable()
	handmade := map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "handmade"}}
	err := h.create("ex", map[string]map[string]any{"A": all["Queue"], "Q": handmade, "ZZZ": {"Type": "AWS::Nope::Thing"}})
	if code(err) != "QueueAlreadyExists" {
		t.Errorf("a template naming a queue made outside CloudFormation answered %v, want QueueAlreadyExists", err)
	}
	if _, err := h.service("aws.sqs", "GetQueueUrl", map[string]any{"QueueName": "handmade"}); err != nil {
		t.Errorf("the failed stack deleted the queue it named: %v", err)
	}
	if n := queues(); n != 1 {
		t.Errorf("%d queues after the refused stack, want only handmade", n)
	}

	member := map[string]string{
		"AWS::S3::Bucket": "BucketName", "AWS::SQS::Queue": "QueueName", "AWS::SNS::Topic": "TopicName", "AWS::DynamoDB::Table": "TableName",
		"AWS::IAM::Role": "RoleName", "AWS::SSM::Parameter": "Name", "AWS::SecretsManager::Secret": "Name", "AWS::Logs::LogGroup": "LogGroupName",
		"AWS::Events::Rule": "Name", "AWS::Lambda::Function": "FunctionName", "AWS::Kinesis::Stream": "Name", "AWS::ApiGateway::RestApi": "Name",
	}
	taken := map[string]string{
		"AWS::S3::Bucket": "BucketAlreadyOwnedByYou", "AWS::SQS::Queue": "QueueAlreadyExists", "AWS::SNS::Topic": "AlreadyExistsException",
		"AWS::DynamoDB::Table": "ResourceInUseException", "AWS::IAM::Role": "EntityAlreadyExists", "AWS::SSM::Parameter": "ParameterAlreadyExists",
		"AWS::SecretsManager::Secret": "ResourceExistsException", "AWS::Logs::LogGroup": "ResourceAlreadyExistsException",
		"AWS::Events::Rule": "ResourceAlreadyExistsException", "AWS::Lambda::Function": "ResourceConflictException",
		"AWS::Kinesis::Stream": "ResourceInUseException",
	}
	named := map[string]map[string]any{}
	for logical, r := range all {
		props := map[string]any{}
		for k, v := range r["Properties"].(map[string]any) {
			props[k] = v
		}
		props[member[r["Type"].(string)]] = "taken-" + strings.ToLower(logical)
		named[logical] = map[string]any{"Type": r["Type"], "Properties": props}
	}
	h.mustCreate("first", named)
	first := h.resources("first")
	for logical, r := range named {
		typ := r["Type"].(string)
		if typ == "AWS::ApiGateway::RestApi" {
			continue // API Gateway does not hold a REST API's name to one API
		}
		err := h.create("second-"+strings.ToLower(logical), map[string]map[string]any{"A": all["Queue"], logical: r})
		if code(err) != taken[typ] {
			t.Errorf("a second stack naming %s's %s answered %v, want %s", "first", logical, err, taken[typ])
		}
	}
	if n := queues(); n != 2 {
		t.Errorf("%d queues after the refused stacks, want handmade and first's", n)
	}
	for _, r := range first {
		if _, err := h.lookup(r); err != nil {
			t.Errorf("a refused stack deleted first's %s: %v", r.Logical, err)
		}
	}
}

// A resource made before part of its create failed -- a bucket whose
// configuration S3 refused, a rule whose targets EventBridge refused -- is
// recorded the moment it exists, so the rollback deletes it with the rest.
// Before, a resource was recorded only once its whole create succeeded, and
// a half-made one outlived the rollback.
func TestCloudFormationARollbackDeletesAHalfMadeResource(t *testing.T) {
	h := newStacks(t)
	for stack, props := range map[string]map[string]any{
		"versioning": {"BucketName": "half-versioning", "VersioningConfiguration": map[string]any{"Status": "Bogus"}},
		"tags":       {"BucketName": "half-tags", "Tags": "notalist"},
		"ownership":  {"BucketName": "half-ownership", "OwnershipControls": map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "Nope"}}}},
	} {
		// SQS refuses a queue name deleted in the last 60 seconds, so each stack's queue has its own.
		queue := map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "before-" + stack}}
		if err := h.create(stack, map[string]map[string]any{"A": queue, "B": {"Type": "AWS::S3::Bucket", "Properties": props}}); code(err) != "MalformedXML" {
			t.Fatalf("%s: a bucket S3 refused to configure answered %v, want S3's MalformedXML", stack, err)
		}
		if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": props["BucketName"]}); err == nil {
			t.Errorf("%s: the half-made bucket %s outlived the rollback", stack, props["BucketName"])
		}
		if _, err := h.service("aws.sqs", "GetQueueUrl", map[string]any{"QueueName": "before-" + stack}); err == nil {
			t.Errorf("%s: the queue made before the bucket outlived the rollback", stack)
		}
	}
	rule := map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"Name": "half-rule", "ScheduleExpression": "rate(5 minutes)", "Targets": "notalist"}}
	if err := h.create("rule", map[string]map[string]any{"R": rule}); code(err) != "ValidationException" {
		t.Fatalf("a rule whose targets EventBridge refused answered %v, want its ValidationException", err)
	}
	if _, err := h.service("aws.events", "DescribeRule", map[string]any{"Name": "half-rule"}); err == nil {
		t.Error("the half-made rule outlived the rollback")
	}
}

// A stack name "must be unique in the Region in which you are creating the
// stack", so a CreateStack that names a stack already there is
// AlreadyExistsException ("The resource with the name requested already
// exists.") and makes nothing; the stack keeps its resources. Before, the
// second CreateStack made a second set of resources and wrote its record
// over the first, so the first set belonged to no stack.
func TestCloudFormationCreateStackRefusesAStackThatExists(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	h.mustCreate("dup", map[string]map[string]any{"Secret": all["Secret"]})
	before := h.resources("dup")
	err := h.create("dup", map[string]map[string]any{"Secret": all["Secret"], "Queue": all["Queue"]})
	if code(err) != "AlreadyExistsException" {
		t.Errorf("a second CreateStack of dup answered %v, want AlreadyExistsException", err)
	}
	if after := h.resources("dup"); !reflect.DeepEqual(after, before) {
		t.Errorf("dup's resources went from %v to %v", before, after)
	}
	secrets, _ := h.service("aws.secretsmanager", "ListSecrets", map[string]any{})
	queues, _ := h.service("aws.sqs", "ListQueues", map[string]any{})
	if n, q := len(secrets["SecretList"].([]any)), queues["QueueUrls"]; n != 1 || q != nil && len(q.([]any)) != 0 {
		t.Errorf("after the refused CreateStack there are %d secrets and queues %v, want dup's one secret and no queue", n, q)
	}
}

// A failed CreateStack keeps its stack: ROLLBACK_COMPLETE once the rollback
// has deleted what it made, holding its name -- a CreateStack of it is
// AlreadyExistsException and an UpdateStack ValidationError ("can not be
// updated") -- until DeleteStack removes it, after which the name is free.
// Before, a failed create left no stack at all.
func TestCloudFormationAFailedCreateLeavesARolledBackStack(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	failing := map[string]map[string]any{"Queue": all["Queue"], "ZZZ": {"Type": "AWS::Nope::Thing"}}
	if err := h.create("rb", failing); err == nil {
		t.Fatal("a template with an unsupported resource type was created")
	}
	if status, _, err := h.status("rb"); status != "ROLLBACK_COMPLETE" || err != nil {
		t.Fatalf("after the failed create rb is %q (%v), want ROLLBACK_COMPLETE", status, err)
	}
	if left := h.statuses("rb"); len(left) != 0 {
		t.Errorf("the rolled-back stack still lists %v", left)
	}
	valid := map[string]map[string]any{"Secret": all["Secret"]}
	if err := h.create("rb", valid); code(err) != "AlreadyExistsException" {
		t.Errorf("CreateStack of the rolled-back rb answered %v, want AlreadyExistsException", err)
	}
	tpl, _ := json.Marshal(map[string]any{"Resources": valid})
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "rb", "TemplateBody": string(tpl)}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "ROLLBACK_COMPLETE state and can not be updated") {
		t.Errorf("UpdateStack of the rolled-back rb answered %v, want ValidationError", err)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "rb"}); err != nil {
		t.Fatalf("DeleteStack rb: %v", err)
	}
	if _, _, err := h.status("rb"); code(err) != "ValidationError" {
		t.Errorf("DescribeStacks after DeleteStack answered %v, want ValidationError (no such stack)", err)
	}
	h.mustCreate("rb", valid)
}

// A DeleteStack that cannot delete a resource -- a bucket S3 refuses to
// delete while it holds an object -- keeps the stack, DELETE_FAILED, with the
// resource listed as DELETE_FAILED and S3's error as StackStatusReason
// ("Success/failure message associated with the stack status"), and answers
// that error. The next DeleteStack tries it again. Before, the error was
// dropped, the stack record deleted and the bucket left behind with no stack.
func TestCloudFormationADeleteThatFailsKeepsItsStack(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	h.mustCreate("keep", map[string]map[string]any{"Bucket": all["Bucket"], "Queue": all["Queue"]})
	var bucket string
	for _, r := range h.resources("keep") {
		if r.Logical == "Bucket" {
			bucket = r.Physical
		}
	}
	h.fill(bucket, true)
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "keep"}); code(err) != "BucketNotEmpty" {
		t.Errorf("DeleteStack of a stack whose bucket holds an object answered %v, want S3's BucketNotEmpty", err)
	}
	if status, reason, err := h.status("keep"); status != "DELETE_FAILED" || !strings.Contains(reason, "BucketNotEmpty") || err != nil {
		t.Errorf("after the failed delete keep is %q, reason %q (%v), want DELETE_FAILED with S3's error", status, reason, err)
	}
	if left := h.statuses("keep"); !reflect.DeepEqual(left, map[string]string{"Bucket": "DELETE_FAILED"}) {
		t.Errorf("after the failed delete keep lists %v, want only its bucket, DELETE_FAILED", left)
	}
	h.fill(bucket, false)
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "keep"}); err != nil {
		t.Fatalf("DeleteStack again: %v", err)
	}
	if _, _, err := h.status("keep"); code(err) != "ValidationError" {
		t.Errorf("DescribeStacks after the second DeleteStack answered %v, want ValidationError (no such stack)", err)
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": bucket}); err == nil {
		t.Error("the second DeleteStack left the bucket")
	}
}

// A rollback that cannot delete a resource leaves the stack ROLLBACK_FAILED,
// with the resource listed as DELETE_FAILED and the error as
// StackStatusReason, and DeleteStack tries the resource again. Here an
// object lands in the stack's bucket before its rollback runs, so S3 refuses
// to delete the bucket; it is written into S3's store ahead of the create,
// since nothing runs between a create and its rollback here.
func TestCloudFormationARollbackThatFailsLeavesItsStack(t *testing.T) {
	h := newStacks(t)
	objects := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("objects")
	if err := objects.Put(h.ctx, "landed/k", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	bucket := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "landed"}}
	if err := h.create("rf", map[string]map[string]any{"Bucket": bucket, "ZZZ": {"Type": "AWS::Nope::Thing"}}); code(err) != "ValidationError" {
		t.Fatalf("the create answered %v, want its own ValidationError for the unsupported type", err)
	}
	if status, reason, err := h.status("rf"); status != "ROLLBACK_FAILED" || !strings.Contains(reason, "BucketNotEmpty") || err != nil {
		t.Errorf("rf is %q, reason %q (%v), want ROLLBACK_FAILED with S3's error", status, reason, err)
	}
	if left := h.statuses("rf"); !reflect.DeepEqual(left, map[string]string{"Bucket": "DELETE_FAILED"}) {
		t.Errorf("rf lists %v, want its bucket, DELETE_FAILED", left)
	}
	if err := h.create("rf", map[string]map[string]any{"Q": nameable()["Queue"]}); code(err) != "AlreadyExistsException" {
		t.Errorf("CreateStack of the ROLLBACK_FAILED rf answered %v, want AlreadyExistsException", err)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "rf"}); code(err) != "BucketNotEmpty" {
		t.Errorf("DeleteStack of rf while its bucket holds an object answered %v, want BucketNotEmpty", err)
	}
	if status, _, _ := h.status("rf"); status != "DELETE_FAILED" {
		t.Errorf("rf is %q after the refused delete, want DELETE_FAILED", status)
	}
	if err := objects.Delete(h.ctx, "landed/k"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "rf"}); err != nil {
		t.Fatalf("DeleteStack rf: %v", err)
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "landed"}); err == nil {
		t.Error("DeleteStack left the bucket the rollback could not delete")
	}
	if _, _, err := h.status("rf"); code(err) != "ValidationError" {
		t.Errorf("DescribeStacks after DeleteStack answered %v, want ValidationError (no such stack)", err)
	}
}

// GenerateSecretString's properties are read as a template's are: a number
// or a boolean may be given as its text ("16", "true"), which is how a Ref
// to a Number parameter, or a YAML scalar quoted, gives one. Any other value
// of the wrong type is ValidationError naming the property, and each way a
// GenerateSecretString can be wrong has its own message. Before, "16" and a
// Ref'd length were refused as no password, "true" read as false, and the
// refusals shared one message.
func TestCloudFormationReadsGenerateSecretStringAsATemplateDoes(t *testing.T) {
	h := newStacks(t)
	stacks := 0
	generate := func(props map[string]any, params map[string]any) (string, error) {
		t.Helper()
		stacks++
		stack := "gen" + strings.Repeat("x", stacks)
		tpl, _ := json.Marshal(map[string]any{"Parameters": params, "Resources": map[string]any{"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": props}}})
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": stack, "TemplateBody": string(tpl)}); err != nil {
			return "", err
		}
		out, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": h.resources(stack)[0].Physical})
		if err != nil {
			t.Fatalf("GetSecretValue for %s: %v", stack, err)
		}
		return str(out["SecretString"]), nil
	}
	spec := func(kv ...any) map[string]any {
		m := map[string]any{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i].(string)] = kv[i+1]
		}
		return map[string]any{"GenerateSecretString": m}
	}
	const punctuation = "!\"#$%&'()*+,-./:;?@[\\]^_`{|}~"
	for _, c := range []struct {
		name   string
		props  map[string]any
		params map[string]any
		check  func(string) bool
	}{
		{"PasswordLength \"16\"", spec("PasswordLength", "16"), nil, func(v string) bool { return len(v) == 16 }},
		{"PasswordLength from a Number parameter", spec("PasswordLength", map[string]any{"Ref": "Len"}),
			map[string]any{"Len": map[string]any{"Type": "Number", "Default": "20"}}, func(v string) bool { return len(v) == 20 }},
		{"ExcludePunctuation \"true\"", spec("PasswordLength", 200, "ExcludePunctuation", "true"), nil, func(v string) bool { return !strings.ContainsAny(v, punctuation) }},
		{"ExcludePunctuation \"false\"", spec("PasswordLength", 4, "ExcludePunctuation", "false"), nil, func(v string) bool { return strings.ContainsAny(v, punctuation) }},
	} {
		if v, err := generate(c.props, c.params); err != nil || !c.check(v) {
			t.Errorf("%s generated %q (%v)", c.name, v, err)
		}
	}
	yaml := "Resources:\n  S:\n    Type: AWS::SecretsManager::Secret\n    Properties:\n      GenerateSecretString:\n        PasswordLength: 16\n        ExcludePunctuation: 'true'\n"
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "yaml", "TemplateBody": yaml}); err != nil {
		t.Errorf("a YAML GenerateSecretString: %v", err)
	} else if out, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": h.resources("yaml")[0].Physical}); err != nil ||
		len(str(out["SecretString"])) != 16 || strings.ContainsAny(str(out["SecretString"]), punctuation) {
		t.Errorf("the YAML GenerateSecretString generated %v (%v)", out["SecretString"], err)
	}

	seen := map[string]string{}
	for _, c := range []struct {
		cause, says string
		props       map[string]any
	}{
		{"not an object", "GenerateSecretString", map[string]any{"GenerateSecretString": "notamap"}},
		{"SecretString beside it", "SecretString", map[string]any{"SecretString": "x", "GenerateSecretString": map[string]any{}}},
		{"GenerateStringKey alone", "GenerateStringKey", spec("GenerateStringKey", "password")},
		{"SecretStringTemplate alone", "SecretStringTemplate", spec("SecretStringTemplate", `{"username":"u"}`)},
		{"SecretStringTemplate not JSON", "JSON", spec("SecretStringTemplate", `{"username"`, "GenerateStringKey", "password")},
		{"SecretStringTemplate not a JSON object", "object", spec("SecretStringTemplate", `[1]`, "GenerateStringKey", "password")},
		{"parameters no password meets", "PasswordLength", spec("PasswordLength", 0)},
	} {
		_, err := generate(c.props, nil)
		var f *spi.Fault
		if !errors.As(err, &f) || f.Code != "ValidationError" || !strings.Contains(f.Message, c.says) {
			t.Errorf("%s answered %v, want a ValidationError that says %s", c.cause, err, c.says)
			continue
		}
		if other, dup := seen[f.Message]; dup {
			t.Errorf("%s and %s share the message %q", c.cause, other, f.Message)
		}
		seen[f.Message] = c.cause
	}
	for member, value := range map[string]any{"PasswordLength": "sixteen", "ExcludePunctuation": "yes", "IncludeSpace": 1.0, "ExcludeCharacters": true} {
		if _, err := generate(spec(member, value), nil); code(err) != "ValidationError" || !strings.Contains(err.Error(), member) {
			t.Errorf("%s %v answered %v, want a ValidationError naming %s", member, value, err, member)
		}
	}
}

// "CloudFormation supports the YAML Version 1.1 specification with a few
// exceptions. CloudFormation doesn't support the following features: ...
// Aliases[,] Hash merges" (User Guide, Template formats). An anchor, an alias
// or a merge key is ValidationError, so no alias can recurse (a: &a [*a]),
// multiply (a billion laughs) or carry a tag away from its node; and a
// template nesting deeper than 100 levels or holding more than 100,000 nodes
// is ValidationError too, whatever its shape. Before, the self-alias
// overflowed the stack and killed the process, the billion laughs ran without
// end, and a !Ref behind an alias became a plain string.
func TestCloudFormationRefusesYAMLAliases(t *testing.T) {
	h := newStacks(t)
	queue := "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      Tags: "
	laughs := "a0: &a0 [x, x, x, x, x, x, x, x, x, x]\n"
	for i := 1; i < 10; i++ {
		laughs += fmt.Sprintf("a%d: &a%d [%s]\n", i, i, strings.TrimSuffix(strings.Repeat(fmt.Sprintf("*a%d, ", i-1), 10), ", "))
	}
	for _, c := range []struct{ name, body, says string }{
		{"self-alias", "a: &a [*a]\n" + queue + "[]\n", "aliases"},
		{"billion laughs", laughs + queue + "*a9\n", "aliases"},
		{"tag behind an alias", "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: &x !Ref AWS::StackName\n      RedrivePolicy: *x\n", "aliases"},
		{"merge key", "Resources:\n  Q:\n    <<: {Type: AWS::SQS::Queue}\n", "merge"},
		{"101 levels", queue + strings.Repeat("[", 101) + strings.Repeat("]", 101) + "\n", "levels"},
		{"100,001 nodes", queue + "[" + strings.Repeat("1, ", 100_000) + "1]\n", "nodes"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, op := range []string{"ValidateTemplate", "CreateStack", "GetTemplateSummary"} {
				start := time.Now()
				_, err := h.invoke(op, map[string]any{"StackName": "y", "TemplateBody": c.body})
				if code(err) != "ValidationError" || !strings.Contains(err.Error(), c.says) {
					t.Errorf("%s answered %v, want a ValidationError about %s", op, err, c.says)
				}
				if took := time.Since(start); took > 2*time.Second {
					t.Errorf("%s took %v", op, took)
				}
			}
		})
	}
	within := queue + strings.Repeat("[", 90) + strings.Repeat("]", 90) + "\n  W:\n    Type: AWS::SQS::Queue\n    Properties:\n      Tags: [" + strings.Repeat("1, ", 90_000) + "1]\n"
	if _, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": within}); err != nil {
		t.Errorf("a template of 90 levels and 90,000 nodes was refused: %v", err)
	}
}

// A rollback that deletes everything leaves a stack with no resources, and
// every read of the stack answers it. Before, the record held a null
// Resources and ListStackResources and DescribeStackEvents failed on it
// ("expected iterable type"), an HTTP 500. And "The Resources section is
// required in every CloudFormation template" (User Guide, Template sections):
// a template without one is ValidationError, "Template format error: At least
// one Resources member must be defined.", on create, update, ValidateTemplate
// and GetTemplateSummary. Before, such a stack was created.
func TestCloudFormationAStackWithNoResourcesReads(t *testing.T) {
	h := newStacks(t)
	if err := h.create("rc", map[string]map[string]any{"Q": nameable()["Queue"], "ZZZ": {"Type": "AWS::Nope::Thing"}}); err == nil {
		t.Fatal("a template with an unsupported resource type was created")
	}
	for _, op := range []string{"DescribeStacks", "ListStacks", "DescribeStackResources", "ListStackResources", "DescribeStackEvents", "GetTemplate"} {
		if _, err := h.invoke(op, map[string]any{"StackName": "rc"}); err != nil {
			t.Errorf("%s of the rolled-back rc answered %v", op, err)
		}
	}
	if _, err := h.invoke("DescribeStackResource", map[string]any{"StackName": "rc", "LogicalResourceId": "Q"}); code(err) != "ValidationError" {
		t.Errorf("DescribeStackResource of a resource rc no longer has answered %v, want ValidationError", err)
	}
	h.mustCreate("upd", map[string]map[string]any{"Q": nameable()["Queue"]})
	const msg = "Template format error: At least one Resources member must be defined."
	for _, body := range []string{`{}`, `{"Resources":{}}`, `{"Resources":null}`, "Resources: {}\n", "AWSTemplateFormatVersion: '2010-09-09'\n"} {
		for _, call := range []struct {
			op string
			in map[string]any
		}{
			{"ValidateTemplate", map[string]any{"TemplateBody": body}},
			{"GetTemplateSummary", map[string]any{"TemplateBody": body}},
			{"CreateStack", map[string]any{"StackName": "none", "TemplateBody": body}},
			{"UpdateStack", map[string]any{"StackName": "upd", "TemplateBody": body}},
		} {
			if _, err := h.invoke(call.op, call.in); code(err) != "ValidationError" || !strings.Contains(err.Error(), msg) {
				t.Errorf("%s of %q answered %v, want ValidationError: %s", call.op, body, err, msg)
			}
		}
	}
	if _, _, err := h.status("none"); code(err) != "ValidationError" {
		t.Errorf("a refused template left the stack none: %v", err)
	}
}

// A parameter is a string: for a Number, "when you use the parameter
// elsewhere in your template (for example, by using the Ref intrinsic
// function), the parameter value becomes a string" (User Guide, Parameters).
// So a numeric Default is its text, Default: 16 is "16", and a Ref to it
// feeds PasswordLength like any other. Before, a Default that was not a
// string became "", in a JSON template and a YAML one alike.
func TestCloudFormationANumericDefaultIsItsText(t *testing.T) {
	h := newStacks(t)
	json1 := `{"Parameters":{"Len":{"Type":"Number","Default":16}},"Resources":{"S":{"Type":"AWS::SecretsManager::Secret","Properties":{"GenerateSecretString":{"PasswordLength":{"Ref":"Len"}}}}}}`
	yaml1 := "Parameters:\n  Len:\n    Type: Number\n    Default: 16\nResources:\n  S:\n    Type: AWS::SecretsManager::Secret\n    Properties:\n      GenerateSecretString:\n        PasswordLength: !Ref Len\n"
	for stack, body := range map[string]string{"json-default": json1, "yaml-default": yaml1} {
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": stack, "TemplateBody": body}); err != nil {
			t.Errorf("%s: %v", stack, err)
			continue
		}
		out, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": h.resources(stack)[0].Physical})
		if err != nil || len(str(out["SecretString"])) != 16 {
			t.Errorf("%s generated %v (%v), want 16 characters", stack, out["SecretString"], err)
		}
	}
}

// DescribeStackEvents answers each resource's status as the other resource
// reads do, DELETE_FAILED included, with its ResourceStatusReason
// ("Success/failure message associated with the resource", a member of the
// model's StackEvent): all four reads derive both from the stack's record.
// Before, every event said CREATE_COMPLETE and none carried a reason.
func TestCloudFormationStackEventsAgreeWithTheResources(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	h.mustCreate("ev", map[string]map[string]any{"Bucket": all["Bucket"], "Queue": all["Queue"]})
	var bucket string
	for _, r := range h.resources("ev") {
		if r.Logical == "Bucket" {
			bucket = r.Physical
		}
	}
	h.fill(bucket, true)
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "ev"}); code(err) != "BucketNotEmpty" {
		t.Fatalf("DeleteStack answered %v, want BucketNotEmpty", err)
	}
	read := func(op, member string, in map[string]any) []any {
		t.Helper()
		out, err := h.invoke(op, in)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		if one, ok := out[member].(map[string]any); ok {
			return []any{one}
		}
		return out[member].([]any)
	}
	for _, c := range []struct {
		op, member string
		in         map[string]any
	}{
		{"DescribeStackEvents", "StackEvents", map[string]any{"StackName": "ev"}},
		{"DescribeStackResources", "StackResources", map[string]any{"StackName": "ev"}},
		{"ListStackResources", "StackResourceSummaries", map[string]any{"StackName": "ev"}},
		{"DescribeStackResource", "StackResourceDetail", map[string]any{"StackName": "ev", "LogicalResourceId": "Bucket"}},
	} {
		found := false
		for _, raw := range read(c.op, c.member, c.in) {
			if r := raw.(map[string]any); r["LogicalResourceId"] == "Bucket" {
				found = r["ResourceStatus"] == "DELETE_FAILED" && strings.Contains(str(r["ResourceStatusReason"]), "BucketNotEmpty")
				if !found {
					t.Errorf("%s answered the bucket as %v / %v, want DELETE_FAILED with S3's error", c.op, r["ResourceStatus"], r["ResourceStatusReason"])
				}
			}
		}
		if !found {
			t.Errorf("%s did not answer the bucket as DELETE_FAILED with S3's error", c.op)
		}
	}
}
