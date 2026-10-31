package cloudformation

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"regexp"
	"strings"
	"sync"
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
	for member, value := range map[string]any{"PasswordLength": "sixteen", "ExcludePunctuation": "yes", "IncludeSpace": 1.0, "ExcludeCharacters": []any{"!"}} {
		if _, err := generate(spec(member, value), nil); code(err) != "ValidationError" || !strings.Contains(err.Error(), member) {
			t.Errorf("%s %v answered %v, want a ValidationError naming %s", member, value, err, member)
		}
	}
}

// A failed UpdateStack keeps every resource it made that its rollback could
// not delete: the stack is UPDATE_ROLLBACK_FAILED, with the resource listed
// as DELETE_FAILED and the error as StackStatusReason, an UpdateStack of it
// is refused, and DeleteStack removes the resource with the rest. A resource
// the update half made is rolled back too. Here an object lands in the
// update's new bucket before the rollback runs, so S3 refuses to delete it,
// and the update's rule is half made (EventBridge refuses its targets).
// Before, the rollback dropped the delete's error and wrote the previous
// stack without the bucket, which no DeleteStack would then reach.
func TestCloudFormationAFailedUpdateKeepsWhatItCouldNotRollBack(t *testing.T) {
	h := newStacks(t)
	secret := func(value string) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "a", "SecretString": value}}
	}
	h.mustCreate("u", map[string]map[string]any{"A": secret("v1")})
	objects := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("objects")
	if err := objects.Put(h.ctx, "landed-u/k", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	update, _ := json.Marshal(map[string]any{"Resources": map[string]any{
		"A": secret("v2"),
		"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "landed-u"}},
		"C": map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"Name": "half-u", "ScheduleExpression": "rate(5 minutes)", "Targets": "notalist"}},
	}})
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "u", "TemplateBody": string(update)}); code(err) != "ValidationException" {
		t.Fatalf("the update answered %v, want EventBridge's ValidationException for the targets", err)
	}
	if status, reason, err := h.status("u"); status != "UPDATE_ROLLBACK_FAILED" || !strings.Contains(reason, "BucketNotEmpty") || err != nil {
		t.Errorf("u is %q, reason %q (%v), want UPDATE_ROLLBACK_FAILED with S3's error", status, reason, err)
	}
	if left := h.statuses("u"); !reflect.DeepEqual(left, map[string]string{"A": "CREATE_COMPLETE", "B": "DELETE_FAILED"}) {
		t.Errorf("u lists %v, want its secret and the bucket the rollback could not delete", left)
	}
	if _, err := h.service("aws.events", "DescribeRule", map[string]any{"Name": "half-u"}); err == nil {
		t.Error("the rule the update half made outlived the rollback")
	}
	if out, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": "a"}); err != nil || out["SecretString"] != "v1" {
		t.Errorf("after the rollback the secret holds %v (%v), want v1", out["SecretString"], err)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "u", "TemplateBody": string(update)}); code(err) != "ValidationError" {
		t.Errorf("UpdateStack of the UPDATE_ROLLBACK_FAILED u answered %v, want ValidationError", err)
	}
	if err := objects.Delete(h.ctx, "landed-u/k"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "u"}); err != nil {
		t.Fatalf("DeleteStack u: %v", err)
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "landed-u"}); err == nil {
		t.Error("DeleteStack left the bucket the failed update made")
	}
	if _, err := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": "a"}); err == nil {
		t.Error("DeleteStack left the stack's secret")
	}
}

// PutTargets "can partially fail ... FailedEntryCount is non-zero in the
// response and each entry in FailedEntries provides the ID of the failed
// target and the error code", each entry representing "a target that failed
// to be added to a rule". A rule whose targets were not all added is not the
// rule the template declares, so the resource fails with an error naming
// each failed entry, and the stack rolls back: on create, and on update.
// Before, the stack finished CREATE_COMPLETE or UPDATE_COMPLETE without the
// targets.
func TestCloudFormationARuleWhoseTargetsFailFailsTheStack(t *testing.T) {
	h := newStacks(t)
	rule := func(name string, targets ...map[string]any) map[string]map[string]any {
		list := []any{}
		for _, target := range targets {
			list = append(list, target)
		}
		return map[string]map[string]any{"R": {"Type": "AWS::Events::Rule", "Properties": map[string]any{
			"Name": name, "ScheduleExpression": "rate(5 minutes)", "Targets": list}}}
	}
	queue := "arn:aws:sqs:us-east-1:000000000000:q"
	for i, target := range []map[string]any{{"Id": "no-arn"}, {"Arn": queue}, {}} {
		name := fmt.Sprintf("tr%d", i)
		err := h.create(name, rule(name, target))
		if err == nil || !strings.Contains(err.Error(), "Id or Arn is invalid") || !strings.Contains(err.Error(), fmt.Sprint(target["Id"])) && target["Id"] != nil {
			t.Errorf("a rule with the target %v answered %v, want an error naming the failed entry", target, err)
		}
		if status, _, err := h.status(name); status != "ROLLBACK_COMPLETE" {
			t.Errorf("%s is %q (%v), want ROLLBACK_COMPLETE", name, status, err)
		}
		if _, err := h.service("aws.events", "DescribeRule", map[string]any{"Name": name}); err == nil {
			t.Errorf("%s's rule outlived the rollback", name)
		}
	}
	h.mustCreate("tu", rule("tu", map[string]any{"Id": "kept", "Arn": queue}))
	update, _ := json.Marshal(map[string]any{"Resources": rule("tu", map[string]any{"Id": "kept", "Arn": queue}, map[string]any{"Id": "bad"})})
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "tu", "TemplateBody": string(update)}); err == nil || !strings.Contains(err.Error(), "bad") {
		t.Errorf("an update adding a target EventBridge refused answered %v, want an error naming it", err)
	}
	if status, _, err := h.status("tu"); status != "UPDATE_ROLLBACK_COMPLETE" {
		t.Errorf("tu is %q (%v), want UPDATE_ROLLBACK_COMPLETE", status, err)
	}
}

// CreateStack's StackName: "A stack name can contain only alphanumeric
// characters (case sensitive) and hyphens. It must start with an alphabetical
// character and can't be longer than 128 characters." Any other name is
// ValidationError in the framework's wording ("1 validation error detected:
// Value '...' at 'stackName' failed to satisfy constraint: ...") and makes
// nothing. UpdateStack, DeleteStack and DescribeStacks take "The name or
// unique stack ID", so they check the same pattern and admit a stack id.
// Before, any name was taken, and the naming helper dropped the characters a
// resource name may not hold.
func TestCloudFormationChecksTheStackName(t *testing.T) {
	h := newStacks(t)
	q := map[string]map[string]any{"Q": nameable()["Queue"]}
	tpl, _ := json.Marshal(map[string]any{"Resources": q})
	refused := func(op string, in map[string]any, want string) {
		t.Helper()
		_, err := h.invoke(op, in)
		var f *spi.Fault
		if !errors.As(err, &f) || f.Code != "ValidationError" || !strings.Contains(f.Message, "at 'stackName' failed to satisfy constraint: Member must "+want) {
			t.Errorf("%s %v answered %v, want ValidationError: ... %s", op, in["StackName"], err, want)
		}
	}
	for _, name := range []string{"has space!", "1digit", "under_score", "dotted.name"} {
		refused("CreateStack", map[string]any{"StackName": name, "TemplateBody": string(tpl)}, "satisfy regular expression pattern: [a-zA-Z][-a-zA-Z0-9]*")
		refused("UpdateStack", map[string]any{"StackName": name, "TemplateBody": string(tpl)}, "satisfy regular expression pattern")
		refused("DeleteStack", map[string]any{"StackName": name}, "satisfy regular expression pattern")
		refused("DescribeStacks", map[string]any{"StackName": name}, "satisfy regular expression pattern")
	}
	refused("CreateStack", map[string]any{"StackName": strings.Repeat("a", 129), "TemplateBody": string(tpl)}, "have length less than or equal to 128")
	if queues, _ := h.service("aws.sqs", "ListQueues", map[string]any{}); queues["QueueUrls"] != nil && len(queues["QueueUrls"].([]any)) != 0 {
		t.Errorf("a refused stack made queues: %v", queues["QueueUrls"])
	}
	if stacks, _ := h.invoke("ListStacks", map[string]any{}); stacks["StackSummaries"] != nil && len(stacks["StackSummaries"].([]any)) != 0 {
		t.Errorf("a refused stack left a record: %v", stacks["StackSummaries"])
	}
	h.mustCreate("Ok-1", q)
	described, err := h.invoke("DescribeStacks", map[string]any{"StackName": "Ok-1"})
	if err != nil {
		t.Fatal(err)
	}
	id := str(described["Stacks"].([]any)[0].(map[string]any)["StackId"])
	if _, err := h.invoke("DescribeStacks", map[string]any{"StackName": id}); err != nil && strings.Contains(err.Error(), "failed to satisfy constraint") {
		t.Errorf("DescribeStacks refused the stack id %s as a name: %v", id, err)
	}
}

// "CloudFormation will replace the resource by creating a new one and
// deleting the old one" (ResourceChange.Replacement), and for an update "it
// begins disposing of old resources immediately once the operation
// completes" (RollbackConfiguration.MonitoringTimeInMinutes). So once every
// resource of an update is in place, the cleanup deletes each old resource
// the stack no longer holds: one replaced by a new physical id (a new name
// here) and one the template dropped. A delete that fails there leaves the
// stack UPDATE_COMPLETE, as CloudFormation does, and is not lost: the old
// resource stays listed as DELETE_FAILED, the error its ResourceStatusReason
// ("Success/failure message associated with the resource"), and DeleteStack
// deletes it with the rest. A failed update deletes nothing old. Before, an
// update never deleted the resources it replaced or dropped.
func TestCloudFormationAnUpdateDeletesWhatItReplaced(t *testing.T) {
	h := newStacks(t)
	named := func(typ, member, name string) map[string]any {
		return map[string]any{"Type": typ, "Properties": map[string]any{member: name}}
	}
	queue := func(name string) map[string]any { return named("AWS::SQS::Queue", "QueueName", name) }
	bucket := func(name string) map[string]any { return named("AWS::S3::Bucket", "BucketName", name) }
	update := func(resources map[string]map[string]any) error {
		tpl, _ := json.Marshal(map[string]any{"Resources": resources})
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "rp", "TemplateBody": string(tpl)})
		return err
	}
	alive := func(svc, op, member, name string) bool {
		_, err := h.service(svc, op, map[string]any{member: name})
		return err == nil
	}
	h.mustCreate("rp", map[string]map[string]any{"Q": queue("old-q"), "B": bucket("old-b"), "Gone": queue("gone-q")})
	if err := update(map[string]map[string]any{"Q": queue("new-q"), "B": bucket("old-b"), "Gone": queue("gone-q"), "ZZZ": {"Type": "AWS::Nope::Thing"}}); err == nil {
		t.Fatal("an update with an unsupported resource type succeeded")
	}
	if !alive("aws.sqs", "GetQueueUrl", "QueueName", "old-q") || alive("aws.sqs", "GetQueueUrl", "QueueName", "new-q") {
		t.Error("the failed update deleted the queue it would have replaced, or kept its replacement")
	}
	h.fill("old-b", true) // S3 refuses to delete a bucket that holds an object
	if err := update(map[string]map[string]any{"Q": queue("new-q2"), "B": bucket("new-b")}); err != nil {
		t.Fatalf("the replacing update: %v", err)
	}
	if status, _, err := h.status("rp"); status != "UPDATE_COMPLETE" {
		t.Errorf("rp is %q (%v), want UPDATE_COMPLETE", status, err)
	}
	for _, c := range []struct {
		name            string
		want            bool
		svc, op, member string
	}{
		{"old-q", false, "aws.sqs", "GetQueueUrl", "QueueName"}, {"gone-q", false, "aws.sqs", "GetQueueUrl", "QueueName"},
		{"new-q2", true, "aws.sqs", "GetQueueUrl", "QueueName"}, {"new-b", true, "aws.s3", "HeadBucket", "Bucket"},
		{"old-b", true, "aws.s3", "HeadBucket", "Bucket"},
	} {
		if got := alive(c.svc, c.op, c.member, c.name); got != c.want {
			t.Errorf("after the update %s exists: %v, want %v", c.name, got, c.want)
		}
	}
	listed, err := h.invoke("DescribeStackResources", map[string]any{"StackName": "rp"})
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, raw := range listed["StackResources"].([]any) {
		r := raw.(map[string]any)
		row := str(r["LogicalResourceId"]) + " " + lastSlash(str(r["PhysicalResourceId"])) + " " + str(r["ResourceStatus"])
		if reason := str(r["ResourceStatusReason"]); strings.Contains(reason, "BucketNotEmpty") {
			row += " BucketNotEmpty"
		}
		rows = append(rows, row)
	}
	want := []string{"B new-b CREATE_COMPLETE", "Q new-q2 CREATE_COMPLETE", "B old-b DELETE_FAILED BucketNotEmpty"}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("rp lists %q, want %q", rows, want)
	}
	h.fill("old-b", false)
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "rp"}); err != nil {
		t.Fatalf("DeleteStack rp: %v", err)
	}
	if alive("aws.s3", "HeadBucket", "Bucket", "old-b") {
		t.Error("DeleteStack left the bucket the cleanup could not delete")
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
				answered := make(chan error, 1)
				go func() {
					_, err := h.invoke(op, map[string]any{"StackName": "y", "TemplateBody": c.body})
					answered <- err
				}()
				select {
				case err := <-answered:
					if code(err) != "ValidationError" || !strings.Contains(err.Error(), c.says) {
						t.Errorf("%s answered %v, want a ValidationError about %s", op, err, c.says)
					}
				case <-time.After(2 * time.Second):
					t.Errorf("%s did not answer within 2s", op)
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

// A ROLLBACK_COMPLETE stack keeps why its create failed as its
// StackStatusReason, and ListStacks answers the reason as DescribeStacks
// does: the model's StackSummary has it ("Success/Failure message associated
// with the stack status"). Before, the reason was cleared once the rollback
// completed, and ListStacks never answered one.
func TestCloudFormationAStackKeepsWhyItFailed(t *testing.T) {
	h := newStacks(t)
	if err := h.create("why", map[string]map[string]any{"Q": nameable()["Queue"], "ZZZ": {"Type": "AWS::Nope::Thing"}}); err == nil {
		t.Fatal("a template with an unsupported resource type was created")
	}
	if status, reason, err := h.status("why"); status != "ROLLBACK_COMPLETE" || !strings.Contains(reason, "unsupported resource type") || err != nil {
		t.Errorf("why is %q, reason %q (%v), want ROLLBACK_COMPLETE with the create's error", status, reason, err)
	}
	listed, err := h.invoke("ListStacks", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range listed["StackSummaries"].([]any) {
		if s := raw.(map[string]any); s["StackName"] == "why" && !strings.Contains(str(s["StackStatusReason"]), "unsupported resource type") {
			t.Errorf("ListStacks answered why with %v, want its reason", s)
		}
	}
}

// The rollback family follows the states the API reference gives each
// operation. RollbackStack "Rolls back the specified stack to the last known
// stable state from CREATE_FAILED or UPDATE_FAILED stack statuses";
// ContinueUpdateRollback "Continues rolling back a stack from
// UPDATE_ROLLBACK_FAILED to UPDATE_ROLLBACK_COMPLETE state"; and of
// CancelUpdateStack, "You can cancel only stacks that are in the
// UPDATE_IN_PROGRESS state". Every other stack is ValidationError and is left
// as it was, a ROLLBACK_FAILED one with its DELETE_FAILED orphan included
// (its way forward is DeleteStack), and an absent one "does not exist".
// Before, each marked any stack complete, orphans and all, and made a stack
// out of a name that had none.
func TestCloudFormationTheRollbackFamilyFollowsItsStates(t *testing.T) {
	h := newStacks(t)
	objects := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("objects")
	if err := objects.Put(h.ctx, "stuck/k", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	bucket := map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "stuck"}}
	if err := h.create("failed", map[string]map[string]any{"Bucket": bucket, "ZZZ": {"Type": "AWS::Nope::Thing"}}); err == nil {
		t.Fatal("a template with an unsupported resource type was created")
	}
	h.mustCreate("fine", map[string]map[string]any{"Q": nameable()["Queue"]})
	want := map[string]string{"failed": "ROLLBACK_FAILED", "fine": "CREATE_COMPLETE"}
	for _, op := range []string{"RollbackStack", "ContinueUpdateRollback", "CancelUpdateStack"} {
		for stack, status := range want {
			if _, err := h.invoke(op, map[string]any{"StackName": stack}); code(err) != "ValidationError" {
				t.Errorf("%s of the %s stack %s answered %v, want ValidationError", op, status, stack, err)
			}
			if got, _, _ := h.status(stack); got != status {
				t.Errorf("after %s, %s is %s, want %s", op, stack, got, status)
			}
		}
		if _, err := h.invoke(op, map[string]any{"StackName": "absent"}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("%s of an absent stack answered %v, want ValidationError: does not exist", op, err)
		}
		if _, _, err := h.status("absent"); err == nil {
			t.Errorf("%s made a stack out of the name absent", op)
		}
	}
	if left := h.statuses("failed"); !reflect.DeepEqual(left, map[string]string{"Bucket": "DELETE_FAILED"}) {
		t.Errorf("failed lists %v, want its bucket, DELETE_FAILED", left)
	}
}

// A stack recorded before resources carried the name they were made under
// still updates: a resource with no recorded name is known by its physical
// id, which is its name or ends in it (a queue's URL, a secret's ARN), so
// the update's check of a name the template gives finds it the stack's own.
// Before, the check found the stack's own queue taken, and the update failed.
func TestCloudFormationAnOldRecordStillUpdates(t *testing.T) {
	h := newStacks(t)
	q, err := h.service("aws.sqs", "CreateQueue", map[string]any{"QueueName": "legacy-q"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.service("aws.secretsmanager", "CreateSecret", map[string]any{"Name": "legacy-s", "SecretString": "v1"})
	if err != nil {
		t.Fatal(err)
	}
	tpl := func(value string) string {
		return `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"legacy-q"}},` +
			`"S":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"legacy-s","SecretString":"` + value + `"}}}}`
	}
	record, _ := json.Marshal(map[string]any{ // the shape a stack was stored in before res.Name
		"Name": "old", "ID": "arn:aws:cloudformation:us-east-1:000000000000:stack/old/1", "Status": "CREATE_COMPLETE", "Template": tpl("v1"),
		"Resources": []any{
			map[string]any{"Logical": "Q", "Physical": str(q["QueueUrl"]), "Type": "AWS::SQS::Queue"},
			map[string]any{"Logical": "S", "Physical": str(s["ARN"]), "Type": "AWS::SecretsManager::Secret"},
		},
		"Refs": map[string]any{"Q": str(q["QueueUrl"]), "S": str(s["ARN"])},
	})
	if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "old", record); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "old", "TemplateBody": tpl("v2")}); err != nil {
		t.Fatalf("UpdateStack of an old record: %v", err)
	}
	if out, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": "legacy-s"}); err != nil || out["SecretString"] != "v2" {
		t.Errorf("after the update the secret holds %v (%v), want v2", out["SecretString"], err)
	}
}

// EventBridge keys a rule by its event bus and its name, and the rule's ARN
// carries the bus (rule/<bus>/<name>), so every check, write and delete of a
// rule carries its bus: the EventBusName property, or the bus in the ARN. A
// named rule already on bus b fails the stack, a rule of that name on the
// default bus does not, a rule's targets go to the rule on its own bus, and
// DeleteStack deletes the stack's rule on bus b, never a same-named rule on
// the default bus. Before, the check, PutTargets and the delete looked at the
// default bus whatever the rule's bus was.
func TestCloudFormationARuleLivesOnItsBus(t *testing.T) {
	h := newStacks(t)
	events := func(op string, in map[string]any) (map[string]any, error) { return h.service("aws.events", op, in) }
	if _, err := events("CreateEventBus", map[string]any{"Name": "b"}); err != nil {
		t.Fatal(err)
	}
	queue := "arn:aws:sqs:us-east-1:000000000000:q"
	rule := func(name string, targets bool) map[string]map[string]any {
		props := map[string]any{"EventBusName": "b", "EventPattern": map[string]any{"source": []any{"app"}}} // a schedule is the default bus's
		if name != "" {
			props["Name"] = name
		}
		if targets {
			props["Targets"] = []any{map[string]any{"Id": "t", "Arn": queue}}
		}
		return map[string]map[string]any{"R": {"Type": "AWS::Events::Rule", "Properties": props}}
	}
	onBus := func(name, bus string) bool {
		_, err := events("DescribeRule", map[string]any{"Name": name, "EventBusName": bus})
		return err == nil
	}
	for _, in := range []map[string]any{{"Name": "x", "EventBusName": "b", "EventPattern": `{"source":["app"]}`}, {"Name": "y", "ScheduleExpression": "rate(1 hour)"}} {
		if _, err := events("PutRule", in); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.create("taken", rule("x", false)); code(err) != "ResourceAlreadyExistsException" {
		t.Errorf("a rule named for one already on bus b answered %v, want ResourceAlreadyExistsException", err)
	}
	if !onBus("x", "b") {
		t.Error("the refused stack touched the rule already on bus b")
	}
	h.mustCreate("ony", rule("y", true))
	targets, err := events("ListTargetsByRule", map[string]any{"Rule": "y", "EventBusName": "b"})
	if err != nil || len(targets["Targets"].([]any)) != 1 {
		t.Errorf("the bus-b rule y has targets %v (%v), want the template's one", targets["Targets"], err)
	}
	h.mustCreate("unnamed", rule("", false))
	unnamed := lastSlash(h.resources("unnamed")[0].Physical)
	for _, stack := range []string{"ony", "unnamed"} {
		if _, err := h.invoke("DeleteStack", map[string]any{"StackName": stack}); err != nil {
			t.Fatalf("DeleteStack %s: %v", stack, err)
		}
	}
	if onBus("y", "b") || onBus(unnamed, "b") {
		t.Error("DeleteStack left a stack's rule on bus b")
	}
	if !onBus("y", "default") {
		t.Error("DeleteStack deleted the default bus's rule y, which no stack made")
	}
}

// A stack name is reserved before any resource is made: the record is
// written, CREATE_IN_PROGRESS, in the same transaction that finds no stack
// of that name. Of eight CreateStacks of one name at once, exactly one makes
// the stack and seven are AlreadyExistsException with nothing made for them.
// Before, all eight passed the check and made their resources, and seven
// sets were left behind when the last record written won.
func TestCloudFormationConcurrentCreatesMakeOneStack(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = h.create("race", map[string]map[string]any{"Q": all["Queue"], "S": all["Secret"]})
		}()
	}
	wg.Wait()
	made := 0
	for _, err := range errs {
		if err == nil {
			made++
		} else if code(err) != "AlreadyExistsException" {
			t.Errorf("a concurrent CreateStack answered %v, want AlreadyExistsException", err)
		}
	}
	if made != 1 {
		t.Errorf("%d of 8 concurrent CreateStacks of one name succeeded, want 1", made)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "race"}); err != nil {
		t.Fatal(err)
	}
	queues, _ := h.service("aws.sqs", "ListQueues", map[string]any{})
	secrets, _ := h.service("aws.secretsmanager", "ListSecrets", map[string]any{})
	if q, s := queues["QueueUrls"], secrets["SecretList"]; q != nil && len(q.([]any)) > 0 || len(s.([]any)) > 0 {
		t.Errorf("after DeleteStack, queues %v and secrets %v are left", q, s)
	}
}

// Every property is read as the type the template reference gives it, by one
// helper, whether the template gives the value itself or its text (as a Ref to
// a parameter always does: "the parameter value becomes a string", User Guide,
// Parameters). A Boolean is true or false in any case (YAML 1.1, the
// templates' YAML, spells it true, True or TRUE), so FifoQueue "True" makes a
// FIFO queue. An Integer is decimal digits, as a Number parameter's text is:
// "016" is 16, and "16.0", "1e1", "+16" and " 16" are none. A String is a
// scalar's text: ExcludeCharacters 123 is "123", an SSM Parameter's Value 123
// is "123". Before, "True" made a standard queue, "16.0", "1e1" and "+16" made
// 16, 10 and 16 characters, ExcludeCharacters 123 was refused and Value 123
// was "".
func TestCloudFormationReadsEveryPropertyAsItsType(t *testing.T) {
	h := newStacks(t)
	make1 := func(stack, typ string, props map[string]any) (res, error) {
		t.Helper()
		tpl, _ := json.Marshal(map[string]any{"Resources": map[string]any{"R": map[string]any{"Type": typ, "Properties": props}}})
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": stack, "TemplateBody": string(tpl)}); err != nil {
			return res{}, err
		}
		return h.resources(stack)[0], nil
	}
	secret := func(stack string, spec map[string]any) (string, error) {
		t.Helper()
		r, err := make1(stack, "AWS::SecretsManager::Secret", map[string]any{"GenerateSecretString": spec})
		if err != nil {
			return "", err
		}
		out, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": r.Physical})
		return str(out["SecretString"]), err
	}
	for i, given := range []string{"True", "TRUE", "true"} {
		r, err := make1(fmt.Sprintf("fifo%d", i), "AWS::SQS::Queue", map[string]any{"FifoQueue": given})
		if err != nil {
			t.Errorf("FifoQueue %q: %v", given, err)
			continue
		}
		out, err := h.service("aws.sqs", "GetQueueAttributes", map[string]any{"QueueUrl": r.Physical, "AttributeNames": []any{"All"}})
		if !strings.HasSuffix(r.Physical, ".fifo") || err != nil || asMap(out["Attributes"])["FifoQueue"] != "true" {
			t.Errorf("FifoQueue %q made %s, attributes %v (%v), want a FIFO queue", given, r.Physical, out["Attributes"], err)
		}
	}
	if v, err := secret("len016", map[string]any{"PasswordLength": "016"}); err != nil || len(v) != 16 {
		t.Errorf("PasswordLength \"016\" generated %q (%v), want 16 characters", v, err)
	}
	for i, given := range []string{"16.0", "1e1", "+16", " 16"} {
		if v, err := secret(fmt.Sprintf("badlen%d", i), map[string]any{"PasswordLength": given}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "PasswordLength") {
			t.Errorf("PasswordLength %q generated %q (%v), want a ValidationError naming PasswordLength", given, v, err)
		}
	}
	if v, err := secret("exclude123", map[string]any{"PasswordLength": 200, "ExcludeCharacters": 123}); err != nil || len(v) != 200 || strings.ContainsAny(v, "123") {
		t.Errorf("ExcludeCharacters 123 generated %q (%v), want 200 characters and no 1, 2 or 3", v, err)
	}
	if r, err := make1("ssm123", "AWS::SSM::Parameter", map[string]any{"Value": 123}); err != nil {
		t.Errorf("an SSM Parameter of Value 123: %v", err)
	} else if out, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": r.Physical}); err != nil || asMap(out["Parameter"])["Value"] != "123" {
		t.Errorf("an SSM Parameter of Value 123 holds %v (%v), want \"123\"", out["Parameter"], err)
	}
}

// A Ref answers what its resource's Ref really answers, never its logical id.
// Before anything is made, each resource's id is planned (planID) in the
// order resources are made, each after those its properties name ("if the
// properties of resource A use a !Ref to resource B ... Resource B is created
// before resource A", Template Reference, DependsOn attribute), so the check
// of a name built on another resource's id checks that id; one built on an id
// known only once made (a KMS key's) is checked as it is made. A cycle is
// ValidationError and makes no stack. Before, a Ref to a resource not yet made
// answered its logical id: a parameter named for a bucket was checked, and
// made, as "ABucket", and a parameter sorted before the key it names held
// "ZKey".
func TestCloudFormationPlansEveryIdBeforeMakingIt(t *testing.T) {
	h := newStacks(t)
	if _, err := h.service("aws.ssm", "PutParameter", map[string]any{"Name": "ABucket", "Value": "x", "Type": "String"}); err != nil {
		t.Fatal(err)
	}
	ref := func(logical string) map[string]any { return map[string]any{"Ref": logical} }
	if err := h.create("named-for", map[string]map[string]any{
		"ABucket": {"Type": "AWS::S3::Bucket"},
		"ZParam":  {"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": ref("ABucket"), "Value": "v"}},
	}); err != nil {
		t.Errorf("a parameter named for an unnamed bucket, beside a parameter named ABucket: %v", err)
	} else {
		rs := map[string]string{}
		for _, r := range h.resources("named-for") {
			rs[r.Logical] = r.Physical
		}
		if rs["ZParam"] != rs["ABucket"] || rs["ZParam"] == "ABucket" {
			t.Errorf("ZParam is %q, want the bucket's name %q", rs["ZParam"], rs["ABucket"])
		}
	}

	if err := h.create("before", map[string]map[string]any{
		"AParam":  {"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": ref("ZBucket"), "Value": ref("ZKey")}},
		"BParam":  {"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": map[string]any{"Fn::Sub": "p-${ZKey}"}, "Value": "v"}},
		"ZBucket": {"Type": "AWS::S3::Bucket"},
		"ZKey":    {"Type": "AWS::KMS::Key", "Properties": map[string]any{"Description": "d"}},
	}); err != nil {
		t.Errorf("parameters named for a bucket and a key made after them: %v", err)
	} else {
		rs := map[string]string{}
		for _, r := range h.resources("before") {
			rs[r.Logical] = r.Physical
		}
		out, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": rs["AParam"]})
		if rs["AParam"] != rs["ZBucket"] || err != nil || asMap(out["Parameter"])["Value"] != rs["ZKey"] || rs["BParam"] != "p-"+rs["ZKey"] {
			t.Errorf("AParam %q holds %v (%v) and BParam is %q, want the bucket's name %q holding the key's id %q, and p-%s", rs["AParam"], out["Parameter"], err, rs["BParam"], rs["ZBucket"], rs["ZKey"], rs["ZKey"])
		}
	}

	cycle := `{"Resources":{"A":{"Type":"AWS::SSM::Parameter","Properties":{"Name":{"Ref":"B"},"Value":"v"}},"B":{"Type":"AWS::SSM::Parameter","Properties":{"Name":{"Fn::Sub":"${A}-b"},"Value":"v"}}}}`
	for _, op := range []string{"CreateStack", "ValidateTemplate"} {
		if _, err := h.invoke(op, map[string]any{"StackName": "cycle", "TemplateBody": cycle}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Circular dependency between resources: [A, B]") {
			t.Errorf("%s of a cycle answered %v, want ValidationError naming A and B", op, err)
		}
	}
	if _, _, err := h.status("cycle"); err == nil {
		t.Error("a cycle made a stack")
	}

	if _, err := h.service("aws.events", "CreateEventBus", map[string]any{"Name": "b"}); err != nil {
		t.Fatal(err)
	}
	all := nameable()
	all["BusRule"] = map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"EventBusName": "b", "EventPattern": map[string]any{"source": []any{"app"}}}}
	all["Key"] = map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{}}
	all["Named"] = map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "named-topic"}}
	h.mustCreate("every", all)
	out, err := h.invoke("DescribeStacks", map[string]any{"StackName": "every"})
	if err != nil {
		t.Fatal(err)
	}
	id := str(asMap(out["Stacks"].([]any)[0])["StackId"])
	for _, r := range h.resources("every") {
		props := asMap(all[r.Logical]["Properties"])
		n := cmp.Or(str(props[kinds[r.Type].member]), physicalName("every", r.Logical, id, r.Type, props))
		got, unknowable := planID(&spi.Request{Identity: h.id}, r.Type, n, props), r.Type == "AWS::SQS::Queue" || r.Type == "AWS::KMS::Key" || r.Type == "AWS::ApiGateway::RestApi"
		if unknowable && got != unknown || !unknowable && got != r.Physical {
			t.Errorf("%s (%s) was planned as %q and made as %q", r.Logical, r.Type, got, r.Physical)
		}
	}
}

// Every operation that takes "The name or unique stack ID" finds a stack by
// its StackId too, and only while that stack has that id; no record is ever
// keyed by an id. UpdateStack, and ExecuteChangeSet of an UPDATE change set,
// of a stack that does not exist is ValidationError, "Stack [x] does not
// exist", and makes nothing. Before,
// DescribeStacks and the other reads did not find a stack by its id, an
// UpdateStack of a missing stack created it, and an UpdateStack by StackId
// made a second stack keyed by the id.
func TestCloudFormationFindsAStackByItsId(t *testing.T) {
	h := newStacks(t)
	q := map[string]map[string]any{"Q": nameable()["Queue"]}
	tpl, _ := json.Marshal(map[string]any{"Resources": q})
	h.mustCreate("byid", q)
	out, err := h.invoke("DescribeStacks", map[string]any{"StackName": "byid"})
	if err != nil {
		t.Fatal(err)
	}
	id := str(asMap(out["Stacks"].([]any)[0])["StackId"])
	stale := id[:strings.LastIndex(id, "/")] + "/0000000000000000"
	for _, op := range []string{"DescribeStacks", "DescribeStackResources", "ListStackResources", "DescribeStackEvents", "GetTemplate", "GetTemplateSummary"} {
		if _, err := h.invoke(op, map[string]any{"StackName": id}); err != nil {
			t.Errorf("%s by StackId: %v", op, err)
		}
		if out, err := h.invoke(op, map[string]any{"StackName": stale}); op == "DescribeStackResources" && (err != nil || len(out["StackResources"].([]any)) != 0) {
			t.Errorf("%s by another id of the name answered %v (%v), want none, as for a missing stack", op, out, err)
		} else if op != "DescribeStackResources" && code(err) != "ValidationError" {
			t.Errorf("%s by another id of the name answered %v, want ValidationError", op, err)
		}
	}
	if _, err := h.invoke("DescribeStackResource", map[string]any{"StackName": id, "LogicalResourceId": "Q"}); err != nil {
		t.Errorf("DescribeStackResource by StackId: %v", err)
	}
	if _, err := h.invoke("UpdateTerminationProtection", map[string]any{"StackName": id, "EnableTerminationProtection": false}); err != nil {
		t.Errorf("UpdateTerminationProtection by StackId: %v", err)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": id, "TemplateBody": string(tpl)}); err != nil {
		t.Errorf("UpdateStack by StackId: %v", err)
	}
	for _, missing := range []map[string]any{
		{"op": "UpdateStack", "StackName": "missing", "TemplateBody": string(tpl)},
		{"op": "CreateChangeSet", "StackName": "missing", "ChangeSetName": "cs", "TemplateBody": string(tpl)},
		{"op": "ExecuteChangeSet", "StackName": "missing", "ChangeSetName": "cs"},
	} {
		op := str(missing["op"])
		delete(missing, "op")
		if _, err := h.invoke(op, missing); op != "CreateChangeSet" && (code(err) != "ValidationError" || !strings.Contains(err.Error(), "Stack [missing] does not exist")) {
			t.Errorf("%s of a missing stack answered %v, want ValidationError: Stack [missing] does not exist", op, err)
		}
	}
	if _, _, err := h.status("missing"); err == nil {
		t.Error("an update of a missing stack made it")
	}
	stacks, _ := h.invoke("ListStacks", map[string]any{})
	for _, s := range stacks["StackSummaries"].([]any) {
		if name := str(asMap(s)["StackName"]); name != "byid" {
			t.Errorf("ListStacks lists %q beside byid", name)
		}
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": id}); err != nil {
		t.Errorf("DeleteStack by StackId: %v", err)
	} else if _, _, err := h.status("byid"); err == nil {
		t.Error("DeleteStack by StackId left the stack")
	}
}

// Where the model's input uses StackNameOrId, the name is held to its length
// (1 or more) and pattern,
// ^([a-zA-Z][-a-zA-Z0-9]*)|(arn:\b(aws|aws-us-gov|aws-cn)\b:[-a-zA-Z0-9:/._+]*)$,
// each alternative matched whole, so a bare "arn:" or another partition is
// refused, in the framework's wording. Before, every such operation took any
// name, and DescribeStacks admitted any "arn:..." text.
func TestCloudFormationHoldsAStackNameOrIdToItsPattern(t *testing.T) {
	h := newStacks(t)
	tpl, _ := json.Marshal(map[string]any{"Resources": map[string]map[string]any{"Q": nameable()["Queue"]}})
	const pattern = `Member must satisfy regular expression pattern: ^([a-zA-Z][-a-zA-Z0-9]*)|(arn:\b(aws|aws-us-gov|aws-cn)\b:[-a-zA-Z0-9:/._+]*)$`
	for _, op := range []string{"ContinueUpdateRollback", "RollbackStack", "UpdateTerminationProtection", "CreateChangeSet", "DescribeChangeSet",
		"DeleteChangeSet", "ListChangeSets", "ExecuteChangeSet", "GetTemplateSummary", "SignalResource", "DetectStackDrift",
		"DetectStackResourceDrift", "DescribeStackResourceDrifts", "DescribeChangeSetHooks", "DescribeEvents"} {
		for name, want := range map[string]string{"arn:": pattern, "under_score": pattern, "arn:aws-mars:cloudformation:x": pattern, "": "Member must have length greater than or equal to 1"} {
			in := map[string]any{"StackName": name, "ChangeSetName": "cs", "TemplateBody": string(tpl), "LogicalResourceId": "Q", "UniqueId": "u", "Status": "SUCCESS", "EnableTerminationProtection": true}
			if _, err := h.invoke(op, in); code(err) != "ValidationError" || !strings.Contains(err.Error(), "at 'stackName' failed to satisfy constraint: "+want) {
				t.Errorf("%s of %q answered %v, want ValidationError: ... %s", op, name, err, want)
			}
		}
	}
}

// "Before you can delete the rule, you must remove all targets, using
// RemoveTargets" (EventBridge, DeleteRule), and Force "is ignored for rules
// that are not managed rules". So EventBridge refuses to delete a rule that
// still has targets, with ValidationException, and CloudFormation removes a
// rule's targets before it deletes the rule, on DeleteStack and on a create's
// rollback alike. A later rule of that name starts with no targets. Before,
// DeleteRule deleted a rule and kept its targets, and a new rule of the name
// inherited them.
func TestCloudFormationARuleLeavesNoTargets(t *testing.T) {
	h := newStacks(t)
	events := func(op string, in map[string]any) (map[string]any, error) { return h.service("aws.events", op, in) }
	target := []any{map[string]any{"Id": "t", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}
	if _, err := events("PutRule", map[string]any{"Name": "direct", "ScheduleExpression": "rate(1 hour)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := events("PutTargets", map[string]any{"Rule": "direct", "Targets": target}); err != nil {
		t.Fatal(err)
	}
	if _, err := events("DeleteRule", map[string]any{"Name": "direct", "Force": true}); code(err) != "ValidationException" {
		t.Errorf("DeleteRule of a rule with targets answered %v, want ValidationException", err)
	}
	none := func(rule string) bool {
		t.Helper()
		out, err := events("ListTargetsByRule", map[string]any{"Rule": rule})
		return err != nil || len(asMap(out)["Targets"].([]any)) == 0
	}
	rule := map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"Name": "cfn-rule", "ScheduleExpression": "rate(1 hour)", "Targets": target}}
	h.mustCreate("rules", map[string]map[string]any{"R": rule})
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "rules"}); err != nil {
		t.Fatalf("DeleteStack: %v", err)
	}
	if !none("cfn-rule") {
		t.Error("DeleteStack left the rule's targets")
	}
	if err := h.create("rolled", map[string]map[string]any{"R": rule, "Z": {"Type": "AWS::Nothing::Here"}}); err == nil {
		t.Fatal("a stack with an unsupported type was created")
	}
	if !none("cfn-rule") {
		t.Error("the rollback left the rule's targets")
	}
	if _, err := events("PutRule", map[string]any{"Name": "cfn-rule", "ScheduleExpression": "rate(2 hours)"}); err != nil || !none("cfn-rule") {
		t.Errorf("a new rule of the name holds targets (%v)", err)
	}
}

// An update compares each resource's type and resolved properties with what
// the stack recorded, as CloudFormation "updates resources based on
// differences between what you submit and the stack's current template"
// (Template Reference, UpdateReplacePolicy). An unchanged resource is not
// touched: no call, the same physical id. A change to properties the type
// updates in place ("Update requires: No interruption" or "Some
// interruptions" on its Template Reference page) is made through its
// service's update call (UpdateKeyDescription, PutParameter with Overwrite,
// UpdateAssumeRolePolicy, UpdateFunctionConfiguration and the rest), under
// the same id. A change to one that "Update requires: Replacement" makes a
// new resource and the cleanup deletes the old one; a custom-named resource
// that needs replacing under the same name is CloudFormation's error. Before,
// every update made a new KMS key and REST API and deleted the old ones, and
// failed on a parameter, table, log group, function, role or stream, which
// the engine could only create.
func TestCloudFormationAnUpdateTouchesOnlyWhatChanged(t *testing.T) {
	h := newStacks(t)
	all := nameable()
	small := map[string]map[string]any{"Key": {"Type": "AWS::KMS::Key", "Properties": map[string]any{"Description": "d"}}, "Api": all["Api"],
		"Q": {"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "kq-1"}}}
	h.mustCreate("kq", small)
	first := map[string]string{}
	for _, r := range h.resources("kq") {
		first[r.Logical] = r.Physical
	}
	small["Q"] = map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "kq-2"}}
	tpl, _ := json.Marshal(map[string]any{"Resources": small})
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "kq", "TemplateBody": string(tpl)}); err != nil {
		t.Errorf("renaming the queue beside a key and a REST API: %v", err)
	}
	if out, err := h.service("aws.kms", "DescribeKey", map[string]any{"KeyId": first["Key"]}); err != nil || asMap(out["KeyMetadata"])["KeyState"] != "Enabled" {
		t.Errorf("after renaming the queue, the unchanged key is %v (%v), want Enabled", asMap(out["KeyMetadata"])["KeyState"], err)
	}
	if _, err := h.service("aws.apigateway", "GetRestApi", map[string]any{"restApiId": first["Api"]}); err != nil {
		t.Errorf("after renaming the queue, the unchanged REST API is gone: %v", err)
	}
	keep := map[string]map[string]any{"Key": {"Type": "AWS::KMS::Key", "Properties": map[string]any{"Description": "d"}}}
	for _, logical := range []string{"Api", "Parameter", "Table", "Log", "Function", "Role", "Stream"} {
		keep[logical] = all[logical]
	}
	resources := maps.Clone(keep)
	resources["Q"] = map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "first-q"}}
	h.mustCreate("k", resources)
	ids := func() map[string]string {
		m := map[string]string{}
		for _, r := range h.resources("k") {
			m[r.Logical] = r.Physical
		}
		return m
	}
	update := func(resources map[string]map[string]any) error {
		tpl, _ := json.Marshal(map[string]any{"Resources": resources})
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "k", "TemplateBody": string(tpl)})
		return err
	}
	before := ids()
	resources["Q"] = map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "second-q"}}
	if err := update(resources); err != nil {
		t.Fatalf("an update renaming only the queue: %v", err)
	}
	after := ids()
	for logical := range keep {
		if after[logical] != before[logical] {
			t.Errorf("%s was %s and is %s after an update that did not change it", logical, before[logical], after[logical])
		}
	}
	if out, err := h.service("aws.kms", "DescribeKey", map[string]any{"KeyId": before["Key"]}); err != nil || asMap(out["KeyMetadata"])["KeyState"] != "Enabled" {
		t.Errorf("the unchanged key is %v (%v), want Enabled", asMap(out["KeyMetadata"])["KeyState"], err)
	}
	if _, err := h.service("aws.apigateway", "GetRestApi", map[string]any{"restApiId": before["Api"]}); err != nil {
		t.Errorf("the unchanged REST API is gone: %v", err)
	}
	if _, err := h.service("aws.sqs", "GetQueueUrl", map[string]any{"QueueName": "first-q"}); err == nil || after["Q"] == before["Q"] {
		t.Errorf("the renamed queue was not replaced: %s, then %s", before["Q"], after["Q"])
	}

	resources["Key"] = map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{"Description": "e"}}
	resources["Parameter"] = map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Value": "w"}}
	role := maps.Clone(asMap(all["Role"]["Properties"]))
	role["AssumeRolePolicyDocument"] = map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}}}
	resources["Role"] = map[string]any{"Type": "AWS::IAM::Role", "Properties": role}
	fn := maps.Clone(asMap(all["Function"]["Properties"]))
	fn["Timeout"] = 30
	resources["Function"] = map[string]any{"Type": "AWS::Lambda::Function", "Properties": fn}
	if err := update(resources); err != nil {
		t.Fatalf("an update changing properties that update in place: %v", err)
	}
	inPlace := ids()
	for _, logical := range []string{"Key", "Parameter", "Role", "Function"} {
		if inPlace[logical] != after[logical] {
			t.Errorf("%s was %s and is %s after an in-place change", logical, after[logical], inPlace[logical])
		}
	}
	if out, _ := h.service("aws.kms", "DescribeKey", map[string]any{"KeyId": inPlace["Key"]}); asMap(out["KeyMetadata"])["Description"] != "e" {
		t.Errorf("the key's description is %v, want e", asMap(out["KeyMetadata"])["Description"])
	}
	if out, _ := h.service("aws.ssm", "GetParameter", map[string]any{"Name": inPlace["Parameter"]}); asMap(out["Parameter"])["Value"] != "w" {
		t.Errorf("the parameter holds %v, want w", asMap(out["Parameter"])["Value"])
	}
	if out, _ := h.service("aws.lambda", "GetFunctionConfiguration", map[string]any{"FunctionName": lastColon(inPlace["Function"])}); jsonText(out["Timeout"]) != "30" {
		t.Errorf("the function's Timeout is %v, want 30", out["Timeout"])
	}

	h.mustCreate("fixed", map[string]map[string]any{"R": {"Type": "AWS::IAM::Role", "Properties": map[string]any{"RoleName": "fixed-role", "AssumeRolePolicyDocument": role["AssumeRolePolicyDocument"]}}})
	tpl, _ = json.Marshal(map[string]any{"Resources": map[string]any{"R": map[string]any{"Type": "AWS::IAM::Role", "Properties": map[string]any{"RoleName": "fixed-role", "Path": "/moved/", "AssumeRolePolicyDocument": role["AssumeRolePolicyDocument"]}}}})
	const want = "CloudFormation cannot update a stack when a custom-named resource requires replacing. Rename fixed-role and update the stack again."
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "fixed", "TemplateBody": string(tpl)}); code(err) != "ValidationError" || !strings.Contains(err.Error(), want) {
		t.Errorf("replacing the custom-named role answered %v, want ValidationError: %s", err, want)
	}
	if _, err := h.service("aws.iam", "GetRole", map[string]any{"RoleName": "fixed-role"}); err != nil {
		t.Errorf("the custom-named role is gone: %v", err)
	}
}

// A failed update's rollback puts back exactly the properties the stack had,
// through the same in-place update an update uses, given the previous
// properties: a property the failed update added is taken away again, a
// rule's targets are its previous template's and no others, and a secret
// the failed update gave a description has none again. A resource the update
// did not change is not called. Before, the rollback only provisioned the
// previous properties on top: the rule kept the target the failed update
// added, and the secret its description.
func TestCloudFormationARollbackRestoresExactly(t *testing.T) {
	h := newStacks(t)
	target := func(id string) map[string]any {
		return map[string]any{"Id": id, "Arn": "arn:aws:sqs:us-east-1:000000000000:" + id}
	}
	resources := map[string]map[string]any{
		"R": {"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(1 hour)", "Targets": []any{target("t1")}}},
		"S": {"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "v"}},
	}
	h.mustCreate("exact", resources)
	ids := map[string]string{}
	for _, r := range h.resources("exact") {
		ids[r.Logical] = r.Physical
	}
	resources["R"] = map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(1 hour)", "Targets": []any{target("t1"), target("t2")}}}
	resources["S"] = map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "v", "Description": "added"}}
	resources["Z"] = map[string]any{"Type": "AWS::Nothing::Here"}
	tpl, _ := json.Marshal(map[string]any{"Resources": resources})
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "exact", "TemplateBody": string(tpl)}); err == nil {
		t.Fatal("an update with an unsupported type succeeded")
	}
	if status, reason, _ := h.status("exact"); status != "UPDATE_ROLLBACK_COMPLETE" {
		t.Errorf("exact is %s (%s), want UPDATE_ROLLBACK_COMPLETE", status, reason)
	}
	out, err := h.service("aws.events", "ListTargetsByRule", map[string]any{"Rule": lastSlash(ids["R"])})
	if targets, _ := out["Targets"].([]any); err != nil || len(targets) != 1 || asMap(targets[0])["Id"] != "t1" {
		t.Errorf("after the rollback the rule's targets are %v (%v), want t1 alone", out["Targets"], err)
	}
	if out, err := h.service("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]}); err != nil || out["Description"] != nil {
		t.Errorf("after the rollback the secret's description is %v (%v), want none", out["Description"], err)
	}
}
