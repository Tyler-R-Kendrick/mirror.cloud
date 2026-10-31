package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	// CloudFormation provisions through each resource's owning service.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigateway"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestTemplateURLLoadsFromS3(t *testing.T) {
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	s3Pack := s3.New(deps)
	if _, err := s3Pack.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "templates"}}); err != nil {
		t.Fatal(err)
	}
	template := `{"Resources":{"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"from-url"}}}}`
	if _, err := s3Pack.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "templates", "Key": "stack.json"}, Body: io.NopCloser(strings.NewReader(template))}); err != nil {
		t.Fatal(err)
	}
	p := bundled.Handler("aws.cloudformation", deps)
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "url", "TemplateURL": "s3://templates/stack.json"}}); err != nil {
		t.Fatal(err)
	}
	resources, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListStackResources", Input: map[string]any{"StackName": "url"}})
	if err != nil {
		t.Fatal(err)
	}
	items := resources.Output["StackResourceSummaries"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["PhysicalResourceId"] == "" {
		t.Fatalf("resources = %#v", items)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ValidateTemplate", Input: map[string]any{"TemplateURL": "https://templates.s3.amazonaws.com/missing.json"}}); err == nil {
		t.Fatal("missing TemplateURL object should fail validation")
	}
}

func TestKinesisResourcePolicyLifecycle(t *testing.T) {
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	arn := "arn:aws:kinesis:us-east-1:000000000000:stream/events"
	template := `{"Resources":{"Policy":{"Type":"AWS::Kinesis::ResourcePolicy","Properties":{"ResourceArn":"` + arn + `","ResourcePolicy":{"Version":"2012-10-17","Statement":[]}}}}}`
	p := bundled.Handler("aws.cloudformation", deps)
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "policy", "TemplateBody": template}}); err != nil {
		t.Fatal(err)
	}
	kinesisPack := bundled.Handler("aws.kinesis", deps)
	get := func() string {
		resp, err := kinesisPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetResourcePolicy", Input: map[string]any{"ResourceARN": arn}})
		if err != nil {
			t.Fatal(err)
		}
		value, _ := resp.Output["Policy"].(string)
		return value
	}
	if policy := get(); !strings.Contains(policy, "2012-10-17") {
		t.Fatalf("created policy = %q", policy)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteStack", Input: map[string]any{"StackName": "policy"}}); err != nil {
		t.Fatal(err)
	}
	if policy := get(); policy != "" {
		t.Fatalf("deleted policy = %q", policy)
	}
}

func TestBootedServerCloudFormationCreateStackS3SQS(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudformation", "aws.s3", "aws.sqs"}
	cfg.Seed = "cfn-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	authCFN := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=00"
	authS3 := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	authSQS := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00"
	tpl := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-bucket"}},"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-q"}}},"Outputs":{"Bucket":{"Value":{"Ref":"B"}}}}`
	vals := url.Values{"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"demo"}, "TemplateBody": {tpl}}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", authCFN)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || !strings.Contains(string(b), "StackId") {
		t.Fatalf("create %d %s", res.StatusCode, b)
	}
	if res.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
	}

	desc := url.Values{"Action": {"DescribeStacks"}, "Version": {"2010-05-15"}, "StackName": {"demo"}}
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(desc.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", authCFN)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || !strings.Contains(string(b), "CREATE_COMPLETE") || !strings.Contains(string(b), "cfn-bucket") {
		t.Fatalf("describe %d %s", res.StatusCode, b)
	}

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/cfn-bucket", nil)
	req.Header.Set("Authorization", authS3)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("head bucket %d", res.StatusCode)
	}

	sqs := url.Values{"Action": {"GetQueueUrl"}, "Version": {"2012-11-05"}, "QueueName": {"cfn-q"}}
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(sqs.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", authSQS)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || !strings.Contains(string(b), "cfn-q") {
		t.Fatalf("queue %d %s", res.StatusCode, b)
	}
}

func TestBootedServerCloudFormationYAML(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudformation", "aws.s3"}
	cfg.Seed = "cfn-yaml"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	authCFN := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=00"
	authS3 := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	yml := "AWSTemplateFormatVersion: \"2010-09-09\"\nResources:\n  B:\n    Type: AWS::S3::Bucket\n    Properties:\n      BucketName: yaml-bucket\n"
	vals := url.Values{"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"yml"}, "TemplateBody": {yml}}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", authCFN)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || !strings.Contains(string(b), "StackId") {
		t.Fatalf("yaml create %d %s", res.StatusCode, b)
	}
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/yaml-bucket", nil)
	req.Header.Set("Authorization", authS3)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("yaml bucket %d", res.StatusCode)
	}
}

func TestBootedServerCloudFormationRemainder(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudformation", "aws.s3"}
	cfg.Seed = "cfn-rem"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=00"
	call := func(vals url.Values) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", vals.Get("Action"), res.StatusCode, b)
		}
		return string(b)
	}
	tpl := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-rem-b"}}},"Outputs":{"Bucket":{"Value":{"Ref":"B"}}}}`
	call(url.Values{"Action": {"ValidateTemplate"}, "Version": {"2010-05-15"}, "TemplateBody": {tpl}})
	call(url.Values{"Action": {"GetTemplateSummary"}, "Version": {"2010-05-15"}, "TemplateBody": {tpl}})
	call(url.Values{"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "TemplateBody": {tpl}})
	call(url.Values{"Action": {"DescribeStacks"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	call(url.Values{"Action": {"ListStacks"}, "Version": {"2010-05-15"}})
	call(url.Values{"Action": {"GetTemplate"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	call(url.Values{"Action": {"ListStackResources"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	call(url.Values{"Action": {"DescribeStackEvents"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	call(url.Values{"Action": {"DescribeStackResource"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "LogicalResourceId": {"B"}})
	call(url.Values{"Action": {"ListExports"}, "Version": {"2010-05-15"}})
	call(url.Values{"Action": {"SignalResource"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "LogicalResourceId": {"B"}, "UniqueId": {"1"}, "Status": {"SUCCESS"}})
	tpl2 := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-rem-b2"}}}}`
	call(url.Values{"Action": {"CreateChangeSet"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "ChangeSetName": {"cs1"}, "TemplateBody": {tpl2}})
	call(url.Values{"Action": {"DescribeChangeSet"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "ChangeSetName": {"cs1"}})
	call(url.Values{"Action": {"ListChangeSets"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	call(url.Values{"Action": {"ExecuteChangeSet"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "ChangeSetName": {"cs1"}})
	call(url.Values{"Action": {"DeleteChangeSet"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "ChangeSetName": {"cs1"}})
	call(url.Values{"Action": {"UpdateStack"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "TemplateBody": {tpl2}})
	call(url.Values{"Action": {"UpdateTerminationProtection"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "EnableTerminationProtection": {"true"}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(url.Values{"Action": {"DeleteStack"}, "Version": {"2010-05-15"}, "StackName": {"rem"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode < 300 {
		t.Fatalf("protected delete succeeded %s", b)
	}
	call(url.Values{"Action": {"UpdateTerminationProtection"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "EnableTerminationProtection": {"false"}})
	call(url.Values{"Action": {"DeleteStack"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	ss := call(url.Values{"Action": {"CreateStackSet"}, "Version": {"2010-05-15"}, "StackSetName": {"ss1"}})
	if !strings.Contains(ss, "StackSetId") {
		t.Fatalf("stackset %s", ss)
	}
	call(url.Values{"Action": {"DescribeStackSet"}, "Version": {"2010-05-15"}, "StackSetName": {"ss1"}})
	call(url.Values{"Action": {"ListStackSets"}, "Version": {"2010-05-15"}})
	call(url.Values{"Action": {"SetStackPolicy"}, "Version": {"2010-05-15"}, "StackName": {"rem"}, "StackPolicyBody": {"{}"}})
	call(url.Values{"Action": {"GetStackPolicy"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	call(url.Values{"Action": {"DetectStackDrift"}, "Version": {"2010-05-15"}, "StackName": {"rem"}})
	// The change set replaced cfn-rem-b with cfn-rem-b2 and its cleanup deleted
	// cfn-rem-b, so rem2 can take the name again.
	call(url.Values{"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"rem2"}, "TemplateBody": {tpl}})
	// RollbackStack rolls back "from CREATE_FAILED or UPDATE_FAILED stack
	// statuses"; rem2 is CREATE_COMPLETE, so it is refused.
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(url.Values{"Action": {"RollbackStack"}, "Version": {"2010-05-15"}, "StackName": {"rem2"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", auth)
	if res, err = http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 400 || !strings.Contains(string(b), "cannot be called from current stack status") {
		t.Fatalf("RollbackStack of a CREATE_COMPLETE stack answered %d %s", res.StatusCode, b)
	}
}

func TestBootedServerCloudFormationExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudformation"}
	cfg.Seed = "cfn-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=00"
	hard := func(vals url.Values) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s %d %s %s", vals.Get("Action"), res.StatusCode, res.Header.Get("x-mirror-fidelity"), b)
		}
		return string(b)
	}
	created := hard(url.Values{"Action": {"CreateStackSet"}, "Version": {"2010-05-15"}, "StackSetName": {"ssboot"}})
	if !strings.Contains(created, "ssboot") && !strings.Contains(created, "StackSetId") {
		t.Fatalf("create stackset %s", created)
	}
	got := hard(url.Values{"Action": {"DescribeStackSet"}, "Version": {"2010-05-15"}, "StackSetName": {"ssboot"}})
	if !strings.Contains(got, "ssboot") {
		t.Fatalf("describe stackset %s", got)
	}
	hard(url.Values{"Action": {"DeleteStackSet"}, "Version": {"2010-05-15"}, "StackSetName": {"ssboot"}})
	listed := hard(url.Values{"Action": {"ListStackSets"}, "Version": {"2010-05-15"}})
	if strings.Contains(listed, "<StackSetName>ssboot</StackSetName>") {
		t.Fatalf("stackset still present %s", listed)
	}
}

func TestParseYAMLRefGetAtt(t *testing.T) {
	m, err := parseYAML("Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: !Ref Name\n")
	if err != nil {
		t.Fatal(err)
	}
	res := m["Resources"].(map[string]any)
	q := res["Q"].(map[string]any)
	props := q["Properties"].(map[string]any)
	ref := props["QueueName"].(map[string]any)
	if str(ref["Ref"]) != "Name" {
		t.Fatalf("%v", m)
	}
}

func TestCloudFormationProvisionedResourceLifecycle(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.cloudformation", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	resources := map[string]any{
		"Api": map[string]any{"Type": "AWS::ApiGateway::RestApi", "Properties": map[string]any{}},
		"Bucket": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{
			"VersioningConfiguration": map[string]any{"Status": "Enabled"}, "Tags": []any{map[string]any{"Key": "env", "Value": "test"}},
			"PublicAccessBlockConfiguration": map[string]any{"BlockPublicAcls": true},
			"OwnershipControls":              map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "BucketOwnerEnforced"}}},
		}},
		"Function": map[string]any{"Type": "AWS::Lambda::Function", "Properties": map[string]any{
			"Runtime": "provided.al2", "Handler": "bootstrap", "Role": "arn:aws:iam::000000000000:role/fn", "Code": map[string]any{"ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="},
		}},
		"Key":       map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{}},
		"Log":       map[string]any{"Type": "AWS::Logs::LogGroup", "Properties": map[string]any{}},
		"Parameter": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Value": "value"}},
		"Policy": map[string]any{"Type": "AWS::Kinesis::ResourcePolicy", "Properties": map[string]any{
			"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/events", "Policy": `{"Version":"2012-10-17"}`,
		}},
		"Queue":  map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
		"Role":   map[string]any{"Type": "AWS::IAM::Role", "Properties": map[string]any{"AssumeRolePolicyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{}}}},
		"Rule":   map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(1 hour)"}},
		"Secret": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": "value"}},
		"Stream": map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": map[string]any{}},
		"Table": map[string]any{"Type": "AWS::DynamoDB::Table", "Properties": map[string]any{
			"KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "AttributeDefinitions": []any{map[string]any{"AttributeName": "id", "AttributeType": "S"}}, "BillingMode": "PAY_PER_REQUEST",
		}},
		"Topic": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{}},
	}
	template, _ := json.Marshal(map[string]any{"Resources": resources, "Outputs": map[string]any{
		"BucketArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": "Bucket.Arn"}},
		"QueueArn":  map[string]any{"Value": map[string]any{"Fn::GetAtt": []any{"Queue", "Arn"}}},
		"QueueName": map[string]any{"Value": map[string]any{"Fn::GetAtt": []any{"Queue", "QueueName"}}},
		"Joined":    map[string]any{"Value": map[string]any{"Fn::Join": []any{"/", []any{map[string]any{"Ref": "AWS::Region"}, map[string]any{"Ref": "Bucket"}}}}},
		"Subbed":    map[string]any{"Value": map[string]any{"Fn::Sub": "${AWS::StackName}-${Bucket}"}},
	}})
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, HTTP: &http.Request{Host: "localhost:4566"}, Operation: "CreateStack", Input: map[string]any{
		"StackName": "all", "TemplateBody": string(template),
	}})
	if err != nil || created.Output["StackId"] == nil {
		t.Fatalf("create all resources: %#v %v", created, err)
	}
	described, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DescribeStacks", Input: map[string]any{"StackName": "all"}})
	if err != nil {
		t.Fatal(err)
	}
	stacks := described.Output["Stacks"].([]any)
	if len(stacks) != 1 || len(stacks[0].(map[string]any)["Outputs"].([]any)) != 5 {
		t.Fatalf("stack outputs %#v", stacks)
	}
	stackResources, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DescribeStackResources", Input: map[string]any{"StackName": "all"}})
	if err != nil || len(stackResources.Output["StackResources"].([]any)) != len(resources) {
		t.Fatalf("DescribeStackResources %#v %v", stackResources, err)
	}
	physical := map[string]string{} // unnamed, each is <stack>-<logical id>-<suffix>
	for _, raw := range stackResources.Output["StackResources"].([]any) {
		if r := raw.(map[string]any); r["PhysicalResourceId"] == "" || r["ResourceType"] == "" {
			t.Fatalf("stack resource %#v", r)
		}
		physical[str(raw.(map[string]any)["LogicalResourceId"])] = str(raw.(map[string]any)["PhysicalResourceId"])
	}
	listed, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListStackResources", Input: map[string]any{"StackName": "all"}})
	if err != nil || len(listed.Output["StackResourceSummaries"].([]any)) != len(resources) {
		t.Fatalf("resources %#v %v", listed, err)
	}
	// Each resource exists in its owning service, as that service stores it.
	owned := func(svc, op string, in map[string]any) error {
		_, err := New(deps).call(ctx, &spi.Request{Identity: id}, svc, op, in)
		return err
	}
	versioning, err := New(deps).call(ctx, &spi.Request{Identity: id}, "aws.s3", "GetBucketVersioning", map[string]any{"Bucket": physical["Bucket"]})
	if err != nil || versioning["Status"] != "Enabled" {
		t.Fatalf("bucket versioning %#v %v", versioning, err)
	}
	for _, check := range []struct {
		svc, op string
		in      map[string]any
	}{
		{"aws.iam", "GetRole", map[string]any{"RoleName": lastSlash(physical["Role"])}},
		{"aws.sqs", "GetQueueUrl", map[string]any{"QueueName": lastSlash(physical["Queue"])}},
		{"aws.dynamodb", "DescribeTable", map[string]any{"TableName": physical["Table"]}},
		{"aws.logs", "DescribeLogGroups", map[string]any{"logGroupNamePrefix": physical["Log"]}},
	} {
		if err := owned(check.svc, check.op, check.in); err != nil {
			t.Errorf("%s %s after CreateStack: %v", check.svc, check.op, err)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteStack", Input: map[string]any{"StackName": "all"}}); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		svc, op string
		in      map[string]any
	}{
		{"aws.iam", "GetRole", map[string]any{"RoleName": lastSlash(physical["Role"])}},
		{"aws.sqs", "GetQueueUrl", map[string]any{"QueueName": lastSlash(physical["Queue"])}},
		{"aws.dynamodb", "DescribeTable", map[string]any{"TableName": physical["Table"]}},
	} {
		if err := owned(check.svc, check.op, check.in); err == nil {
			t.Errorf("%s %s still answers after DeleteStack", check.svc, check.op)
		}
	}
	unsupported, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Nope": map[string]any{"Type": "AWS::Nope::Thing"}}})
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "bad", "TemplateBody": string(unsupported)}}); err == nil {
		t.Fatal("unsupported resource type succeeded")
	}
}

func TestCloudFormationIntrinsicAndYAMLUnits(t *testing.T) {
	p := New(spitest.Deps(t))
	req := &spi.Request{Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"}}
	refs := map[string]string{"Bucket": "bucket", "Queue": "http://localhost/000000000000/queue"}
	if p.ref("AWS::AccountId", refs, req, "stack") != "000000000000" || p.ref("AWS::Partition", refs, req, "stack") != "aws" || p.ref("AWS::URLSuffix", refs, req, "stack") != "amazonaws.com" || p.ref("missing", refs, req, "stack") != "missing" {
		t.Fatal("pseudo-parameter references")
	}
	if p.getAtt("Bucket.Arn", refs, req) != "arn:aws:s3:::bucket" || p.getAtt([]any{"Queue", "Arn"}, refs, req) != "arn:aws:sqs:us-east-1:000000000000:queue" || p.getAtt([]any{"Queue", "QueueName"}, refs, req) != "queue" {
		t.Fatal("GetAtt resolution")
	}
	if got := p.fnJoin([]any{"-", []any{"a", map[string]any{"Ref": "AWS::Region"}}}, refs, req, "stack"); got != "a-us-east-1" {
		t.Fatalf("join %q", got)
	}
	if got := p.fnSub([]any{"${AWS::StackName}-${Bucket}"}, refs, req, "stack"); got != "stack-bucket" {
		t.Fatalf("sub %q", got)
	}
	if got := p.resolve([]any{map[string]any{"Ref": "Bucket"}}, refs, req, "stack").([]any); len(got) != 1 || got[0] != "bucket" {
		t.Fatalf("nested resolution %#v", got)
	}
	if got := p.fnJoin([]any{"only"}, refs, req, "stack"); got != "" {
		t.Fatalf("short join %q", got)
	}
	parsed, err := parseYAML("# comment\nValues:\n  - one\n  - Key: two\n    Enabled: True\n  -\n    nested: 3.5\nEmpty:\n  -\nNull: ~\nQuoted: 'value'\n")
	if err != nil {
		t.Fatal(err)
	}
	values := parsed["Values"].([]any)
	if len(values) != 3 || values[0] != "one" || values[1].(map[string]any)["Enabled"] != true {
		t.Fatalf("YAML values %#v", parsed)
	}
	for _, body := range []string{"", "{", "- item", "Key: value\n  Too: deep", "Key\n"} {
		if _, err := parseTemplate(body); err == nil {
			t.Fatalf("invalid template accepted %q", body)
		}
	}
	params := formParams(map[string]any{
		"Parameters.member.1.ParameterKey": "Explicit", "Parameters.member.1.ParameterValue": "set", "ignored": "value",
	})
	tpl := map[string]any{"Parameters": map[string]any{
		"Defaulted": map[string]any{"Default": "fallback"}, "Explicit": map[string]any{"Default": "wrong"}, "Required": map[string]any{},
	}}
	mergeParamDefaults(tpl, params)
	if params["Explicit"] != "set" || params["Defaulted"] != "fallback" || len(paramDecls(tpl)) != 3 {
		t.Fatalf("parameters %#v %#v", params, tpl)
	}
	if lastSlash("plain") != "plain" || lastColon("plain") != "plain" {
		t.Fatal("scalar helpers")
	}
}

// An UpdateStack that changes an AWS::SecretsManager::Secret's value and
// description updates the secret it made: UpdateSecret, which carries both,
// rather than CreateSecret again on a name that exists. Secrets Manager answers
// a second CreateSecret of one name with ResourceExistsException ("A resource
// with the ID you requested already exists").
func TestCloudFormationUpdateStackUpdatesItsSecret(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.cloudformation", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	template := func(value, description string) string {
		raw, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Secret": map[string]any{
			"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "app", "SecretString": value, "Description": description},
		}}})
		return string(raw)
	}
	physical := func() any {
		t.Helper()
		out, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DescribeStackResources", Input: map[string]any{"StackName": "app"}})
		if err != nil {
			t.Fatal(err)
		}
		return out.Output["StackResources"].([]any)[0].(map[string]any)["PhysicalResourceId"]
	}
	secrets := func(op string, in map[string]any) map[string]any {
		t.Helper()
		out, err := New(deps).call(ctx, &spi.Request{Identity: id}, "aws.secretsmanager", op, in)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return out
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "app", "TemplateBody": template("v1", "first")}}); err != nil {
		t.Fatal(err)
	}
	if got := secrets("DescribeSecret", map[string]any{"SecretId": "app"}); got["Description"] != "first" {
		t.Fatalf("after CreateStack, Description = %#v", got["Description"])
	}
	before := physical()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "UpdateStack", Input: map[string]any{"StackName": "app", "TemplateBody": template("v2", "second")}}); err != nil {
		t.Fatalf("UpdateStack: %v", err)
	}
	if after := physical(); after != before {
		t.Fatalf("UpdateStack replaced the secret: %v, then %v", before, after)
	}
	if got := secrets("GetSecretValue", map[string]any{"SecretId": "app"}); got["SecretString"] != "v2" {
		t.Fatalf("after UpdateStack, the value is %#v", got["SecretString"])
	}
	if got := secrets("DescribeSecret", map[string]any{"SecretId": "app"}); got["Description"] != "second" {
		t.Fatalf("after UpdateStack, Description = %#v", got["Description"])
	}
	if versions, _ := secrets("ListSecretVersionIds", map[string]any{"SecretId": "app"})["Versions"].([]any); len(versions) != 2 {
		t.Fatalf("after UpdateStack, %d versions, want 2", len(versions))
	}
}

// A template's AWS::SecretsManager::Secret without a SecretString makes a
// secret with no value. CreateSecret makes a version only "If you include
// SecretString or SecretBinary". CloudFormation sent "" in its place, which
// stored a version holding the empty string; the real service refuses that
// value outright (SecretStringType has a minimum length of 1).
func TestCloudFormationSecretWithoutAValue(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.cloudformation", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	template := `{"Resources":{"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"bare","Description":"value set elsewhere"}}}}`
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "bare", "TemplateBody": template}}); err != nil {
		t.Fatalf("CreateStack: %v", err)
	}
	secrets := New(deps)
	described, err := secrets.call(ctx, &spi.Request{Identity: id}, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": "bare"})
	if err != nil || described["Description"] != "value set elsewhere" {
		t.Fatalf("DescribeSecret: %#v %v", described, err)
	}
	_, err = secrets.call(ctx, &spi.Request{Identity: id}, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": "bare"})
	if f, isFault := err.(*spi.Fault); !isFault || f.Code != "ResourceNotFoundException" {
		t.Fatalf("GetSecretValue of a secret the template gave no value: want ResourceNotFoundException, got %v", err)
	}
}

// A secret a template does not name is named for its stack, in the modelled
// form <stack>-<logical id>-<suffix>, the suffix a hash of the stack id and
// logical id, so an update finds the same name. Two stacks from one template each get their own
// secret, which a second CreateSecret of one bare logical id refused.
func TestCloudFormationNamesAnUnnamedSecretForItsStack(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.cloudformation", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	template := func(value string) string {
		return `{"Resources":{"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"SecretString":"` + value + `"}}}}`
	}
	physical := func(stack string) string {
		t.Helper()
		out, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DescribeStackResources", Input: map[string]any{"StackName": stack}})
		if err != nil {
			t.Fatal(err)
		}
		return str(out.Output["StackResources"].([]any)[0].(map[string]any)["PhysicalResourceId"])
	}
	value := func(secretID string) any {
		t.Helper()
		out, err := New(deps).call(ctx, &spi.Request{Identity: id}, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": secretID})
		if err != nil {
			t.Fatalf("GetSecretValue %s: %v", secretID, err)
		}
		return out["SecretString"]
	}
	for _, stack := range []string{"dev", "staging"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": stack, "TemplateBody": template(stack)}}); err != nil {
			t.Fatalf("CreateStack %s: %v", stack, err)
		}
	}
	dev, staging := physical("dev"), physical("staging")
	if dev == staging || !strings.Contains(dev, ":secret:dev-Secret-") || !strings.Contains(staging, ":secret:staging-Secret-") {
		t.Fatalf("the two stacks' secrets are %q and %q, want one each named <stack>-Secret-<suffix>", dev, staging)
	}
	if value(dev) != "dev" || value(staging) != "staging" {
		t.Fatalf("the two secrets hold %v and %v", value(dev), value(staging))
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "UpdateStack", Input: map[string]any{"StackName": "dev", "TemplateBody": template("dev2")}}); err != nil {
		t.Fatalf("UpdateStack: %v", err)
	}
	if after := physical("dev"); after != dev || value(dev) != "dev2" {
		t.Fatalf("after UpdateStack the secret is %q holding %v, want %q holding dev2", after, value(after), dev)
	}
}

// A CreateStack that fails rolls back what it had created, as CloudFormation
// does by default (OnFailure ROLLBACK). Rolled-back resources are deleted the
// way DeleteStack deletes them -- a secret without a recovery window -- so a
// later stack can take their names; the stack itself stays, ROLLBACK_COMPLETE,
// until DeleteStack removes it.
func TestCloudFormationAFailedCreateRollsBack(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.cloudformation", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	failing := `{"Resources":{
		"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"rq"}},
		"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"retry","SecretString":"v"}},
		"Thing":{"Type":"AWS::Nope::Thing"}}}`
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "r", "TemplateBody": failing}}); err == nil {
		t.Fatal("a template with an unsupported resource type was created")
	}
	owned := func(svc, op string, in map[string]any) error {
		_, err := New(deps).call(ctx, &spi.Request{Identity: id}, svc, op, in)
		return err
	}
	if err := owned("aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": "retry"}); err == nil {
		t.Error("the failed stack's secret is still there")
	}
	if err := owned("aws.sqs", "GetQueueUrl", map[string]any{"QueueName": "rq"}); err == nil {
		t.Error("the failed stack's queue is still there")
	}
	// The rolled-back stack keeps its name until DeleteStack removes it; a
	// stack of another name can take the names its resources had.
	retry := `{"Resources":{"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"retry","SecretString":"v"}}}}`
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "r", "TemplateBody": retry}}); err == nil {
		t.Fatal("re-creating r before DeleteStack removed the rolled-back stack succeeded")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "r2", "TemplateBody": retry}}); err != nil {
		t.Fatalf("creating r2 after the rollback: %v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteStack", Input: map[string]any{"StackName": "r"}}); err != nil {
		t.Fatalf("DeleteStack of the rolled-back r: %v", err)
	}
	unnamed := `{"Resources":{"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"SecretString":"v"}}}}`
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "r", "TemplateBody": unnamed}}); err != nil {
		t.Fatalf("re-creating r after DeleteStack: %v", err)
	}
}

// GenerateSecretString makes the secret's value as GetRandomPassword makes a
// password -- PasswordLength (32 by default), ExcludeCharacters, the Exclude*
// classes, IncludeSpace, RequireEachIncludedType -- seeded by the stack and
// logical id. With SecretStringTemplate and GenerateStringKey, the password
// goes into the template's JSON under that key. A template that also gives
// SecretString is refused.
func TestCloudFormationGeneratesASecretString(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.cloudformation", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	stacks := 0
	generate := func(spec map[string]any) (string, error) {
		t.Helper()
		stacks++
		name := fmt.Sprintf("gen%d", stacks)
		tpl, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Db": map[string]any{
			"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": name, "GenerateSecretString": spec},
		}}})
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": name, "TemplateBody": string(tpl)}}); err != nil {
			return "", err
		}
		out, err := New(deps).call(ctx, &spi.Request{Identity: id}, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": name})
		if err != nil {
			t.Fatalf("GetSecretValue %s: %v", name, err)
		}
		return str(out["SecretString"]), nil
	}
	must := func(spec map[string]any) string {
		t.Helper()
		v, err := generate(spec)
		if err != nil {
			t.Fatalf("CreateStack with %v: %v", spec, err)
		}
		return v
	}
	const lower, upper, digits, punctuation = "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789", "!\"#$%&'()*+,-./:;?@[\\]^_`{|}~"

	if v := must(map[string]any{}); len(v) != 32 {
		t.Errorf("the default generated value %q has %d characters, want 32", v, len(v))
	}
	if v := must(map[string]any{"PasswordLength": 16}); len(v) != 16 {
		t.Errorf("PasswordLength 16 generated %q", v)
	}
	if v := must(map[string]any{"PasswordLength": 4}); !strings.ContainsAny(v, lower) || !strings.ContainsAny(v, upper) ||
		!strings.ContainsAny(v, digits) || !strings.ContainsAny(v, punctuation) {
		t.Errorf("RequireEachIncludedType, the default, generated %q without one of each type", v)
	}
	if v := must(map[string]any{"PasswordLength": 200, "ExcludeCharacters": "abcXYZ019!@"}); strings.ContainsAny(v, "abcXYZ019!@") {
		t.Errorf("ExcludeCharacters left one in %q", v)
	}
	for member, set := range map[string]string{"ExcludeLowercase": lower, "ExcludeUppercase": upper, "ExcludeNumbers": digits, "ExcludePunctuation": punctuation} {
		if v := must(map[string]any{"PasswordLength": 200, member: true}); strings.ContainsAny(v, set) {
			t.Errorf("%s generated %q", member, v)
		}
	}
	if v := must(map[string]any{"PasswordLength": 4096, "IncludeSpace": true}); !strings.Contains(v, " ") {
		t.Errorf("IncludeSpace drew no space in 4096 characters") // may, not must: a space is never required
	}
	var doc map[string]any
	v := must(map[string]any{"SecretStringTemplate": `{"username":"admin"}`, "GenerateStringKey": "password", "PasswordLength": 20})
	if err := json.Unmarshal([]byte(v), &doc); err != nil || doc["username"] != "admin" || len(str(doc["password"])) != 20 {
		t.Errorf("the template form generated %q, want the template with a 20-character password under password", v)
	}

	both, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Db": map[string]any{
		"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "both", "SecretString": "v", "GenerateSecretString": map[string]any{}},
	}}})
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "both", "TemplateBody": string(both)}}); err == nil {
		t.Error("a secret with both SecretString and GenerateSecretString was created")
	}
	if _, err := generate(map[string]any{"GenerateStringKey": "password"}); err == nil {
		t.Error("GenerateStringKey without SecretStringTemplate was accepted")
	}
}

// An UpdateStack that fails rolls back as CloudFormation's UPDATE_ROLLBACK
// does: what it created is deleted, what it changed is put back from the
// previous template, and the stack keeps its previous template and resources,
// as UPDATE_ROLLBACK_COMPLETE. Here the update changes a secret's value, adds
// a good secret and a resource of an unsupported type; afterwards the stack
// and its resources are exactly as they were before the update.
func TestCloudFormationAFailedUpdateRollsBack(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.cloudformation", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	cfn := func(op string, in map[string]any) (map[string]any, error) {
		out, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			return nil, err
		}
		return out.Output, nil
	}
	secrets := func(op string, in map[string]any) (map[string]any, error) {
		return New(deps).call(ctx, &spi.Request{Identity: id}, "aws.secretsmanager", op, in)
	}
	before := `{"Resources":{"A":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"a","SecretString":"v1","Description":"first"}}}}`
	failing := `{"Resources":{
		"A":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"a","SecretString":"v2","Description":"second"}},
		"B":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"b","SecretString":"new"}},
		"Z":{"Type":"AWS::Nope::Thing"}}}`
	if _, err := cfn("CreateStack", map[string]any{"StackName": "u", "TemplateBody": before}); err != nil {
		t.Fatal(err)
	}
	snapshot := func() map[string]any {
		t.Helper()
		stack, err := cfn("DescribeStackResources", map[string]any{"StackName": "u"})
		if err != nil {
			t.Fatal(err)
		}
		template, err := cfn("GetTemplate", map[string]any{"StackName": "u"})
		if err != nil {
			t.Fatal(err)
		}
		value, err := secrets("GetSecretValue", map[string]any{"SecretId": "a"})
		if err != nil {
			t.Fatal(err)
		}
		described, err := secrets("DescribeSecret", map[string]any{"SecretId": "a"})
		if err != nil {
			t.Fatal(err)
		}
		return map[string]any{"resources": stack["StackResources"], "template": template["TemplateBody"],
			"value": value["SecretString"], "description": described["Description"]}
	}
	was := snapshot()
	if _, err := cfn("UpdateStack", map[string]any{"StackName": "u", "TemplateBody": failing}); err == nil {
		t.Fatal("an update with an unsupported resource type succeeded")
	}
	if now := snapshot(); !reflect.DeepEqual(now, was) {
		t.Errorf("after the failed update the stack is\n%#v\nwant\n%#v", now, was)
	}
	if _, err := secrets("DescribeSecret", map[string]any{"SecretId": "b"}); err == nil {
		t.Error("the secret the failed update created is still there")
	}
	stacks, err := cfn("DescribeStacks", map[string]any{"StackName": "u"})
	if err != nil {
		t.Fatal(err)
	}
	if status := stacks["Stacks"].([]any)[0].(map[string]any)["StackStatus"]; status != "UPDATE_ROLLBACK_COMPLETE" {
		t.Errorf("the stack's status is %v, want UPDATE_ROLLBACK_COMPLETE", status)
	}
}

// A YAML template is YAML: flow collections, block scalars and the short-form
// intrinsics (!Ref, !GetAtt, !Sub, and !Name for any other Fn::Name) all read
// as their JSON-template equivalents. The hand-written subset this replaced
// read a flow sequence as a string and refused a block scalar.
func TestParseYAMLReadsYAML(t *testing.T) {
	m, err := parseYAML(`Resources:
  Q:
    Type: AWS::SQS::Queue
    Properties:
      Tags: [{Key: env, Value: dev}, {Key: team, Value: "a: b"}]
      Policy: |
        line one
        line two
      Arn: !GetAtt [Other, Arn]
      Joined: !Join ["-", [a, !Ref B]]
      Port: 8080
      Ratio: 0.5
      On: true
`)
	if err != nil {
		t.Fatal(err)
	}
	props := m["Resources"].(map[string]any)["Q"].(map[string]any)["Properties"].(map[string]any)
	want := map[string]any{
		"Tags":   []any{map[string]any{"Key": "env", "Value": "dev"}, map[string]any{"Key": "team", "Value": "a: b"}},
		"Policy": "line one\nline two\n",
		"Arn":    map[string]any{"Fn::GetAtt": []any{"Other", "Arn"}},
		"Joined": map[string]any{"Fn::Join": []any{"-", []any{"a", map[string]any{"Ref": "B"}}}},
		"Port":   8080, "Ratio": 0.5, "On": true,
	}
	if !reflect.DeepEqual(props, want) {
		t.Fatalf("parsed %#v\nwant   %#v", props, want)
	}
}
