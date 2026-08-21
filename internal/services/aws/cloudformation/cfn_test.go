package cloudformation

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
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
	p := New(deps)
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
	p := New(deps)
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "policy", "TemplateBody": template}}); err != nil {
		t.Fatal(err)
	}
	kinesisPack := kinesis.New(deps)
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
	call(url.Values{"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"rem2"}, "TemplateBody": {tpl}})
	call(url.Values{"Action": {"RollbackStack"}, "Version": {"2010-05-15"}, "StackName": {"rem2"}})
}

func TestCloudFormationHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 90 {
		t.Fatalf("cfn Operations() %d want 90", n)
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
	soft := func(vals url.Values) string {
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
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", vals.Get("Action"), res.Header.Get("x-mirror-fidelity"), b)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", vals.Get("Action"), res.StatusCode, b)
		}
		return string(b)
	}
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
	base := url.Values{
		"Version": {"2010-05-15"}, "StackSetName": {"ssboot"}, "StackName": {"s1"},
		"TypeName": {"AWS::S3::Bucket"}, "GeneratedTemplateName": {"gt1"}, "GeneratedTemplateId": {"gt1"},
		"ResourceScanId": {"rs1"}, "StackRefactorId": {"rf1"}, "OperationId": {"op1"},
		"StackPolicyBody": {"{}"}, "TemplateBody": {`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`},
	}
	for _, op := range extraOps() {
		vals := url.Values{}
		for k, v := range base {
			vals[k] = v
		}
		vals.Set("Action", op)
		soft(vals)
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
