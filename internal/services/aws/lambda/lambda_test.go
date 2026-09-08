package lambda

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestInvokeEventAndDryRunStatus(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "async", "Runtime": "unsupported", "Handler": "handler",
	}})
	if err != nil {
		t.Fatal(err)
	}
	for kind, status := range map[string]int{"DryRun": http.StatusNoContent, "Event": http.StatusAccepted} {
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Invoke", Input: map[string]any{"FunctionName": "async", "InvocationType": kind}})
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if resp.Status != status || resp.Output["StatusCode"] != status {
			t.Fatalf("%s response %#v", kind, resp)
		}
	}
}

func TestInvokeAcceptsRawArrayPayload(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	src := "def lambda_handler(event, context):\n    return {'length': len(event)}\n"
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "batch", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(src))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{
		Identity: id, Operation: "Invoke", Input: map[string]any{"FunctionName": "batch"},
		Body: io.NopCloser(bytes.NewBufferString(`[{"id":1},{"id":2}]`)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(response.Output["Payload"].(json.RawMessage)), `"length": 2`) {
		t.Fatalf("payload %s", response.Output["Payload"])
	}
}

func TestInvokeReceivesFunctionEnvironment(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	src := "import os\ndef lambda_handler(event, context):\n    return {'name': os.environ['AWS_LAMBDA_FUNCTION_NAME'], 'bucket': os.environ['BUCKET_NAME']}\n"
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "s3-reader", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code":        map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(src))},
		"Environment": map[string]any{"Variables": map[string]any{"BUCKET_NAME": "objects"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Invoke", Input: map[string]any{"FunctionName": "s3-reader"}})
	if err != nil {
		t.Fatal(err)
	}
	payload := string(response.Output["Payload"].(json.RawMessage))
	if !strings.Contains(payload, `"name": "s3-reader"`) || !strings.Contains(payload, `"bucket": "objects"`) {
		t.Fatalf("payload %s", payload)
	}
}

func TestSQSEventSourceMappingInvokesAndDeletes(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	deps := spitest.Deps(t)
	identity := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	queue := sqs.New(deps)
	function := New(deps)
	ctx := context.Background()
	if _, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateQueue", Input: map[string]any{"QueueName": "source"}}); err != nil {
		t.Fatal(err)
	}
	sourceARN := "arn:aws:sqs:us-east-1:123456789012:source"
	code := "def lambda_handler(event, context):\n    record = event['Records'][0]\n    if record['eventSource'] != 'aws:sqs' or record['eventSourceARN'] != 'arn:aws:sqs:us-east-1:123456789012:source':\n        raise RuntimeError('bad source event')\n    return {'count': len(event['Records'])}\n"
	if _, err := function.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "consumer", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := function.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateEventSourceMapping", Input: map[string]any{
		"FunctionName": "consumer", "EventSourceArn": sourceARN,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "SendMessage", Input: map[string]any{"QueueName": "source", "MessageBody": "one"}}); err != nil {
		t.Fatal(err)
	}
	got, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "source", "VisibilityTimeout": 0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(anySlice(got.Output["Messages"])) != 0 {
		t.Fatalf("event source mapping left messages: %#v", got.Output)
	}
	_ = function.Close()
}

func TestSQSEventSourceMappingPreservesPartialFailures(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	deps := spitest.Deps(t)
	identity := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	queue := sqs.New(deps)
	function := New(deps)
	ctx := context.Background()
	if _, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateQueue", Input: map[string]any{"QueueName": "partial"}}); err != nil {
		t.Fatal(err)
	}
	code := "def lambda_handler(event, context):\n    return {'batchItemFailures': [{'itemIdentifier': event['Records'][0]['messageId']}] }\n"
	if _, err := function.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "partial", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := function.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateEventSourceMapping", Input: map[string]any{
		"FunctionName": "partial", "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:partial",
	}}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"one", "two"} {
		if _, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "SendMessage", Input: map[string]any{"QueueName": "partial", "MessageBody": body}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "partial", "VisibilityTimeout": 0, "MaxNumberOfMessages": 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(anySlice(got.Output["Messages"])) != 1 {
		t.Fatalf("partial failure deletion %#v", got.Output)
	}
	_ = function.Close()
}

func TestSQSEventSourceMappingRedrivesFailedMessage(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	deps := spitest.Deps(t)
	identity := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	queue := sqs.New(deps)
	function := New(deps)
	ctx := context.Background()
	create := func(name string, attrs map[string]any) {
		t.Helper()
		if _, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateQueue", Input: map[string]any{"QueueName": name, "Attributes": attrs}}); err != nil {
			t.Fatal(err)
		}
	}
	create("dead", nil)
	create("source", map[string]any{"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dead","maxReceiveCount":"1"}`})
	code := "def lambda_handler(event, context):\n    raise RuntimeError('failed')\n"
	if _, err := function.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "failing", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := function.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateEventSourceMapping", Input: map[string]any{
		"FunctionName": "failing", "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:source",
	}}); err != nil {
		t.Fatal(err)
	}
	sent, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "SendMessage", Input: map[string]any{"QueueName": "source", "MessageBody": "preserve-id"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := queue.Invoke(ctx, &spi.Request{Identity: identity, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "dead", "VisibilityTimeout": 0}})
	if err != nil {
		t.Fatal(err)
	}
	messages := anySlice(got.Output["Messages"])
	if len(messages) != 1 {
		t.Fatalf("dead-letter message %#v sent=%#v", got.Output, sent.Output)
	}
	message, _ := messages[0].(map[string]any)
	if stringValue(message["MessageId"]) != stringValue(sent.Output["MessageId"]) {
		t.Fatalf("dead-letter id %#v sent=%#v", message, sent.Output)
	}
	_ = function.Close()
}

func TestBootedServerLambdaPythonInvoke(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	cfg := config.Default()
	cfg.Services = []string{"aws.lambda"}
	cfg.Seed = "lam-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	src := "def lambda_handler(event, context):\n    return {\"echo\": event.get(\"n\", 0)}\n"
	create := `{"FunctionName":"echo","Runtime":"python3.12","Handler":"lambda_function.lambda_handler","Code":{"ZipFile":"` + base64.StdEncoding.EncodeToString([]byte(src)) + `"}}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/2015-03-31/functions", strings.NewReader(create))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=00")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("create %d %s", res.StatusCode, b)
	}
	if res.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
	}

	inv, _ := http.NewRequest(http.MethodPost, ts.URL+"/2015-03-31/functions/echo/invocations", strings.NewReader(`{"n":7}`))
	inv.Header.Set("Content-Type", "application/json")
	inv.Header.Set("Authorization", req.Header.Get("Authorization"))
	res, err = http.DefaultClient.Do(inv)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("invoke %d %s", res.StatusCode, out)
	}
	if !strings.Contains(string(out), `"echo"`) || !strings.Contains(string(out), "7") {
		t.Fatalf("payload %s", out)
	}
}

func TestBootedServerLambdaRemainder(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.lambda"}
	cfg.Seed = "lam-2"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	src := "def lambda_handler(event, context):\n    return {}\n"
	create := `{"FunctionName":"echo","Runtime":"python3.12","Handler":"lambda_function.lambda_handler","Code":{"ZipFile":"` + base64.StdEncoding.EncodeToString([]byte(src)) + `"}}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/2015-03-31/functions", strings.NewReader(create))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=00")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("create %d %s", res.StatusCode, b)
	}
	auth := req.Header.Get("Authorization")
	do := func(method, path, body string, want ...string) string {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		r, _ := http.NewRequest(method, ts.URL+path, rdr)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s %d %s", method, path, res.StatusCode, b)
		}
		s := string(b)
		for _, w := range want {
			if !strings.Contains(s, w) {
				t.Fatalf("%s %s missing %q in %s", method, path, w, s)
			}
		}
		return s
	}
	do(http.MethodPut, "/2015-03-31/functions/echo/code", `{"ZipFile":"`+base64.StdEncoding.EncodeToString([]byte(src))+`"}`)
	do(http.MethodPut, "/2015-03-31/functions/echo/configuration", `{"Timeout":10}`, "echo")
	do(http.MethodGet, "/2015-03-31/functions/echo/configuration", "", "echo")
	do(http.MethodPost, "/2015-03-31/functions/echo/versions", `{}`, "Version")
	do(http.MethodGet, "/2015-03-31/functions/echo/versions", "", "Versions")
	do(http.MethodPost, "/2015-03-31/functions/echo/aliases", `{"Name":"live","FunctionVersion":"1"}`, "live")
	do(http.MethodGet, "/2015-03-31/functions/echo/aliases/live", "", "live")
	do(http.MethodPut, "/2015-03-31/functions/echo/aliases/live", `{"FunctionVersion":"$LATEST"}`)
	do(http.MethodGet, "/2015-03-31/functions/echo/aliases", "", "live")
	do(http.MethodPost, "/2015-03-31/functions/echo/policy", `{"StatementId":"s1","Action":"lambda:InvokeFunction","Principal":"*"}`, "Statement")
	do(http.MethodGet, "/2015-03-31/functions/echo/policy", "", "s1")
	do(http.MethodDelete, "/2015-03-31/functions/echo/policy?StatementId=s1", "")
	do(http.MethodPut, "/2015-03-31/functions/echo/concurrency", `{"ReservedConcurrentExecutions":5}`, "5")
	do(http.MethodGet, "/2015-03-31/functions/echo/concurrency", "", "5")
	do(http.MethodDelete, "/2015-03-31/functions/echo/concurrency", "")
	do(http.MethodPost, "/2015-03-31/tags/arn:aws:lambda:us-east-1:000000000000:function:echo", `{"Tags":{"k":"v"}}`)
	do(http.MethodGet, "/2015-03-31/tags/arn:aws:lambda:us-east-1:000000000000:function:echo", "")
	do(http.MethodDelete, "/2015-03-31/tags/arn:aws:lambda:us-east-1:000000000000:function:echo", "")
	esm := do(http.MethodPost, "/2015-03-31/event-source-mappings", `{"FunctionName":"echo","EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:q"}`, "UUID")
	uuid := ""
	if i := strings.Index(esm, `"UUID":"`); i >= 0 {
		rest := esm[i+8:]
		if j := strings.Index(rest, `"`); j >= 0 {
			uuid = rest[:j]
		}
	}
	do(http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, "", uuid)
	do(http.MethodGet, "/2015-03-31/event-source-mappings", "", uuid)
	do(http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid, `{"Enabled":false}`)
	do(http.MethodDelete, "/2015-03-31/event-source-mappings/"+uuid, "")
	do(http.MethodDelete, "/2015-03-31/functions/echo/aliases/live", "")
	do(http.MethodGet, "/2015-03-31/functions", "", "echo")
	do(http.MethodDelete, "/2015-03-31/functions/echo", "")
	do(http.MethodPost, "/?Action=CreateFunctionUrlConfig", `{"FunctionName":"echo","AuthType":"NONE"}`, "FunctionUrl")
	do(http.MethodPost, "/?Action=GetFunctionUrlConfig", `{"FunctionName":"echo"}`, "FunctionUrl")
	do(http.MethodPost, "/?Action=PublishLayerVersion", `{"LayerName":"shared"}`, "LayerVersionArn")
	do(http.MethodPost, "/?Action=ListLayers", `{}`, "Layers")
	do(http.MethodPost, "/?Action=CreateCodeSigningConfig", `{"Description":"c"}`, "CodeSigningConfig")
	do(http.MethodPost, "/?Action=PutFunctionEventInvokeConfig", `{"FunctionName":"echo","MaximumRetryAttempts":1}`, "echo")
	do(http.MethodPost, "/?Action=GetAccountSettings", `{}`, "AccountLimit")
	do(http.MethodPost, "/?Action=PutProvisionedConcurrencyConfig", `{"FunctionName":"echo","Qualifier":"1","ProvisionedConcurrentExecutions":2}`, "READY")
}

func TestLambdaHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 88 {
		t.Fatalf("lambda Operations() %d want 88", n)
	}
}

func TestBootedServerLambdaExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.lambda"}
	cfg.Seed = "lam-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action="+op, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", op, res.Header.Get("x-mirror-fidelity"), raw)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return string(raw)
	}
	hard := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action="+op, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s %d %s %s", op, res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
		}
		return string(raw)
	}
	created := hard("CreateFunctionUrlConfig", `{"FunctionName":"bootfn","AuthType":"NONE"}`)
	if !strings.Contains(created, "FunctionUrl") {
		t.Fatalf("create url %s", created)
	}
	got := hard("GetFunctionUrlConfig", `{"FunctionName":"bootfn"}`)
	if !strings.Contains(got, "FunctionUrl") {
		t.Fatalf("get url %s", got)
	}
	hard("DeleteFunctionUrlConfig", `{"FunctionName":"bootfn"}`)
	gone := hard("GetFunctionUrlConfig", `{"FunctionName":"bootfn"}`)
	if strings.Contains(gone, "lambda-url/bootfn") {
		t.Fatalf("url still present %s", gone)
	}
	payload := `{"FunctionName":"bootfn","LayerName":"shared","VersionNumber":"1","CapacityProviderName":"cp1","CodeSigningConfigId":"csc1","Qualifier":"1","DurableExecutionArn":"d1","AuthType":"NONE","StatementId":"s1","RecursiveLoop":"Allow","Policy":"{}","ProvisionedConcurrentExecutions":1}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
