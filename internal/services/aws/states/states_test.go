package states

import (
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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
)

func TestStatesHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 22 {
		t.Fatalf("states Operations() %d want 22", n)
	}
}

func TestStartSyncRejectsStandardWorkflow(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateStateMachine", Input: map[string]any{
		"name": "standard", "definition": `{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`, "roleArn": "arn:aws:iam::1:role/states",
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "StartSyncExecution", Input: map[string]any{"stateMachineArn": created.Output["stateMachineArn"]}})
	fault, ok := err.(*spi.Fault)
	if !ok || fault.Code != "StateMachineTypeNotSupported" {
		t.Fatalf("sync standard error %#v", err)
	}
}

func TestStatesTagLifecycle(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	arn := "arn:aws:states:us-east-1:1:stateMachine:tagged"
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	must("TagResource", map[string]any{"resourceArn": arn, "tags": []any{
		map[string]any{"key": "env", "value": "dev"}, map[string]any{"key": "team", "value": "platform"},
	}})
	must("TagResource", map[string]any{"resourceArn": arn, "tags": []any{map[string]any{"key": "env", "value": "prod"}}})
	must("UntagResource", map[string]any{"resourceArn": arn, "tagKeys": []any{"team"}})
	tags := must("ListTagsForResource", map[string]any{"resourceArn": arn}).Output["tags"].([]any)
	if len(tags) != 1 || tags[0].(map[string]any)["key"] != "env" || tags[0].(map[string]any)["value"] != "prod" {
		t.Fatalf("tags %#v", tags)
	}
}

func TestBootedServerStatesPassSucceed(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.states"}
	cfg.Seed = "sfn-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/states/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AWSStepFunctions."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	def := `{"StartAt":"Hi","States":{"Hi":{"Type":"Pass","Result":{"ok":true},"End":true}}}`
	created := call("CreateStateMachine", `{"name":"sm1","definition":`+mustJSON(def)+`,"roleArn":"arn:aws:iam::000000000000:role/x","type":"EXPRESS"}`)
	arn, _ := created["stateMachineArn"].(string)
	if arn == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeStateMachine", `{"stateMachineArn":"`+arn+`"}`)
	if got["name"] != "sm1" {
		t.Fatalf("describe %v", got)
	}
	started := call("StartExecution", `{"stateMachineArn":"`+arn+`","name":"ex1","input":"{\"n\":1}"}`)
	ex, _ := started["executionArn"].(string)
	if ex == "" {
		t.Fatalf("start %v", started)
	}
	desc := call("DescribeExecution", `{"executionArn":"`+ex+`"}`)
	if desc["status"] != "SUCCEEDED" {
		t.Fatalf("exec %v", desc)
	}
	if !strings.Contains(fmtString(desc["output"]), `"ok":true`) && !strings.Contains(fmtString(desc["output"]), `"ok": true`) {
		t.Fatalf("output %v", desc["output"])
	}
	sync := call("StartSyncExecution", `{"stateMachineArn":"`+arn+`","input":"{\"n\":2}"}`)
	if sync["status"] != "SUCCEEDED" || !strings.Contains(fmtString(sync["output"]), `"ok":true`) {
		t.Fatalf("sync %v", sync)
	}
	listed := call("ListStateMachines", `{}`)
	if listed["stateMachines"] == nil {
		t.Fatalf("list %v", listed)
	}
	call("DeleteStateMachine", `{"stateMachineArn":"`+arn+`"}`)
	gone := call("ListStateMachines", `{}`)
	raw, _ := json.Marshal(gone)
	if strings.Contains(string(raw), `"name":"sm1"`) {
		t.Fatalf("sm still present %s", raw)
	}
}

func TestBootedServerStatesMapLambdaActivity(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.states", "aws.lambda"}
	cfg.Seed = "sfn-map-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	sfnAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/states/aws4_request, SignedHeaders=host, Signature=00"
	lamAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=00"
	sfn := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AWSStepFunctions."+op)
		req.Header.Set("Authorization", sfnAuth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}

	mapDef := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.nums","Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	created := sfn("CreateStateMachine", `{"name":"mapsm","definition":`+mustJSON(mapDef)+`,"roleArn":"arn:aws:iam::000000000000:role/x"}`)
	arn, _ := created["stateMachineArn"].(string)
	started := sfn("StartExecution", `{"stateMachineArn":"`+arn+`","name":"mapex","input":"{\"nums\":[{\"n\":1},{\"n\":2}]}"}`)
	desc := sfn("DescribeExecution", `{"executionArn":"`+started["executionArn"].(string)+`"}`)
	if desc["status"] != "SUCCEEDED" {
		t.Fatalf("map exec %v", desc)
	}
	if !strings.Contains(fmtString(desc["output"]), `"n":1`) || !strings.Contains(fmtString(desc["output"]), `"n":2`) {
		t.Fatalf("map output %v", desc["output"])
	}

	act := sfn("CreateActivity", `{"name":"act1"}`)
	actARN, _ := act["activityArn"].(string)
	if actARN == "" {
		t.Fatalf("activity %v", act)
	}
	actDef := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + actARN + `","End":true}}}`
	created = sfn("CreateStateMachine", `{"name":"actsm","definition":`+mustJSON(actDef)+`,"roleArn":"arn:aws:iam::000000000000:role/x"}`)
	arn, _ = created["stateMachineArn"].(string)
	started = sfn("StartExecution", `{"stateMachineArn":"`+arn+`","name":"actex","input":"{\"v\":9}"}`)
	ex, _ := started["executionArn"].(string)
	mid := sfn("DescribeExecution", `{"executionArn":"`+ex+`"}`)
	if mid["status"] != "RUNNING" {
		t.Fatalf("want RUNNING %v", mid)
	}
	task := sfn("GetActivityTask", `{"activityArn":"`+actARN+`"}`)
	tok, _ := task["taskToken"].(string)
	if tok == "" || !strings.Contains(fmtString(task["input"]), `"v":9`) {
		t.Fatalf("activity task %v", task)
	}
	sfn("SendTaskSuccess", `{"taskToken":"`+tok+`","output":"{\"v\":10}"}`)
	done := sfn("DescribeExecution", `{"executionArn":"`+ex+`"}`)
	if done["status"] != "SUCCEEDED" {
		t.Fatalf("after success %v", done)
	}
	if !strings.Contains(fmtString(done["output"]), `"v":10`) {
		t.Fatalf("activity output %v", done["output"])
	}

	if _, err := exec.LookPath("python3"); err != nil {
		return
	}
	src := "def lambda_handler(event, context):\n    return {\"n\": event.get(\"n\", 0) + 1}\n"
	create := `{"FunctionName":"inc","Runtime":"python3.12","Handler":"lambda_function.lambda_handler","Code":{"ZipFile":"` + base64.StdEncoding.EncodeToString([]byte(src)) + `"}}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/2015-03-31/functions", strings.NewReader(create))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", lamAuth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("lambda create %d %s", res.StatusCode, raw)
	}
	lamDef := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:000000000000:function:inc","End":true}}}`
	created = sfn("CreateStateMachine", `{"name":"lamsm","definition":`+mustJSON(lamDef)+`,"roleArn":"arn:aws:iam::000000000000:role/x"}`)
	arn, _ = created["stateMachineArn"].(string)
	started = sfn("StartExecution", `{"stateMachineArn":"`+arn+`","name":"lamex","input":"{\"n\":3}"}`)
	ld := sfn("DescribeExecution", `{"executionArn":"`+started["executionArn"].(string)+`"}`)
	if ld["status"] != "SUCCEEDED" {
		t.Fatalf("lambda exec %v", ld)
	}
	if !strings.Contains(fmtString(ld["output"]), `"n":4`) {
		t.Fatalf("lambda output %v", ld["output"])
	}
}

func mustJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func fmtString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
