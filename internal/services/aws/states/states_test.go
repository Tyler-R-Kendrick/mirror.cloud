package states

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/batch"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codebuild"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ecs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/emr"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/glue"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

const testRoleARN = "arn:aws:iam::1:role/states"

type zeroIntRand struct{ spi.Rand }

func (zeroIntRand) Intn(int) int { return 0 }

func TestStatesHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 37 {
		t.Fatalf("states Operations() %d want 37", n)
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

func TestStateMachineVersionsAndAliases(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	fault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		if got, ok := err.(*spi.Fault); !ok || got.Code != code {
			t.Fatalf("%s fault %#v want %s", operation, err, code)
		}
	}

	definition1 := `{"StartAt":"Version","States":{"Version":{"Type":"Pass","Result":{"version":1},"End":true}}}`
	arn := must("CreateStateMachine", map[string]any{"name": "versioned", "definition": definition1, "roleArn": testRoleARN, "type": "EXPRESS"}).Output["stateMachineArn"].(string)
	v1 := must("PublishStateMachineVersion", map[string]any{"stateMachineArn": arn, "description": "one"}).Output["stateMachineVersionArn"].(string)
	if again := must("PublishStateMachineVersion", map[string]any{"stateMachineArn": arn}).Output["stateMachineVersionArn"]; again != v1 {
		t.Fatalf("idempotent publish %#v want %s", again, v1)
	}
	fault("PublishStateMachineVersion", map[string]any{"stateMachineArn": arn, "revisionId": "stale"}, "ConflictException")

	definition2 := `{"StartAt":"Version","States":{"Version":{"Type":"Pass","Result":{"version":2},"End":true}}}`
	must("UpdateStateMachine", map[string]any{"stateMachineArn": arn, "definition": definition2})
	v2 := must("PublishStateMachineVersion", map[string]any{"stateMachineArn": arn, "description": "two"}).Output["stateMachineVersionArn"].(string)
	versions := must("ListStateMachineVersions", map[string]any{"stateMachineArn": arn}).Output["stateMachineVersions"].([]any)
	if len(versions) != 2 || versions[0].(map[string]any)["stateMachineVersionArn"] != v2 || versions[1].(map[string]any)["stateMachineVersionArn"] != v1 {
		t.Fatalf("versions %#v", versions)
	}
	if sync := must("StartSyncExecution", map[string]any{"stateMachineArn": v1, "name": "v1"}).Output; !strings.Contains(fmtString(sync["output"]), `"version":1`) {
		t.Fatalf("immutable version output %#v", sync)
	}

	fault("CreateStateMachineAlias", map[string]any{"name": "123", "routingConfiguration": []any{map[string]any{"stateMachineVersionArn": v1, "weight": 100.0}}}, "ValidationException")
	fault("CreateStateMachineAlias", map[string]any{"name": "live", "routingConfiguration": []any{map[string]any{"stateMachineVersionArn": v1, "weight": 99.0}}}, "ValidationException")
	alias := must("CreateStateMachineAlias", map[string]any{"Name": "live", "Description": "first", "RoutingConfiguration": []any{map[string]any{"StateMachineVersionArn": v1, "Weight": 100.0}}}).Output["stateMachineAliasArn"].(string)
	if described := must("DescribeStateMachineAlias", map[string]any{"stateMachineAliasArn": alias}).Output; described["description"] != "first" {
		t.Fatalf("alias %#v", described)
	}
	if aliases := must("ListStateMachineAliases", map[string]any{"stateMachineArn": arn}).Output["stateMachineAliases"].([]any); len(aliases) != 1 {
		t.Fatalf("aliases %#v", aliases)
	}
	fault("DeleteStateMachineVersion", map[string]any{"stateMachineVersionArn": v1}, "ConflictException")
	must("UpdateStateMachineAlias", map[string]any{"StateMachineAliasArn": alias, "Description": "second", "RoutingConfiguration": []any{map[string]any{"StateMachineVersionArn": v2, "Weight": 100.0}}})
	fault("UpdateStateMachineAlias", map[string]any{"stateMachineAliasArn": alias}, "ValidationException")
	definition3 := `{"StartAt":"Version","States":{"Version":{"Type":"Pass","Result":{"version":3},"End":true}}}`
	must("UpdateStateMachine", map[string]any{"stateMachineArn": arn, "definition": definition3})
	if sync := must("StartSyncExecution", map[string]any{"stateMachineArn": alias, "name": "alias"}).Output; !strings.Contains(fmtString(sync["output"]), `"version":2`) {
		t.Fatalf("alias output %#v", sync)
	}
	must("DeleteStateMachineAlias", map[string]any{"stateMachineAliasArn": alias})
	fault("DeleteStateMachineAlias", map[string]any{"stateMachineAliasArn": alias}, "ResourceNotFound")
	must("DeleteStateMachineVersion", map[string]any{"stateMachineVersionArn": v1})
	must("DeleteStateMachineVersion", map[string]any{"stateMachineVersionArn": v2})
	fault("DeleteStateMachineVersion", map[string]any{"stateMachineVersionArn": v2}, "StateMachineDoesNotExist")
	if versions := must("ListStateMachineVersions", map[string]any{"stateMachineArn": arn}).Output["stateMachineVersions"].([]any); len(versions) != 0 {
		t.Fatalf("deleted versions %#v", versions)
	}
}

func TestExecutionNamesAreScopedToStateMachine(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output
	}
	create := func(name string) string {
		t.Helper()
		definition := `{"StartAt":"Done","States":{"Done":{"Type":"Pass","Result":"` + name + `","End":true}}}`
		return invoke("CreateStateMachine", map[string]any{"name": name, "definition": definition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	}
	firstARN := invoke("StartExecution", map[string]any{"stateMachineArn": create("first"), "name": "shared"})["executionArn"].(string)
	secondARN := invoke("StartExecution", map[string]any{"stateMachineArn": create("second"), "name": "shared"})["executionArn"].(string)
	if first := invoke("DescribeExecution", map[string]any{"executionArn": firstARN}); !strings.Contains(fmtString(first["output"]), "first") {
		t.Fatalf("first execution %#v", first)
	}
	if second := invoke("DescribeExecution", map[string]any{"executionArn": secondARN}); !strings.Contains(fmtString(second["output"]), "second") {
		t.Fatalf("second execution %#v", second)
	}
}

func TestDescribeForExecutionAndRedrive(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	fault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		if got, ok := err.(*spi.Fault); !ok || got.Code != code {
			t.Fatalf("%s fault %#v want %s", operation, err, code)
		}
	}

	activityARN := must("CreateActivity", map[string]any{"name": "redrive"})["activityArn"].(string)
	definition := `{"StartAt":"Before","States":{"Before":{"Type":"Pass","Next":"Work"},"Work":{"Type":"Task","Resource":"` + activityARN + `","End":true}}}`
	machineARN := must("CreateStateMachine", map[string]any{"name": "redrive", "definition": definition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	executionARN := must("StartExecution", map[string]any{"stateMachineArn": machineARN, "name": "attempt", "input": `{"job":1}`})["executionArn"].(string)
	originalToken := must("GetActivityTask", map[string]any{"activityArn": activityARN})["taskToken"].(string)
	described := must("DescribeStateMachineForExecution", map[string]any{"executionArn": executionARN})
	if described["definition"] != definition || described["name"] != "redrive" || described["roleArn"] != testRoleARN {
		t.Fatalf("state machine for execution %#v", described)
	}
	must("StopExecution", map[string]any{"executionArn": executionARN})
	redriveDate := must("RedriveExecution", map[string]any{"executionArn": executionARN, "clientToken": "same"})["redriveDate"]
	if again := must("RedriveExecution", map[string]any{"executionArn": executionARN, "clientToken": "same"})["redriveDate"]; again != redriveDate {
		t.Fatalf("idempotent redrive date %#v want %#v", again, redriveDate)
	}
	newToken := must("GetActivityTask", map[string]any{"activityArn": activityARN})["taskToken"].(string)
	if newToken == originalToken {
		t.Fatal("redrive reused activity token")
	}
	must("SendTaskSuccess", map[string]any{"taskToken": newToken, "output": `{"done":true}`})
	redriven := must("DescribeExecution", map[string]any{"executionArn": executionARN})
	if redriven["status"] != "SUCCEEDED" || redriven["redriveCount"] != 1.0 || !strings.Contains(fmtString(redriven["output"]), `"done":true`) {
		t.Fatalf("redriven execution %#v", redriven)
	}
	passEntries := 0
	for _, raw := range must("GetExecutionHistory", map[string]any{"executionArn": executionARN})["events"].([]any) {
		if raw.(map[string]any)["type"] == "PassStateEntered" {
			passEntries++
		}
	}
	if passEntries != 1 {
		t.Fatalf("redrive repeated successful state %d times", passEntries)
	}
	fault("RedriveExecution", map[string]any{"executionArn": executionARN, "clientToken": "different"}, "ExecutionNotRedrivable")
	fault("RedriveExecution", map[string]any{"executionArn": "missing"}, "ExecutionDoesNotExist")
	fault("DescribeStateMachineForExecution", map[string]any{"executionArn": "missing"}, "ExecutionDoesNotExist")

	expressARN := must("CreateStateMachine", map[string]any{"name": "express-describe", "definition": `{"StartAt":"Fail","States":{"Fail":{"Type":"Fail"}}}`, "roleArn": testRoleARN, "type": "EXPRESS"})["stateMachineArn"].(string)
	expressExecutionARN := must("StartExecution", map[string]any{"stateMachineArn": expressARN})["executionArn"].(string)
	fault("DescribeStateMachineForExecution", map[string]any{"executionArn": expressExecutionARN}, "StateMachineTypeNotSupported")
	fault("RedriveExecution", map[string]any{"executionArn": expressExecutionARN}, "ExecutionNotRedrivable")
}

func TestDistributedMapRuns(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	fault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		if got, ok := err.(*spi.Fault); !ok || got.Code != code {
			t.Fatalf("%s fault %#v want %s", operation, err, code)
		}
	}

	processor := `{"StartAt":"Done","ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"States":{"Done":{"Type":"Succeed"}}}`
	definition := `{"StartAt":"First","States":{"First":{"Type":"Map","Label":"first","ItemsPath":"$.items","ItemProcessor":` + processor + `,"ResultPath":null,"Next":"Second"},"Second":{"Type":"Map","Label":"second","ItemsPath":"$.items","ItemProcessor":` + processor + `,"End":true}}}`
	machineARN := must("CreateStateMachine", map[string]any{"name": "distributed", "definition": definition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	executionARN := must("StartExecution", map[string]any{"stateMachineArn": machineARN, "input": `{"items":[1,2,3]}`})["executionArn"].(string)
	firstPage := must("ListMapRuns", map[string]any{"executionArn": executionARN, "maxResults": 1.0})
	if len(firstPage["mapRuns"].([]any)) != 1 || firstPage["nextToken"] == nil {
		t.Fatalf("first map run page %#v", firstPage)
	}
	secondPage := must("ListMapRuns", map[string]any{"executionArn": executionARN, "maxResults": 1.0, "nextToken": firstPage["nextToken"]})
	if len(secondPage["mapRuns"].([]any)) != 1 || secondPage["nextToken"] != nil {
		t.Fatalf("second map run page %#v", secondPage)
	}
	mapRunARN := firstPage["mapRuns"].([]any)[0].(map[string]any)["mapRunArn"].(string)
	described := must("DescribeMapRun", map[string]any{"mapRunArn": mapRunARN})
	if described["status"] != "SUCCEEDED" || described["executionCounts"].(map[string]any)["total"] != 3.0 || described["itemCounts"].(map[string]any)["succeeded"] != 3.0 {
		t.Fatalf("map run %#v", described)
	}
	children := must("ListExecutions", map[string]any{"mapRunArn": mapRunARN, "maxResults": 2.0})
	if len(children["executions"].([]any)) != 2 || children["nextToken"] == nil {
		t.Fatalf("map child execution page %#v", children)
	}
	childARN := children["executions"].([]any)[0].(map[string]any)["executionArn"].(string)
	child := must("DescribeExecution", map[string]any{"executionArn": childARN})
	if child["status"] != "SUCCEEDED" || child["mapRunArn"] != mapRunARN || child["itemCount"] != 1.0 || child["output"] == nil {
		t.Fatalf("map child execution %#v", child)
	}
	childMachine := must("DescribeStateMachineForExecution", map[string]any{"executionArn": childARN})
	childHistory := must("GetExecutionHistory", map[string]any{"executionArn": childARN})["events"].([]any)
	if childMachine["roleArn"] != testRoleARN || !strings.Contains(childMachine["definition"].(string), `"ProcessorConfig"`) || len(childHistory) == 0 {
		t.Fatalf("map child state machine %#v history=%#v", childMachine, childHistory)
	}
	remainingChildren := must("ListExecutions", map[string]any{"mapRunArn": mapRunARN, "maxResults": 2.0, "nextToken": children["nextToken"]})
	if len(remainingChildren["executions"].([]any)) != 1 || remainingChildren["nextToken"] != nil {
		t.Fatalf("remaining map child executions %#v", remainingChildren)
	}
	fault("UpdateMapRun", map[string]any{"mapRunArn": mapRunARN, "maxConcurrency": 2.0}, "ValidationException")
	described["status"] = "RUNNING"
	encoded, _ := json.Marshal(described)
	_ = p.col(&spi.Request{Identity: id}, "maprun").Put(ctx, mapRunARN, encoded)
	must("UpdateMapRun", map[string]any{"MapRunArn": mapRunARN, "MaxConcurrency": 2.0, "ToleratedFailureCount": 1.0, "ToleratedFailurePercentage": 50.0})
	updated := must("DescribeMapRun", map[string]any{"mapRunArn": mapRunARN})
	if updated["maxConcurrency"] != 2.0 || updated["toleratedFailureCount"] != 1.0 || updated["toleratedFailurePercentage"] != 50.0 {
		t.Fatalf("updated map run %#v", updated)
	}
	fault("DescribeMapRun", map[string]any{"mapRunArn": "missing"}, "ResourceNotFound")
	fault("UpdateMapRun", map[string]any{"mapRunArn": "missing"}, "ResourceNotFound")
	fault("ListMapRuns", map[string]any{"executionArn": "missing"}, "ExecutionDoesNotExist")
	fault("ListMapRuns", map[string]any{"executionArn": executionARN, "nextToken": "bad"}, "InvalidToken")

	failingProcessor := `{"StartAt":"Fail","ProcessorConfig":{"Mode":"DISTRIBUTED"},"States":{"Fail":{"Type":"Fail","Error":"ItemFailed"}}}`
	toleratedDefinition := `{"StartAt":"Map","States":{"Map":{"Type":"Map","ItemsPath":"$.items","ToleratedFailureCountPath":"$.limit","ItemProcessor":` + failingProcessor + `,"End":true}}}`
	toleratedARN := must("CreateStateMachine", map[string]any{"name": "tolerated", "definition": toleratedDefinition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	toleratedExecution := must("StartExecution", map[string]any{"stateMachineArn": toleratedARN, "input": `{"items":[1,2],"limit":2}`})["executionArn"].(string)
	if execution := must("DescribeExecution", map[string]any{"executionArn": toleratedExecution}); execution["status"] != "SUCCEEDED" {
		t.Fatalf("tolerated distributed map %#v", execution)
	}
	toleratedRuns := must("ListMapRuns", map[string]any{"executionArn": toleratedExecution})["mapRuns"].([]any)
	toleratedRun := must("DescribeMapRun", map[string]any{"mapRunArn": toleratedRuns[0].(map[string]any)["mapRunArn"]})
	if toleratedRun["status"] != "SUCCEEDED" || toleratedRun["itemCounts"].(map[string]any)["failed"] != 2.0 {
		t.Fatalf("tolerated map run %#v", toleratedRun)
	}
	failedChildren := must("ListExecutions", map[string]any{"mapRunArn": toleratedRun["mapRunArn"], "statusFilter": "FAILED"})["executions"].([]any)
	if len(failedChildren) != 2 {
		t.Fatalf("failed map child executions %#v", failedChildren)
	}
	objectDefinition := `{"StartAt":"Map","States":{"Map":{"Type":"Map","ItemsPath":"$.items","ItemSelector":{"key.$":"$$.Map.Item.Key","value.$":"$$.Map.Item.Value","index.$":"$$.Map.Item.Index"},"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"EXPRESS"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`
	objectARN := must("CreateStateMachine", map[string]any{"name": "object-items", "definition": objectDefinition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	objectExecution := must("StartExecution", map[string]any{"stateMachineArn": objectARN, "input": `{"items":{"b":2,"a":1}}`})["executionArn"].(string)
	if execution := must("DescribeExecution", map[string]any{"executionArn": objectExecution}); execution["status"] != "SUCCEEDED" || execution["output"] != `[{"index":0,"key":"a","value":1},{"index":1,"key":"b","value":2}]` {
		t.Fatalf("JSONPath object Map %#v", execution)
	}
	objectRuns := must("ListMapRuns", map[string]any{"executionArn": objectExecution})["mapRuns"].([]any)
	objectChildren := must("ListExecutions", map[string]any{"mapRunArn": objectRuns[0].(map[string]any)["mapRunArn"]})["executions"].([]any)
	objectChildARN := objectChildren[0].(map[string]any)["executionArn"].(string)
	if child := must("DescribeExecution", map[string]any{"executionArn": objectChildARN}); child["status"] != "SUCCEEDED" || child["mapRunArn"] != objectRuns[0].(map[string]any)["mapRunArn"] {
		t.Fatalf("Express map child execution %#v", child)
	}
	fault("DescribeStateMachineForExecution", map[string]any{"executionArn": objectChildARN}, "StateMachineTypeNotSupported")
	fault("GetExecutionHistory", map[string]any{"executionArn": objectChildARN}, "StateMachineTypeNotSupported")
	fault("StopExecution", map[string]any{"executionArn": objectChildARN}, "StateMachineTypeNotSupported")
	if originalRuns := must("ListMapRuns", map[string]any{"executionArn": executionARN})["mapRuns"].([]any); len(originalRuns) != 2 {
		t.Fatalf("cross-execution map runs %#v", originalRuns)
	}
}

func TestDistributedMapS3ItemReader(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	storage := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, operation string, input map[string]any, body []byte) map[string]any {
		t.Helper()
		request := &spi.Request{Identity: id, Operation: operation, Input: input}
		if body != nil {
			request.Body = io.NopCloser(bytes.NewReader(body))
		}
		response, err := handler.Invoke(ctx, request)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	invoke(storage, "CreateBucket", map[string]any{"Bucket": "items"}, nil)
	objects := map[string]string{
		"items.json":                  `[{"id":1},{"id":2}]`,
		"items.jsonl":                 "{\"id\":3}\n{\"id\":4}\n",
		"items.csv":                   "id,name,path,note\n5\n6,Lin,C:\\\\Program Files\\\\App.exe,say \\\"hi\\\",ignored\n",
		"items-path.json":             `{"data":{"items":[{"id":13},{"id":14}]}}`,
		"listed/a":                    `{"alpha":{"id":21}}`,
		"listed/b":                    `{"beta":{"id":22}}`,
		"flat-array/a":                `[{"id":23}]`,
		"broken.json.gz":              "not gzip",
		"athena/a.jsonl":              "{\"id\":31}\n",
		"athena/b.jsonl":              "{\"id\":32}\n{\"id\":33}\n",
		"manifest.csv":                "s3://items/athena/a.jsonl\ns3://items/athena/b.jsonl\n",
		"bad-manifest.csv":            "https://items/athena/a.jsonl\n",
		"inventory-manifest.json":     `{"sourceBucket":"source","destinationBucket":"arn:aws:s3:::items","version":"2016-11-30","creationTimestamp":"1787529600000","fileFormat":"CSV","fileSchema":"Bucket, Key, Size, LastModifiedDate","files":[{"key":"inventory/data.csv.gz"}]}`,
		"bad-inventory-manifest.json": `{"sourceBucket":"source","destinationBucket":"arn:aws:s3:::items","version":"2016-11-30","creationTimestamp":"1787529600000","fileFormat":"ORC","fileSchema":"Bucket,Key","files":[]}`,
		"nested.json":                 `{"data":{"a/b":{"~key":[{"id":9},{"id":10}]}}}`,
		"objects.json":                `{"b":{"id":12},"a":{"id":11}}`,
	}
	objects["late-items.json"] = `{"padding":"` + strings.Repeat("x", 16*1024*1024) + `","items":[{"id":40}]}`
	objects["large-item.json"] = `["` + strings.Repeat("x", 8*1024*1024) + `"]`
	objects["selectable-item.json"] = `["` + strings.Repeat("x", 512*1024) + `"]`
	for key, body := range objects {
		invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": key}, []byte(body))
	}
	var gzipBody bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzipBody)
	if _, err := gzipWriter.Write([]byte(objects["items.json"])); err != nil {
		t.Fatalf("write gzip: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": "items.json.gz"}, gzipBody.Bytes())
	var inventoryBody bytes.Buffer
	inventoryWriter := gzip.NewWriter(&inventoryBody)
	if _, err := inventoryWriter.Write([]byte("source,alpha,1,2026-08-24T00:00:00Z\nsource,beta,2,2026-08-24T00:01:00Z\n")); err != nil {
		t.Fatalf("write inventory gzip: %v", err)
	}
	if err := inventoryWriter.Close(); err != nil {
		t.Fatalf("close inventory gzip: %v", err)
	}
	invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": "inventory/data.csv.gz"}, inventoryBody.Bytes())
	zstdWriter, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("create zstd writer: %v", err)
	}
	invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": "items.jsonl.zstd"}, zstdWriter.EncodeAll([]byte(objects["items.jsonl"]), nil))
	zstdWriter.Close()
	invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": "listed/a", "StorageClass": "STANDARD_IA"}, []byte(objects["listed/a"]))
	type parquetItem struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	var parquetBody bytes.Buffer
	writer := parquet.NewGenericWriter[parquetItem](&parquetBody)
	if _, err := writer.Write([]parquetItem{{ID: 7, Name: "Ada"}, {ID: 8, Name: "Lin"}}); err != nil || writer.Close() != nil {
		t.Fatalf("write parquet: %v", err)
	}
	invoke(storage, "PutBucketVersioning", map[string]any{"Bucket": "items", "Status": "Enabled"}, nil)
	parquetRequest := &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "items", "Key": "items.parquet"}, Body: io.NopCloser(bytes.NewReader(parquetBody.Bytes()))}
	parquetResponse, err := storage.Invoke(ctx, parquetRequest)
	if err != nil {
		t.Fatalf("put versioned parquet: %v", err)
	}
	parquetVersionID := parquetResponse.Headers.Get("x-amz-version-id")
	if parquetVersionID == "" {
		t.Fatal("missing parquet version ID")
	}
	invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": "broken.parquet"}, []byte("not parquet"))
	processor := map[string]any{"StartAt": "Done", "ProcessorConfig": map[string]any{"Mode": "DISTRIBUTED"}, "States": map[string]any{"Done": map[string]any{"Type": "Succeed"}}}
	for _, test := range []struct {
		key, inputType, pointer, itemsPath string
		limit                              int
	}{{"items.json", "JSON", "", "", 0}, {"items.json.gz", "JSON", "", "", 0}, {"items.jsonl", "JSONL", "", "", 0}, {"items.jsonl.zstd", "JSONL", "", "", 0}, {"items.csv", "CSV", "", "", 0}, {"items.parquet", "PARQUET", "", "", 0}, {"nested.json", "JSON", "/data/a~1b/~0key", "", 0}, {"objects.json", "JSON", "", "", 1}, {"items-path.json", "JSON", "", "$.data.items", 0}} {
		readerConfig := map[string]any{"InputType": test.inputType}
		if test.inputType == "CSV" {
			readerConfig["CSVHeaderLocation"] = "FIRST_ROW"
		}
		if test.pointer != "" {
			readerConfig["ItemsPointer"] = test.pointer
		}
		if test.limit != 0 {
			readerConfig["MaxItems"] = test.limit
		}
		state := map[string]any{
			"Type": "Map", "ItemProcessor": processor, "ItemSelector": map[string]any{"value.$": "$$.Map.Item.Value", "source.$": "$$.Map.Item.Source"}, "End": true,
			"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": test.key}, "ReaderConfig": readerConfig},
		}
		if test.itemsPath != "" {
			state["ItemsPath"] = test.itemsPath
		}
		definition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": state}})
		machine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-" + strings.ReplaceAll(test.key, ".", "-"), "definition": string(definition), "roleArn": testRoleARN}, nil)
		started := invoke(p, "StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"]}, nil)
		execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": started["executionArn"]}, nil)
		output := execution["output"].(string)
		expected := 2
		if test.limit != 0 {
			expected = test.limit
		}
		if execution["status"] != "SUCCEEDED" || strings.Count(output, `"source":"S3://items/`+test.key+`"`) != expected || test.inputType == "PARQUET" && !strings.Contains(output, `"name":"Ada"`) || test.inputType == "CSV" && (!strings.Contains(output, `"name":""`) || !strings.Contains(output, `"path":"C:\\Program Files\\App.exe"`) || !strings.Contains(output, `"note":"say \"hi\""`) || strings.Contains(output, "ignored")) || test.pointer != "" && !strings.Contains(output, `"id":9`) || test.itemsPath != "" && !strings.Contains(output, `"id":13`) {
			t.Fatalf("%s ItemReader execution %#v", test.inputType, execution)
		}
	}
	ownerState := map[string]any{
		"Type": "Map", "ItemProcessor": processor, "End": true,
		"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": "items.json", "ExpectedBucketOwner": "1"}, "ReaderConfig": map[string]any{"InputType": "JSON"}},
	}
	ownerDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": ownerState}})
	ownerMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-expected-owner", "definition": string(ownerDefinition), "roleArn": testRoleARN}, nil)
	ownerStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": ownerMachine["stateMachineArn"]}, nil)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": ownerStarted["executionArn"]}, nil); execution["status"] != "SUCCEEDED" {
		t.Fatalf("matching ExpectedBucketOwner execution %#v", execution)
	}
	ownerState["ItemReader"].(map[string]any)["Parameters"].(map[string]any)["ExpectedBucketOwner"] = "2"
	ownerDefinition, _ = json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": ownerState}})
	ownerMachine = invoke(p, "CreateStateMachine", map[string]any{"name": "reader-wrong-owner", "definition": string(ownerDefinition), "roleArn": testRoleARN}, nil)
	ownerStarted = invoke(p, "StartExecution", map[string]any{"stateMachineArn": ownerMachine["stateMachineArn"]}, nil)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": ownerStarted["executionArn"]}, nil); execution["status"] != "FAILED" || execution["error"] != "States.ItemReaderFailed" {
		t.Fatalf("mismatched ExpectedBucketOwner execution %#v", execution)
	}
	if _, _, valid := p.mapItems(ctx, &spi.Request{Identity: id}, map[string]any{"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": "items.parquet", "VersionId": parquetVersionID}, "ReaderConfig": map[string]any{"InputType": "PARQUET"}}}, nil, nil); valid {
		t.Fatal("accepted Parquet ItemReader VersionId at runtime")
	}
	manifestState := map[string]any{
		"Type": "Map", "ItemProcessor": processor, "ItemSelector": map[string]any{"value.$": "$$.Map.Item.Value", "source.$": "$$.Map.Item.Source"}, "End": true,
		"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": "manifest.csv"}, "ReaderConfig": map[string]any{"ManifestType": "ATHENA_DATA", "InputType": "JSONL", "MaxItems": 2}},
	}
	manifestDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": manifestState}})
	manifestMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-athena-manifest", "definition": string(manifestDefinition), "roleArn": testRoleARN}, nil)
	manifestStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": manifestMachine["stateMachineArn"]}, nil)
	manifestExecution := invoke(p, "DescribeExecution", map[string]any{"executionArn": manifestStarted["executionArn"]}, nil)
	manifestOutput := manifestExecution["output"].(string)
	if manifestExecution["status"] != "SUCCEEDED" || !strings.Contains(manifestOutput, `"source":"S3://items/athena/a.jsonl"`) || !strings.Contains(manifestOutput, `"source":"S3://items/athena/b.jsonl"`) || !strings.Contains(manifestOutput, `"id":31`) || !strings.Contains(manifestOutput, `"id":32`) || strings.Contains(manifestOutput, `"id":33`) {
		t.Fatalf("Athena manifest ItemReader execution %#v", manifestExecution)
	}
	manifestState["ItemReader"].(map[string]any)["Parameters"] = map[string]any{"Bucket": "items", "Key": "bad-manifest.csv"}
	manifestDefinition, _ = json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": manifestState}})
	manifestMachine = invoke(p, "CreateStateMachine", map[string]any{"name": "reader-bad-athena-manifest", "definition": string(manifestDefinition), "roleArn": testRoleARN}, nil)
	manifestStarted = invoke(p, "StartExecution", map[string]any{"stateMachineArn": manifestMachine["stateMachineArn"]}, nil)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": manifestStarted["executionArn"]}, nil); execution["status"] != "FAILED" || execution["error"] != "States.ItemReaderFailed" {
		t.Fatalf("bad Athena manifest ItemReader execution %#v", execution)
	}
	inventoryState := map[string]any{
		"Type": "Map", "ItemProcessor": processor, "ItemSelector": map[string]any{"value.$": "$$.Map.Item.Value", "source.$": "$$.Map.Item.Source"}, "End": true,
		"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": "inventory-manifest.json"}, "ReaderConfig": map[string]any{"ManifestType": "S3_INVENTORY", "MaxItems": 1}},
	}
	inventoryDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": inventoryState}})
	inventoryMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-s3-inventory", "definition": string(inventoryDefinition), "roleArn": testRoleARN}, nil)
	inventoryStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": inventoryMachine["stateMachineArn"]}, nil)
	inventoryExecution := invoke(p, "DescribeExecution", map[string]any{"executionArn": inventoryStarted["executionArn"]}, nil)
	inventoryOutput := inventoryExecution["output"].(string)
	if inventoryExecution["status"] != "SUCCEEDED" || !strings.Contains(inventoryOutput, `"source":"S3://items/inventory/data.csv.gz"`) || !strings.Contains(inventoryOutput, `"Bucket":"source"`) || !strings.Contains(inventoryOutput, `"Key":"alpha"`) || !strings.Contains(inventoryOutput, `"LastModifiedDate":"2026-08-24T00:00:00Z"`) || strings.Contains(inventoryOutput, `"Key":"beta"`) {
		t.Fatalf("S3 inventory ItemReader execution %#v", inventoryExecution)
	}
	inventoryState["ItemReader"].(map[string]any)["Parameters"] = map[string]any{"Bucket": "items", "Key": "bad-inventory-manifest.json"}
	inventoryDefinition, _ = json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": inventoryState}})
	inventoryMachine = invoke(p, "CreateStateMachine", map[string]any{"name": "reader-bad-s3-inventory", "definition": string(inventoryDefinition), "roleArn": testRoleARN}, nil)
	inventoryStarted = invoke(p, "StartExecution", map[string]any{"stateMachineArn": inventoryMachine["stateMachineArn"]}, nil)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": inventoryStarted["executionArn"]}, nil); execution["status"] != "FAILED" || execution["error"] != "States.ItemReaderFailed" {
		t.Fatalf("bad S3 inventory ItemReader execution %#v", execution)
	}
	listState := map[string]any{
		"Type": "Map", "ItemProcessor": processor, "ItemSelector": map[string]any{"value.$": "$$.Map.Item.Value", "source.$": "$$.Map.Item.Source"}, "End": true,
		"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:listObjectsV2", "Parameters": map[string]any{"Bucket": "items", "Prefix": "listed/", "MaxKeys": 1}},
	}
	listDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": listState}})
	listMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-list-objects", "definition": string(listDefinition), "roleArn": testRoleARN}, nil)
	listStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": listMachine["stateMachineArn"]}, nil)
	listExecution := invoke(p, "DescribeExecution", map[string]any{"executionArn": listStarted["executionArn"]}, nil)
	listOutput := listExecution["output"].(string)
	if listExecution["status"] != "SUCCEEDED" || strings.Count(listOutput, `"source":"S3://items"`) != 2 || !strings.Contains(listOutput, `"Key":"listed/a"`) || !strings.Contains(listOutput, `"StorageClass":"STANDARD_IA"`) {
		t.Fatalf("list ItemReader execution %#v", listExecution)
	}
	flattenState := map[string]any{
		"Type": "Map", "ItemProcessor": processor, "ItemSelector": map[string]any{"value.$": "$$.Map.Item.Value", "source.$": "$$.Map.Item.Source", "key.$": "$$.Map.Item.Key"}, "End": true,
		"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:listObjectsV2", "Parameters": map[string]any{"Bucket": "items", "Prefix": "listed/", "MaxKeys": 1}, "ReaderConfig": map[string]any{"InputType": "JSON", "Transformation": "LOAD_AND_FLATTEN"}},
	}
	flattenDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": flattenState}})
	flattenMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-load-flatten", "definition": string(flattenDefinition), "roleArn": testRoleARN}, nil)
	flattenStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": flattenMachine["stateMachineArn"]}, nil)
	flattenExecution := invoke(p, "DescribeExecution", map[string]any{"executionArn": flattenStarted["executionArn"]}, nil)
	flattenOutput := flattenExecution["output"].(string)
	if flattenExecution["status"] != "SUCCEEDED" || !strings.Contains(flattenOutput, `"source":"S3://items/listed/a"`) || !strings.Contains(flattenOutput, `"source":"S3://items/listed/b"`) || !strings.Contains(flattenOutput, `"key":"alpha"`) || !strings.Contains(flattenOutput, `"key":"beta"`) || !strings.Contains(flattenOutput, `"id":21`) || !strings.Contains(flattenOutput, `"id":22`) {
		t.Fatalf("flatten ItemReader execution %#v", flattenExecution)
	}
	flattenState["ItemSelector"] = map[string]any{"value.$": "$$.Map.Item.Value", "source.$": "$$.Map.Item.Source"}
	flattenState["ItemReader"].(map[string]any)["Parameters"] = map[string]any{"Bucket": "items", "Prefix": "flat-array/"}
	flattenDefinition, _ = json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": flattenState}})
	flattenMachine = invoke(p, "CreateStateMachine", map[string]any{"name": "reader-load-flatten-array", "definition": string(flattenDefinition), "roleArn": testRoleARN}, nil)
	flattenStarted = invoke(p, "StartExecution", map[string]any{"stateMachineArn": flattenMachine["stateMachineArn"]}, nil)
	flattenExecution = invoke(p, "DescribeExecution", map[string]any{"executionArn": flattenStarted["executionArn"]}, nil)
	if flattenExecution["status"] != "SUCCEEDED" || !strings.Contains(flattenExecution["output"].(string), `"source":"S3://items/flat-array/a"`) || !strings.Contains(flattenExecution["output"].(string), `"id":23`) {
		t.Fatalf("flatten array ItemReader execution %#v", flattenExecution)
	}

	for _, test := range []struct{ key, inputType, pointer string }{{"missing", "PARQUET", ""}, {"broken.parquet", "PARQUET", ""}, {"broken.json.gz", "JSON", ""}, {"late-items.json", "JSON", "/items"}} {
		readerConfig := map[string]any{"InputType": test.inputType}
		if test.pointer != "" {
			readerConfig["ItemsPointer"] = test.pointer
		}
		missingState := map[string]any{"Type": "Map", "ItemProcessor": processor, "ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": test.key}, "ReaderConfig": readerConfig}, "End": true}
		missingDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": missingState}})
		missingMachine := invoke(p, "CreateStateMachine", map[string]any{"name": strings.ReplaceAll(test.key, ".", "-") + "-reader", "definition": string(missingDefinition), "roleArn": testRoleARN}, nil)
		missingExecution := invoke(p, "StartExecution", map[string]any{"stateMachineArn": missingMachine["stateMachineArn"]}, nil)
		if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": missingExecution["executionArn"]}, nil); execution["status"] != "FAILED" || execution["error"] != "States.ItemReaderFailed" {
			t.Fatalf("%s ItemReader execution %#v", test.key, execution)
		}
	}
	largeState := map[string]any{
		"Type": "Map", "ItemProcessor": processor, "ItemSelector": map[string]any{"small": true}, "End": true,
		"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": "large-item.json"}, "ReaderConfig": map[string]any{"InputType": "JSON"}},
	}
	largeDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": largeState}})
	largeMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-large-item", "definition": string(largeDefinition), "roleArn": testRoleARN}, nil)
	largeStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": largeMachine["stateMachineArn"]}, nil)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": largeStarted["executionArn"]}, nil); execution["status"] != "FAILED" || execution["error"] != "States.ItemReaderFailed" {
		t.Fatalf("large ItemReader item execution %#v", execution)
	}
	largeState["ItemReader"].(map[string]any)["Parameters"] = map[string]any{"Bucket": "items", "Key": "selectable-item.json"}
	largeDefinition, _ = json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": largeState}})
	largeMachine = invoke(p, "CreateStateMachine", map[string]any{"name": "reader-selectable-item", "definition": string(largeDefinition), "roleArn": testRoleARN}, nil)
	largeStarted = invoke(p, "StartExecution", map[string]any{"stateMachineArn": largeMachine["stateMachineArn"]}, nil)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": largeStarted["executionArn"]}, nil); execution["status"] != "SUCCEEDED" || !strings.Contains(execution["output"].(string), `"small":true`) {
		t.Fatalf("selectable ItemReader item execution %#v", execution)
	}
}

func TestFlattenedMapDatasetLimit(t *testing.T) {
	limited, _, valid := limitReaderItems(mapDataset{values: []any{1, 2}, keys: []any{"a", "b"}, sources: []string{"S3://items/a", "S3://items/b"}}, "", map[string]any{"MaxItems": 1}, nil, nil)
	dataset, typed := limited.(mapDataset)
	if !valid || !typed || len(dataset.values) != 1 || len(dataset.keys) != 1 || len(dataset.sources) != 1 || dataset.values[0] != 1 || dataset.keys[0] != "a" || dataset.sources[0] != "S3://items/a" {
		t.Fatalf("limited flattened dataset %#v", limited)
	}
}

func TestJSONPointerOffset(t *testing.T) {
	body := []byte(`[0,{"items": []}]`)
	offset, valid := jsonPointerOffset(body, []string{"1", "items"})
	if !valid || !bytes.HasPrefix(body[offset:], []byte("[]")) {
		t.Fatalf("JSON pointer offset %d valid %t", offset, valid)
	}
}

func TestStateMachineControlPlaneParity(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	fault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		if got, ok := err.(*spi.Fault); !ok || got.Code != code {
			t.Fatalf("%s fault %#v want %s", operation, err, code)
		}
	}

	definition1 := `{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	createInput := map[string]any{
		"name": "published", "definition": definition1, "roleArn": testRoleARN, "publish": true, "versionDescription": "initial",
		"loggingConfiguration": map[string]any{"level": "OFF"}, "tracingConfiguration": map[string]any{"enabled": false},
		"tags": []any{map[string]any{"key": "owner", "value": "first"}},
	}
	created := must("CreateStateMachine", createInput)
	arn, version1 := created["stateMachineArn"].(string), created["stateMachineVersionArn"].(string)
	if !strings.HasSuffix(version1, ":1") {
		t.Fatalf("published version %#v", created)
	}
	idempotentInput := map[string]any{}
	for key, value := range createInput {
		idempotentInput[key] = value
	}
	idempotentInput["roleArn"] = "arn:aws:iam::1:role/ignored"
	idempotentInput["tags"] = []any{map[string]any{"key": "owner", "value": "ignored"}}
	if repeated := must("CreateStateMachine", idempotentInput); repeated["stateMachineArn"] != arn || repeated["stateMachineVersionArn"] != version1 {
		t.Fatalf("idempotent create %#v", repeated)
	}
	if described := must("DescribeStateMachine", map[string]any{"stateMachineArn": arn}); described["roleArn"] != testRoleARN || described["_publish"] != nil {
		t.Fatalf("described state machine %#v", described)
	}
	if tags := must("ListTagsForResource", map[string]any{"resourceArn": arn})["tags"].([]any); len(tags) != 1 || tags[0].(map[string]any)["value"] != "first" {
		t.Fatalf("create tags %#v", tags)
	}
	changedCreate := map[string]any{}
	for key, value := range createInput {
		changedCreate[key] = value
	}
	changedCreate["definition"] = `{"StartAt":"Other","States":{"Other":{"Type":"Succeed"}}}`
	fault("CreateStateMachine", changedCreate, "StateMachineAlreadyExists")
	fault("CreateStateMachine", map[string]any{"name": "bad-description", "definition": definition1, "roleArn": testRoleARN, "versionDescription": "invalid"}, "ValidationException")
	if version := must("DescribeStateMachine", map[string]any{"stateMachineArn": version1}); version["stateMachineArn"] != version1 || version["definition"] != definition1 {
		t.Fatalf("described version %#v", version)
	}

	revision1 := must("DescribeStateMachine", map[string]any{"stateMachineArn": arn})["revisionId"]
	definition2 := `{"StartAt":"Result","States":{"Result":{"Type":"Pass","Result":2,"End":true}}}`
	updated := must("UpdateStateMachine", map[string]any{"stateMachineArn": arn, "definition": definition2, "publish": true, "versionDescription": "second"})
	version2 := updated["stateMachineVersionArn"].(string)
	if !strings.HasSuffix(version2, ":2") || updated["revisionId"] == nil || updated["revisionId"] == revision1 {
		t.Fatalf("published update %#v", updated)
	}
	if repeated := must("UpdateStateMachine", map[string]any{"stateMachineArn": arn, "publish": true}); repeated["stateMachineVersionArn"] != version2 {
		t.Fatalf("idempotent update publish %#v", repeated)
	}
	fault("UpdateStateMachine", map[string]any{"stateMachineArn": arn, "versionDescription": "invalid"}, "ValidationException")
	alias := must("CreateStateMachineAlias", map[string]any{"name": "live", "routingConfiguration": []any{map[string]any{"stateMachineVersionArn": version2, "weight": 100.0}}})["stateMachineAliasArn"].(string)
	versionExecution := must("StartExecution", map[string]any{"stateMachineArn": version1, "name": "version-association"})["executionArn"].(string)
	if execution := must("DescribeExecution", map[string]any{"executionArn": versionExecution}); execution["stateMachineVersionArn"] != version1 || execution["stateMachineAliasArn"] != nil {
		t.Fatalf("version execution association %#v", execution)
	}
	aliasExecution := must("StartExecution", map[string]any{"stateMachineArn": alias, "name": "alias-association"})["executionArn"].(string)
	if execution := must("DescribeExecution", map[string]any{"executionArn": aliasExecution}); execution["stateMachineVersionArn"] != version2 || execution["stateMachineAliasArn"] != alias {
		t.Fatalf("alias execution association %#v", execution)
	}
	fault("DeleteStateMachine", map[string]any{"stateMachineArn": version1}, "ValidationException")
	must("DeleteStateMachine", map[string]any{"stateMachineArn": arn})
	fault("DescribeStateMachine", map[string]any{"stateMachineArn": arn}, "StateMachineDoesNotExist")
	fault("ListStateMachineVersions", map[string]any{"stateMachineArn": arn}, "StateMachineDoesNotExist")
	fault("DescribeStateMachineAlias", map[string]any{"stateMachineAliasArn": alias}, "ResourceNotFound")
	if _, exists := getRecord(ctx, p.col(&spi.Request{Identity: id}, "ver"), "published:1"); exists {
		t.Fatal("delete retained state machine version")
	}
	fault("ListTagsForResource", map[string]any{"resourceArn": arn}, "ResourceNotFound")
	fault("DeleteStateMachine", map[string]any{"stateMachineArn": arn}, "StateMachineDoesNotExist")
}

func TestStartExecutionAdmission(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatal(err)
		}
		return response.Output
	}
	fault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		if got, ok := err.(*spi.Fault); !ok || got.Code != code {
			t.Fatalf("%s fault %#v want %s", operation, err, code)
		}
	}
	activityARN := must("CreateActivity", map[string]any{"name": "admission"})["activityArn"].(string)
	definition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + activityARN + `","End":true}}}`
	machineARN := must("CreateStateMachine", map[string]any{"name": "admission", "definition": definition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	input := map[string]any{"stateMachineArn": machineARN, "name": "same", "input": `{"n":1}`}
	started := must("StartExecution", input)
	if repeated := must("StartExecution", input); repeated["executionArn"] != started["executionArn"] || repeated["startDate"] != started["startDate"] {
		t.Fatalf("idempotent running execution %#v %#v", started, repeated)
	}
	if pending, _, _ := p.col(&spi.Request{Identity: id}, "pending").List(ctx, "", "", 0); len(pending) != 1 {
		t.Fatalf("idempotent start created %d activity tasks", len(pending))
	}
	fault("StartExecution", map[string]any{"stateMachineArn": machineARN, "name": "same", "input": `{"n":2}`}, "ExecutionAlreadyExists")
	fault("StartExecution", map[string]any{"stateMachineArn": machineARN, "name": "bad name"}, "InvalidName")
	fault("StartExecution", map[string]any{"stateMachineArn": machineARN, "name": "invalid-input", "input": `{`}, "InvalidExecutionInput")
	for _, invalid := range []map[string]any{
		{"executionArn": started["executionArn"], "error": 1},
		{"executionArn": started["executionArn"], "cause": true},
		{"executionArn": started["executionArn"], "error": strings.Repeat("e", 257)},
		{"executionArn": started["executionArn"], "cause": strings.Repeat("c", 32769)},
	} {
		fault("StopExecution", invalid, "ValidationException")
	}
	if execution := must("DescribeExecution", map[string]any{"executionArn": started["executionArn"]}); execution["status"] != "RUNNING" {
		t.Fatalf("invalid stop changed execution %#v", execution)
	}
	errorText, causeText := strings.Repeat("e", 256), strings.Repeat("c", 32768)
	must("StopExecution", map[string]any{"executionArn": started["executionArn"], "error": errorText, "cause": causeText})
	if execution := must("DescribeExecution", map[string]any{"executionArn": started["executionArn"]}); execution["status"] != "ABORTED" || execution["error"] != errorText || execution["cause"] != causeText {
		t.Fatalf("stop details %#v", execution)
	}
	fault("StartExecution", input, "ExecutionAlreadyExists")
}

func TestStateListsAndHistoryPagination(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	fault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		if got, ok := err.(*spi.Fault); !ok || got.Code != code {
			t.Fatalf("%s fault %#v want %s", operation, err, code)
		}
	}
	definition := `{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Next":"Done"},"Done":{"Type":"Succeed"}}}`
	arns := map[string]string{}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		arns[name] = must("CreateStateMachine", map[string]any{"name": name, "definition": definition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	}
	machines1 := must("ListStateMachines", map[string]any{"maxResults": 2.0})
	if listed := machines1["stateMachines"].([]any); len(listed) != 2 || listed[0].(map[string]any)["definition"] != nil || machines1["nextToken"] == nil {
		t.Fatalf("state machine page %#v", machines1)
	}
	if machines2 := must("ListStateMachines", map[string]any{"nextToken": machines1["nextToken"]}); len(machines2["stateMachines"].([]any)) != 1 {
		t.Fatalf("state machine second page %#v", machines2)
	}
	fault("ListStateMachines", map[string]any{"nextToken": "bad"}, "InvalidToken")
	fault("ListStateMachines", map[string]any{"maxResults": 1.5}, "ValidationException")

	version1 := must("PublishStateMachineVersion", map[string]any{"stateMachineArn": arns["alpha"]})["stateMachineVersionArn"].(string)
	must("UpdateStateMachine", map[string]any{"stateMachineArn": arns["alpha"], "roleArn": "arn:aws:iam::1:role/second"})
	version2 := must("PublishStateMachineVersion", map[string]any{"stateMachineArn": arns["alpha"]})["stateMachineVersionArn"].(string)
	must("UpdateStateMachine", map[string]any{"stateMachineArn": arns["alpha"], "roleArn": "arn:aws:iam::1:role/third"})
	version3 := must("PublishStateMachineVersion", map[string]any{"stateMachineArn": arns["alpha"]})["stateMachineVersionArn"].(string)
	versions1 := must("ListStateMachineVersions", map[string]any{"stateMachineArn": arns["alpha"], "maxResults": 2.0})
	listedVersions := versions1["stateMachineVersions"].([]any)
	if len(listedVersions) != 2 || listedVersions[0].(map[string]any)["stateMachineVersionArn"] != version3 || listedVersions[1].(map[string]any)["stateMachineVersionArn"] != version2 {
		t.Fatalf("version page %#v", versions1)
	}
	if versions2 := must("ListStateMachineVersions", map[string]any{"stateMachineArn": arns["alpha"], "nextToken": versions1["nextToken"]}); len(versions2["stateMachineVersions"].([]any)) != 1 || versions2["stateMachineVersions"].([]any)[0].(map[string]any)["stateMachineVersionArn"] != version1 {
		t.Fatalf("version second page %#v", versions2)
	}

	alias1 := must("CreateStateMachineAlias", map[string]any{"name": "one", "routingConfiguration": []any{map[string]any{"stateMachineVersionArn": version3, "weight": 100.0}}})["stateMachineAliasArn"].(string)
	must("CreateStateMachineAlias", map[string]any{"name": "two", "routingConfiguration": []any{map[string]any{"stateMachineVersionArn": version3, "weight": 100.0}}})
	must("CreateStateMachineAlias", map[string]any{"name": "old", "routingConfiguration": []any{map[string]any{"stateMachineVersionArn": version1, "weight": 100.0}}})
	aliasPage := must("ListStateMachineAliases", map[string]any{"stateMachineArn": version3, "maxResults": 1.0})
	if len(aliasPage["stateMachineAliases"].([]any)) != 1 || aliasPage["nextToken"] == nil {
		t.Fatalf("alias page %#v", aliasPage)
	}
	if next := must("ListStateMachineAliases", map[string]any{"stateMachineArn": version3, "nextToken": aliasPage["nextToken"]}); len(next["stateMachineAliases"].([]any)) != 1 {
		t.Fatalf("alias second page %#v", next)
	}

	var executionARN string
	for _, name := range []string{"first", "second", "third"} {
		executionARN = must("StartExecution", map[string]any{"stateMachineArn": arns["alpha"], "name": name})["executionArn"].(string)
	}
	must("StartExecution", map[string]any{"stateMachineArn": version3, "name": "version"})
	must("StartExecution", map[string]any{"stateMachineArn": alias1, "name": "alias"})
	must("UpdateStateMachine", map[string]any{"stateMachineArn": arns["alpha"], "definition": `{"StartAt":"Fail","States":{"Fail":{"Type":"Fail"}}}`})
	must("StartExecution", map[string]any{"stateMachineArn": arns["alpha"], "name": "failed"})
	executions1 := must("ListExecutions", map[string]any{"stateMachineArn": arns["alpha"], "statusFilter": "SUCCEEDED", "maxResults": 2.0})
	if listed := executions1["executions"].([]any); len(listed) != 2 || listed[0].(map[string]any)["input"] != nil || executions1["nextToken"] == nil {
		t.Fatalf("execution page %#v", executions1)
	}
	if executions2 := must("ListExecutions", map[string]any{"stateMachineArn": arns["alpha"], "statusFilter": "SUCCEEDED", "nextToken": executions1["nextToken"]}); len(executions2["executions"].([]any)) != 3 {
		t.Fatalf("execution second page %#v", executions2)
	}
	if versions := must("ListExecutions", map[string]any{"stateMachineArn": version3})["executions"].([]any); len(versions) != 2 {
		t.Fatalf("version executions %#v", versions)
	}
	if aliases := must("ListExecutions", map[string]any{"stateMachineArn": alias1})["executions"].([]any); len(aliases) != 1 {
		t.Fatalf("alias executions %#v", aliases)
	}
	fault("ListExecutions", map[string]any{"stateMachineArn": arns["alpha"], "redriveFilter": "REDRIVEN"}, "ValidationException")
	fault("ListExecutions", map[string]any{}, "ValidationException")
	expressARN := must("CreateStateMachine", map[string]any{"name": "express-list", "definition": definition, "roleArn": testRoleARN, "type": "EXPRESS"})["stateMachineArn"].(string)
	fault("ListExecutions", map[string]any{"stateMachineArn": expressARN}, "StateMachineTypeNotSupported")

	history1 := must("GetExecutionHistory", map[string]any{"executionArn": executionARN, "maxResults": 1.0})
	if len(history1["events"].([]any)) != 1 || history1["nextToken"] == nil {
		t.Fatalf("history page %#v", history1)
	}
	history2 := must("GetExecutionHistory", map[string]any{"executionArn": executionARN, "nextToken": history1["nextToken"]})
	if len(history2["events"].([]any)) != 1 {
		t.Fatalf("history second page %#v", history2)
	}
	reversed := must("GetExecutionHistory", map[string]any{"executionArn": executionARN, "reverseOrder": true, "maxResults": 1.0})["events"].([]any)
	if reversed[0].(map[string]any)["type"] != "SucceedStateEntered" {
		t.Fatalf("reversed history %#v", reversed)
	}
	expressExecution := must("StartExecution", map[string]any{"stateMachineArn": expressARN})["executionArn"].(string)
	fault("DescribeExecution", map[string]any{"executionArn": expressExecution}, "StateMachineTypeNotSupported")
	fault("GetExecutionHistory", map[string]any{"executionArn": expressExecution}, "StateMachineTypeNotSupported")
	fault("StopExecution", map[string]any{"executionArn": expressExecution}, "StateMachineTypeNotSupported")

	for _, name := range []string{"first", "second"} {
		must("CreateActivity", map[string]any{"name": name})
	}
	activities := must("ListActivities", map[string]any{"maxResults": 1.0})
	if len(activities["activities"].([]any)) != 1 || activities["nextToken"] == nil {
		t.Fatalf("activity page %#v", activities)
	}
}

func TestChoiceRules(t *testing.T) {
	data := map[string]any{
		"s": "alpha/beta", "s2": "omega", "literal": "alpha/*", "n": 2.0, "n2": 3.0, "b": true, "b2": true,
		"ts": "2026-01-02T03:04:05Z", "tsOffset": "2026-01-02T05:04:05+02:00", "ts2": "2026-01-03T03:04:05Z", "null": nil,
	}
	matching := []map[string]any{
		{"Variable": "$.s", "StringEquals": "alpha/beta"},
		{"Variable": "$.s", "StringEqualsPath": "$.s"},
		{"Variable": "$.s", "StringLessThan": "z"},
		{"Variable": "$.s", "StringLessThanEqualsPath": "$.s"},
		{"Variable": "$.s2", "StringGreaterThan": "alpha"},
		{"Variable": "$.s2", "StringGreaterThanEqualsPath": "$.s2"},
		{"Variable": "$.s", "StringMatches": `alpha/*`},
		{"Variable": "$.literal", "StringMatches": `alpha/\*`},
		{"Variable": "$.n", "NumericEquals": 2.0},
		{"Variable": "$.n", "NumericEqualsPath": "$.n"},
		{"Variable": "$.n", "NumericLessThan": 3.0},
		{"Variable": "$.n", "NumericLessThanEqualsPath": "$.n"},
		{"Variable": "$.n2", "NumericGreaterThan": 2.0},
		{"Variable": "$.n2", "NumericGreaterThanEqualsPath": "$.n2"},
		{"Variable": "$.b", "BooleanEquals": true},
		{"Variable": "$.b", "BooleanEqualsPath": "$.b2"},
		{"Variable": "$.ts", "TimestampEquals": "2026-01-02T03:04:05Z"},
		{"Variable": "$.ts", "TimestampEqualsPath": "$.tsOffset"},
		{"Variable": "$.ts", "TimestampLessThan": "2026-01-03T03:04:05Z"},
		{"Variable": "$.ts", "TimestampLessThanEqualsPath": "$.ts"},
		{"Variable": "$.ts2", "TimestampGreaterThan": "2026-01-02T03:04:05Z"},
		{"Variable": "$.ts2", "TimestampGreaterThanEqualsPath": "$.ts2"},
		{"Variable": "$.s", "IsString": true},
		{"Variable": "$.n", "IsNumeric": true},
		{"Variable": "$.b", "IsBoolean": true},
		{"Variable": "$.ts", "IsTimestamp": true},
		{"Variable": "$.null", "IsNull": true},
		{"Variable": "$.null", "IsPresent": true},
		{"Variable": "$.missing", "IsPresent": false},
		{"And": []any{map[string]any{"Variable": "$.b", "BooleanEquals": true}, map[string]any{"Variable": "$.n", "NumericGreaterThan": 1.0}}},
		{"Or": []any{map[string]any{"Variable": "$.missing", "IsPresent": true}, map[string]any{"Variable": "$.s", "StringEquals": "alpha/beta"}}},
		{"Not": map[string]any{"Variable": "$.n", "NumericEquals": 3.0}},
	}
	for _, rule := range matching {
		if !matchChoice(rule, data) {
			t.Fatalf("choice did not match %#v", rule)
		}
	}
	notMatching := []map[string]any{
		{"Variable": "$.missing", "IsPresent": true},
		{"Variable": "$.null", "IsNull": false},
		{"Variable": "$.n", "StringEquals": "2"},
		{"Variable": "$.s", "NumericEquals": 0.0},
		{"Variable": "$.s", "StringMatches": `alpha/\*`},
		{"Variable": "$.s", "TimestampEquals": "not-a-time"},
		{"And": []any{map[string]any{"Variable": "$.b", "BooleanEquals": true}, map[string]any{"Variable": "$.n", "NumericGreaterThan": 3.0}}},
		{"Or": []any{map[string]any{"Variable": "$.missing", "IsPresent": true}}},
		{"Not": map[string]any{"Variable": "$.n", "NumericEquals": 2.0}},
	}
	for _, rule := range notMatching {
		if matchChoice(rule, data) {
			t.Fatalf("choice unexpectedly matched %#v", rule)
		}
	}
	if value, present := jsonPathLookup(data, "$.null"); !present || value != nil {
		t.Fatalf("null lookup %#v %v", value, present)
	}
}

func TestStatesJSONataBehavior(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := handler.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	queue := sqs.New(deps)
	queueURL := invoke(queue, "CreateQueue", map[string]any{"QueueName": "jsonata"})["QueueUrl"].(string)
	scope := jsonataScope{input: map[string]any{"values": []any{1.0, 2.0, 3.0}, "encoded": `{"n":3}`}, context: map[string]any{}, variables: map[string]any{}, random: deps.Rand}
	for _, expression := range []string{
		"{% $partition($states.input.values, 2) %}", "{% $parse($states.input.encoded).n %}", "{% $hash('mirror', 'SHA-256') %}", "{% $random(7) %}", "{% $uuid() %}",
	} {
		if value, ok := evalJSONataValue(expression, scope); !ok || value == nil {
			t.Fatalf("jsonata expression %s: %#v %v", expression, value, ok)
		}
	}
	itemScope := jsonataScope{input: map[string]any{"batch": []any{1.0, 2.0}, "index": 0.0}, context: map[string]any{}, variables: map[string]any{"prefix": "v"}, random: deps.Rand}
	for expression, expected := range map[string]any{
		"{% $prefix %}": "v", "{% $string($states.input.index) %}": "0", "{% $join($map($states.input.batch, function($v){$string($v)}), ',') %}": "1,2",
	} {
		if value, ok := evalJSONataValue(expression, itemScope); !ok || value != expected {
			t.Fatalf("jsonata item subexpression %s: %#v %v", expression, value, ok)
		}
	}
	if value, ok := evalJSONataValue("{% $prefix & $string($states.input.index) & ':' & $join($map($states.input.batch, function($v){$string($v)}), ',') %}", itemScope); !ok || value != "v0:1,2" {
		t.Fatalf("jsonata item expression %#v %v", value, ok)
	}
	definition, _ := json.Marshal(map[string]any{
		"QueryLanguage": "JSONata", "StartAt": "Prepare", "States": map[string]any{
			"Prepare": map[string]any{
				"Type": "Pass", "Assign": map[string]any{"prefix": "v", "threshold": "{% 2 %}"},
				"Output": map[string]any{
					"values": "{% $partition($states.input.values, 2) %}", "parsed": "{% $parse($states.input.encoded).n %}",
					"hash": "{% $hash('mirror', 'SHA-256') %}", "seeded": "{% $random(7) %}", "uuid": "{% $uuid() %}",
				}, "Next": "Choose",
			},
			"Choose": map[string]any{
				"Type": "Choice", "Assign": map[string]any{"chosen": "{% true %}"},
				"Choices": []any{map[string]any{"Condition": "{% $threshold = 2 and $states.input.parsed = 3 %}", "Next": "Map"}}, "Default": "Wrong",
			},
			"Map": map[string]any{
				"Type": "Map", "Items": "{% $states.input.values %}",
				"ItemSelector": map[string]any{
					"batch": "{% $states.context.Map.Item.Value %}", "index": "{% $states.context.Map.Item.Index %}", "prefix": "{% $prefix %}",
				},
				"ItemProcessor": map[string]any{"StartAt": "Format", "States": map[string]any{
					"Format": map[string]any{"Type": "Pass", "Output": "{% $prefix & $string($states.input.index) & ':' & $join($map($states.input.batch, function($v){$string($v)}), ',') %}", "End": true},
				}},
				"Assign": map[string]any{"mapped": "{% $count($states.result) %}"}, "Next": "Parallel",
			},
			"Parallel": map[string]any{
				"Type": "Parallel", "Branches": []any{
					map[string]any{"StartAt": "Left", "States": map[string]any{"Left": map[string]any{"Type": "Pass", "Output": map[string]any{"left": "L"}, "End": true}}},
					map[string]any{"StartAt": "Right", "States": map[string]any{"Right": map[string]any{"Type": "Pass", "Output": map[string]any{"right": "R"}, "End": true}}},
				}, "Output": "{% $merge($states.result) %}", "Next": "Send",
			},
			"Send": map[string]any{
				"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage",
				"Arguments": map[string]any{"QueueUrl": queueURL, "MessageBody": "{% $states.input.left & $states.input.right & ':' & $string($mapped) %}"},
				"Assign":    map[string]any{"sentId": "{% $states.result.MessageId %}"},
				"Output":    map[string]any{"body": "{% $states.input.left & $states.input.right %}", "count": "{% $mapped %}"}, "Next": "Done",
			},
			"Done":  map[string]any{"Type": "Succeed", "Output": map[string]any{"body": "{% $states.input.body %}", "count": "{% $states.input.count %}", "sent": "{% $sentId %}"}},
			"Wrong": map[string]any{"Type": "Fail"},
		},
	})
	if diagnostics := validateDefinition(string(definition), "EXPRESS"); len(diagnostics) != 0 {
		t.Fatalf("jsonata diagnostics %#v", diagnostics)
	}
	invalidDefinitions := []string{
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Pass","Parameters":{},"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","Output":"{% 1 %}","End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Choice","Choices":[{"Variable":"$.x","NumericEquals":1,"Next":"Done"}]},"Done":{"Type":"Succeed"}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"QueryLanguage":"JSONPath","Type":"Succeed"}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Succeed","Output":" {% 1 %}"}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Pass","Output":"{% $states.result %}","End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Succeed","Output":"{% $states.errorOutput %}"}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Succeed","Output":"{% $eval('1') %}"}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.error","Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Outer","States":{"Outer":{"Type":"Pass","Assign":{"value":"{% 1 %}"},"Next":"Map"},"Map":{"Type":"Map","Items":"{% [] %}","ItemProcessor":{"StartAt":"Inner","States":{"Inner":{"Type":"Pass","Assign":{"value":"{% 2 %}"},"End":true}}},"End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","TimeoutSeconds":"later","End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Map","Items":"{% [] %}","ToleratedFailurePercentage":101,"ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":1,"MaxConcurrencyPath":"$.limit","ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Map","Items":{"a":1},"ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Map","ItemsPath":"$.items","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"INVALID"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`,
	}
	for _, invalid := range invalidDefinitions {
		if diagnostics := validateDefinition(invalid); len(diagnostics) == 0 {
			t.Fatalf("invalid JSONata definition accepted %s", invalid)
		}
	}
	expressDistributed := `{"StartAt":"Bad","States":{"Bad":{"Type":"Map","ItemsPath":"$.items","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`
	if diagnostics := validateDefinition(expressDistributed, "EXPRESS"); len(diagnostics) == 0 {
		t.Fatal("Express Distributed Map accepted")
	}
	for _, name := range []string{"alpha", "éclair", "a\u0301", "a‿b", "℘value"} {
		if !validVariableName(name) {
			t.Fatalf("valid UAX31 variable rejected %q", name)
		}
	}
	for _, name := range []string{"_private", "1st", "a-b", strings.Repeat("x", 81)} {
		if validVariableName(name) {
			t.Fatalf("invalid UAX31 variable accepted %q", name)
		}
	}
	machine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata", "definition": string(definition), "roleArn": testRoleARN, "type": "EXPRESS"})
	execution := invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"], "input": `{"values":[1,2,3],"encoded":"{\"n\":3}"}`})
	if execution["status"] != "SUCCEEDED" {
		t.Fatalf("jsonata execution %#v", execution)
	}
	var output map[string]any
	if json.Unmarshal([]byte(execution["output"].(string)), &output) != nil || output["body"] != "LR" || output["count"] != 2.0 || output["sent"] == "" {
		t.Fatalf("jsonata output %#v", execution)
	}
	messages := invoke(queue, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})["Messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["Body"] != "LR:2" {
		t.Fatalf("jsonata task arguments %#v", messages)
	}

	bad := `{"QueryLanguage":"JSONata","StartAt":"Bad","States":{"Bad":{"Type":"Pass","Output":"{% $states.input.missing %}","End":true}}}`
	badMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata-error", "definition": bad, "roleArn": testRoleARN, "type": "EXPRESS"})
	failed := invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": badMachine["stateMachineArn"]})
	if failed["status"] != "FAILED" || failed["error"] != "States.QueryEvaluationError" {
		t.Fatalf("jsonata error %#v", failed)
	}
}

func TestStatesJSONPathVariables(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := handler.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	queue := sqs.New(deps)
	queueURL := invoke(queue, "CreateQueue", map[string]any{"QueueName": "jsonpath-variables"})["QueueUrl"].(string)
	definition, _ := json.Marshal(map[string]any{
		"StartAt": "Seed", "States": map[string]any{
			"Seed": map[string]any{
				"Type": "Pass", "Result": map[string]any{"prefix": "p"}, "ResultPath": nil,
				"Assign": map[string]any{"prefix.$": "$.prefix"}, "Next": "Prepare",
			},
			"Prepare": map[string]any{
				"Type": "Pass", "Parameters": map[string]any{
					"items.$": "$.seed.items", "wait": 0, "choice": "go", "expected": "go",
				},
				"Assign": map[string]any{
					"items.$": "$.items", "delay.$": "$.wait", "choice.$": "$.choice", "expected.$": "$.expected",
					"details":    map[string]any{"lineItems.$": "$.items", "start.$": "$$.Execution.StartTime"},
					"prepared.$": "States.Format('{}:{}', $prefix, $.choice)",
				}, "Next": "Wait",
			},
			"Wait": map[string]any{
				"Type": "Wait", "SecondsPath": "$delay", "Assign": map[string]any{"waitSeen.$": "$choice"}, "Next": "Choose",
			},
			"Choose": map[string]any{
				"Type": "Choice", "Choices": []any{map[string]any{
					"Variable": "$choice", "StringEqualsPath": "$expected", "Assign": map[string]any{"matched.$": "$choice"}, "Next": "Map",
				}}, "Default": "Wrong",
			},
			"Map": map[string]any{
				"Type": "Map", "ItemsPath": "$items", "ItemSelector": map[string]any{
					"value.$": "$$.Map.Item.Value", "label.$": "States.Format('{}{}', $prefix, $$.Map.Item.Value)",
				}, "ItemProcessor": map[string]any{"StartAt": "Item", "States": map[string]any{
					"Item": map[string]any{"Type": "Pass", "End": true},
				}}, "Assign": map[string]any{"mapCount.$": "States.ArrayLength($)"}, "Next": "Parallel",
			},
			"Parallel": map[string]any{
				"Type": "Parallel", "Branches": []any{
					map[string]any{"StartAt": "Left", "States": map[string]any{"Left": map[string]any{"Type": "Pass", "Result": "L", "End": true}}},
					map[string]any{"StartAt": "Right", "States": map[string]any{"Right": map[string]any{"Type": "Pass", "Result": "R", "End": true}}},
				}, "Assign": map[string]any{"parallelCount.$": "States.ArrayLength($)"}, "Next": "Send",
			},
			"Send": map[string]any{
				"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage", "Parameters": map[string]any{
					"QueueUrl": queueURL, "MessageBody.$": "States.Format('{}:{}:{}', $prefix, $mapCount, $parallelCount)",
				}, "ResultSelector": map[string]any{"id.$": "$.MessageId", "prefix.$": "$prefix"},
				"Assign": map[string]any{"messageId.$": "$.MessageId", "message.$": "States.Format('{}:{}:{}', $prefix, $mapCount, $parallelCount)"}, "Next": "Collect",
			},
			"Collect": map[string]any{
				"Type": "Pass", "Parameters": map[string]any{
					"details.$": "$details", "first.$": "$details.lineItems[0]", "prepared.$": "$prepared", "wait.$": "$waitSeen", "matched.$": "$matched", "message.$": "$message", "id.$": "$messageId",
				}, "End": true,
			},
			"Wrong": map[string]any{"Type": "Fail"},
		},
	})
	if diagnostics := validateDefinition(string(definition), "EXPRESS"); len(diagnostics) != 0 {
		t.Fatalf("jsonpath variable diagnostics %#v", diagnostics)
	}
	machine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonpath-variables", "definition": string(definition), "roleArn": testRoleARN, "type": "EXPRESS"})
	execution := invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"], "input": `{"seed":{"items":[1,2]}}`})
	if execution["status"] != "SUCCEEDED" {
		t.Fatalf("jsonpath variable execution %#v", execution)
	}
	var output map[string]any
	if json.Unmarshal([]byte(execution["output"].(string)), &output) != nil || output["first"] != 1.0 || output["prepared"] != "p:go" || output["wait"] != "go" || output["matched"] != "go" || output["message"] != "p:2:2" || output["id"] == "" {
		t.Fatalf("jsonpath variable output %#v", execution)
	}
	details, _ := output["details"].(map[string]any)
	if fmtString(details["lineItems"]) != `[1,2]` || details["start"] == "" {
		t.Fatalf("jsonpath nested assignment %#v", details)
	}
	messages := invoke(queue, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})["Messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["Body"] != "p:2:2" {
		t.Fatalf("jsonpath task variables %#v", messages)
	}

	isolationDefinition := `{"StartAt":"Store","States":{"Store":{"Type":"Pass","Assign":{"items.$":"$.items"},"Next":"Map"},"Map":{"Type":"Map","ItemsPath":"$items","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`
	isolationMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonpath-variable-isolation", "definition": isolationDefinition, "roleArn": testRoleARN})
	isolationARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": isolationMachine["stateMachineArn"], "input": `{"items":[1]}`})["executionArn"].(string)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": isolationARN}); execution["status"] != "FAILED" || execution["cause"] != "States.Runtime" {
		t.Fatalf("JSONPath Distributed Map variable isolation %#v", execution)
	}
	badDefinition := `{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","Assign":{"missing.$":"$.missing"},"End":true}}}`
	badMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonpath-variable-error", "definition": badDefinition, "roleArn": testRoleARN, "type": "EXPRESS"})
	if execution := invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": badMachine["stateMachineArn"]}); execution["status"] != "FAILED" || execution["error"] != "States.Runtime" {
		t.Fatalf("JSONPath missing variable assignment %#v", execution)
	}
}

func TestVariableAssignmentLimits(t *testing.T) {
	variables := map[string]any{}
	if !commitAssignments(variables, map[string]any{"ok": "value"}) || variables["ok"] != "value" {
		t.Fatalf("small assignment rejected %#v", variables)
	}
	if commitAssignments(variables, map[string]any{"oversized": strings.Repeat("x", 256*1024)}) {
		t.Fatal("oversized variable accepted")
	}
	if commitAssignments(variables, map[string]any{"left": strings.Repeat("x", 128*1024), "right": strings.Repeat("x", 128*1024)}) {
		t.Fatal("oversized Assign accepted")
	}
	large := map[string]any{}
	for index := range 40 {
		large[string(rune('A'+index))] = strings.Repeat("x", 250*1024)
	}
	if commitAssignments(large, map[string]any{"more": strings.Repeat("x", 250*1024)}) {
		t.Fatal("execution variable limit exceeded")
	}
}

func TestStatesPayloadLimits(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	large := strings.Repeat("x", 256*1024)
	if validStatePayload(large) || !validStatePayload(strings.Repeat("x", 256*1024-2)) {
		t.Fatal("payload byte boundary")
	}
	walk := func(machine map[string]any, input any) walkResult {
		t.Helper()
		definition, _ := json.Marshal(machine)
		return p.walk(ctx, &spi.Request{Identity: id}, string(definition), "", input, nil)
	}
	pass := walk(map[string]any{"StartAt": "Pass", "States": map[string]any{"Pass": map[string]any{"Type": "Pass", "Result": large, "End": true}}}, nil)
	if pass.status != "FAILED" || pass.errorName != "States.DataLimitExceeded" {
		t.Fatalf("oversized Pass %#v", pass)
	}
	oversizedInput := walk(map[string]any{"StartAt": "Done", "States": map[string]any{"Done": map[string]any{"Type": "Pass", "InputPath": nil, "End": true}}}, large)
	if oversizedInput.status != "FAILED" || oversizedInput.errorName != "States.DataLimitExceeded" {
		t.Fatalf("oversized state input %#v", oversizedInput)
	}
	branch := func() map[string]any {
		return map[string]any{"StartAt": "Pass", "States": map[string]any{"Pass": map[string]any{"Type": "Pass", "Result": strings.Repeat("x", 140*1024), "End": true}}}
	}
	parallel := walk(map[string]any{"StartAt": "Parallel", "States": map[string]any{"Parallel": map[string]any{"Type": "Parallel", "Branches": []any{branch(), branch()}, "ResultSelector": map[string]any{"count": 2.0}, "End": true}}}, nil)
	if parallel.status != "FAILED" || parallel.errorName != "States.DataLimitExceeded" {
		t.Fatalf("oversized Parallel %#v", parallel)
	}
	iterator := branch()
	mapped := walk(map[string]any{"StartAt": "Map", "States": map[string]any{"Map": map[string]any{"Type": "Map", "ItemsPath": "$.items", "ItemProcessor": iterator, "ResultSelector": map[string]any{"count": 2.0}, "End": true}}}, map[string]any{"items": []any{1.0, 2.0}})
	if mapped.status != "FAILED" || mapped.errorName != "States.DataLimitExceeded" {
		t.Fatalf("oversized Map %#v", mapped)
	}

	queue := sqs.New(deps)
	queueResponse, err := queue.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "payload-limit"}})
	if err != nil {
		t.Fatal(err)
	}
	queueURL := queueResponse.Output["QueueUrl"].(string)
	taskDefinition, _ := json.Marshal(map[string]any{"StartAt": "Task", "States": map[string]any{
		"Task":      map[string]any{"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage", "Parameters": map[string]any{"QueueUrl": queueURL, "MessageBody": large}, "Catch": []any{map[string]any{"ErrorEquals": []any{"States.DataLimitExceeded"}, "Next": "Recovered"}}, "End": true},
		"Recovered": map[string]any{"Type": "Succeed"},
	}})
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStateMachine", Input: map[string]any{"name": "payload-limit", "definition": string(taskDefinition), "roleArn": testRoleARN, "type": "EXPRESS"}})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "StartSyncExecution", Input: map[string]any{"stateMachineArn": created.Output["stateMachineArn"]}})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Output["status"] != "FAILED" || execution.Output["error"] != "States.DataLimitExceeded" {
		t.Fatalf("oversized Task payload %#v", execution.Output)
	}
	messages, err := queue.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueUrl": queueURL}})
	if err != nil || len(messages.Output["Messages"].([]any)) != 0 {
		t.Fatalf("oversized Task invoked SQS %#v %v", messages, err)
	}

	table := dynamodb.New(deps)
	if _, err := table.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTable", Input: map[string]any{"TableName": "payload-limit", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutItem", Input: map[string]any{"TableName": "payload-limit", "Item": map[string]any{"id": map[string]any{"S": "1"}, "body": map[string]any{"S": large}}}}); err != nil {
		t.Fatal(err)
	}
	resultDefinition := `{"StartAt":"Read","States":{"Read":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:dynamodb:getItem","Parameters":{"TableName":"payload-limit","Key":{"id":{"S":"1"}}},"ResultSelector":{"id.$":"$.Item.id"},"End":true}}}`
	resultMachine, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStateMachine", Input: map[string]any{"name": "result-payload-limit", "definition": resultDefinition, "roleArn": testRoleARN, "type": "EXPRESS"}})
	if err != nil {
		t.Fatal(err)
	}
	resultExecution, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "StartSyncExecution", Input: map[string]any{"stateMachineArn": resultMachine.Output["stateMachineArn"]}})
	if err != nil {
		t.Fatal(err)
	}
	if resultExecution.Output["status"] != "FAILED" || resultExecution.Output["error"] != "States.DataLimitExceeded" {
		t.Fatalf("oversized Task result %#v", resultExecution.Output)
	}
}

func TestStatesDurableWait(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	definition := `{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":10,"Assign":{"waited":"yes"},"Next":"Done"},"Done":{"Type":"Succeed","QueryLanguage":"JSONata","Output":"{% $waited %}"}}}`
	machine := invoke("CreateStateMachine", map[string]any{"name": "durable-wait", "definition": definition, "roleArn": testRoleARN})
	executionARN := invoke("StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"]})["executionArn"].(string)
	if execution := invoke("DescribeExecution", map[string]any{"executionArn": executionARN}); execution["status"] != "RUNNING" {
		t.Fatalf("Wait completed early %#v", execution)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	if err := deps.Clock.Advance(9 * time.Second); err != nil {
		t.Fatal(err)
	}
	if execution := invoke("DescribeExecution", map[string]any{"executionArn": executionARN}); execution["status"] != "RUNNING" {
		t.Fatalf("restored Wait completed early %#v", execution)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	var execution map[string]any
	for range 100 {
		execution = invoke("DescribeExecution", map[string]any{"executionArn": executionARN})
		if execution["status"] == "SUCCEEDED" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if execution["status"] != "SUCCEEDED" || execution["output"] != `"yes"` {
		t.Fatalf("restored Wait did not resume %#v", execution)
	}

	express := invoke("CreateStateMachine", map[string]any{"name": "sync-wait", "definition": `{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":3,"End":true}}}`, "roleArn": testRoleARN, "type": "EXPRESS"})
	result := make(chan map[string]any, 1)
	go func() {
		result <- invoke("StartSyncExecution", map[string]any{"stateMachineArn": express["stateMachineArn"]})
	}()
	select {
	case early := <-result:
		t.Fatalf("Express Wait completed early %#v", early)
	case <-time.After(10 * time.Millisecond):
	}
	if err := deps.Clock.Advance(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case finished := <-result:
		if finished["status"] != "SUCCEEDED" {
			t.Fatalf("Express Wait %#v", finished)
		}
	case <-time.After(time.Second):
		t.Fatal("Express Wait did not resume")
	}
}

func TestStatesRetryScheduling(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	queue := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := handler.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	queueURL := invoke(queue, "CreateQueue", map[string]any{"QueueName": "retry-scheduling"})["QueueUrl"].(string)
	definition := `{"QueryLanguage":"JSONata","StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Arguments":{"QueueUrl":"{% $states.context.State.RetryCount > 0 ? '` + queueURL + `' : $states.input.missing %}","MessageBody":"retry"},"Output":{"retry":"{% $states.context.State.RetryCount %}"},"Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"IntervalSeconds":5,"MaxDelaySeconds":2,"BackoffRate":3,"MaxAttempts":1}],"End":true}}}`
	machine := invoke(p, "CreateStateMachine", map[string]any{"name": "durable-retry", "definition": definition, "roleArn": testRoleARN})
	executionARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"]})["executionArn"].(string)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": executionARN}); execution["status"] != "RUNNING" {
		t.Fatalf("Retry completed early %#v", execution)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": executionARN}); execution["status"] != "RUNNING" {
		t.Fatalf("restored Retry completed early %#v", execution)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	var execution map[string]any
	for range 100 {
		execution = invoke(p, "DescribeExecution", map[string]any{"executionArn": executionARN})
		if execution["status"] == "SUCCEEDED" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if execution["status"] != "SUCCEEDED" || execution["output"] != `{"retry":1}` {
		t.Fatalf("restored Retry did not resume %#v", execution)
	}

	express := invoke(p, "CreateStateMachine", map[string]any{"name": "sync-retry", "definition": definition, "roleArn": testRoleARN, "type": "EXPRESS"})
	result := make(chan map[string]any, 1)
	go func() {
		result <- invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": express["stateMachineArn"]})
	}()
	select {
	case early := <-result:
		t.Fatalf("Express Retry completed early %#v", early)
	case <-time.After(10 * time.Millisecond):
	}
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case finished := <-result:
		if finished["status"] != "SUCCEEDED" || finished["output"] != `{"retry":1}` {
			t.Fatalf("Express Retry %#v", finished)
		}
	case <-time.After(time.Second):
		t.Fatal("Express Retry did not resume")
	}

	retrier := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Boom"}, "IntervalSeconds": 3.0, "BackoffRate": 2.0, "MaxDelaySeconds": 5.0}}}
	attempts := map[int]int{}
	for i, want := range []time.Duration{3 * time.Second, 5 * time.Second, 5 * time.Second} {
		if got, retry := retryTask(retrier, "Boom", attempts, p.deps.Rand); !retry || got != want {
			t.Fatalf("retry delay %d = %s, %v want %s", i, got, retry, want)
		}
	}
	jitter := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Boom"}, "IntervalSeconds": 2.0, "JitterStrategy": "FULL"}}}
	if delay, retry := retryTask(jitter, "Boom", map[int]int{}, p.deps.Rand); !retry || delay < 0 || delay >= 2*time.Second {
		t.Fatalf("jitter delay %s, %v", delay, retry)
	}
	if delay, retry := retryTask(map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Boom"}}}}, "Boom", map[int]int{}, p.deps.Rand); !retry || delay != time.Second {
		t.Fatalf("default retry delay %s, %v", delay, retry)
	}

	invalid := `{"StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Retry":[{"ErrorEquals":["States.ALL","Boom"],"IntervalSeconds":0,"MaxAttempts":-1,"BackoffRate":0.5,"MaxDelaySeconds":31622401,"JitterStrategy":"SOME"}],"End":true}}}`
	if diagnostics := validateDefinition(invalid); len(diagnostics) != 6 {
		t.Fatalf("retry diagnostics %#v", diagnostics)
	}
}

func TestStatesCatchValidation(t *testing.T) {
	valid := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Catch":[{"ErrorEquals":["Boom"],"Next":"Done"},{"ErrorEquals":["States.ALL"],"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`
	if diagnostics := validateDefinition(valid); len(diagnostics) != 0 {
		t.Fatalf("valid Catch diagnostics %#v", diagnostics)
	}
	for _, resultPath := range []string{"null", `"$.error"`} {
		definition := fmt.Sprintf(`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["Boom"],"ResultPath":%s,"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`, resultPath)
		if diagnostics := validateDefinition(definition); len(diagnostics) != 0 {
			t.Fatalf("valid Catch ResultPath diagnostics %#v", diagnostics)
		}
	}
	for _, definition := range []string{
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","Catch":[{"ErrorEquals":["Boom"],"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":{},"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[],"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[1],"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":"Boom","Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":[1],"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["States.ALL","Boom"],"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["States.ALL"],"Next":"Done"},{"ErrorEquals":["Boom"],"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["Boom"]}],"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["Boom"],"ResultPath":1,"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["Boom"],"ResultPath":"error","Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) == 0 {
			t.Fatalf("accepted invalid Catch %s", definition)
		}
	}
}

func TestStatesTransitionValidation(t *testing.T) {
	for _, definition := range []string{
		`{"StartAt":"First","States":{"First":{"Type":"Pass","Next":"Done"},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Done","States":{"Done":{"Type":"Pass","End":true}}}`,
		`{"StartAt":"Choose","States":{"Choose":{"Type":"Choice","Choices":[{"Variable":"$.ready","IsPresent":true,"Next":"Done"}],"Default":"Done"},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Done","States":{"Done":{"Type":"Fail"}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 0 {
			t.Fatalf("valid transition diagnostics %#v", diagnostics)
		}
	}
	for _, definition := range []string{
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","Next":"Done","End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","End":false}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","End":"true"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Succeed","Next":"Done"},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Succeed","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Fail","Next":"Done"},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Fail","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Choice","Choices":[{"Variable":"$.ready","IsPresent":true,"Next":"Done"}],"Default":"Done","Next":"Done"},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Choice","Choices":[{"Variable":"$.ready","IsPresent":true,"Next":"Done"}],"Default":"Done","End":true},"Done":{"Type":"Succeed"}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("transition diagnostics = %#v for %s", diagnostics, definition)
		}
	}
}

func TestStatesChoiceValidation(t *testing.T) {
	definition := func(queryLanguage, rule string) string {
		return fmt.Sprintf(`{"QueryLanguage":%q,"StartAt":"Choose","States":{"Choose":{"Type":"Choice","Choices":[%s],"Default":"Done"},"Done":{"Type":"Succeed"}}}`, queryLanguage, rule)
	}
	for _, test := range []struct{ queryLanguage, rule string }{
		{"JSONPath", `{"Variable":"$.value","StringEquals":"ok","Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","StringEqualsPath":"$.other","Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","TimestampEquals":"2026-08-24T12:00:00Z","Next":"Done"}`},
		{"JSONPath", `{"And":[{"Variable":"$.value","IsPresent":true},{"Variable":"$.value","NumericGreaterThan":1}],"Next":"Done"}`},
		{"JSONata", `{"Condition":true,"Next":"Done"}`},
		{"JSONata", `{"Condition":"{% $states.input.value = 1 %}","Next":"Done"}`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.rule)); len(diagnostics) != 0 {
			t.Fatalf("valid Choice diagnostics %#v for %s", diagnostics, test.rule)
		}
	}
	for _, test := range []struct{ queryLanguage, rule string }{
		{"JSONPath", `{"Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","StringEquals":"ok","IsPresent":true,"Next":"Done"}`},
		{"JSONPath", `{"Condition":true,"Next":"Done"}`},
		{"JSONPath", `{"And":[],"Next":"Done"}`},
		{"JSONPath", `{"And":{},"Next":"Done"}`},
		{"JSONPath", `{"And":[{"Variable":"$.value","IsPresent":true,"Next":"Done"}],"Next":"Done"}`},
		{"JSONPath", `{"Not":1,"Next":"Done"}`},
		{"JSONPath", `{"StringEquals":"ok","Next":"Done"}`},
		{"JSONPath", `{"Variable":"value","StringEquals":"ok","Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","NumericEquals":"1","Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","BooleanEquals":"true","Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","IsPresent":1,"Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","StringEqualsPath":"other","Next":"Done"}`},
		{"JSONPath", `{"Variable":"$.value","TimestampEquals":"2026-08-24T12:00:00+01:00","Next":"Done"}`},
		{"JSONata", `{"Next":"Done"}`},
		{"JSONata", `{"Condition":1,"Next":"Done"}`},
		{"JSONata", `{"Condition":"plain text","Next":"Done"}`},
		{"JSONata", `{"Condition":true,"StringEquals":"ok","Next":"Done"}`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.rule)); len(diagnostics) != 1 {
			t.Fatalf("Choice diagnostics = %#v for %s", diagnostics, test.rule)
		}
	}
}

func TestStatesWaitValidation(t *testing.T) {
	definition := func(queryLanguage, fields string) string {
		return fmt.Sprintf(`{"QueryLanguage":%q,"StartAt":"Wait","States":{"Wait":{"Type":"Wait",%s,"End":true}}}`, queryLanguage, fields)
	}
	for _, test := range []struct{ queryLanguage, fields string }{
		{"JSONPath", `"Seconds":0`},
		{"JSONPath", `"Seconds":99999999`},
		{"JSONPath", `"Timestamp":"2026-08-24T12:00:00Z"`},
		{"JSONPath", `"SecondsPath":"$.delay"`},
		{"JSONPath", `"TimestampPath":"$.time"`},
		{"JSONata", `"Seconds":"{% $states.input.delay %}"`},
		{"JSONata", `"Timestamp":"{% $states.input.time %}"`},
		{"JSONata", `"Timestamp":"2026-08-24T12:00:00Z"`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.fields)); len(diagnostics) != 0 {
			t.Fatalf("valid Wait diagnostics %#v for %s", diagnostics, test.fields)
		}
	}
	for _, test := range []struct{ queryLanguage, fields string }{
		{"JSONPath", `"Comment":"missing"`},
		{"JSONPath", `"Seconds":1,"Timestamp":"2026-08-24T12:00:00Z"`},
		{"JSONPath", `"Seconds":-1`},
		{"JSONPath", `"Seconds":100000000`},
		{"JSONPath", `"Seconds":1.5`},
		{"JSONPath", `"Seconds":"1"`},
		{"JSONPath", `"SecondsPath":"delay"`},
		{"JSONPath", `"SecondsPath":1`},
		{"JSONPath", `"Timestamp":"later"`},
		{"JSONPath", `"Timestamp":"2026-08-24T12:00:00+01:00"`},
		{"JSONPath", `"Timestamp":1`},
		{"JSONPath", `"TimestampPath":"time"`},
		{"JSONPath", `"TimestampPath":1`},
		{"JSONata", `"Seconds":"1"`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.fields)); len(diagnostics) != 1 {
			t.Fatalf("Wait diagnostics = %#v for %s", diagnostics, test.fields)
		}
	}
}

func TestStatesFailValidation(t *testing.T) {
	definition := func(queryLanguage, fields string) string {
		if fields != "" {
			fields = "," + fields
		}
		return fmt.Sprintf(`{"QueryLanguage":%q,"StartAt":"Fail","States":{"Fail":{"Type":"Fail"%s}}}`, queryLanguage, fields)
	}
	for _, test := range []struct{ queryLanguage, fields string }{
		{"JSONPath", ""},
		{"JSONPath", `"Error":"Boom","Cause":"failed"`},
		{"JSONPath", `"ErrorPath":"$.error","CausePath":"$cause"`},
		{"JSONPath", `"ErrorPath":"States.Format('{}', $.error)","CausePath":"States.UUID()"`},
		{"JSONata", `"Error":"Boom","Cause":"failed"`},
		{"JSONata", `"Error":"{% $states.input.error %}","Cause":"{% $states.input.cause %}"`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.fields)); len(diagnostics) != 0 {
			t.Fatalf("valid Fail diagnostics %#v for %s", diagnostics, test.fields)
		}
	}
	for _, test := range []struct{ queryLanguage, fields string }{
		{"JSONPath", `"Error":1`},
		{"JSONPath", `"Cause":false`},
		{"JSONPath", `"ErrorPath":1`},
		{"JSONPath", `"CausePath":"cause"`},
		{"JSONPath", `"ErrorPath":"States.ArrayLength($.errors)"`},
		{"JSONPath", `"Error":"Boom","ErrorPath":"$.error"`},
		{"JSONPath", `"Cause":"failed","CausePath":"$.cause"`},
		{"JSONata", `"ErrorPath":"$.error"`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.fields)); len(diagnostics) != 1 {
			t.Fatalf("Fail diagnostics = %#v for %s", diagnostics, test.fields)
		}
	}
}

func TestStatesPassValidation(t *testing.T) {
	definition := func(queryLanguage, fields string) string {
		return fmt.Sprintf(`{"QueryLanguage":%q,"StartAt":"Pass","States":{"Pass":{"Type":"Pass",%s,"End":true}}}`, queryLanguage, fields)
	}
	for _, test := range []struct{ queryLanguage, fields string }{
		{"JSONPath", `"Result":{"ok":true},"ResultPath":"$.result"`},
		{"JSONPath", `"Parameters":{"value.$":"$.value"}`},
		{"JSONata", `"Assign":{"value":1},"Output":"{% $value %}"`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.fields)); len(diagnostics) != 0 {
			t.Fatalf("valid Pass diagnostics %#v for %s", diagnostics, test.fields)
		}
	}
	for _, test := range []struct{ queryLanguage, fields string }{
		{"JSONata", `"Result":{"ok":true}`},
		{"JSONata", `"Arguments":{"value":1}`},
		{"JSONPath", `"ResultSelector":{"value.$":"$.value"}`},
	} {
		if diagnostics := validateDefinition(definition(test.queryLanguage, test.fields)); len(diagnostics) != 1 {
			t.Fatalf("Pass diagnostics = %#v for %s", diagnostics, test.fields)
		}
	}
}

func TestStatesTaskFieldOwnershipValidation(t *testing.T) {
	valid := `{"QueryLanguage":"JSONata","StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"x","Arguments":{"value":1},"Credentials":{"RoleArn":"arn:aws:iam::1:role/task"},"TimeoutSeconds":2,"HeartbeatSeconds":1,"End":true}}}`
	if diagnostics := validateDefinition(valid); len(diagnostics) != 0 {
		t.Fatalf("valid Task fields diagnostics %#v", diagnostics)
	}
	for _, test := range []struct{ queryLanguage, field string }{
		{"JSONata", `"Arguments":{}`},
		{"JSONPath", `"Credentials":{"RoleArn":"arn:aws:iam::1:role/task"}`},
		{"JSONPath", `"HeartbeatSeconds":1`},
		{"JSONPath", `"HeartbeatSecondsPath":"$.heartbeat"`},
		{"JSONPath", `"Resource":"x"`},
		{"JSONPath", `"TimeoutSeconds":1`},
		{"JSONPath", `"TimeoutSecondsPath":"$.timeout"`},
	} {
		definition := fmt.Sprintf(`{"QueryLanguage":%q,"StartAt":"Pass","States":{"Pass":{"Type":"Pass",%s,"End":true}}}`, test.queryLanguage, test.field)
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("Task field diagnostics = %#v for %s", diagnostics, test.field)
		}
	}
}

func TestStatesFieldOwnershipValidation(t *testing.T) {
	for _, test := range []struct{ queryLanguage, field string }{
		{"JSONPath", `"Choices":[]`},
		{"JSONPath", `"Default":"Done"`},
		{"JSONPath", `"Cause":"failed"`},
		{"JSONPath", `"CausePath":"$.cause"`},
		{"JSONPath", `"Error":"Boom"`},
		{"JSONPath", `"ErrorPath":"$.error"`},
		{"JSONPath", `"ItemBatcher":{}`},
		{"JSONPath", `"ItemProcessor":{}`},
		{"JSONPath", `"ItemReader":{}`},
		{"JSONata", `"Items":[]`},
		{"JSONPath", `"ItemsPath":"$.items"`},
		{"JSONPath", `"ItemSelector":{}`},
		{"JSONPath", `"Iterator":{}`},
		{"JSONPath", `"Label":"map"`},
		{"JSONPath", `"MaxConcurrency":1`},
		{"JSONPath", `"MaxConcurrencyPath":"$.limit"`},
		{"JSONPath", `"ResultWriter":{}`},
		{"JSONPath", `"ToleratedFailureCount":1`},
		{"JSONPath", `"ToleratedFailureCountPath":"$.count"`},
		{"JSONPath", `"ToleratedFailurePercentage":1`},
		{"JSONPath", `"ToleratedFailurePercentagePath":"$.percentage"`},
		{"JSONPath", `"Branches":[]`},
		{"JSONPath", `"Seconds":1`},
		{"JSONPath", `"SecondsPath":"$.seconds"`},
		{"JSONPath", `"Timestamp":"2026-08-24T12:00:00Z"`},
		{"JSONPath", `"TimestampPath":"$.time"`},
	} {
		definition := fmt.Sprintf(`{"QueryLanguage":%q,"StartAt":"Pass","States":{"Pass":{"Type":"Pass",%s,"End":true},"Done":{"Type":"Succeed"}}}`, test.queryLanguage, test.field)
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("field ownership diagnostics = %#v for %s", diagnostics, test.field)
		}
	}
	result := `{"StartAt":"Done","States":{"Done":{"Type":"Succeed","Result":1}}}`
	if diagnostics := validateDefinition(result); len(diagnostics) != 1 {
		t.Fatalf("Result ownership diagnostics %#v", diagnostics)
	}
}

func TestStatesDataFlowValidation(t *testing.T) {
	for _, definition := range []string{
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","InputPath":"$.input","Parameters":{"value.$":"$.value"},"ResultPath":null,"OutputPath":null,"End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","InputPath":"$$.Execution.Input","ResultPath":"$['result']","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","InputPath":"$input.value","OutputPath":"$[*]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[0,2]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$['a','b']","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$..value","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[0:3:2]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.price < 10)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.active == true && @.price < @.limit)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.products[?(@.price < $.limit)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(10 > @.price)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.name =~ /[AB]/i)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.name =~ /a b/x)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.name =~ /a.b/dU)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.name in ['a','c'])]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.inside)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.tags size 2)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.tags empty false)]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.name IN ['a','c'])]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$[?(@.tags contains 'sale')]","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.items.length()","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.items.sum()","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.items.first()","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.object.keys()","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.items.index(-1)","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.items.size()","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.words.concat(\"!\")","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.items.append($.extra)","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.ignored.sum(10,$.numbers)","End":true}}}`,
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","OutputPath":"$.ignored.length($.object)","End":true}}}`,
		`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"x","ResultSelector":{"value.$":"$.value"},"End":true}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 0 {
			t.Fatalf("valid data-flow diagnostics %#v", diagnostics)
		}
	}
	for _, definition := range []string{
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","InputPath":1,"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":true,"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","ResultPath":1,"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","InputPath":"input","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"output","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","ResultPath":"result","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","InputPath":"$.['input']","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[0]output","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","InputPath":"$.foo bar","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","InputPath":"$.foo-bar","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","InputPath":"$.2foo","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","InputPath":"$.foo*","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[0:3:0]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.price = 10)]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.price < true)]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.active &&)]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.price < @.*)]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.name =~ /(/)]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.name =~ /a/z)]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.tags size '2')]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$[?(@.tags empty 1)]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$.items.index(1.5)","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$.items.append(unknown)","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","OutputPath":"$.items.append($.bad*)","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","ResultPath":"$[*]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","ResultPath":"$[0:2]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","ResultPath":"$$.Execution.Input","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","ResultPath":"$result","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","Parameters":[],"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","ResultSelector":[],"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Pass","ResultSelector":{},"End":true}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("data-flow diagnostics = %#v for %s", diagnostics, definition)
		}
	}
}

func TestStatesReferencePathValidation(t *testing.T) {
	processor := `"ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	distributedProcessor := `"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	for _, definition := range []string{
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Wait","SecondsPath":"$[*]","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","TimeoutSecondsPath":"$..timeout","End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Choice","Choices":[{"Variable":"$[*]","StringEquals":"x","Next":"Done"}]},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Choice","Choices":[{"Variable":"$.actual","StringEqualsPath":"$[0]expected","Next":"Done"}]},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Fail","ErrorPath":"$[*]"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["Boom"],"ResultPath":"$[*]","Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Map","ItemsPath":"$[*]",` + processor + `,"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Map","ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItemsPath":"$[*]"}},` + distributedProcessor + `,"End":true}}}`,
		`{"StartAt":"Bad","States":{"Bad":{"Type":"Map","ItemBatcher":{"MaxItemsPerBatchPath":"$[*]"},` + distributedProcessor + `,"End":true}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("reference-path diagnostics = %#v for %s", diagnostics, definition)
		}
	}
	for path, valid := range map[string]bool{"$$.Execution.Name": true, "$value.limit": true, "$.items[0]": true, "$.foo_bar2": true, "$['a:b']": true, `$.store\.book`: true, "$.foo-bar": false, "$.*": false, "$[*]": false, "$[0:2]": false, "$['a','b']": false, "$..items": false, "$[?(@.active)]": false} {
		if got := validJSONPath(path, true); got != valid {
			t.Fatalf("validJSONPath(%q, true) = %t, want %t", path, got, valid)
		}
	}
}

func TestStatesMapValidation(t *testing.T) {
	inlineProcessor := `"ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	processor := `"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	for _, fields := range []string{
		`"ItemsPath":"$.items","ItemSelector":{"value.$":"$$.Map.Item.Value"},` + processor,
		`"Iterator":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"Parameters":{"value.$":"$$.Map.Item.Value"}`,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItems":100000000}},"ItemBatcher":{"MaxItemsPerBatch":1,"MaxInputBytesPerBatch":262144},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItemsPath":"$.limit"}},"ItemBatcher":{"MaxItemsPerBatchPath":"$.batch"},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"JSON","ItemsPointer":"/data/a~1b/~0key"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVDelimiter":"PIPE","CSVHeaderLocation":"FIRST_ROW"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"GIVEN","CSVHeaders":["id","name"]}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"MANIFEST","CSVHeaderLocation":"FIRST_ROW"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"ManifestType":"ATHENA_DATA","InputType":"JSONL"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"ManifestType":"S3_INVENTORY"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","ReaderConfig":{"InputType":"JSON","Transformation":"LOAD_AND_FLATTEN"}},` + processor,
		`"ResultWriter":{"WriterConfig":{"Transformation":"COMPACT","OutputType":"JSONL"}},` + processor,
		`"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"bucket"}},` + processor,
		`"Label":"valid-label","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
	} {
		definition := `{"StartAt":"Map","States":{"Map":{"Type":"Map",` + fields + `,"End":true}}}`
		if diagnostics := validateDefinition(definition); len(diagnostics) != 0 {
			t.Fatalf("valid Map diagnostics %#v for %s", diagnostics, fields)
		}
	}
	for _, fields := range []string{
		`"ItemsPath":1,` + processor,
		`"ItemSelector":[],` + processor,
		processor + `,"Iterator":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`"ItemSelector":{},"Parameters":{},` + processor,
		`"ItemReader":[],` + processor,
		`"ItemBatcher":[],` + processor,
		`"ResultWriter":[],` + processor,
		`"ItemProcessor":{"ProcessorConfig":[],"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`"ItemReader":{},` + processor,
		`"ItemReader":{"Resource":1},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":[]},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItems":0}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItems":1.5}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItems":100000001}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItems":1,"MaxItemsPath":"$.limit"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"MaxItemsPath":"limit"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"JSON","ItemsPointer":1}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"JSON","ItemsPointer":"data"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"JSON","ItemsPointer":"/bad~2escape"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"JSON","ItemsPointer":"/` + strings.Repeat("x", 1999) + `"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","ItemsPointer":"/data"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":1}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"INVALID"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","Parameters":{"Bucket":"items","Key":"items.parquet","VersionId":"1"},"ReaderConfig":{"InputType":"PARQUET"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"ManifestType":1,"InputType":"JSONL"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"ManifestType":"INVALID","InputType":"JSONL"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"ManifestType":"ATHENA_DATA","InputType":"JSON"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"ManifestType":"S3_INVENTORY","InputType":"CSV"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","ReaderConfig":{"ManifestType":"ATHENA_DATA","InputType":"JSONL"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVDelimiter":"INVALID"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"INVALID"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"GIVEN"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"GIVEN","CSVHeaders":[]}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"GIVEN","CSVHeaders":[1]}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"GIVEN","CSVHeaders":["` + strings.Repeat("x", 10*1024+1) + `"]}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"FIRST_ROW","CSVHeaders":["id"]}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"JSON","CSVDelimiter":"COMMA"}},` + processor,
		`"ItemReader":{"Resource":"reader","ReaderConfig":{"InputType":"JSON","Transformation":"LOAD_AND_FLATTEN"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","ReaderConfig":{"InputType":"JSON","Transformation":"INVALID"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","ReaderConfig":{"Transformation":"LOAD_AND_FLATTEN"}},` + processor,
		`"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","ReaderConfig":{"InputType":"MANIFEST","Transformation":"LOAD_AND_FLATTEN"}},` + processor,
		`"ItemReader":{"Resource":"reader","Parameters":[]},` + processor,
		`"ItemBatcher":{},` + processor,
		`"ItemBatcher":{"MaxItemsPerBatch":0},` + processor,
		`"ItemBatcher":{"MaxItemsPerBatch":1.5},` + processor,
		`"ItemBatcher":{"MaxItemsPerBatch":1,"MaxItemsPerBatchPath":"$.batch"},` + processor,
		`"ItemBatcher":{"MaxItemsPerBatchPath":"batch"},` + processor,
		`"ItemBatcher":{"MaxInputBytesPerBatch":262145},` + processor,
		`"ItemBatcher":{"MaxInputBytesPerBatch":1,"MaxInputBytesPerBatchPath":"$.bytes"},` + processor,
		`"ItemBatcher":{"MaxInputBytesPerBatchPath":"bytes"},` + processor,
		`"ItemBatcher":{"MaxItemsPerBatch":1,"BatchInput":[]},` + processor,
		`"ResultWriter":{},` + processor,
		`"ResultWriter":{"Resource":1},` + processor,
		`"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":[]},` + processor,
		`"ItemProcessor":{"ProcessorConfig":{"Mode":1},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":1},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`"ResultWriter":{"WriterConfig":[]},` + processor,
		`"ResultWriter":{"WriterConfig":{"Transformation":1}},` + processor,
		`"ResultWriter":{"WriterConfig":{"Transformation":"INVALID"}},` + processor,
		`"ResultWriter":{"WriterConfig":{"OutputType":"INVALID"}},` + processor,
		`"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject"},` + processor,
		`"ResultWriter":{"Resource":"arn:aws:states:::lambda:invoke","Parameters":{}},` + processor,
		`"Label":1,"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`"Label":"` + strings.Repeat("x", 41) + `","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`"Label":"invalid label","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`"Label":"inline",` + inlineProcessor,
		`"ItemReader":{"Resource":"reader"},` + inlineProcessor,
		`"ItemBatcher":{"MaxItemsPerBatch":1},` + inlineProcessor,
		`"ResultWriter":{"WriterConfig":{}},` + inlineProcessor,
		`"ToleratedFailureCount":1,` + inlineProcessor,
	} {
		definition := `{"StartAt":"Map","States":{"Map":{"Type":"Map",` + fields + `,"End":true}}}`
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("Map diagnostics = %#v for %s", diagnostics, fields)
		}
	}
	duplicateLabels := `{"StartAt":"First","States":{"First":{"Type":"Map","Label":"duplicate",` + processor + `,"Next":"Second"},"Second":{"Type":"Map","Label":"duplicate",` + processor + `,"End":true}}}`
	if diagnostics := validateDefinition(duplicateLabels); len(diagnostics) != 1 {
		t.Fatalf("duplicate Map label diagnostics %#v", diagnostics)
	}
	mapState := `{"Type":"Map","Label":"duplicate",` + processor + `,"End":true}`
	duplicateLabels = `{"StartAt":"Parallel","States":{"Parallel":{"Type":"Parallel","Branches":[{"StartAt":"Map","States":{"Map":` + mapState + `}},{"StartAt":"Map","States":{"Map":` + mapState + `}}],"End":true}}}`
	if diagnostics := validateDefinition(duplicateLabels); len(diagnostics) != 1 {
		t.Fatalf("duplicate Parallel branch Map label diagnostics %#v", diagnostics)
	}
	duplicateLabels = `{"StartAt":"Outer","States":{"Outer":{"Type":"Map","Label":"duplicate","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Nested","States":{"Nested":` + mapState + `}},"End":true}}}`
	if diagnostics := validateDefinition(duplicateLabels); len(diagnostics) != 1 {
		t.Fatalf("duplicate nested Map label diagnostics %#v", diagnostics)
	}
	jsonata := `{"QueryLanguage":"JSONata","StartAt":"Map","States":{"Map":{"Type":"Map","Items":[],"ItemReader":{"Resource":"reader","Arguments":[]},` + processor + `,"End":true}}}`
	if diagnostics := validateDefinition(jsonata); len(diagnostics) != 1 {
		t.Fatalf("JSONata Map diagnostics %#v", diagnostics)
	}
	jsonata = `{"QueryLanguage":"JSONata","StartAt":"Map","States":{"Map":{"Type":"Map","Items":[],"ItemReader":{"Resource":"reader","Arguments":{},"ReaderConfig":{"MaxItems":"{% 3 %}"}},"ItemBatcher":{"MaxItemsPerBatch":"{% 2 %}","MaxInputBytesPerBatch":"{% 1024 %}"},` + processor + `,"End":true}}}`
	if diagnostics := validateDefinition(jsonata); len(diagnostics) != 0 {
		t.Fatalf("valid JSONata Map diagnostics %#v", diagnostics)
	}
}

func TestStatesAssignValidation(t *testing.T) {
	for _, definition := range []string{
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Assign":{"value":1},"End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Choose","States":{"Choose":{"Type":"Choice","Choices":[{"Condition":true,"Assign":{"value":1},"Next":"Done"}]},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["States.ALL"],"Assign":{"value":1},"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 0 {
			t.Fatalf("valid Assign diagnostics %#v", diagnostics)
		}
	}
	for _, definition := range []string{
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Assign":[],"End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Choose","States":{"Choose":{"Type":"Choice","Choices":[{"Condition":true,"Assign":[],"Next":"Done"}]},"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"x","Catch":[{"ErrorEquals":["States.ALL"],"Assign":[],"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("Assign diagnostics = %#v for %s", diagnostics, definition)
		}
	}
}

func TestStatesVariableNameValidation(t *testing.T) {
	for _, definition := range []string{
		`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Assign":{"value.$":"$.input"},"End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Pass","States":{"Pass":{"Type":"Pass","Assign":{"value":"{% 1 %}"},"End":true}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 0 {
			t.Fatalf("valid variable diagnostics %#v", diagnostics)
		}
	}
	for _, definition := range []string{
		`{"QueryLanguage":"JSONata","StartAt":"Pass","States":{"Pass":{"Type":"Pass","Assign":{"value.$":"{% 1 %}"},"End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"Pass","States":{"Pass":{"Type":"Pass","Assign":{"value.test":1},"End":true}}}`,
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("variable diagnostics = %#v for %s", diagnostics, definition)
		}
	}
}

func TestStatesStructuralMetadataValidation(t *testing.T) {
	validName := strings.Repeat("é", 80)
	valid := fmt.Sprintf(`{"Comment":"machine","Version":"1.0","StartAt":%q,"States":{%q:{"Type":"Succeed","Comment":"state"}}}`, validName, validName)
	if diagnostics := validateDefinition(valid); len(diagnostics) != 0 {
		t.Fatalf("valid metadata diagnostics %#v", diagnostics)
	}
	longName := strings.Repeat("é", 81)
	for _, definition := range []string{
		`{"Comment":1,"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`{"Version":1,"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`{"Version":"2.0","StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
		`{"StartAt":"Done","States":{"Done":{"Type":"Succeed","Comment":1}}}`,
		fmt.Sprintf(`{"StartAt":%q,"States":{%q:{"Type":"Succeed"}}}`, longName, longName),
	} {
		if diagnostics := validateDefinition(definition); len(diagnostics) != 1 {
			t.Fatalf("metadata diagnostics = %#v for %s", diagnostics, definition)
		}
	}
}

func TestStatesExecutionTimeout(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	activityARN := invoke("CreateActivity", map[string]any{"name": "execution-timeout"})["activityArn"].(string)
	definition := `{"TimeoutSeconds":2,"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + activityARN + `","End":true}}}`
	machine := invoke("CreateStateMachine", map[string]any{"name": "execution-timeout", "definition": definition, "roleArn": testRoleARN})
	executionARN := invoke("StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"]})["executionArn"].(string)
	token := invoke("GetActivityTask", map[string]any{"activityArn": activityARN})["taskToken"].(string)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	if execution := invoke("DescribeExecution", map[string]any{"executionArn": executionARN}); execution["status"] != "RUNNING" {
		t.Fatalf("execution timed out early %#v", execution)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	var execution map[string]any
	for range 100 {
		execution = invoke("DescribeExecution", map[string]any{"executionArn": executionARN})
		if execution["status"] == "TIMED_OUT" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if execution["status"] != "TIMED_OUT" || execution["error"] != "States.Timeout" || execution["cause"] != "States.Timeout" {
		t.Fatalf("execution did not time out %#v", execution)
	}
	history := invoke("GetExecutionHistory", map[string]any{"executionArn": executionARN})["events"].([]any)
	if history[len(history)-1].(map[string]any)["type"] != "ExecutionTimedOut" {
		t.Fatalf("timeout history %#v", history)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendTaskSuccess", Input: map[string]any{"taskToken": token, "output": `{}`}}); err == nil {
		t.Fatal("timed-out activity token remained valid")
	}

	expressDefinition := `{"TimeoutSeconds":2,"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":10,"End":true}}}`
	express := invoke("CreateStateMachine", map[string]any{"name": "sync-execution-timeout", "definition": expressDefinition, "roleArn": testRoleARN, "type": "EXPRESS"})
	result := make(chan map[string]any, 1)
	go func() {
		result <- invoke("StartSyncExecution", map[string]any{"stateMachineArn": express["stateMachineArn"]})
	}()
	select {
	case early := <-result:
		t.Fatalf("Express execution timeout completed early %#v", early)
	case <-time.After(10 * time.Millisecond):
	}
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case finished := <-result:
		if finished["status"] != "TIMED_OUT" || finished["error"] != "States.Timeout" {
			t.Fatalf("Express execution timeout %#v", finished)
		}
	case <-time.After(time.Second):
		t.Fatal("Express execution timeout did not fire")
	}

	start := time.Unix(0, 0).UTC()
	if got := executionDeadline(`{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`, "STANDARD", start); got != start.Add(365*24*time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("Standard quota deadline %s", got)
	}
	if got := executionDeadline(`{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`, "EXPRESS", start); got != start.Add(5*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("Express quota deadline %s", got)
	}
	for _, timeout := range []string{"0", "1.5", `"2"`, "100000000"} {
		invalid := `{"TimeoutSeconds":` + timeout + `,"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
		if diagnostics := validateDefinition(invalid); len(diagnostics) != 1 {
			t.Fatalf("TimeoutSeconds %s diagnostics %#v", timeout, diagnostics)
		}
	}
}

func TestStatesTaskTimeoutAndHeartbeat(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	pollTask := func(activityARN string) string {
		t.Helper()
		for range 100 {
			if token, ok := invoke("GetActivityTask", map[string]any{"activityArn": activityARN})["taskToken"].(string); ok {
				return token
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("activity task was not available")
		return ""
	}
	waitStatus := func(executionARN, status string) map[string]any {
		t.Helper()
		var execution map[string]any
		for range 100 {
			execution = invoke("DescribeExecution", map[string]any{"executionArn": executionARN})
			if execution["status"] == status {
				return execution
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("execution did not reach %s: %#v", status, execution)
		return nil
	}

	heartbeatActivity := invoke("CreateActivity", map[string]any{"name": "heartbeat-timeout"})["activityArn"].(string)
	heartbeatDefinition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + heartbeatActivity + `","TimeoutSeconds":5,"HeartbeatSeconds":2,"Catch":[{"ErrorEquals":["States.Timeout"],"ResultPath":"$.failure","Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed"}}}`
	heartbeatMachine := invoke("CreateStateMachine", map[string]any{"name": "heartbeat-timeout", "definition": heartbeatDefinition, "roleArn": testRoleARN})
	heartbeatExecution := invoke("StartExecution", map[string]any{"stateMachineArn": heartbeatMachine["stateMachineArn"], "input": `{}`})["executionArn"].(string)
	heartbeatToken := pollTask(heartbeatActivity)
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	invoke("SendTaskHeartbeat", map[string]any{"taskToken": heartbeatToken})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	invoke("SendTaskHeartbeat", map[string]any{"taskToken": heartbeatToken})
	heartbeatBody, found, _ := p.col(&spi.Request{Identity: id}, "pending").Get(ctx, heartbeatToken)
	var heartbeatPending pending
	_ = json.Unmarshal(heartbeatBody, &heartbeatPending)
	if !found || heartbeatPending.HeartbeatDeadline != deps.Clock.Now().Add(2*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("heartbeat deadline was not reset %#v", heartbeatPending)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	if execution := invoke("DescribeExecution", map[string]any{"executionArn": heartbeatExecution}); execution["status"] != "RUNNING" {
		t.Fatalf("heartbeat was not extended %#v", execution)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	heartbeatResult := waitStatus(heartbeatExecution, "SUCCEEDED")
	if !strings.Contains(heartbeatResult["output"].(string), `"Error":"States.Timeout"`) {
		t.Fatalf("heartbeat timeout was not caught %#v", heartbeatResult)
	}

	timeoutActivity := invoke("CreateActivity", map[string]any{"name": "task-timeout"})["activityArn"].(string)
	timeoutDefinition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + timeoutActivity + `","TimeoutSeconds":2,"Retry":[{"ErrorEquals":["States.Timeout"],"IntervalSeconds":1,"MaxAttempts":1}],"Catch":[{"ErrorEquals":["States.Timeout"],"ResultPath":"$.failure","Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed"}}}`
	timeoutMachine := invoke("CreateStateMachine", map[string]any{"name": "task-timeout", "definition": timeoutDefinition, "roleArn": testRoleARN})
	timeoutExecution := invoke("StartExecution", map[string]any{"stateMachineArn": timeoutMachine["stateMachineArn"], "input": `{}`})["executionArn"].(string)
	if err := deps.Clock.Advance(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	if execution := invoke("DescribeExecution", map[string]any{"executionArn": timeoutExecution}); execution["status"] != "RUNNING" {
		t.Fatalf("unclaimed activity timed out %#v", execution)
	}
	firstToken := pollTask(timeoutActivity)
	if task := invoke("GetActivityTask", map[string]any{"activityArn": timeoutActivity}); task["taskToken"] != nil {
		t.Fatalf("claimed activity was delivered twice %#v", task)
	}
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	retryScheduled := false
	for range 100 {
		kvs, _, _ := p.col(&spi.Request{Identity: id}, "pending").List(ctx, "", "", 0)
		for _, kv := range kvs {
			var pending pending
			_ = json.Unmarshal(kv.Value, &pending)
			if pending.ExecARN == timeoutExecution && pending.Retry {
				retryScheduled = true
				break
			}
		}
		if retryScheduled {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !retryScheduled {
		t.Fatal("task timeout retry was not scheduled")
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	secondToken := pollTask(timeoutActivity)
	if secondToken == firstToken {
		t.Fatal("task timeout retry reused its token")
	}
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	timeoutResult := waitStatus(timeoutExecution, "SUCCEEDED")
	if !strings.Contains(timeoutResult["output"].(string), `"Error":"States.Timeout"`) {
		t.Fatalf("task timeout was not caught %#v", timeoutResult)
	}

	queue := sqs.New(deps)
	queueResponse, err := queue.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "callback-timeout"}})
	if err != nil {
		t.Fatal(err)
	}
	queueURL := queueResponse.Output["QueueUrl"].(string)
	callbackDefinition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.waitForTaskToken","Parameters":{"QueueUrl":"` + queueURL + `","MessageBody.$":"$$.Task.Token"},"TimeoutSeconds":5,"HeartbeatSeconds":1,"Catch":[{"ErrorEquals":["States.Timeout"],"ResultPath":"$.failure","Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed"}}}`
	callbackMachine := invoke("CreateStateMachine", map[string]any{"name": "callback-timeout", "definition": callbackDefinition, "roleArn": testRoleARN})
	callbackExecution := invoke("StartExecution", map[string]any{"stateMachineArn": callbackMachine["stateMachineArn"], "input": `{}`})["executionArn"].(string)
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	callbackResult := waitStatus(callbackExecution, "SUCCEEDED")
	if !strings.Contains(callbackResult["output"].(string), `"Error":"States.Timeout"`) {
		t.Fatalf("callback timeout was not caught %#v", callbackResult)
	}
}

func TestStatesJSONataErrorsAndFields(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	storage := s3.New(deps)
	queue := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, operation string, input map[string]any, body ...[]byte) map[string]any {
		t.Helper()
		request := &spi.Request{Identity: id, Operation: operation, Input: input}
		if len(body) != 0 {
			request.Body = io.NopCloser(bytes.NewReader(body[0]))
		}
		response, err := handler.Invoke(ctx, request)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	start := func(name, definition, input string) map[string]any {
		t.Helper()
		machine := invoke(p, "CreateStateMachine", map[string]any{"name": name, "definition": definition, "roleArn": testRoleARN, "type": "EXPRESS"})
		return invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"], "input": input})
	}
	startAfterRetry := func(name, definition, input string) map[string]any {
		t.Helper()
		result := make(chan map[string]any, 1)
		go func() { result <- start(name, definition, input) }()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case execution := <-result:
				return execution
			case <-tick.C:
				if err := deps.Clock.Advance(100 * time.Millisecond); err != nil {
					t.Fatal(err)
				}
			case <-timeout:
				t.Fatal("Express Retry did not resume")
				return nil
			}
		}
	}
	resumeRetry := func(executionARN string) map[string]any {
		t.Helper()
		if err := deps.Clock.Advance(time.Second); err != nil {
			t.Fatal(err)
		}
		for range 100 {
			execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": executionARN})
			if execution["status"] != "RUNNING" {
				return execution
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("Standard Retry did not resume")
		return nil
	}

	invoke(storage, "CreateBucket", map[string]any{"Bucket": "jsonata-items"})
	invoke(storage, "PutObject", map[string]any{"Bucket": "jsonata-items", "Key": "items.json"}, []byte(`[1,2,3,4]`))
	mapDefinition := `{"QueryLanguage":"JSONata","StartAt":"Read","States":{"Read":{"Type":"Map","ItemReader":{"Resource":"arn:aws:states:::s3:getObject","Arguments":{"Bucket":"{% $states.input.bucket %}","Key":"{% $states.input.key %}"},"ReaderConfig":{"InputType":"JSON","MaxItems":"{% 3 %}"}},"ItemBatcher":{"MaxItemsPerBatch":"{% 2 %}","BatchInput":{"tag":"{% $states.input.tag %}"}},"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Echo","States":{"Echo":{"Type":"Pass","Output":"{% $states.input %}","End":true}}},"Next":"Transform"},"Transform":{"Type":"Parallel","Branches":[{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}],"Output":"{% $states.result.missing %}","Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"MaxAttempts":1}],"Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Assign":{"caught":"{% $states.errorOutput.Error %}"},"Output":{"map":"{% $states.input %}"},"Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed","Output":{"error":"{% $caught %}","batches":"{% $states.input.map %}"}}}}`
	if diagnostics := validateDefinition(mapDefinition, "STANDARD"); len(diagnostics) != 0 {
		t.Fatalf("jsonata field diagnostics %#v", diagnostics)
	}
	mapMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata-fields", "definition": mapDefinition, "roleArn": testRoleARN})
	mapARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": mapMachine["stateMachineArn"], "input": `{"bucket":"jsonata-items","key":"items.json","tag":"t"}`})["executionArn"].(string)
	mapExecution := resumeRetry(mapARN)
	if mapExecution["status"] != "SUCCEEDED" {
		t.Fatalf("jsonata map fields %#v", mapExecution)
	}
	var mapOutput map[string]any
	if json.Unmarshal([]byte(mapExecution["output"].(string)), &mapOutput) != nil || mapOutput["error"] != "States.QueryEvaluationError" || len(mapOutput["batches"].([]any)) != 2 {
		t.Fatalf("jsonata map output %#v", mapExecution)
	}
	firstBatch := mapOutput["batches"].([]any)[0].(map[string]any)
	secondBatch := mapOutput["batches"].([]any)[1].(map[string]any)
	if len(firstBatch["Items"].([]any)) != 2 || len(secondBatch["Items"].([]any)) != 1 || firstBatch["BatchInput"].(map[string]any)["tag"] != "t" {
		t.Fatalf("jsonata batch %#v", firstBatch)
	}

	taskDefinition := `{"QueryLanguage":"JSONata","StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Arguments":{"QueueUrl":"http://localhost/1/missing","MessageBody":"x"},"Catch":[{"ErrorEquals":["SQS.AmazonSQSException"],"Assign":{"caught":"{% $states.errorOutput.Error %}"},"Output":{"error":"{% $states.errorOutput.Error %}","original":"{% $states.input %}"},"Next":"Done"}],"End":true},"Done":{"Type":"Succeed","Output":{"caught":"{% $caught %}","error":"{% $states.input.error %}"}}}}`
	if execution := start("jsonata-task-catch", taskDefinition, `{"id":1}`); execution["status"] != "SUCCEEDED" || execution["output"] != `{"caught":"SQS.AmazonSQSException","error":"SQS.AmazonSQSException"}` {
		t.Fatalf("jsonata task catch %#v", execution)
	}

	waitFail := `{"QueryLanguage":"JSONata","StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":"{% $states.input.delay %}","Next":"Fail"},"Fail":{"Type":"Fail","Error":"{% $states.input.error %}","Cause":"{% $states.input.cause %}"}}}`
	if execution := start("jsonata-wait-fail", waitFail, `{"delay":0,"error":"DynamicError","cause":"dynamic cause"}`); execution["status"] != "FAILED" || execution["error"] != "DynamicError" || execution["cause"] != "dynamic cause" {
		t.Fatalf("jsonata wait/fail %#v", execution)
	}
	invalidWait := `{"QueryLanguage":"JSONata","StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":"{% 'later' %}","End":true}}}`
	if execution := start("jsonata-invalid-wait", invalidWait, `{}`); execution["status"] != "FAILED" || execution["error"] != "States.QueryEvaluationError" {
		t.Fatalf("jsonata invalid wait %#v", execution)
	}

	override := `{"StartAt":"Map","States":{"Map":{"Type":"Map","QueryLanguage":"JSONata","Items":"{% $states.input %}","ItemProcessor":{"StartAt":"Shape","States":{"Shape":{"Type":"Pass","Parameters":{"v.$":"$"},"End":true}}},"End":true}}}`
	if diagnostics := validateDefinition(override, "EXPRESS"); len(diagnostics) != 0 {
		t.Fatalf("per-state JSONata inheritance diagnostics %#v", diagnostics)
	}
	if execution := start("jsonata-override", override, `[1]`); execution["status"] != "SUCCEEDED" || execution["output"] != `[{"v":1}]` {
		t.Fatalf("per-state JSONata inheritance %#v", execution)
	}

	queueURL := invoke(queue, "CreateQueue", map[string]any{"QueueName": "jsonpath-variables"})["QueueUrl"].(string)
	credentialRetry := `{"QueryLanguage":"JSONata","StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Credentials":{"RoleArn":"{% $states.context.State.RetryCount > 0 ? '` + testRoleARN + `' : $states.input.missing %}"},"Arguments":{"QueueUrl":"` + queueURL + `","MessageBody":"credentials"},"Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"MaxAttempts":1}],"End":true}}}`
	if execution := startAfterRetry("jsonata-credential-retry", credentialRetry, `{}`); execution["status"] != "SUCCEEDED" {
		t.Fatalf("JSONata credential retry %#v", execution)
	}
	timeoutRetry := `{"QueryLanguage":"JSONata","StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","TimeoutSeconds":"{% $states.context.State.RetryCount > 0 ? 2 : 'later' %}","HeartbeatSeconds":"{% 1 %}","Arguments":{"QueueUrl":"` + queueURL + `","MessageBody":"timeout"},"Output":{"retry":"{% $states.context.State.RetryCount %}"},"Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"MaxAttempts":1}],"End":true}}}`
	if execution := startAfterRetry("jsonata-timeout-retry", timeoutRetry, `{}`); execution["status"] != "SUCCEEDED" || execution["output"] != `{"retry":1}` {
		t.Fatalf("JSONata timeout retry %#v", execution)
	}
	numericMap := `{"QueryLanguage":"JSONata","StartAt":"Map","States":{"Map":{"Type":"Map","Items":"{% $states.context.State.RetryCount > 0 ? [1,2] : $states.input.missing %}","MaxConcurrency":"{% 2 %}","ToleratedFailureCount":"{% 1 %}","ToleratedFailurePercentage":"{% 50 %}","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Choose","States":{"Choose":{"Type":"Choice","Choices":[{"Condition":"{% $states.input = 1 %}","Next":"Bad"}],"Default":"Done"},"Bad":{"Type":"Fail","Error":"Expected"},"Done":{"Type":"Succeed","Output":{"value":"{% $states.input %}","execution":"{% $states.context.Execution.Id %}","machine":"{% $states.context.StateMachine.Id %}","input":"{% $states.context.Execution.Input %}"}}}},"Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"MaxAttempts":1}],"End":true}}}`
	numericMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata-numeric-map", "definition": numericMap, "roleArn": testRoleARN})
	numericARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": numericMachine["stateMachineArn"]})["executionArn"].(string)
	numericExecution := resumeRetry(numericARN)
	var numericOutput []map[string]any
	if numericExecution["status"] != "SUCCEEDED" || json.Unmarshal([]byte(numericExecution["output"].(string)), &numericOutput) != nil || len(numericOutput) != 1 || numericOutput[0]["value"] != 2.0 || numericOutput[0]["input"] != 2.0 || numericOutput[0]["execution"] == numericARN || !strings.Contains(numericOutput[0]["execution"].(string), ":execution:jsonata-numeric-map/Map:") || !strings.HasSuffix(numericOutput[0]["machine"].(string), ":stateMachine:jsonata-numeric-map/Map") {
		t.Fatalf("JSONata numeric Map %#v", numericExecution)
	}
	numericRuns := invoke(p, "ListMapRuns", map[string]any{"executionArn": numericARN})["mapRuns"].([]any)
	numericChildren := invoke(p, "ListExecutions", map[string]any{"mapRunArn": numericRuns[0].(map[string]any)["mapRunArn"]})["executions"].([]any)
	if len(numericChildren) != 2 || numericChildren[0].(map[string]any)["executionArn"] != numericOutput[0]["execution"] && numericChildren[1].(map[string]any)["executionArn"] != numericOutput[0]["execution"] {
		t.Fatalf("JSONata child context %#v %#v", numericOutput, numericChildren)
	}
	objectMap := `{"QueryLanguage":"JSONata","StartAt":"Map","States":{"Map":{"Type":"Map","Items":"{% {'b':2,'a':1} %}","ItemSelector":{"key":"{% $states.context.Map.Item.Key %}","value":"{% $states.context.Map.Item.Value %}","index":"{% $states.context.Map.Item.Index %}"},"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`
	objectMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata-object-map", "definition": objectMap, "roleArn": testRoleARN})
	objectARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": objectMachine["stateMachineArn"]})["executionArn"].(string)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": objectARN}); execution["status"] != "SUCCEEDED" || execution["output"] != `[{"index":0,"key":"a","value":1},{"index":1,"key":"b","value":2}]` {
		t.Fatalf("JSONata object Map %#v", execution)
	}
	variableIsolation := `{"QueryLanguage":"JSONata","StartAt":"Prepare","States":{"Prepare":{"Type":"Pass","Assign":{"items":[1],"outer":7},"Next":"Config"},"Config":{"Type":"Map","Items":"{% $items %}","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Assign":{"configBlocked":true},"Next":"Child"}],"Next":"Child"},"Child":{"Type":"Map","Items":[1],"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Read","States":{"Read":{"Type":"Succeed","Output":"{% $outer %}"}}},"Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Assign":{"childBlocked":true},"Next":"Output"}],"Next":"Output"},"Output":{"Type":"Map","Items":[1],"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"Output":"{% $outer %}","Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Assign":{"outputBlocked":true},"Next":"Assign"}],"Next":"Assign"},"Assign":{"Type":"Map","Items":[1],"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"Assign":{"leaked":"{% $outer %}"},"Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Assign":{"assignBlocked":true},"Next":"Done"}],"Next":"Done"},"Done":{"Type":"Succeed","Output":{"config":"{% $configBlocked %}","child":"{% $childBlocked %}","output":"{% $outputBlocked %}","assign":"{% $assignBlocked %}"}}}}`
	isolationMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata-variable-isolation", "definition": variableIsolation, "roleArn": testRoleARN})
	isolationARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": isolationMachine["stateMachineArn"]})["executionArn"].(string)
	if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": isolationARN}); execution["status"] != "SUCCEEDED" || execution["output"] != `{"assign":true,"child":true,"config":true,"output":true}` {
		t.Fatalf("Distributed Map variable isolation %#v", execution)
	}
	writerRetry := `{"QueryLanguage":"JSONata","StartAt":"Map","States":{"Map":{"Type":"Map","Items":"{% [1] %}","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Arguments":{"Bucket":"{% $states.context.State.RetryCount > 0 ? 'jsonata-items' : $states.input.missing %}","Prefix":"results"}},"Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"MaxAttempts":1}],"End":true}}}`
	writerMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata-writer-retry", "definition": writerRetry, "roleArn": testRoleARN})
	writerARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": writerMachine["stateMachineArn"]})["executionArn"].(string)
	if execution := resumeRetry(writerARN); execution["status"] != "SUCCEEDED" || !strings.Contains(execution["output"].(string), "ResultWriterDetails") {
		t.Fatalf("JSONata ResultWriter retry %#v", execution)
	}
	variablesDefinition := `{"StartAt":"Store","States":{"Store":{"Type":"Pass","Assign":{"saved.$":"$.value"},"Next":"Choose"},"Choose":{"Type":"Choice","Choices":[{"Variable":"$.value","NumericEquals":7,"Assign":{"chosen":"yes"},"Next":"Send"}]},"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{"QueueUrl":"` + queueURL + `","MessageBody":"x"},"Assign":{"messageId.$":"$.MessageId"},"ResultPath":null,"Next":"Done"},"Done":{"Type":"Succeed","QueryLanguage":"JSONata","Output":{"saved":"{% $saved %}","chosen":"{% $chosen %}","messageId":"{% $messageId %}"}}}}`
	if execution := start("jsonpath-variables", variablesDefinition, `{"value":7}`); execution["status"] != "SUCCEEDED" || !strings.Contains(execution["output"].(string), `"saved":7`) || !strings.Contains(execution["output"].(string), `"chosen":"yes"`) {
		t.Fatalf("JSONPath variables %#v", execution)
	}
	catchVariables := `{"StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{"QueueUrl":"http://localhost/1/missing","MessageBody":"x"},"Catch":[{"ErrorEquals":["SQS.AmazonSQSException"],"Assign":{"caught.$":"$.Error"},"Next":"Done"}],"End":true},"Done":{"Type":"Succeed","QueryLanguage":"JSONata","Output":"{% $caught %}"}}}`
	if execution := start("jsonpath-catch-variables", catchVariables, `{}`); execution["status"] != "SUCCEEDED" || execution["output"] != `"SQS.AmazonSQSException"` {
		t.Fatalf("JSONPath catch variables %#v", execution)
	}

	contextDefinition := `{"QueryLanguage":"JSONata","StartAt":"Done","States":{"Done":{"Type":"Succeed","Output":{"execution":"{% $states.context.Execution.Name %}","input":"{% $states.context.Execution.Input.value %}","role":"{% $states.context.Execution.RoleArn %}","redrives":"{% $states.context.Execution.RedriveCount %}","machine":"{% $states.context.StateMachine.Name %}","state":"{% $states.context.State.Name %}","retry":"{% $states.context.State.RetryCount %}","entered":"{% $states.context.State.EnteredTime %}"}}}}`
	contextExecution := start("jsonata-context", contextDefinition, `{"value":9}`)
	var contextOutput map[string]any
	if contextExecution["status"] != "SUCCEEDED" || json.Unmarshal([]byte(contextExecution["output"].(string)), &contextOutput) != nil || contextOutput["execution"] == "" || contextOutput["input"] != 9.0 || contextOutput["role"] != testRoleARN || contextOutput["redrives"] != 0.0 || contextOutput["machine"] != "jsonata-context" || contextOutput["state"] != "Done" || contextOutput["retry"] != 0.0 || contextOutput["entered"] == "" {
		t.Fatalf("JSONata context %#v", contextExecution)
	}

	redriveDefinition := `{"QueryLanguage":"JSONata","StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Arguments":{"QueueUrl":"` + queueURL + `","MessageBody":"redrive"},"Output":"{% $states.context.Execution.RedriveCount > 0 ? {'redrives': $states.context.Execution.RedriveCount} : $states.input.missing %}","End":true}}}`
	redriveMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "jsonata-redrive", "definition": redriveDefinition, "roleArn": testRoleARN})
	redriveARN := invoke(p, "StartExecution", map[string]any{"stateMachineArn": redriveMachine["stateMachineArn"]})["executionArn"].(string)
	if before := invoke(p, "DescribeExecution", map[string]any{"executionArn": redriveARN}); before["status"] != "FAILED" || before["error"] != "States.QueryEvaluationError" {
		t.Fatalf("JSONata before redrive %#v", before)
	}
	invoke(p, "RedriveExecution", map[string]any{"executionArn": redriveARN})
	if after := invoke(p, "DescribeExecution", map[string]any{"executionArn": redriveARN}); after["status"] != "SUCCEEDED" || after["output"] != `{"redrives":1}` {
		t.Fatalf("JSONata after redrive %#v", after)
	}
}

func TestStatesTagLifecycle(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	arn := must("CreateStateMachine", map[string]any{
		"name": "tagged", "definition": `{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`, "roleArn": testRoleARN,
	}).Output["stateMachineArn"].(string)
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

func TestStatesRequestValidation(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	fault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		if got, ok := err.(*spi.Fault); !ok || got.Code != code {
			t.Fatalf("%s fault %#v want %s", operation, err, code)
		}
	}
	definition := `{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	validCreate := func(name string) map[string]any {
		return map[string]any{"name": name, "definition": definition, "roleArn": testRoleARN}
	}

	for _, test := range []struct {
		name  string
		input map[string]any
		code  string
	}{
		{"missing role", map[string]any{"name": "missing-role", "definition": definition}, "InvalidArn"},
		{"invalid name", validCreate("bad;name"), "InvalidName"},
		{"invalid type", map[string]any{"name": "bad-type", "definition": definition, "roleArn": testRoleARN, "type": "FAST"}, "ValidationException"},
		{"invalid logging", map[string]any{"name": "bad-log", "definition": definition, "roleArn": testRoleARN, "loggingConfiguration": map[string]any{"level": "ALL"}}, "InvalidLoggingConfiguration"},
		{"invalid logging destinations", map[string]any{"name": "bad-log-list", "definition": definition, "roleArn": testRoleARN, "loggingConfiguration": map[string]any{"level": "OFF", "destinations": "wrong"}}, "InvalidLoggingConfiguration"},
		{"invalid tracing", map[string]any{"name": "bad-trace", "definition": definition, "roleArn": testRoleARN, "tracingConfiguration": map[string]any{}}, "InvalidTracingConfiguration"},
		{"invalid encryption", map[string]any{"name": "bad-key", "definition": definition, "roleArn": testRoleARN, "encryptionConfiguration": map[string]any{"type": "CUSTOMER_MANAGED_KMS_KEY"}}, "InvalidEncryptionConfiguration"},
		{"invalid owned encryption", map[string]any{"name": "bad-owned-key", "definition": definition, "roleArn": testRoleARN, "encryptionConfiguration": map[string]any{"type": "AWS_OWNED_KEY", "kmsKeyId": ""}}, "InvalidEncryptionConfiguration"},
		{"reserved tag", map[string]any{"name": "bad-tag", "definition": definition, "roleArn": testRoleARN, "tags": []any{map[string]any{"key": "aws:owner", "value": "me"}}}, "ValidationException"},
		{"invalid tag list", map[string]any{"name": "bad-tag-list", "definition": definition, "roleArn": testRoleARN, "tags": "wrong"}, "ValidationException"},
	} {
		t.Run(test.name, func(t *testing.T) { fault("CreateStateMachine", test.input, test.code) })
	}
	tooManyTags := make([]any, 51)
	for index := range tooManyTags {
		tooManyTags[index] = map[string]any{"key": fmtString(index), "value": "v"}
	}
	fault("CreateStateMachine", map[string]any{"name": "too-many-tags", "definition": definition, "roleArn": testRoleARN, "tags": tooManyTags}, "TooManyTags")

	created, err := call("CreateStateMachine", validCreate("plus+allowed"))
	if err != nil {
		t.Fatal(err)
	}
	arn := created.Output["stateMachineArn"].(string)
	fault("StartExecution", map[string]any{"stateMachineArn": arn, "name": "bad;name"}, "InvalidName")
	fault("StartExecution", map[string]any{"stateMachineArn": arn, "input": `"` + strings.Repeat("a", 262143) + `"`}, "InvalidExecutionInput")
	fault("StartExecution", map[string]any{"stateMachineArn": arn, "traceHeader": "non-ascii-é"}, "ValidationException")

	encryption := map[string]any{"type": "CUSTOMER_MANAGED_KMS_KEY", "kmsKeyId": "alias/activity", "kmsDataKeyReusePeriodSeconds": float64(60)}
	activity, err := call("CreateActivity", map[string]any{"name": "encrypted", "encryptionConfiguration": encryption, "tags": []any{map[string]any{"key": "owner", "value": "first"}}})
	if err != nil {
		t.Fatal(err)
	}
	activityARN := activity.Output["activityArn"].(string)
	if _, err := call("CreateActivity", map[string]any{"name": "encrypted", "encryptionConfiguration": encryption, "tags": []any{map[string]any{"key": "owner", "value": "ignored"}}}); err != nil {
		t.Fatalf("idempotent activity: %v", err)
	}
	fault("CreateActivity", map[string]any{"name": "encrypted", "encryptionConfiguration": map[string]any{"type": "AWS_OWNED_KEY"}}, "ActivityAlreadyExists")
	described, err := call("DescribeActivity", map[string]any{"activityArn": activityARN})
	if err != nil || !reflect.DeepEqual(described.Output["encryptionConfiguration"], encryption) || described.Output["_encryption"] != nil {
		t.Fatalf("described activity %#v, %v", described, err)
	}
	tags, err := call("ListTagsForResource", map[string]any{"resourceArn": activityARN})
	if err != nil || tags.Output["tags"].([]any)[0].(map[string]any)["value"] != "first" {
		t.Fatalf("activity tags %#v, %v", tags, err)
	}
	fault("GetActivityTask", map[string]any{"activityArn": activityARN, "workerName": ""}, "ValidationException")
	fault("GetActivityTask", map[string]any{"activityArn": "not-an-arn"}, "InvalidArn")
	activityDefinition := `{"StartAt":"Work","States":{"Work":{"Type":"Task","Resource":"` + activityARN + `","End":true}}}`
	activityMachine, err := call("CreateStateMachine", map[string]any{"name": "validated-task", "definition": activityDefinition, "roleArn": testRoleARN})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = call("StartExecution", map[string]any{"stateMachineArn": activityMachine.Output["stateMachineArn"]}); err != nil {
		t.Fatal(err)
	}
	task, err := call("GetActivityTask", map[string]any{"activityArn": activityARN, "workerName": "worker"})
	if err != nil {
		t.Fatal(err)
	}
	token := task.Output["taskToken"].(string)
	fault("SendTaskSuccess", map[string]any{"taskToken": token, "output": "not-json"}, "InvalidOutput")
	if _, err = call("SendTaskHeartbeat", map[string]any{"taskToken": token}); err != nil {
		t.Fatalf("invalid output consumed task token: %v", err)
	}
	if _, err = call("SendTaskSuccess", map[string]any{"taskToken": token, "output": `{}`}); err != nil {
		t.Fatal(err)
	}
	fault("TagResource", map[string]any{"resourceArn": "not-an-arn", "tags": []any{}}, "InvalidArn")
	fault("TagResource", map[string]any{"resourceArn": "arn:aws:states:us-east-1:1:activity:missing", "tags": []any{}}, "ResourceNotFound")
	fault("TagResource", map[string]any{"resourceArn": arn}, "ValidationException")
}

func TestStatesServiceIntegrations(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := handler.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}

	queue := sqs.New(deps)
	queueURL := invoke(queue, "CreateQueue", map[string]any{"QueueName": "workflow"})["QueueUrl"].(string)
	topicARN := invoke(sns.New(deps), "CreateTopic", map[string]any{"Name": "workflow"})["TopicArn"].(string)
	table := dynamodb.New(deps)
	invoke(table, "CreateTable", map[string]any{"TableName": "workflow", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})

	definition, _ := json.Marshal(map[string]any{"StartAt": "Queue", "States": map[string]any{
		"Queue":  map[string]any{"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage", "Parameters": map[string]any{"QueueUrl": queueURL, "MessageBody": "queued"}, "ResultPath": nil, "Next": "Notify"},
		"Notify": map[string]any{"Type": "Task", "Resource": "arn:aws:states:::sns:publish", "Parameters": map[string]any{"TopicArn": topicARN, "Message": "published"}, "ResultPath": nil, "Next": "Write"},
		"Write":  map[string]any{"Type": "Task", "Resource": "arn:aws:states:::dynamodb:putItem", "Parameters": map[string]any{"TableName": "workflow", "Item": map[string]any{"id": map[string]any{"S": "1"}, "value": map[string]any{"S": "stored"}}}, "ResultPath": nil, "Next": "Read"},
		"Read":   map[string]any{"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:dynamodb:getItem", "Parameters": map[string]any{"TableName": "workflow", "Key": map[string]any{"id": map[string]any{"S": "1"}}}, "End": true},
	}})
	if diagnostics := validateDefinition(string(definition)); len(diagnostics) != 0 {
		t.Fatalf("integration definition diagnostics %#v\n%s", diagnostics, definition)
	}
	machine := invoke(p, "CreateStateMachine", map[string]any{"name": "integrations", "definition": string(definition), "roleArn": testRoleARN, "type": "EXPRESS"})
	execution := invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"]})
	if execution["status"] != "SUCCEEDED" || !strings.Contains(execution["output"].(string), `"stored"`) {
		t.Fatalf("service integration execution %#v", execution)
	}
	if messages := invoke(queue, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})["Messages"].([]any); len(messages) != 1 || messages[0].(map[string]any)["Body"] != "queued" {
		t.Fatalf("queued messages %#v", messages)
	}

	recoveryDefinition := `{"StartAt":"Optimized","States":{"Optimized":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{"QueueUrl":"http://localhost/1/missing","MessageBody":"x"},"Catch":[{"ErrorEquals":["SQS.AmazonSQSException"],"Next":"SDK"}],"End":true},"SDK":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sqs:sendMessage","Parameters":{"QueueUrl":"http://localhost/1/missing","MessageBody":"x"},"Catch":[{"ErrorEquals":["Sqs.QueueDoesNotExistException"],"Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed"}}}`
	recoveryMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "integration-error", "definition": recoveryDefinition, "roleArn": testRoleARN, "type": "EXPRESS"})
	if recovered := invoke(p, "StartSyncExecution", map[string]any{"stateMachineArn": recoveryMachine["stateMachineArn"]}); recovered["status"] != "SUCCEEDED" {
		t.Fatalf("service error recovery %#v", recovered)
	}
}

func TestStatesCallbackServiceIntegration(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	queue := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(handler spi.Handler, operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return handler.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(handler spi.Handler, operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := call(handler, operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}

	queueURL := must(queue, "CreateQueue", map[string]any{"QueueName": "callbacks"})["QueueUrl"].(string)
	definition := `{"StartAt":"Callback","States":{"Callback":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.waitForTaskToken","Parameters":{"QueueUrl":"` + queueURL + `","MessageBody.$":"$$.Task.Token"},"Retry":[{"ErrorEquals":["Retryable"],"IntervalSeconds":1,"MaxAttempts":1,"JitterStrategy":"FULL"}],"ResultPath":"$.callback","End":true}}}`
	machine := must(p, "CreateStateMachine", map[string]any{"name": "callbacks", "definition": definition, "roleArn": testRoleARN})
	invalidDefinition := `{"StartAt":"Callback","States":{"Callback":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:putItem.waitForTaskToken","End":true}}}`
	if _, err := call(p, "CreateStateMachine", map[string]any{"name": "invalid-callback", "definition": invalidDefinition, "roleArn": testRoleARN}); err == nil {
		t.Fatal("unsupported callback integration accepted")
	}
	if _, err := call(p, "CreateStateMachine", map[string]any{"name": "express-callback", "definition": definition, "roleArn": testRoleARN, "type": "EXPRESS"}); err == nil {
		t.Fatal("Express callback integration accepted")
	}
	arn := machine["stateMachineArn"].(string)
	task := func() (string, string) {
		t.Helper()
		var messages []any
		for range 100 {
			messages = must(queue, "ReceiveMessage", map[string]any{"QueueUrl": queueURL, "VisibilityTimeout": 0})["Messages"].([]any)
			if len(messages) != 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if len(messages) != 1 {
			t.Fatalf("callback messages %#v", messages)
		}
		message := messages[0].(map[string]any)
		return message["Body"].(string), message["ReceiptHandle"].(string)
	}
	describe := func(executionARN string) map[string]any {
		t.Helper()
		return must(p, "DescribeExecution", map[string]any{"executionArn": executionARN})
	}

	retrying := must(p, "StartExecution", map[string]any{"stateMachineArn": arn, "name": "retry"})["executionArn"].(string)
	firstToken, firstHandle := task()
	must(queue, "DeleteMessage", map[string]any{"QueueUrl": queueURL, "ReceiptHandle": firstHandle})
	p.deps.Rand = zeroIntRand{p.deps.Rand}
	must(p, "SendTaskFailure", map[string]any{"taskToken": firstToken, "error": "Retryable"})
	secondToken, secondHandle := task()
	if secondToken == firstToken {
		t.Fatal("callback retry reused task token")
	}
	must(queue, "DeleteMessage", map[string]any{"QueueUrl": queueURL, "ReceiptHandle": secondHandle})
	must(p, "SendTaskFailure", map[string]any{"taskToken": secondToken, "error": "Retryable"})
	if execution := describe(retrying); execution["status"] != "FAILED" || execution["error"] != "Retryable" {
		t.Fatalf("callback retry exhaustion %#v", execution)
	}

	succeeding := must(p, "StartExecution", map[string]any{"stateMachineArn": arn, "name": "success"})["executionArn"].(string)
	successToken, successHandle := task()
	must(queue, "DeleteMessage", map[string]any{"QueueUrl": queueURL, "ReceiptHandle": successHandle})
	must(p, "SendTaskSuccess", map[string]any{"taskToken": successToken, "output": `{"done":true}`})
	if execution := describe(succeeding); execution["status"] != "SUCCEEDED" || !strings.Contains(execution["output"].(string), `"done":true`) {
		t.Fatalf("callback success %#v", execution)
	}

	stopped := must(p, "StartExecution", map[string]any{"stateMachineArn": arn, "name": "stop"})["executionArn"].(string)
	stopToken, _ := task()
	must(p, "StopExecution", map[string]any{"executionArn": stopped})
	if _, err := call(p, "SendTaskHeartbeat", map[string]any{"taskToken": stopToken}); err == nil {
		t.Fatal("stopped callback token remained valid")
	}
	if execution := describe(stopped); execution["status"] != "ABORTED" {
		t.Fatalf("stopped callback execution %#v", execution)
	}

	jsonataQueueURL := must(queue, "CreateQueue", map[string]any{"QueueName": "callbacks-jsonata"})["QueueUrl"].(string)
	jsonataDefinition := `{"QueryLanguage":"JSONata","StartAt":"Prepare","States":{"Prepare":{"Type":"Pass","Assign":{"prefix":"ready"},"Next":"Callback"},"Callback":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.waitForTaskToken","Arguments":{"QueueUrl":"` + jsonataQueueURL + `","MessageBody":"{% $states.context.Task.Token %}"},"Assign":{"done":"{% $states.result.done %}"},"Output":{"done":"{% $states.result.done %}","prefix":"{% $prefix %}"},"Catch":[{"ErrorEquals":["Oops","States.QueryEvaluationError"],"Assign":{"caught":"{% $states.errorOutput.Error %}"},"Output":{"error":"{% $states.errorOutput.Error %}","prefix":"{% $prefix %}"},"Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed","Output":{"error":"{% $caught %}","prefix":"{% $states.input.prefix %}"}}}}`
	jsonataMachine := must(p, "CreateStateMachine", map[string]any{"name": "callbacks-jsonata", "definition": jsonataDefinition, "roleArn": testRoleARN})
	jsonataExecution := must(p, "StartExecution", map[string]any{"stateMachineArn": jsonataMachine["stateMachineArn"]})["executionArn"].(string)
	jsonataMessage := must(queue, "ReceiveMessage", map[string]any{"QueueUrl": jsonataQueueURL})["Messages"].([]any)[0].(map[string]any)
	must(queue, "DeleteMessage", map[string]any{"QueueUrl": jsonataQueueURL, "ReceiptHandle": jsonataMessage["ReceiptHandle"]})
	must(p, "SendTaskSuccess", map[string]any{"taskToken": jsonataMessage["Body"], "output": `{"done":true}`})
	if execution := describe(jsonataExecution); execution["status"] != "SUCCEEDED" || execution["output"] != `{"done":true,"prefix":"ready"}` {
		t.Fatalf("jsonata callback %#v", execution)
	}
	jsonataFailed := must(p, "StartExecution", map[string]any{"stateMachineArn": jsonataMachine["stateMachineArn"]})["executionArn"].(string)
	jsonataFailureMessage := must(queue, "ReceiveMessage", map[string]any{"QueueUrl": jsonataQueueURL})["Messages"].([]any)[0].(map[string]any)
	must(p, "SendTaskFailure", map[string]any{"taskToken": jsonataFailureMessage["Body"], "error": "Oops", "cause": "nope"})
	if execution := describe(jsonataFailed); execution["status"] != "SUCCEEDED" || execution["output"] != `{"error":"Oops","prefix":"ready"}` {
		t.Fatalf("jsonata callback catch %#v", execution)
	}
	jsonataOutputFailed := must(p, "StartExecution", map[string]any{"stateMachineArn": jsonataMachine["stateMachineArn"]})["executionArn"].(string)
	jsonataOutputMessage := must(queue, "ReceiveMessage", map[string]any{"QueueUrl": jsonataQueueURL})["Messages"].([]any)[0].(map[string]any)
	must(p, "SendTaskSuccess", map[string]any{"taskToken": jsonataOutputMessage["Body"], "output": `{}`})
	if execution := describe(jsonataOutputFailed); execution["status"] != "SUCCEEDED" || execution["output"] != `{"error":"States.QueryEvaluationError","prefix":"ready"}` {
		t.Fatalf("jsonata callback output catch %#v", execution)
	}
}

func TestStatesSyncServiceIntegrations(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(handler spi.Handler, operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return handler.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(handler spi.Handler, operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := call(handler, operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}

	definition := `{"StartAt":"Batch","States":{"Batch":{"Type":"Task","Resource":"arn:aws:states:::batch:submitJob.sync","Parameters":{"JobName":"job","JobQueue":"queue"},"ResultPath":null,"Next":"Build"},"Build":{"Type":"Task","Resource":"arn:aws:states:::codebuild:startBuild.sync","Parameters":{"ProjectName":"project"},"ResultPath":null,"Next":"Glue"},"Glue":{"Type":"Task","Resource":"arn:aws:states:::glue:startJobRun.sync","Parameters":{"JobName":"job"},"ResultPath":null,"Next":"Cluster"},"Cluster":{"Type":"Task","Resource":"arn:aws:states:::elasticmapreduce:createCluster.sync","Parameters":{"Name":"cluster"},"ResultPath":null,"Next":"Step"},"Step":{"Type":"Task","Resource":"arn:aws:states:::elasticmapreduce:addStep.sync","Parameters":{"JobFlowId":"j-test"},"End":true}}}`
	machine := must(p, "CreateStateMachine", map[string]any{"name": "sync-jobs", "definition": definition, "roleArn": testRoleARN})
	executionARN := must(p, "StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"]})["executionArn"].(string)
	if execution := must(p, "DescribeExecution", map[string]any{"executionArn": executionARN}); execution["status"] != "SUCCEEDED" || !strings.Contains(execution["output"].(string), "StepIds") {
		t.Fatalf("sync execution %#v", execution)
	}
	if jobs := must(batch.New(deps), "DescribeJobs", nil)["jobs"].([]any); len(jobs) != 1 || jobs[0].(map[string]any)["jobName"] != "job" || jobs[0].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("batch sync jobs %#v", jobs)
	}
	if builds := must(codebuild.New(deps), "ListBuilds", nil)["ids"].([]any); len(builds) != 1 || !strings.HasPrefix(builds[0].(string), "project:") {
		t.Fatalf("codebuild sync builds %#v", builds)
	}
	if runs := must(glue.New(deps), "GetJobRuns", map[string]any{"JobName": "job"})["JobRuns"].([]any); len(runs) != 1 || runs[0].(map[string]any)["JobRunState"] != "SUCCEEDED" {
		t.Fatalf("glue sync runs %#v", runs)
	}
	if steps := must(emr.New(deps), "ListSteps", map[string]any{"ClusterId": "j-test"})["Steps"].([]any); len(steps) != 1 {
		t.Fatalf("emr sync steps %#v", steps)
	}
	childDefinition := `{"StartAt":"Done","States":{"Done":{"Type":"Pass","Result":{"ok":true},"End":true}}}`
	child := must(p, "CreateStateMachine", map[string]any{"name": "sync-child", "definition": childDefinition, "roleArn": testRoleARN})
	parentDefinition := `{"StartAt":"String","States":{"String":{"Type":"Task","Resource":"arn:aws:states:::states:startExecution.sync","Parameters":{"StateMachineArn":"` + child["stateMachineArn"].(string) + `","Input":{"value":1}},"ResultPath":"$.string","Next":"JSON"},"JSON":{"Type":"Task","Resource":"arn:aws:states:::states:startExecution.sync:2","Parameters":{"StateMachineArn":"` + child["stateMachineArn"].(string) + `","Input":{"value":2}},"ResultPath":"$.json","End":true}}}`
	parent := must(p, "CreateStateMachine", map[string]any{"name": "sync-parent", "definition": parentDefinition, "roleArn": testRoleARN})
	parentExecutionARN := must(p, "StartExecution", map[string]any{"stateMachineArn": parent["stateMachineArn"]})["executionArn"].(string)
	parentExecution := must(p, "DescribeExecution", map[string]any{"executionArn": parentExecutionARN})
	var nested map[string]any
	_ = json.Unmarshal([]byte(parentExecution["output"].(string)), &nested)
	stringResult, jsonResult := nested["string"].(map[string]any), nested["json"].(map[string]any)
	if parentExecution["status"] != "SUCCEEDED" || stringResult["Output"] != `{"ok":true}` || stringResult["Input"] != `{"value":1}` || jsonResult["Output"].(map[string]any)["ok"] != true {
		t.Fatalf("nested sync execution %#v %#v", parentExecution, nested)
	}
	ecsDefinition := `{"StartAt":"Run","States":{"Run":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask.sync","Parameters":{"Cluster":"default","TaskDefinition":"web"},"End":true}}}`
	ecsMachine := must(p, "CreateStateMachine", map[string]any{"name": "sync-ecs", "definition": ecsDefinition, "roleArn": testRoleARN})
	ecsExecutionARN := must(p, "StartExecution", map[string]any{"stateMachineArn": ecsMachine["stateMachineArn"]})["executionArn"].(string)
	ecsExecution := must(p, "DescribeExecution", map[string]any{"executionArn": ecsExecutionARN})
	var ecsOutput map[string]any
	_ = json.Unmarshal([]byte(ecsExecution["output"].(string)), &ecsOutput)
	task := ecsOutput["tasks"].([]any)[0].(map[string]any)
	storedTasks := must(ecs.New(deps), "DescribeTasks", map[string]any{"cluster": "default", "tasks": []any{task["taskArn"]}})["tasks"].([]any)
	if ecsExecution["status"] != "SUCCEEDED" || task["lastStatus"] != "STOPPED" || task["taskDefinitionArn"] != "web" || len(storedTasks) != 1 || storedTasks[0].(map[string]any)["lastStatus"] != "STOPPED" {
		t.Fatalf("ECS sync execution %#v stored=%#v", ecsExecution, storedTasks)
	}
	if _, err := call(p, "CreateStateMachine", map[string]any{"name": "express-sync", "definition": definition, "roleArn": testRoleARN, "type": "EXPRESS"}); err == nil {
		t.Fatal("Express .sync integration accepted")
	}
	if _, err := call(p, "CreateStateMachine", map[string]any{"name": "express-sync-json", "definition": parentDefinition, "roleArn": testRoleARN, "type": "EXPRESS"}); err == nil {
		t.Fatal("Express .sync:2 integration accepted")
	}
	invalid := `{"StartAt":"Queue","States":{"Queue":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.sync","End":true}}}`
	if _, err := call(p, "CreateStateMachine", map[string]any{"name": "invalid-sync", "definition": invalid, "roleArn": testRoleARN}); err == nil {
		t.Fatal("unsupported .sync integration accepted")
	}
}

func TestStatesTaskCredentials(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	queue := sqs.New(deps)
	ctx := context.Background()
	owner := spi.Identity{Account: "2", Region: "us-east-1"}
	caller := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, identity spi.Identity, operation string, input map[string]any) (map[string]any, error) {
		response, err := handler.Invoke(ctx, &spi.Request{Identity: identity, Operation: operation, Input: input})
		if response == nil {
			return nil, err
		}
		return response.Output, err
	}
	queueOutput, _ := invoke(queue, owner, "CreateQueue", map[string]any{"QueueName": "assumed"})
	queueURL := queueOutput["QueueUrl"].(string)
	definition := `{"StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Credentials":{"RoleArn.$":"$.role"},"Parameters":{"QueueUrl":"` + queueURL + `","MessageBody":"assumed"},"End":true}}}`
	machine, err := invoke(p, caller, "CreateStateMachine", map[string]any{"name": "credentials", "definition": definition, "roleArn": testRoleARN})
	if err != nil {
		t.Fatal(err)
	}
	started, err := invoke(p, caller, "StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"], "input": `{"role":"arn:aws:iam::2:role/target"}`})
	if err != nil {
		t.Fatal(err)
	}
	execution, _ := invoke(p, caller, "DescribeExecution", map[string]any{"executionArn": started["executionArn"]})
	if execution["status"] != "SUCCEEDED" {
		t.Fatalf("credentialed execution %#v", execution)
	}
	received, _ := invoke(queue, owner, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})
	if messages := received["Messages"].([]any); len(messages) != 1 || messages[0].(map[string]any)["Body"] != "assumed" {
		t.Fatalf("credentialed queue messages %#v", messages)
	}
	invalid := `{"StartAt":"Send","States":{"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Credentials":{"RoleArn":"invalid"},"End":true}}}`
	if _, err := invoke(p, caller, "CreateStateMachine", map[string]any{"name": "invalid-credentials", "definition": invalid, "roleArn": testRoleARN}); err == nil {
		t.Fatal("invalid Task Credentials accepted")
	}
}

func TestDistributedMapItemBatcher(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	processor := map[string]any{"StartAt": "Done", "ProcessorConfig": map[string]any{"Mode": "DISTRIBUTED"}, "States": map[string]any{"Done": map[string]any{"Type": "Succeed"}}}
	state := map[string]any{
		"Type": "Map", "ItemsPath": "$.items", "ItemSelector": map[string]any{"selected.$": "$$.Map.Item.Value"}, "ItemProcessor": processor, "End": true,
		"ItemBatcher": map[string]any{"MaxItemsPerBatchPath": "$.size", "BatchInput": map[string]any{"factor.$": "$.factor"}},
	}
	definition, _ := json.Marshal(map[string]any{"StartAt": "Batch", "States": map[string]any{"Batch": state}})
	machine := invoke("CreateStateMachine", map[string]any{"name": "batches", "definition": string(definition), "roleArn": testRoleARN})
	started := invoke("StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"], "input": `{"items":[1,2,3,4,5],"size":2,"factor":7}`})
	execution := invoke("DescribeExecution", map[string]any{"executionArn": started["executionArn"]})
	var batches []any
	_ = json.Unmarshal([]byte(execution["output"].(string)), &batches)
	if execution["status"] != "SUCCEEDED" || len(batches) != 3 || len(batches[0].(map[string]any)["Items"].([]any)) != 2 ||
		batches[0].(map[string]any)["Items"].([]any)[0].(map[string]any)["selected"] != 1.0 ||
		batches[0].(map[string]any)["BatchInput"].(map[string]any)["factor"] != 7.0 || len(batches[2].(map[string]any)["Items"].([]any)) != 1 {
		t.Fatalf("batched execution %#v %#v", execution, batches)
	}
	if _, ok := batchMapItems(map[string]any{"ItemBatcher": map[string]any{"MaxItemsPerBatch": 1, "MaxItemsPerBatchPath": "$.size"}}, map[string]any{"size": 2.0}, []any{1.0}, p.deps.Rand, nil); ok {
		t.Fatal("conflicting batch limits accepted")
	}
	if _, ok := batchMapItems(map[string]any{"ItemBatcher": map[string]any{"MaxInputBytesPerBatch": 1}}, nil, []any{"too large"}, p.deps.Rand, nil); ok {
		t.Fatal("oversized batch item accepted")
	}
	scope := jsonataScope{input: map[string]any{"tag": "jsonata"}, context: map[string]any{}, variables: map[string]any{}, random: p.deps.Rand}
	jsonataBatches, ok := batchMapItems(map[string]any{"ItemBatcher": map[string]any{"MaxItemsPerBatch": "{% 2 %}", "BatchInput": "{% [$states.input.tag, 2] %}"}}, scope.input, []any{1.0, 2.0}, p.deps.Rand, &scope)
	if !ok || len(jsonataBatches) != 1 {
		t.Fatalf("JSONata array BatchInput %#v", jsonataBatches)
	}
	arrayInput, _ := jsonataBatches[0].(map[string]any)["BatchInput"].([]any)
	if len(arrayInput) != 2 || arrayInput[0] != "jsonata" {
		t.Fatalf("JSONata array BatchInput %#v", jsonataBatches)
	}
	number, numeric := exactNumber(arrayInput[1])
	if !numeric || number != 2 {
		t.Fatalf("JSONata array BatchInput %#v", jsonataBatches)
	}
	nullBatches, ok := batchMapItems(map[string]any{"ItemBatcher": map[string]any{"MaxItemsPerBatch": 1, "BatchInput": "{% null %}"}}, scope.input, []any{1.0}, p.deps.Rand, &scope)
	if !ok || len(nullBatches) != 1 {
		t.Fatalf("JSONata null BatchInput %#v", nullBatches)
	}
	batchInput, exists := nullBatches[0].(map[string]any)["BatchInput"]
	if !exists || batchInput != nil {
		t.Fatalf("JSONata null BatchInput %#v", nullBatches)
	}
}

func TestDistributedMapResultWriter(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	storage := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(handler spi.Handler, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := handler.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	invoke(storage, "CreateBucket", map[string]any{"Bucket": "results"})
	processor := map[string]any{"StartAt": "Done", "ProcessorConfig": map[string]any{"Mode": "DISTRIBUTED"}, "States": map[string]any{"Done": map[string]any{"Type": "Succeed"}}}
	state := map[string]any{
		"Type": "Map", "ItemsPath": "$.items", "ItemProcessor": processor, "End": true,
		"ResultWriter": map[string]any{
			"WriterConfig": map[string]any{"Transformation": "FLATTEN", "OutputType": "JSON"},
			"Resource":     "arn:aws:states:::s3:putObject",
			"Parameters":   map[string]any{"Bucket.$": "$.bucket", "Prefix.$": "$.prefix"},
		},
	}
	definition, _ := json.Marshal(map[string]any{"StartAt": "Write", "States": map[string]any{"Write": state}})
	machine := invoke(p, "CreateStateMachine", map[string]any{"name": "writer", "definition": string(definition), "roleArn": testRoleARN}).Output
	started := invoke(p, "StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"], "input": `{"items":[[1,2],[3]],"bucket":"results","prefix":"jobs"}`}).Output
	execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": started["executionArn"]}).Output
	var output map[string]any
	_ = json.Unmarshal([]byte(execution["output"].(string)), &output)
	details, _ := output["ResultWriterDetails"].(map[string]any)
	if execution["status"] != "SUCCEEDED" || output["MapRunArn"] == "" || details["Bucket"] != "results" || !strings.HasSuffix(details["Key"].(string), "/manifest.json") {
		t.Fatalf("ResultWriter execution %#v %#v", execution, output)
	}
	read := func(key string) []byte {
		t.Helper()
		response := invoke(storage, "GetObject", map[string]any{"Bucket": "results", "Key": key})
		body, err := io.ReadAll(response.Stream)
		if err != nil || response.Stream.Close() != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		return body
	}
	var manifest map[string]any
	_ = json.Unmarshal(read(details["Key"].(string)), &manifest)
	succeeded := manifest["ResultFiles"].(map[string]any)["SUCCEEDED"].([]any)[0].(map[string]any)
	if !strings.HasSuffix(succeeded["Key"].(string), "/SUCCEEDED_0.json") {
		t.Fatalf("ResultWriter success file %#v", succeeded)
	}
	var flattened []any
	_ = json.Unmarshal(read(succeeded["Key"].(string)), &flattened)
	if manifest["MapRunArn"] != output["MapRunArn"] || len(flattened) != 3 || flattened[2] != 3.0 {
		t.Fatalf("ResultWriter artifacts %#v %#v", manifest, flattened)
	}
	runs := invoke(p, "ListMapRuns", map[string]any{"executionArn": started["executionArn"]}).Output["mapRuns"].([]any)
	described := invoke(p, "DescribeMapRun", map[string]any{"mapRunArn": runs[0].(map[string]any)["mapRunArn"]}).Output
	if described["itemCounts"].(map[string]any)["resultsWritten"] != 2.0 {
		t.Fatalf("ResultWriter Map Run %#v", described)
	}

	state["ResultWriter"] = map[string]any{"WriterConfig": map[string]any{"Transformation": "COMPACT", "OutputType": "JSONL"}}
	previewDefinition, _ := json.Marshal(map[string]any{"StartAt": "Preview", "States": map[string]any{"Preview": state}})
	previewMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "writer-preview", "definition": string(previewDefinition), "roleArn": testRoleARN}).Output
	previewStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": previewMachine["stateMachineArn"], "input": `{"items":[1,2]}`}).Output
	previewExecution := invoke(p, "DescribeExecution", map[string]any{"executionArn": previewStarted["executionArn"]}).Output
	var preview string
	_ = json.Unmarshal([]byte(previewExecution["output"].(string)), &preview)
	if preview != "1\n2\n" {
		t.Fatalf("ResultWriter preview %#v", previewExecution)
	}

	state["ResultWriter"] = map[string]any{"Resource": "arn:aws:states:::s3:putObject", "Parameters": map[string]any{"Bucket": "missing", "Prefix": "jobs"}}
	failingDefinition, _ := json.Marshal(map[string]any{"StartAt": "Write", "States": map[string]any{"Write": state}})
	failingMachine := invoke(p, "CreateStateMachine", map[string]any{"name": "writer-failure", "definition": string(failingDefinition), "roleArn": testRoleARN}).Output
	failingStarted := invoke(p, "StartExecution", map[string]any{"stateMachineArn": failingMachine["stateMachineArn"], "input": `{"items":[1]}`}).Output
	if failed := invoke(p, "DescribeExecution", map[string]any{"executionArn": failingStarted["executionArn"]}).Output; failed["status"] != "FAILED" || failed["error"] != "States.ResultWriterFailed" {
		t.Fatalf("ResultWriter failure %#v", failed)
	}
}

func TestStatesLifecycleAndWalkerUnits(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	if p.ServiceID() != "aws.states" || len(p.Operations()) != 37 {
		t.Fatalf("metadata %s %d", p.ServiceID(), len(p.Operations()))
	}
	if _, err := call("CreateStateMachine", map[string]any{}); err == nil {
		t.Fatal("created nameless state machine")
	}
	if _, err := call("UpdateStateMachine", map[string]any{"stateMachineArn": "missing"}); err == nil {
		t.Fatal("updated missing state machine")
	}
	if _, err := call("DescribeStateMachine", map[string]any{"stateMachineArn": "missing"}); err == nil {
		t.Fatal("described missing state machine")
	}
	if _, err := call("StartExecution", map[string]any{"stateMachineArn": "missing"}); err == nil {
		t.Fatal("started missing state machine")
	}
	definition := `{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":0,"Next":"Done"},"Done":{"Type":"Succeed"}}}`
	created := must("CreateStateMachine", map[string]any{"Name": "lifecycle", "Definition": definition, "RoleArn": testRoleARN, "Type": "EXPRESS"})
	arn := created.Output["stateMachineArn"].(string)
	must("UpdateStateMachine", map[string]any{"StateMachineArn": arn, "Definition": definition, "RoleArn": "arn:aws:iam::1:role/new"})
	if described := must("DescribeStateMachine", map[string]any{"StateMachineArn": arn}).Output; described["roleArn"] != "arn:aws:iam::1:role/new" || described["type"] != "EXPRESS" {
		t.Fatalf("state machine %#v", described)
	}
	if machines := must("ListStateMachines", nil).Output["stateMachines"].([]any); len(machines) != 1 {
		t.Fatalf("state machines %#v", machines)
	}
	started := must("StartExecution", map[string]any{"StateMachineArn": arn, "Input": `{"n":1}`})
	executionARN := started.Output["executionArn"].(string)
	if executions, _, _ := p.col(&spi.Request{Identity: id}, "ex").List(ctx, "", "", 0); len(executions) != 1 {
		t.Fatalf("executions %#v", executions)
	}
	storedExecution, _ := getRecord(ctx, p.col(&spi.Request{Identity: id}, "ex"), executionARN)
	if events := asSlice(storedExecution["history"]); storedExecution["status"] != "SUCCEEDED" || len(events) != 2 {
		t.Fatalf("history %#v", events)
	}
	if sync := must("StartSyncExecution", map[string]any{"StateMachineArn": arn, "Input": `{"n":2}`}).Output; sync["status"] != "SUCCEEDED" || sync["stopDate"] == nil {
		t.Fatalf("sync execution %#v", sync)
	}
	for _, operation := range []string{"DescribeExecution", "GetExecutionHistory", "StopExecution"} {
		if _, err := call(operation, map[string]any{"ExecutionArn": "missing"}); err == nil {
			t.Fatalf("%s missing execution", operation)
		}
	}

	activity := must("CreateActivity", map[string]any{"Name": "activity"})
	activityARN := activity.Output["activityArn"].(string)
	if must("DescribeActivity", map[string]any{"ActivityArn": activityARN}).Output["name"] != "activity" || len(must("ListActivities", nil).Output["activities"].([]any)) != 1 {
		t.Fatal("activity lifecycle")
	}
	if _, err := call("DescribeActivity", map[string]any{"ActivityArn": "missing"}); err == nil {
		t.Fatal("described missing activity")
	}
	if task := must("GetActivityTask", map[string]any{"ActivityArn": activityARN}).Output; task["taskToken"] != nil {
		t.Fatalf("unexpected activity task %#v", task)
	}
	activityDefinition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + activityARN + `","End":true}}}`
	activityMachine := must("CreateStateMachine", map[string]any{"Name": "activity-machine", "Definition": activityDefinition, "RoleArn": testRoleARN}).Output["stateMachineArn"].(string)
	runningARN := must("StartExecution", map[string]any{"StateMachineArn": activityMachine, "Name": "failure"}).Output["executionArn"].(string)
	token := must("GetActivityTask", map[string]any{"ActivityArn": activityARN}).Output["taskToken"].(string)
	must("SendTaskFailure", map[string]any{"TaskToken": token, "Error": "ActivityFailed"})
	if failed := must("DescribeExecution", map[string]any{"ExecutionArn": runningARN}).Output; failed["status"] != "FAILED" || failed["error"] != "ActivityFailed" || failed["cause"] != "ActivityFailed" {
		t.Fatalf("failed task %#v", failed)
	}
	for _, operation := range []string{"SendTaskSuccess", "SendTaskFailure", "SendTaskHeartbeat"} {
		if _, err := call(operation, map[string]any{"TaskToken": "missing"}); err == nil {
			t.Fatalf("%s accepted missing token", operation)
		}
	}
	recoveryDefinition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + activityARN + `","Retry":[{"ErrorEquals":["Retryable"],"IntervalSeconds":1,"MaxAttempts":1,"JitterStrategy":"FULL"}],"Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.failure","Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed"}}}`
	recoveryMachine := must("CreateStateMachine", map[string]any{"Name": "activity-recovery", "Definition": recoveryDefinition, "RoleArn": testRoleARN}).Output["stateMachineArn"].(string)
	recoveryARN := must("StartExecution", map[string]any{"StateMachineArn": recoveryMachine, "Name": "recovery", "Input": `{"keep":true}`}).Output["executionArn"].(string)
	firstToken := must("GetActivityTask", map[string]any{"ActivityArn": activityARN}).Output["taskToken"].(string)
	must("SendTaskHeartbeat", map[string]any{"TaskToken": firstToken})
	p.deps.Rand = zeroIntRand{p.deps.Rand}
	must("SendTaskFailure", map[string]any{"TaskToken": firstToken, "Error": "Retryable", "Cause": "try again"})
	if retrying := must("DescribeExecution", map[string]any{"ExecutionArn": recoveryARN}).Output; retrying["status"] != "RUNNING" {
		t.Fatalf("activity retry %#v", retrying)
	}
	secondToken := ""
	for range 100 {
		if token, ok := must("GetActivityTask", map[string]any{"ActivityArn": activityARN}).Output["taskToken"].(string); ok {
			secondToken = token
			break
		}
		time.Sleep(time.Millisecond)
	}
	if secondToken == "" {
		t.Fatal("activity retry did not become available")
	}
	if secondToken == firstToken {
		t.Fatal("activity retry reused task token")
	}
	must("SendTaskFailure", map[string]any{"TaskToken": secondToken, "Error": "Retryable", "Cause": "exhausted"})
	recovered := must("DescribeExecution", map[string]any{"ExecutionArn": recoveryARN}).Output
	if recovered["status"] != "SUCCEEDED" || !strings.Contains(fmtString(recovered["output"]), `"keep":true`) || !strings.Contains(fmtString(recovered["output"]), `"Error":"Retryable"`) {
		t.Fatalf("activity recovery %#v", recovered)
	}
	preserveDefinition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + activityARN + `","ResultPath":null,"End":true}}}`
	preserveMachine := must("CreateStateMachine", map[string]any{"Name": "activity-result-path", "Definition": preserveDefinition, "RoleArn": testRoleARN}).Output["stateMachineArn"].(string)
	preserveARN := must("StartExecution", map[string]any{"StateMachineArn": preserveMachine, "Name": "preserve", "Input": `{"keep":true}`}).Output["executionArn"].(string)
	preserveToken := must("GetActivityTask", map[string]any{"ActivityArn": activityARN}).Output["taskToken"].(string)
	must("SendTaskSuccess", map[string]any{"TaskToken": preserveToken, "Output": `{"drop":true}`})
	if preserved := must("DescribeExecution", map[string]any{"ExecutionArn": preserveARN}).Output; preserved["status"] != "SUCCEEDED" || !strings.Contains(fmtString(preserved["output"]), `"keep":true`) || strings.Contains(fmtString(preserved["output"]), `"drop":true`) {
		t.Fatalf("activity result path %#v", preserved)
	}
	toStop := must("StartExecution", map[string]any{"StateMachineArn": activityMachine, "Name": "stop"}).Output["executionArn"].(string)
	must("StopExecution", map[string]any{"ExecutionArn": toStop})
	if stopped := must("DescribeExecution", map[string]any{"ExecutionArn": toStop}).Output; stopped["status"] != "ABORTED" {
		t.Fatalf("stopped execution %#v", stopped)
	}
	must("DeleteActivity", map[string]any{"ActivityArn": activityARN})
	if len(must("ListActivities", nil).Output["activities"].([]any)) != 0 {
		t.Fatal("activity was not deleted")
	}
	must("DeleteStateMachine", map[string]any{"StateMachineArn": arn})
	if _, err := call("Unknown", nil); err == nil {
		t.Fatal("unknown operation succeeded")
	}
	valid := must("ValidateStateMachineDefinition", map[string]any{"definition": definition}).Output
	if valid["result"] != "OK" || len(valid["diagnostics"].([]any)) != 0 || valid["truncated"] != false {
		t.Fatalf("valid definition %#v", valid)
	}
	invalid := must("ValidateStateMachineDefinition", map[string]any{"definition": `{"StartAt":"Missing","States":{"One":{"Type":"Unknown","Next":"Gone"}}}`, "maxResults": 1.0}).Output
	if invalid["result"] != "FAIL" || len(invalid["diagnostics"].([]any)) != 1 || invalid["truncated"] != true {
		t.Fatalf("invalid definition %#v", invalid)
	}
	if missingTransition := must("ValidateStateMachineDefinition", map[string]any{"definition": `{"StartAt":"One","States":{"One":{"Type":"Pass"}}}`}).Output; missingTransition["result"] != "FAIL" {
		t.Fatalf("accepted missing transition %#v", missingTransition)
	}
	if _, err := call("ValidateStateMachineDefinition", map[string]any{"definition": definition, "maxResults": 101.0}); err == nil {
		t.Fatal("accepted excessive validation results")
	}
	_, invalidCreateErr := call("CreateStateMachine", map[string]any{"Name": "invalid", "Definition": `{`, "RoleArn": testRoleARN})
	if fault, ok := invalidCreateErr.(*spi.Fault); !ok || fault.Code != "InvalidDefinition" {
		t.Fatalf("invalid state machine error %#v", invalidCreateErr)
	}
	tested := must("TestState", map[string]any{"definition": `{"Type":"Pass","Parameters":{"message.$":"States.Format('Hi {}', $.name)"},"Next":"After"}`, "input": `{"name":"Ada"}`}).Output
	if tested["status"] != "SUCCEEDED" || tested["nextState"] != "After" || !strings.Contains(tested["output"].(string), `"message":"Hi Ada"`) {
		t.Fatalf("tested pass %#v", tested)
	}
	testedFail := must("TestState", map[string]any{"definition": `{"Type":"Fail","Error":"Nope","Cause":"failed"}`}).Output
	if testedFail["status"] != "FAILED" || testedFail["error"] != "Nope" || testedFail["cause"] != "failed" {
		t.Fatalf("tested fail %#v", testedFail)
	}
	choiceDefinition := `{"StartAt":"Pick","States":{"Pick":{"Type":"Choice","Choices":[{"Variable":"$.yes","BooleanEquals":true,"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Succeed"},"No":{"Type":"Fail"}}}`
	testedChoice := must("TestState", map[string]any{"definition": choiceDefinition, "stateName": "Pick", "input": `{"yes":true}`}).Output
	if testedChoice["status"] != "SUCCEEDED" || testedChoice["nextState"] != "Yes" {
		t.Fatalf("tested choice %#v", testedChoice)
	}
	for _, input := range []map[string]any{{"definition": `{`}, {"definition": `{"Type":"Pass"}`, "input": `{`}} {
		if _, err := call("TestState", input); err == nil {
			t.Fatalf("accepted invalid TestState %#v", input)
		}
	}

	walk := func(def string, input any) walkResult {
		t.Helper()
		return p.walk(ctx, &spi.Request{Identity: id}, def, "", input, nil)
	}
	for _, tc := range []struct {
		definition, status, cause string
	}{
		{"{", "FAILED", "InvalidDefinition"},
		{`{"StartAt":"Missing","States":{}}`, "FAILED", "States.Runtime"},
		{`{"StartAt":"Fail","States":{"Fail":{"Type":"Fail","Error":"Nope"}}}`, "FAILED", "Nope"},
		{`{"StartAt":"Unknown","States":{"Unknown":{"Type":"Unknown"}}}`, "FAILED", "States.Runtime"},
		{`{"StartAt":"Map","States":{"Map":{"Type":"Map","ItemsPath":"$.missing","End":true}}}`, "FAILED", "States.Runtime"},
		{`{"StartAt":"Loop","States":{"Loop":{"Type":"Pass","Next":"Loop"}}}`, "FAILED", "States.Runtime"},
	} {
		result := walk(tc.definition, map[string]any{})
		if result.status != tc.status || result.cause != tc.cause {
			t.Fatalf("walk %s: %#v", tc.definition, result)
		}
	}
	longStates := map[string]any{}
	for index := range 100 {
		name := fmt.Sprintf("State%d", index)
		state := map[string]any{"Type": "Pass", "End": true}
		if index < 99 {
			state = map[string]any{"Type": "Pass", "Next": fmt.Sprintf("State%d", index+1)}
		}
		longStates[name] = state
	}
	longDefinition, _ := json.Marshal(map[string]any{"StartAt": "State0", "States": longStates})
	if result := walk(string(longDefinition), nil); result.status != "SUCCEEDED" || len(result.hist) != 100 {
		t.Fatalf("long state chain %#v", result)
	}
	if result := p.walk(ctx, &spi.Request{Identity: id, Input: map[string]any{"_executionType": "EXPRESS"}}, string(longDefinition), "", nil, nil); result.status != "SUCCEEDED" || len(result.hist) != 100 {
		t.Fatalf("long Express state chain %#v", result)
	}
	activityInput := walk(`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"`+activityARN+`","InputPath":"$.task","End":true}}}`, map[string]any{"task": map[string]any{"value": 1.0}, "keep": true})
	if activityInput.status != "RUNNING" || jsonPath(activityInput.pending.Input, "$.value") != 1.0 || jsonPath(activityInput.pending.StateInput, "$.keep") != true {
		t.Fatalf("activity input path %#v", activityInput)
	}
	parallel := walk(`{"StartAt":"Parallel","States":{"Parallel":{"Type":"Parallel","Branches":[{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Result":2,"End":true}}}],"End":true}}}`, map[string]any{})
	if parallel.status != "SUCCEEDED" || len(parallel.out.([]any)) != 2 {
		t.Fatalf("parallel %#v", parallel)
	}
	parallelData := walk(`{"StartAt":"Parallel","States":{"Parallel":{"Type":"Parallel","Parameters":{"value.$":"$.shared"},"Branches":[{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}],"ResultSelector":{"first.$":"$[0].value"},"ResultPath":"$.parallel","OutputPath":"$.parallel","End":true}}}`, map[string]any{"shared": "x", "keep": true})
	if parallelData.status != "SUCCEEDED" || jsonPath(parallelData.out, "$.first") != "x" {
		t.Fatalf("parallel data flow %#v", parallelData)
	}
	parallelRecovery := walk(`{"StartAt":"Parallel","States":{"Parallel":{"Type":"Parallel","Branches":[{"StartAt":"Fail","States":{"Fail":{"Type":"Fail","Error":"ParallelBoom","Cause":"branch failed"}}}],"Retry":[{"ErrorEquals":["ParallelBoom"],"MaxAttempts":1}],"Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.failure","Next":"Recovered"}]},"Recovered":{"Type":"Succeed"}}}`, map[string]any{"keep": true})
	parallelOutput := parallelRecovery.out.(map[string]any)
	if parallelRecovery.status != "SUCCEEDED" || parallelOutput["keep"] != true || jsonPath(parallelOutput, "$.failure.Error") != "ParallelBoom" || jsonPath(parallelOutput, "$.failure.Cause") != "branch failed" {
		t.Fatalf("parallel recovery %#v", parallelRecovery)
	}
	emptyMap := walk(`{"StartAt":"Map","States":{"Map":{"Type":"Map","ItemsPath":"$.items","ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`, map[string]any{"items": []any{}})
	if emptyMap.status != "SUCCEEDED" || len(emptyMap.out.([]any)) != 0 {
		t.Fatalf("empty map %#v", emptyMap)
	}
	selectedMap := walk(`{"StartAt":"Map","States":{"Map":{"Type":"Map","ItemsPath":"$.items","ItemSelector":{"index.$":"$$.Map.Item.Index","value.$":"$$.Map.Item.Value","source.$":"$$.Map.Item.Source","batch.$":"$.batch","nested":{"n.$":"$$.Map.Item.Value.n"}},"ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"End":true}}}`, map[string]any{"batch": "a", "items": []any{map[string]any{"n": 1.0}, map[string]any{"n": 2.0}}})
	selected := selectedMap.out.([]any)
	firstSelected := selected[0].(map[string]any)
	if selectedMap.status != "SUCCEEDED" || len(selected) != 2 || firstSelected["index"] != 0.0 || firstSelected["source"] != "STATE_DATA" || firstSelected["batch"] != "a" || jsonPath(firstSelected, "$.nested.n") != 1.0 {
		t.Fatalf("selected map %#v", selectedMap)
	}
	mapRecovery := walk(`{"StartAt":"Map","States":{"Map":{"Type":"Map","ItemsPath":"$.items","ItemProcessor":{"StartAt":"Fail","States":{"Fail":{"Type":"Fail","Error":"MapBoom"}}},"Catch":[{"ErrorEquals":["States.TaskFailed"],"ResultPath":null,"Next":"Recovered"}]},"Recovered":{"Type":"Succeed"}}}`, map[string]any{"items": []any{1.0}, "keep": true})
	if output := mapRecovery.out.(map[string]any); mapRecovery.status != "SUCCEEDED" || output["keep"] != true {
		t.Fatalf("map recovery %#v", mapRecovery)
	}
	mapData := walk(`{"StartAt":"Map","States":{"Map":{"Type":"Map","InputPath":"$.payload","ItemsPath":"$.items","ItemProcessor":{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}},"ResultSelector":{"flat.$":"$[*][*]"},"ResultPath":"$.mapped","OutputPath":"$.mapped","End":true}}}`, map[string]any{"payload": map[string]any{"items": []any{[]any{1.0, 2.0}, []any{3.0}}}, "keep": true})
	if mapData.status != "SUCCEEDED" || fmtString(jsonPath(mapData.out, "$.flat")) != `[1,2,3]` {
		t.Fatalf("map data flow %#v", mapData)
	}
	data := map[string]any{"s": "yes", "n": 2.0, "b": true, "present": 1}
	if !matchChoice(map[string]any{"Variable": "$.s", "StringEquals": "yes"}, data) ||
		!matchChoice(map[string]any{"Variable": "$.n", "NumericEquals": 2}, data) ||
		!matchChoice(map[string]any{"Variable": "$.b", "BooleanEquals": true}, data) ||
		!matchChoice(map[string]any{"Variable": "$.present", "IsPresent": true}, data) ||
		matchChoice(map[string]any{"Variable": "$.missing", "StringEquals": "yes"}, data) {
		t.Fatal("choice matching")
	}
	choice := map[string]any{"Choices": []any{map[string]any{"Variable": "$.s", "StringEquals": "no", "Next": "No"}}, "Default": "Default"}
	if choiceNext(choice, data) != "Default" {
		t.Fatal("choice default")
	}
	params, paramsOK := taskPayload(map[string]any{"Parameters": map[string]any{
		"Payload": map[string]any{"value.$": "$.n"}, "nested": map[string]any{"value.$": "$.s"},
		"list": []any{map[string]any{"value.$": "$.n"}, []any{map[string]any{"value.$": "$.s"}}},
	}}, data, nil, p.deps.Rand)
	_, missingParamsOK := taskPayload(map[string]any{"Parameters": map[string]any{"missing.$": "$.missing"}}, data, nil, p.deps.Rand)
	if !paramsOK || missingParamsOK || jsonPath(params, "$.Payload.value") != 2.0 || jsonPath(params, "$.nested.value") != "yes" || jsonPath(params, "$.list[0].value") != 2.0 || jsonPath(params, "$.list[1][0].value") != "yes" || jsonPath(data, "$.missing.value") != nil || parseJSON("plain") != "plain" || toFloat(json.Number("3")) != 3 || !toBool("true") {
		t.Fatal("data helpers")
	}
	intrinsicData := map[string]any{
		"s": "world", "arr": []any{1.0, 2.0, 2.0, 3.0},
		"left": map[string]any{"a": 1.0, "same": "old"}, "right": map[string]any{"b": 2.0, "same": "new"},
	}
	for _, tc := range []struct {
		expression, want string
	}{
		{`States.Array($.s, 2)`, `["world",2]`},
		{`States.ArrayPartition($.arr, 3)`, `[[1,2,2],[3]]`},
		{`States.ArrayContains($.arr, 3)`, `true`},
		{`States.ArrayRange(1, 5, 2)`, `[1,3,5]`},
		{`States.ArrayGetItem($.arr, 1)`, `2`},
		{`States.ArrayLength($.arr)`, `4`},
		{`States.ArrayUnique($.arr)`, `[1,2,3]`},
		{`States.Base64Encode('hello')`, `aGVsbG8=`},
		{`States.Base64Decode('aGVsbG8=')`, `hello`},
		{`States.Hash('input data', 'SHA-1')`, `aaff4a450a104cd177d28d18d74485e8cae074b7`},
		{`States.JsonMerge($.left, $.right, false)`, `{"a":1,"b":2,"same":"new"}`},
		{`States.StringToJson('{"a":1}')`, `{"a":1}`},
		{`States.JsonToString($.left)`, `{"a":1,"same":"old"}`},
		{`States.MathAdd(1.4, 2.6)`, `4`},
		{`States.StringSplit('a.b+c', '.+')`, `["a","b","c"]`},
		{`States.Format('Hello, {} {}', $.s, States.ArrayGetItem($.arr, 0))`, `Hello, world 1`},
	} {
		got, ok := evalIntrinsic(tc.expression, intrinsicData, nil, p.deps.Rand)
		if !ok || fmtString(got) != tc.want {
			t.Fatalf("%s = %#v, %v want %s", tc.expression, got, ok, tc.want)
		}
	}
	for _, invalid := range []string{`States.ArrayGetItem($.arr, 99)`, `States.ArrayPartition($.arr, 0)`, `States.ArrayRange(1, 1001, 1)`, `States.Base64Decode('!')`, `States.JsonMerge($.left, $.right, true)`, `States.MathAdd('1', 2)`, `States.Format('{} {}', 1)`, `States.Format('open\')`} {
		if got, ok := evalIntrinsic(invalid, intrinsicData, nil, p.deps.Rand); ok {
			t.Fatalf("invalid intrinsic %s = %#v", invalid, got)
		}
	}
	seeded1, ok1 := evalIntrinsic(`States.MathRandom(10, 20, 42)`, intrinsicData, nil, p.deps.Rand)
	seeded2, ok2 := evalIntrinsic(`States.MathRandom(10, 20, 42)`, intrinsicData, nil, p.deps.Rand)
	uuid, uuidOK := evalIntrinsic(`States.UUID()`, intrinsicData, nil, p.deps.Rand)
	if !ok1 || !ok2 || seeded1 != seeded2 || seeded1.(float64) < 10 || seeded1.(float64) >= 20 || !uuidOK || len(uuid.(string)) != 36 || uuid.(string)[14] != '4' {
		t.Fatalf("random intrinsics %#v %#v %#v", seeded1, seeded2, uuid)
	}
	intrinsicPass := walk(`{"StartAt":"Build","States":{"Build":{"Type":"Pass","Parameters":{"message.$":"States.Format('Hello, {}', $.name)","parts.$":"States.StringSplit($.path, '/')"},"End":true}}}`, map[string]any{"name": "Ada", "path": "a/b"})
	if intrinsicPass.status != "SUCCEEDED" || jsonPath(intrinsicPass.out, "$.message") != "Hello, Ada" || len(jsonPath(intrinsicPass.out, "$.parts").([]any)) != 2 {
		t.Fatalf("intrinsic pass %#v", intrinsicPass)
	}
	dataFlowPass := walk(`{"StartAt":"Build","States":{"Build":{"Type":"Pass","InputPath":"$.selected","Parameters":{"message.$":"States.Format('Hello, {}', $.name)"},"ResultPath":"$.keep.result","OutputPath":"$.keep","End":true}}}`, map[string]any{"selected": map[string]any{"name": "Ada"}, "keep": map[string]any{"original": true}})
	if dataFlowPass.status != "SUCCEEDED" || jsonPath(dataFlowPass.out, "$.original") != true || jsonPath(dataFlowPass.out, "$.result.message") != "Hello, Ada" {
		t.Fatalf("pass data flow %#v", dataFlowPass)
	}
	if invalidPath := walk(`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","InputPath":"$.missing","End":true}}}`, map[string]any{}); invalidPath.status != "FAILED" || invalidPath.cause != "States.Runtime" {
		t.Fatalf("invalid input path %#v", invalidPath)
	}
	if discarded, ok := applyDataPath(map[string]any{"OutputPath": nil}, "OutputPath", map[string]any{"drop": true}); !ok || len(discarded.(map[string]any)) != 0 {
		t.Fatalf("null output path %#v", discarded)
	}

	recovery := walk(`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:1:function:missing","Retry":[{"ErrorEquals":["States.TaskFailed"],"MaxAttempts":2}],"Catch":[{"ErrorEquals":["Lambda.ResourceNotFoundException"],"ResultPath":"$.error","Next":"Recovered"}]},"Recovered":{"Type":"Succeed"}}}`, map[string]any{"original": true})
	got, _ := recovery.out.(map[string]any)
	failure, _ := got["error"].(map[string]any)
	if recovery.status != "SUCCEEDED" || got["original"] != true || failure["Error"] != "Lambda.ResourceNotFoundException" {
		t.Fatalf("task recovery %#v", recovery)
	}
	if unsupported := walk(`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"arn:aws:states:::unknown","End":true}}}`, nil); unsupported.status != "FAILED" || unsupported.cause != "States.Runtime" {
		t.Fatalf("unsupported task %#v", unsupported)
	}
	retrier := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Nope"}}, map[string]any{"ErrorEquals": []any{"States.ALL"}, "MaxAttempts": 2.0}}}
	attempts := map[int]int{}
	retry := func(state map[string]any, attempts map[int]int) bool {
		_, ok := retryTask(state, "Boom", attempts, p.deps.Rand)
		return ok
	}
	if !retry(retrier, attempts) || !retry(retrier, attempts) || retry(retrier, attempts) || attempts[1] != 2 {
		t.Fatalf("retry attempts %#v", attempts)
	}
	defaults := map[int]int{}
	defaultRetrier := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Boom"}}}}
	if !retry(defaultRetrier, defaults) || !retry(defaultRetrier, defaults) || !retry(defaultRetrier, defaults) || retry(defaultRetrier, defaults) {
		t.Fatalf("default retry attempts %#v", defaults)
	}
	if matchesError([]any{"States.ALL"}, "States.Runtime") || matchesError([]any{"States.TaskFailed"}, "States.Timeout") || !matchesError([]any{"States.TaskFailed"}, "Boom") {
		t.Fatal("error wildcard matching")
	}
	preserved := map[string]any{"keep": true}
	if out, ok := applyResultPath(map[string]any{"ResultPath": nil}, preserved, "ignored"); !ok || out.(map[string]any)["keep"] != true {
		t.Fatalf("null ResultPath %#v", out)
	}
	nested := map[string]any{"result": map[string]any{}}
	if out, ok := applyResultPath(map[string]any{"ResultPath": "$.result.value"}, nested, 3); !ok || jsonPath(out, "$.result.value") != 3 {
		t.Fatalf("nested ResultPath %#v", out)
	}
	createdPathInput := map[string]any{"keep": true}
	if out, ok := applyResultPath(map[string]any{"ResultPath": `$['result.value'].status`}, createdPathInput, 3); !ok || jsonPath(out, `$['result.value'].status`) != 3 {
		t.Fatalf("constructed ResultPath %#v", out)
	}
	if _, ok := applyResultPath(map[string]any{"ResultPath": "$.items[999999999]"}, map[string]any{}, 3); ok {
		t.Fatal("sparse ResultPath")
	}
	arrayInput := []any{1.0, map[string]any{}}
	if out, ok := applyResultPath(map[string]any{"ResultPath": `$[-1]['result.value']`}, arrayInput, 3); !ok || jsonPath(out, `$[1]['result.value']`) != 3 {
		t.Fatalf("array ResultPath %#v", out)
	}
	if out, ok := applyResultPath(map[string]any{"ResultPath": "$[-1]"}, []any{1.0, 2.0}, 3); !ok || jsonPath(out, "$[1]") != 3 {
		t.Fatalf("direct array ResultPath %#v", out)
	}
	if _, ok := applyResultPath(map[string]any{"ResultPath": "$.result.value"}, map[string]any{"result": 1.0}, 3); ok {
		t.Fatal("conflicting ResultPath")
	}
	if _, ok := applyResultPath(map[string]any{"ResultPath": "$[*]"}, []any{1.0}, 3); ok {
		t.Fatal("non-reference ResultPath")
	}
	if out, ok := applyResultPath(map[string]any{"ResultPath": "$"}, nested, 4); !ok || out != 4 {
		t.Fatalf("root ResultPath %#v", out)
	}
	selectedResult, selectedOK := applyStateResult(map[string]any{"ResultSelector": map[string]any{"picked.$": "$.value"}, "ResultPath": "$.task"}, map[string]any{"keep": true}, map[string]any{"value": 5.0, "drop": true}, nil, p.deps.Rand)
	if !selectedOK || jsonPath(selectedResult, "$.keep") != true || jsonPath(selectedResult, "$.task.picked") != 5.0 {
		t.Fatalf("selected result %#v", selectedResult)
	}
	paths := map[string]any{"a": []any{map[string]any{"v": 1.0}, map[string]any{"v": 2.0}, map[string]any{"v": 3.0}}}
	if jsonPath(paths, "$.a[1].v") != 2.0 || fmtString(jsonPath(paths, "$.a[0:2].v")) != `[1,2]` || fmtString(jsonPath(paths, "$.a[-2:].v")) != `[2,3]` || fmtString(jsonPath(paths, "$.a[0:3:2].v")) != `[1,3]` || fmtString(jsonPath(paths, "$.a[0,2].v")) != `[1,3]` || jsonPath(paths, "$.a[-1].v") != 3.0 || jsonPath(paths, "$['a'][2].v") != 3.0 || jsonPath(paths, "$.a[9]") != nil {
		t.Fatal("json path arrays")
	}
	if jsonPath(paths, "$.a.length()") != 3.0 || jsonPath(paths, "$.a[*].length()") != 3.0 || jsonPath(paths, "$..a.length()") != 3.0 {
		t.Fatal("json path length function")
	}
	if _, found := jsonPathLookup(paths, "$.a[0].v.length()"); found {
		t.Fatal("json path length accepted non-array")
	}
	numbers := map[string]any{"values": []any{2.0, "ignored", 1.0, 3.0}}
	if jsonPath(numbers, "$.values.min()") != 1.0 || jsonPath(numbers, "$.values.max()") != 3.0 || jsonPath(numbers, "$.values.avg()") != 2.0 || jsonPath(numbers, "$.values.sum()") != 6.0 {
		t.Fatal("json path numeric functions")
	}
	if deviation, _ := jsonPath(numbers, "$.values.stddev()").(float64); math.Abs(deviation-math.Sqrt(2.0/3.0)) > 1e-12 {
		t.Fatalf("json path standard deviation %v", deviation)
	}
	if _, found := jsonPathLookup(map[string]any{"values": []any{"x"}}, "$.values.sum()"); found {
		t.Fatal("json path aggregation accepted empty numeric input")
	}
	structure := map[string]any{"object": map[string]any{"b": 2.0, "a": 1.0}, "values": []any{"a", "b", "c"}}
	if jsonPath(structure, "$.object.length()") != 2.0 || jsonPath(structure, "$.object.size()") != 2.0 || fmtString(jsonPath(structure, "$.object.keys()")) != `["a","b"]` || jsonPath(structure, "$.values.first()") != "a" || jsonPath(structure, "$.values.last()") != "c" || jsonPath(structure, "$.values.index(1)") != "b" || jsonPath(structure, "$.values.index(-1)") != "c" {
		t.Fatal("json path structural functions")
	}
	if _, found := jsonPathLookup(map[string]any{"values": []any{}}, "$.values.first()"); found {
		t.Fatal("json path first accepted empty array")
	}
	if _, found := jsonPathLookup(structure, "$.values.index(3)"); found {
		t.Fatal("json path index accepted out-of-range index")
	}
	functionArgs := map[string]any{"words": []any{"a", 1.0, "b"}, "suffix": "!", "values": []any{1.0}, "extra": 3.0, "more": []any{"x", 2.0}, "numbers": []any{2.0, 3.0}}
	if jsonPath(functionArgs, `$.words.concat(",", $.suffix, $.more, $.more.first())`) != "ab,!x2x" || jsonPath(functionArgs, `$.words.concat()`) != "ab" || fmtString(jsonPath(functionArgs, `$.values.append(2, $.extra)`)) != `[1,2,3]` || fmtString(jsonPath(functionArgs, `$.values.append([2,3])`)) != `[1,[2,3]]` || fmtString(jsonPath(functionArgs, `$.values.append()`)) != `[1]` {
		t.Fatal("json path argument functions")
	}
	if jsonPath(functionArgs, `$.values.sum(4, $.numbers)`) != 10.0 || jsonPath(functionArgs, `$.suffix.min(4, 2)`) != 2.0 || jsonPath(functionArgs, `$.suffix.length($.words)`) != 3.0 {
		t.Fatal("json path aggregate function arguments")
	}
	if _, found := jsonPathLookup(functionArgs, `$.suffix.length($.words, $.values)`); found {
		t.Fatal("json path length accepted multiple arguments")
	}
	if _, found := jsonPathLookup(functionArgs, `$.values.append($.missing)`); found {
		t.Fatal("json path function accepted missing path argument")
	}
	if empty, found := jsonPathLookup([]any{}, "$[*]"); !found || fmtString(empty) != `[]` {
		t.Fatalf("empty wildcard %#v %t", empty, found)
	}
	if empty, found := jsonPathLookup(paths, "$.a[9:10]"); !found || fmtString(empty) != `[]` {
		t.Fatalf("empty slice %#v %t", empty, found)
	}
	if empty, found := jsonPathLookup(paths, "$.a[9,10]"); !found || fmtString(empty) != `[]` {
		t.Fatalf("empty union %#v %t", empty, found)
	}
	if recursive, found := jsonPathLookup(paths, "$..v"); !found || fmtString(recursive) != `[1,2,3]` {
		t.Fatalf("recursive descent %#v %t", recursive, found)
	}
	if empty, found := jsonPathLookup(paths, "$..missing"); !found || fmtString(empty) != `[]` {
		t.Fatalf("empty recursive descent %#v %t", empty, found)
	}
	if recursive, found := jsonPathLookup(map[string]any{"a": []any{1.0}}, "$..*"); !found || fmtString(recursive) != `[[1],1]` {
		t.Fatalf("recursive wildcard %#v %t", recursive, found)
	}
	products := []any{
		map[string]any{"name": "a", "code": "a/b", "price": 5.0, "limit": 10.0, "active": true, "note": nil, "metrics": []any{5.0}, "tags": []any{"red", "sale"}},
		map[string]any{"name": "b", "price": 12.0, "limit": 10.0, "active": false, "metrics": []any{4.0}, "tags": []any{"blue"}},
		map[string]any{"name": "c", "price": 8.0, "limit": 9.0, "active": true, "note": "x", "metrics": []any{6.0}, "tags": []any{"green"}},
	}
	if filtered := jsonPath(products, "$[?(@.price < 10)].name"); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("numeric filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name != 'b')].name`); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("string filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.active == true)].name"); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("boolean filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.note == null)].name"); fmtString(filtered) != `["a"]` {
		t.Fatalf("null filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.note)].name"); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("existence filter %#v", filtered)
	}
	if filtered, found := jsonPathLookup(products, "$[?(@.price > 100)]"); !found || fmtString(filtered) != `[]` {
		t.Fatalf("empty filter %#v %t", filtered, found)
	}
	if filtered := jsonPath(products, "$[?(@.price < @.limit)].name"); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("path filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(10 > @.price)].name"); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("literal-left filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.name =~ /[AB]/i)].name"); fmtString(filtered) != `["a","b"]` {
		t.Fatalf("regex filter %#v", filtered)
	}
	if filtered := jsonPath([]any{map[string]any{"name": "ab"}, map[string]any{"name": "a b"}}, "$[?(@.name =~ /a # comment\n b/x)].name"); fmtString(filtered) != `["ab"]` {
		t.Fatalf("comments regex filter %#v", filtered)
	}
	if filtered := jsonPath([]any{map[string]any{"name": "Ä"}}, `$[?(@.name =~ /ä/iuU)].name`); fmtString(filtered) != `["Ä"]` {
		t.Fatalf("unicode regex filter %#v", filtered)
	}
	unicodePatterns := map[string]string{`\d`: "١", `[\d]+`: "١٢", `\s`: "\u00a0", `[\s]`: "\u00a0", `\w+`: "éclair", `[\w]+`: "éclair", `\D`: "é"}
	for pattern, name := range unicodePatterns {
		if filtered := jsonPath([]any{map[string]any{"name": name}}, `$[?(@.name =~ /`+pattern+`/U)].name`); fmtString(filtered) != fmtString([]any{name}) {
			t.Fatalf("unicode class %s filter %#v", pattern, filtered)
		}
	}
	if filtered := jsonPath([]any{map[string]any{"name": "١"}}, `$[?(@.name =~ /\d/)].name`); fmtString(filtered) != `[]` {
		t.Fatalf("ASCII regex class filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.name =~ /a{1,2}/)].name"); fmtString(filtered) != `["a"]` {
		t.Fatalf("regex comma filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.code =~ /a\/b/)].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("regex slash filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.code =~ /a/)].name`); fmtString(filtered) != `[]` {
		t.Fatalf("regex full-match filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.price =~ /5/)].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("numeric regex filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.active =~ /true/)].name`); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("boolean regex filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.tags =~ /red/)].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("array regex filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name in ['a','c'])].name`); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("in filter %#v", filtered)
	}
	if filtered := jsonPath([]any{map[string]any{"n": 1}}, `$[?(@.n in [1])]`); fmtString(filtered) != `[{"n":1}]` {
		t.Fatalf("numeric in filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name nin ['a','c'])].name`); fmtString(filtered) != `["b"]` {
		t.Fatalf("not-in filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name IN ['a','c'])].name`); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("uppercase in filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name === 'a')].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("strict equality filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name !== 'a')].name`); fmtString(filtered) != `["b","c"]` {
		t.Fatalf("strict inequality filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.code contains '/')].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("string contains filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.tags contains 'sale')].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("array contains filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.tags all ['red','sale'])].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("all filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.tags subsetof ['red','sale','blue'])].name`); fmtString(filtered) != `["a","b"]` {
		t.Fatalf("subset filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.tags anyof ['sale'])].name`); fmtString(filtered) != `["a"]` {
		t.Fatalf("any-of filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.tags noneof ['sale'])].name`); fmtString(filtered) != `["b","c"]` {
		t.Fatalf("none-of filter %#v", filtered)
	}
	containers := []any{
		map[string]any{"name": "array", "value": []any{}},
		map[string]any{"name": "text", "value": ""},
		map[string]any{"name": "map", "value": map[string]any{}},
		map[string]any{"name": "full", "value": []any{1.0}},
		map[string]any{"name": "emoji", "value": "😀"},
	}
	if filtered := jsonPath(containers, `$[?(@.value empty true)].name`); fmtString(filtered) != `["array","text","map"]` {
		t.Fatalf("empty filter %#v", filtered)
	}
	if filtered := jsonPath(containers, `$[?(@.value size 0)].name`); fmtString(filtered) != `["array","text","map"]` {
		t.Fatalf("size filter %#v", filtered)
	}
	if filtered := jsonPath(containers, `$[?(@.value size 2)].name`); fmtString(filtered) != `["emoji"]` {
		t.Fatalf("utf16 size filter %#v", filtered)
	}
	if filtered := jsonPath(map[string]any{"size": 2.0, "items": containers}, `$.items[?(@.value size $.size)].name`); fmtString(filtered) != `["emoji"]` {
		t.Fatalf("size path filter %#v", filtered)
	}
	if filtered := jsonPath([]any{1.0, 2.0, 3.0}, "$[?(@ > 1)]"); fmtString(filtered) != `[2,3]` {
		t.Fatalf("primitive filter %#v", filtered)
	}
	if filtered := jsonPath(map[string]any{"name": "a", "price": 5.0}, "$[?(@.price < 10)].name"); fmtString(filtered) != `["a"]` {
		t.Fatalf("object filter %#v", filtered)
	}
	filterRoot := map[string]any{"limit": 7.0, "products": products}
	if filtered := jsonPath(filterRoot, "$.products[?(@.price < $.limit)].name"); fmtString(filtered) != `["a"]` {
		t.Fatalf("root path filter %#v", filtered)
	}
	if filtered := jsonPath(filterRoot, "$.products[?($.limit > @.price)].name"); fmtString(filtered) != `["a"]` {
		t.Fatalf("root-left path filter %#v", filtered)
	}
	filterRoot["allowed"] = []any{"a", "c"}
	if filtered := jsonPath(filterRoot, "$.products[?(@.name in $.allowed)].name"); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("collection path filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.price < $limit)].name", map[string]any{"limit": 8.0}); fmtString(filtered) != `["a"]` {
		t.Fatalf("variable path filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.active == true && @.price < 6)].name"); fmtString(filtered) != `["a"]` {
		t.Fatalf("and filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name == 'a' || @.name == 'b')].name`); fmtString(filtered) != `["a","b"]` {
		t.Fatalf("or filter %#v", filtered)
	}
	if filtered := jsonPath(products, `$[?(@.name == 'a' || @.name == 'b' && @.active == false)].name`); fmtString(filtered) != `["a","b"]` {
		t.Fatalf("filter precedence %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(!(@.active == true))].name"); fmtString(filtered) != `["b"]` {
		t.Fatalf("not filter %#v", filtered)
	}
	if filtered := jsonPath(products, "$[?(@.metrics[0] >= 5)].name"); fmtString(filtered) != `["a","c"]` {
		t.Fatalf("nested filter %#v", filtered)
	}
	members := map[string]any{"store.book": map[string]any{"a:b": 1.0, "close]key": 2.0, "quote'key": 3.0, "comma,key": 4.0}, "foo bar": 5.0}
	if jsonPath(members, `$.store\.book['a:b']`) != 1.0 || jsonPath(members, `$.foo\ bar`) != 5.0 || jsonPath(members, `$['store.book']['close]key']`) != 2.0 || jsonPath(members, `$['store.book']['quote\'key']`) != 3.0 || fmtString(jsonPath(members, `$['store.book']['comma,key','a:b']`)) != `[4,1]` || fmtString(jsonPath(map[string]any{"b": 2.0, "a": 1.0}, "$.*")) != `[1,2]` {
		t.Fatal("json path members")
	}
	composite := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Boom"}, "MaxAttempts": 1.0}}, "Catch": []any{map[string]any{"ErrorEquals": []any{"States.ALL"}, "Next": "Recovered"}}}
	compositeAttempts := map[int]int{}
	if _, _, _, retry, caught := recoverState(composite, walkResult{cause: "failed", errorName: "Boom"}, nil, compositeAttempts, p.deps.Rand); !retry || caught {
		t.Fatal("composite did not retry")
	}
	if next, _, _, retry, caught := recoverState(composite, walkResult{cause: "failed", errorName: "Boom"}, nil, compositeAttempts, p.deps.Rand); retry || !caught || next != "Recovered" {
		t.Fatal("composite did not catch exhausted retry")
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

	mapDef := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.nums","ItemSelector":{"index.$":"$$.Map.Item.Index","value.$":"$$.Map.Item.Value"},"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	created := sfn("CreateStateMachine", `{"name":"mapsm","definition":`+mustJSON(mapDef)+`,"roleArn":"arn:aws:iam::000000000000:role/x"}`)
	arn, _ := created["stateMachineArn"].(string)
	started := sfn("StartExecution", `{"stateMachineArn":"`+arn+`","name":"mapex","input":"{\"nums\":[{\"n\":1},{\"n\":2}]}"}`)
	desc := sfn("DescribeExecution", `{"executionArn":"`+started["executionArn"].(string)+`"}`)
	if desc["status"] != "SUCCEEDED" {
		t.Fatalf("map exec %v", desc)
	}
	if !strings.Contains(fmtString(desc["output"]), `"index":0`) || !strings.Contains(fmtString(desc["output"]), `"n":1`) || !strings.Contains(fmtString(desc["output"]), `"n":2`) {
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

func TestStatesCompositeConcurrency(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	t.Cleanup(func() { _ = p.Close() })
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}

	isolation := p.walk(ctx, &spi.Request{Identity: id, Input: map[string]any{"_executionArn": "parallel-isolation"}}, `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"A","States":{"A":{"Type":"Pass","Result":1,"ResultPath":"$.branch","End":true}}},{"StartAt":"B","States":{"B":{"Type":"Pass","Result":2,"ResultPath":"$.branch","End":true}}}],"End":true}}}`, "", map[string]any{"shared": true}, nil)
	if isolation.status != "SUCCEEDED" {
		t.Fatalf("parallel isolation %#v", isolation)
	}
	isolated := isolation.out.([]any)
	if jsonPath(isolated[0], "$.branch") != 1.0 || jsonPath(isolated[1], "$.branch") != 2.0 {
		t.Fatalf("branches shared mutable input %#v", isolated)
	}

	if _, err := exec.LookPath("python3"); err != nil {
		return
	}
	src := `import fcntl,json,time
def change(path, delta):
    with open(path, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        raw = f.read()
        state = json.loads(raw) if raw else {"active": 0, "maximum": 0}
        state["active"] += delta
        state["maximum"] = max(state["maximum"], state["active"])
        f.seek(0)
        f.truncate()
        json.dump(state, f)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)
def lambda_handler(event, context):
    change(event["path"], 1)
    time.sleep(event["delay"])
    change(event["path"], -1)
    return event
`
	if _, err := lambda.New(deps).Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "concurrency-worker", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(src))},
	}}); err != nil {
		t.Fatal(err)
	}
	maximum := func(path string) int {
		t.Helper()
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var state struct {
			Maximum int `json:"maximum"`
		}
		if err := json.Unmarshal(encoded, &state); err != nil {
			t.Fatal(err)
		}
		return state.Maximum
	}
	task := `{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:1:function:concurrency-worker","End":true}`
	parallelPath := t.TempDir() + "/parallel.json"
	parallel := p.walk(ctx, &spi.Request{Identity: id, Input: map[string]any{"_executionArn": "parallel-concurrency"}}, `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"T","States":{"T":`+task+`}},{"StartAt":"T","States":{"T":`+task+`}}],"End":true}}}`, "", map[string]any{"path": parallelPath, "delay": 0.2}, nil)
	if parallel.status != "SUCCEEDED" || len(parallel.out.([]any)) != 2 || maximum(parallelPath) != 2 {
		t.Fatalf("parallel did not overlap %#v maximum=%d", parallel, maximum(parallelPath))
	}

	mapPath := t.TempDir() + "/map.json"
	items := make([]any, 4)
	for index := range items {
		items[index] = map[string]any{"path": mapPath, "delay": 0.2, "index": float64(index)}
	}
	mapped := p.walk(ctx, &spi.Request{Identity: id, Input: map[string]any{"_executionArn": "map-concurrency"}}, `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":2,"ItemProcessor":{"StartAt":"T","States":{"T":`+task+`}},"End":true}}}`, "", map[string]any{"items": items}, nil)
	outputs, ok := mapped.out.([]any)
	if mapped.status != "SUCCEEDED" || !ok || len(outputs) != len(items) || maximum(mapPath) != 2 {
		t.Fatalf("map concurrency %#v maximum=%d", mapped, maximum(mapPath))
	}
	for index := range outputs {
		if jsonPath(outputs[index], "$.index") != float64(index) {
			t.Fatalf("map output order %#v", outputs)
		}
	}
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
