package states

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/batch"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codebuild"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ecs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/emr"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/glue"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
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
		"items.json":  `[{"id":1},{"id":2}]`,
		"items.jsonl": "{\"id\":3}\n{\"id\":4}\n",
		"items.csv":   "id,name\n5,Ada\n6,Lin\n",
	}
	for key, body := range objects {
		invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": key}, []byte(body))
	}
	type parquetItem struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	var parquetBody bytes.Buffer
	writer := parquet.NewGenericWriter[parquetItem](&parquetBody)
	if _, err := writer.Write([]parquetItem{{ID: 7, Name: "Ada"}, {ID: 8, Name: "Lin"}}); err != nil || writer.Close() != nil {
		t.Fatalf("write parquet: %v", err)
	}
	invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": "items.parquet"}, parquetBody.Bytes())
	invoke(storage, "PutObject", map[string]any{"Bucket": "items", "Key": "broken.parquet"}, []byte("not parquet"))
	processor := map[string]any{"StartAt": "Done", "ProcessorConfig": map[string]any{"Mode": "DISTRIBUTED"}, "States": map[string]any{"Done": map[string]any{"Type": "Succeed"}}}
	for _, test := range []struct{ key, inputType string }{{"items.json", "JSON"}, {"items.jsonl", "JSONL"}, {"items.csv", "CSV"}, {"items.parquet", "PARQUET"}} {
		state := map[string]any{
			"Type": "Map", "ItemProcessor": processor, "ItemSelector": map[string]any{"value.$": "$$.Map.Item.Value", "source.$": "$$.Map.Item.Source"}, "End": true,
			"ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": test.key}, "ReaderConfig": map[string]any{"InputType": test.inputType, "CSVHeaderLocation": "FIRST_ROW"}},
		}
		definition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": state}})
		machine := invoke(p, "CreateStateMachine", map[string]any{"name": "reader-" + strings.ToLower(test.inputType), "definition": string(definition), "roleArn": testRoleARN}, nil)
		started := invoke(p, "StartExecution", map[string]any{"stateMachineArn": machine["stateMachineArn"]}, nil)
		execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": started["executionArn"]}, nil)
		if execution["status"] != "SUCCEEDED" || strings.Count(execution["output"].(string), `"source":"`+test.inputType+`"`) != 2 || test.inputType == "PARQUET" && !strings.Contains(execution["output"].(string), `"name":"Ada"`) {
			t.Fatalf("%s ItemReader execution %#v", test.inputType, execution)
		}
	}

	for _, key := range []string{"missing", "broken.parquet"} {
		missingState := map[string]any{"Type": "Map", "ItemProcessor": processor, "ItemReader": map[string]any{"Resource": "arn:aws:states:::s3:getObject", "Parameters": map[string]any{"Bucket": "items", "Key": key}, "ReaderConfig": map[string]any{"InputType": "PARQUET"}}, "End": true}
		missingDefinition, _ := json.Marshal(map[string]any{"StartAt": "Read", "States": map[string]any{"Read": missingState}})
		missingMachine := invoke(p, "CreateStateMachine", map[string]any{"name": strings.ReplaceAll(key, ".", "-") + "-reader", "definition": string(missingDefinition), "roleArn": testRoleARN}, nil)
		missingExecution := invoke(p, "StartExecution", map[string]any{"stateMachineArn": missingMachine["stateMachineArn"]}, nil)
		if execution := invoke(p, "DescribeExecution", map[string]any{"executionArn": missingExecution["executionArn"]}, nil); execution["status"] != "FAILED" || execution["error"] != "States.ItemReaderFailed" {
			t.Fatalf("%s ItemReader execution %#v", key, execution)
		}
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
	mapDefinition := `{"QueryLanguage":"JSONata","StartAt":"Read","States":{"Read":{"Type":"Map","ItemReader":{"Resource":"arn:aws:states:::s3:getObject","Arguments":{"Bucket":"{% $states.input.bucket %}","Key":"{% $states.input.key %}"},"ReaderConfig":{"InputType":"JSON","MaxItems":"{% 3 %}"}},"ItemBatcher":{"MaxItemsPerBatch":"{% 2 %}","BatchInput":{"tag":"{% $states.input.tag %}"}},"ItemProcessor":{"StartAt":"Echo","States":{"Echo":{"Type":"Pass","Output":"{% $states.input %}","End":true}}},"Next":"Transform"},"Transform":{"Type":"Parallel","Branches":[{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}],"Output":"{% $states.result.missing %}","Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"MaxAttempts":1}],"Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Assign":{"caught":"{% $states.errorOutput.Error %}"},"Output":{"map":"{% $states.input %}"},"Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed","Output":{"error":"{% $caught %}","batches":"{% $states.input.map %}"}}}}`
	if diagnostics := validateDefinition(mapDefinition, "EXPRESS"); len(diagnostics) != 0 {
		t.Fatalf("jsonata field diagnostics %#v", diagnostics)
	}
	mapExecution := startAfterRetry("jsonata-fields", mapDefinition, `{"bucket":"jsonata-items","key":"items.json","tag":"t"}`)
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
	if out, ok := applyResultPath(map[string]any{"ResultPath": "$"}, nested, 4); !ok || out != 4 {
		t.Fatalf("root ResultPath %#v", out)
	}
	selectedResult, selectedOK := applyStateResult(map[string]any{"ResultSelector": map[string]any{"picked.$": "$.value"}, "ResultPath": "$.task"}, map[string]any{"keep": true}, map[string]any{"value": 5.0, "drop": true}, nil, p.deps.Rand)
	if !selectedOK || jsonPath(selectedResult, "$.keep") != true || jsonPath(selectedResult, "$.task.picked") != 5.0 {
		t.Fatalf("selected result %#v", selectedResult)
	}
	paths := map[string]any{"a": []any{map[string]any{"v": 1.0}, map[string]any{"v": 2.0}, map[string]any{"v": 3.0}}}
	if jsonPath(paths, "$.a[1].v") != 2.0 || fmtString(jsonPath(paths, "$.a[0:2].v")) != `[1,2]` || jsonPath(paths, "$.['a'][2].v") != 3.0 || jsonPath(paths, "$.a[9]") != nil {
		t.Fatal("json path arrays")
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

func fmtString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
