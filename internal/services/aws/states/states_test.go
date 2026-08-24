package states

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
)

const testRoleARN = "arn:aws:iam::1:role/states"

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
	toleratedDefinition := `{"StartAt":"Map","States":{"Map":{"Type":"Map","ItemsPath":"$.items","ToleratedFailureCount":2,"ItemProcessor":` + failingProcessor + `,"End":true}}}`
	toleratedARN := must("CreateStateMachine", map[string]any{"name": "tolerated", "definition": toleratedDefinition, "roleArn": testRoleARN})["stateMachineArn"].(string)
	toleratedExecution := must("StartExecution", map[string]any{"stateMachineArn": toleratedARN, "input": `{"items":[1,2]}`})["executionArn"].(string)
	if execution := must("DescribeExecution", map[string]any{"executionArn": toleratedExecution}); execution["status"] != "SUCCEEDED" {
		t.Fatalf("tolerated distributed map %#v", execution)
	}
	toleratedRuns := must("ListMapRuns", map[string]any{"executionArn": toleratedExecution})["mapRuns"].([]any)
	toleratedRun := must("DescribeMapRun", map[string]any{"mapRunArn": toleratedRuns[0].(map[string]any)["mapRunArn"]})
	if toleratedRun["status"] != "SUCCEEDED" || toleratedRun["itemCounts"].(map[string]any)["failed"] != 2.0 {
		t.Fatalf("tolerated map run %#v", toleratedRun)
	}
	if originalRuns := must("ListMapRuns", map[string]any{"executionArn": executionARN})["mapRuns"].([]any); len(originalRuns) != 2 {
		t.Fatalf("cross-execution map runs %#v", originalRuns)
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
	must("StopExecution", map[string]any{"executionArn": started["executionArn"]})
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
	fault("GetExecutionHistory", map[string]any{"executionArn": expressExecution}, "StateMachineTypeNotSupported")

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
	definition := `{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Next":"Done"},"Done":{"Type":"Succeed"}}}`
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
	if execution := must("DescribeExecution", map[string]any{"ExecutionArn": executionARN}).Output; execution["status"] != "SUCCEEDED" || execution["history"] != nil {
		t.Fatalf("execution %#v", execution)
	}
	if executions, _, _ := p.col(&spi.Request{Identity: id}, "ex").List(ctx, "", "", 0); len(executions) != 1 {
		t.Fatalf("executions %#v", executions)
	}
	storedExecution, _ := getRecord(ctx, p.col(&spi.Request{Identity: id}, "ex"), executionARN)
	if events := asSlice(storedExecution["history"]); len(events) != 2 {
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
	recoveryDefinition := `{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"` + activityARN + `","Retry":[{"ErrorEquals":["Retryable"],"MaxAttempts":1}],"Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.failure","Next":"Recovered"}],"End":true},"Recovered":{"Type":"Succeed"}}}`
	recoveryMachine := must("CreateStateMachine", map[string]any{"Name": "activity-recovery", "Definition": recoveryDefinition, "RoleArn": testRoleARN}).Output["stateMachineArn"].(string)
	recoveryARN := must("StartExecution", map[string]any{"StateMachineArn": recoveryMachine, "Name": "recovery", "Input": `{"keep":true}`}).Output["executionArn"].(string)
	firstToken := must("GetActivityTask", map[string]any{"ActivityArn": activityARN}).Output["taskToken"].(string)
	must("SendTaskHeartbeat", map[string]any{"TaskToken": firstToken})
	must("SendTaskFailure", map[string]any{"TaskToken": firstToken, "Error": "Retryable", "Cause": "try again"})
	if retrying := must("DescribeExecution", map[string]any{"ExecutionArn": recoveryARN}).Output; retrying["status"] != "RUNNING" {
		t.Fatalf("activity retry %#v", retrying)
	}
	secondToken := must("GetActivityTask", map[string]any{"ActivityArn": activityARN}).Output["taskToken"].(string)
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
		return p.walk(ctx, &spi.Request{Identity: id}, def, "", input)
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
	params := taskPayload(map[string]any{"Parameters": map[string]any{"Payload": map[string]any{"value.$": "$.n"}, "nested": map[string]any{"value.$": "$.s"}}}, data, p.deps.Rand)
	if jsonPath(params, "$.Payload.value") != 2.0 || jsonPath(params, "$.nested.value") != "yes" || jsonPath(data, "$.missing.value") != nil || parseJSON("plain") != "plain" || toFloat(json.Number("3")) != 3 || !toBool("true") {
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

	recovery := walk(`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:1:function:missing","Retry":[{"ErrorEquals":["States.TaskFailed"],"MaxAttempts":2}],"Catch":[{"ErrorEquals":["ResourceNotFoundException"],"ResultPath":"$.error","Next":"Recovered"}]},"Recovered":{"Type":"Succeed"}}}`, map[string]any{"original": true})
	got, _ := recovery.out.(map[string]any)
	failure, _ := got["error"].(map[string]any)
	if recovery.status != "SUCCEEDED" || got["original"] != true || failure["Error"] != "ResourceNotFoundException" {
		t.Fatalf("task recovery %#v", recovery)
	}
	if unsupported := walk(`{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"arn:aws:states:::unknown","End":true}}}`, nil); unsupported.status != "FAILED" || unsupported.cause != "States.Runtime" {
		t.Fatalf("unsupported task %#v", unsupported)
	}
	retrier := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Nope"}}, map[string]any{"ErrorEquals": []any{"States.ALL"}, "MaxAttempts": 2.0}}}
	attempts := map[int]int{}
	if !retryTask(retrier, "Boom", attempts) || !retryTask(retrier, "Boom", attempts) || retryTask(retrier, "Boom", attempts) || attempts[1] != 2 {
		t.Fatalf("retry attempts %#v", attempts)
	}
	defaults := map[int]int{}
	defaultRetrier := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Boom"}}}}
	if !retryTask(defaultRetrier, "Boom", defaults) || !retryTask(defaultRetrier, "Boom", defaults) || !retryTask(defaultRetrier, "Boom", defaults) || retryTask(defaultRetrier, "Boom", defaults) {
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
	selectedResult, selectedOK := applyStateResult(map[string]any{"ResultSelector": map[string]any{"picked.$": "$.value"}, "ResultPath": "$.task"}, map[string]any{"keep": true}, map[string]any{"value": 5.0, "drop": true}, p.deps.Rand)
	if !selectedOK || jsonPath(selectedResult, "$.keep") != true || jsonPath(selectedResult, "$.task.picked") != 5.0 {
		t.Fatalf("selected result %#v", selectedResult)
	}
	paths := map[string]any{"a": []any{map[string]any{"v": 1.0}, map[string]any{"v": 2.0}, map[string]any{"v": 3.0}}}
	if jsonPath(paths, "$.a[1].v") != 2.0 || fmtString(jsonPath(paths, "$.a[0:2].v")) != `[1,2]` || jsonPath(paths, "$.['a'][2].v") != 3.0 || jsonPath(paths, "$.a[9]") != nil {
		t.Fatal("json path arrays")
	}
	composite := map[string]any{"Retry": []any{map[string]any{"ErrorEquals": []any{"Boom"}, "MaxAttempts": 1.0}}, "Catch": []any{map[string]any{"ErrorEquals": []any{"States.ALL"}, "Next": "Recovered"}}}
	compositeAttempts := map[int]int{}
	if _, _, retry, caught := recoverState(composite, walkResult{cause: "failed", errorName: "Boom"}, nil, compositeAttempts); !retry || caught {
		t.Fatal("composite did not retry")
	}
	if next, _, retry, caught := recoverState(composite, walkResult{cause: "failed", errorName: "Boom"}, nil, compositeAttempts); retry || !caught || next != "Recovered" {
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
