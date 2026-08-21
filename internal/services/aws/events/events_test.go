package events

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestPutEventsDeliversOnlyMatchingRules(t *testing.T) {
	deps := spitest.Deps(t)
	p, sp, np := New(deps), sqs.New(deps), sns.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(pack spi.BehaviorPack, op string, in map[string]any) *spi.Response {
		t.Helper()
		resp, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp
	}
	for _, name := range []string{"direct", "via-sns", "ignored"} {
		invoke(sp, "CreateQueue", map[string]any{"QueueName": name})
	}
	topic := str(invoke(np, "CreateTopic", map[string]any{"Name": "matched"}).Output["TopicArn"])
	invoke(np, "Subscribe", map[string]any{
		"TopicArn": topic, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:via-sns", "RawMessageDelivery": "true",
	})
	invoke(p, "PutRule", map[string]any{"Name": "matched", "EventPattern": `{"source":["app"]}`})
	invoke(p, "PutTargets", map[string]any{"Rule": "matched", "Targets": []any{
		map[string]any{"Id": "queue", "Arn": "arn:aws:sqs:us-east-1:1:direct"},
		map[string]any{"Id": "topic", "Arn": topic},
	}})
	invoke(p, "PutRule", map[string]any{"Name": "disabled", "EventPattern": `{}`, "State": "DISABLED"})
	invoke(p, "PutTargets", map[string]any{"Rule": "disabled", "Targets": []any{map[string]any{"Id": "ignored", "Arn": "arn:aws:sqs:us-east-1:1:ignored"}}})

	resp := invoke(p, "PutEvents", map[string]any{"Entries": []any{
		map[string]any{"Source": "app", "DetailType": "order", "Detail": `{"n":1}`},
		map[string]any{"Source": "other", "DetailType": "order", "Detail": `{}`},
		map[string]any{"Source": "app", "DetailType": "order", "Detail": `{"n":2}`},
	}})
	if resp.Output["FailedEntryCount"] != 0 || len(resp.Output["Entries"].([]any)) != 3 {
		t.Fatalf("put events response %#v", resp.Output)
	}
	for _, queue := range []string{"direct", "via-sns"} {
		got := invoke(sp, "ReceiveMessage", map[string]any{"QueueName": queue, "MaxNumberOfMessages": 10}).Output["Messages"].([]any)
		if len(got) != 2 {
			t.Fatalf("%s received %d messages, want 2", queue, len(got))
		}
		var event map[string]any
		if json.Unmarshal([]byte(str(got[0].(map[string]any)["Body"])), &event) != nil || event["source"] != "app" || event["detail-type"] != "order" {
			t.Fatalf("%s event %#v", queue, got[0])
		}
	}
	if got := invoke(sp, "ReceiveMessage", map[string]any{"QueueName": "ignored"}).Output["Messages"].([]any); len(got) != 0 {
		t.Fatalf("disabled rule delivered %#v", got)
	}
}

func TestPutEventsReportsInvalidEntries(t *testing.T) {
	p := New(spitest.Deps(t))
	resp, err := p.Invoke(context.Background(), &spi.Request{
		Identity:  spi.Identity{Account: "1", Region: "us-east-1"},
		Operation: "PutEvents",
		Input: map[string]any{"Entries": []any{
			map[string]any{"Source": "app", "DetailType": "x"},
			map[string]any{"Source": "app", "DetailType": "x", "Detail": `{`},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Output["FailedEntryCount"] != 2 || len(resp.Output["Entries"].([]any)) != 2 {
		t.Fatalf("invalid entries %#v", resp.Output)
	}
}

func TestDeliverTargetStepFunctions(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	machine := states.New(deps)
	created, err := machine.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateStateMachine", Input: map[string]any{
		"name": "target", "type": "EXPRESS", "roleArn": "arn:aws:iam::1:role/states",
		"definition": `{"StartAt":"Done","States":{"Done":{"Type":"Pass","End":true}}}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(created.Output["stateMachineArn"])
	for _, invocation := range []string{"REQUEST_RESPONSE", "FIRE_AND_FORGET"} {
		if err := DeliverTarget(context.Background(), deps, id, arn, map[string]any{"StateMachineParameters": map[string]any{"InvocationType": invocation}}, []byte(`{"value":1}`)); err != nil {
			t.Fatalf("%s: %v", invocation, err)
		}
	}
	executions, err := machine.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "ListExecutions", Input: map[string]any{}})
	if err != nil || len(executions.Output["executions"].([]any)) != 2 {
		t.Fatalf("executions %#v err=%v", executions, err)
	}
	if err := DeliverTarget(context.Background(), deps, id, arn, map[string]any{"StateMachineParameters": map[string]any{"InvocationType": "INVALID"}}, []byte(`{}`)); err == nil {
		t.Fatal("invalid invocation type succeeded")
	}
}

func TestTargetsUpsertRemoveAndEventBusIsolation(t *testing.T) {
	deps := spitest.Deps(t)
	p, sp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(pack spi.BehaviorPack, op string, in map[string]any) *spi.Response {
		t.Helper()
		resp, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp
	}
	for _, name := range []string{"old", "updated", "preserved", "custom"} {
		invoke(sp, "CreateQueue", map[string]any{"QueueName": name})
	}
	invoke(p, "PutRule", map[string]any{"Name": "same", "EventPattern": `{}`})
	invoke(p, "PutTargets", map[string]any{"Rule": "same", "Targets": []any{
		map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:1:old"},
		map[string]any{"Id": "b", "Arn": "arn:aws:sqs:us-east-1:1:preserved"},
	}})
	invoke(p, "PutTargets", map[string]any{"Rule": "same", "Targets": []any{
		map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:1:updated"},
	}})
	listed := invoke(p, "ListTargetsByRule", map[string]any{"Rule": "same"}).Output["Targets"].([]any)
	if len(listed) != 2 {
		t.Fatalf("upsert replaced targets %#v", listed)
	}
	invoke(p, "RemoveTargets", map[string]any{"Rule": "same", "Ids": []any{"b"}})
	listed = invoke(p, "ListTargetsByRule", map[string]any{"Rule": "same"}).Output["Targets"].([]any)
	if len(listed) != 1 || str(listed[0].(map[string]any)["Arn"]) != "arn:aws:sqs:us-east-1:1:updated" {
		t.Fatalf("remove targets %#v", listed)
	}
	custom := invoke(p, "PutRule", map[string]any{"Name": "same", "EventBusName": "custom", "EventPattern": `{}`})
	if str(custom.Output["RuleArn"]) != "arn:aws:events:us-east-1:1:rule/custom/same" {
		t.Fatalf("custom rule ARN %#v", custom.Output)
	}
	invoke(p, "PutTargets", map[string]any{"Rule": "same", "EventBusName": "custom", "Targets": []any{
		map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:1:custom"},
	}})
	invoke(p, "PutEvents", map[string]any{"Entries": []any{
		map[string]any{"Source": "app", "DetailType": "x", "Detail": `{}`},
		map[string]any{"Source": "app", "DetailType": "x", "Detail": `{}`, "EventBusName": "arn:aws:events:us-east-1:1:event-bus/custom"},
	}})
	for _, queue := range []string{"updated", "custom"} {
		if got := invoke(sp, "ReceiveMessage", map[string]any{"QueueName": queue}).Output["Messages"].([]any); len(got) != 1 {
			t.Fatalf("%s received %d messages, want 1", queue, len(got))
		}
	}
	for _, queue := range []string{"old", "preserved"} {
		if got := invoke(sp, "ReceiveMessage", map[string]any{"QueueName": queue}).Output["Messages"].([]any); len(got) != 0 {
			t.Fatalf("%s unexpectedly received %#v", queue, got)
		}
	}
}

func TestTargetInputTransformations(t *testing.T) {
	deps := spitest.Deps(t)
	p, sp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(pack spi.BehaviorPack, op string, in map[string]any) *spi.Response {
		t.Helper()
		resp, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp
	}
	for _, name := range []string{"constant", "path", "template"} {
		invoke(sp, "CreateQueue", map[string]any{"QueueName": name})
	}
	invoke(p, "PutRule", map[string]any{"Name": "transform", "EventPattern": `{}`})
	invoke(p, "PutTargets", map[string]any{"Rule": "transform", "Targets": []any{
		map[string]any{"Id": "constant", "Arn": "arn:aws:sqs:us-east-1:1:constant", "Input": `{"fixed":true}`},
		map[string]any{"Id": "path", "Arn": "arn:aws:sqs:us-east-1:1:path", "InputPath": "$.detail"},
		map[string]any{"Id": "template", "Arn": "arn:aws:sqs:us-east-1:1:template", "InputTransformer": map[string]any{
			"InputPathsMap": map[string]any{"kind": "$.detail-type", "n": "$.detail.n"},
			"InputTemplate": `{"kind":<kind>,"n":<n>}`,
		}},
	}})
	invoke(p, "PutEvents", map[string]any{"Entries": []any{
		map[string]any{"Source": "app", "DetailType": "order", "Detail": `{"n":7}`},
	}})
	want := map[string]map[string]any{
		"constant": {"fixed": true},
		"path":     {"n": float64(7)},
		"template": {"kind": "order", "n": float64(7)},
	}
	for queue, expected := range want {
		messages := invoke(sp, "ReceiveMessage", map[string]any{"QueueName": queue}).Output["Messages"].([]any)
		if len(messages) != 1 {
			t.Fatalf("%s received %d messages", queue, len(messages))
		}
		var got map[string]any
		_ = json.Unmarshal([]byte(str(messages[0].(map[string]any)["Body"])), &got)
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("%s body %#v, want %#v", queue, got, expected)
		}
	}
}

func TestBootedServerEventBridgePutEvents(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.events", "aws.sqs"}
	cfg.Seed = "ev-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/events/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSEvents."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return res.StatusCode, out
	}
	call("PutRule", `{"Name":"r","EventPattern":"{}"}`)
	call("PutTargets", `{"Rule":"r","Targets":[{"Id":"1","Arn":"arn:aws:sqs:us-east-1:000000000000:q"}]}`)
	_, listed := call("ListRules", `{}`)
	if listed["Rules"] == nil {
		t.Fatalf("list %v", listed)
	}
	call("PutEvents", `{"Entries":[{"Source":"app","DetailType":"x","Detail":"{}"}]}`)
	_, bus := call("CreateEventBus", `{"Name":"custom"}`)
	if bus["EventBusArn"] == nil {
		t.Fatalf("bus %v", bus)
	}
	_, got := call("DescribeEventBus", `{"Name":"custom"}`)
	if got["Name"] != "custom" && got["EventBusArn"] == nil {
		t.Fatalf("describe bus %v", got)
	}
	call("PutPermission", `{"EventBusName":"custom","StatementId":"s1","Action":"events:PutEvents","Principal":"*"}`)
	call("RemovePermission", `{"EventBusName":"custom","StatementId":"s1"}`)
	call("CreateArchive", `{"ArchiveName":"a1","EventSourceArn":"arn:aws:events:us-east-1:000000000000:event-bus/custom"}`)
	call("DescribeArchive", `{"ArchiveName":"a1"}`)
	call("ListArchives", `{}`)
	call("CreateConnection", `{"Name":"c1","AuthorizationType":"API_KEY"}`)
	call("DescribeConnection", `{"Name":"c1"}`)
	call("ListConnections", `{}`)
	call("CreateApiDestination", `{"Name":"d1","ConnectionArn":"arn:c","InvocationEndpoint":"https://example.test"}`)
	call("ListApiDestinations", `{}`)
	call("CreateEndpoint", `{"Name":"e1"}`)
	call("ListEndpoints", `{}`)
	call("CreatePartnerEventSource", `{"Name":"p1"}`)
	call("ActivateEventSource", `{"Name":"p1"}`)
	call("ListEventSources", `{}`)
	call("StartReplay", `{"ReplayName":"r1","EventSourceArn":"arn:a"}`)
	call("DescribeReplay", `{"ReplayName":"r1"}`)
	call("CancelReplay", `{"ReplayName":"r1"}`)
	call("EnableRule", `{"Name":"r"}`)
	call("DescribeRule", `{"Name":"r"}`)
	_, pat := call("TestEventPattern", `{"EventPattern":"{\"source\":[\"app\"]}","Event":"{\"source\":\"app\"}"}`)
	if pat["Result"] != true {
		t.Fatalf("pattern %v", pat)
	}
	call("TagResource", `{"ResourceARN":"arn:r","Tags":[{"Key":"k","Value":"v"}]}`)
	call("ListTagsForResource", `{"ResourceARN":"arn:r"}`)
	call("UntagResource", `{"ResourceARN":"arn:r"}`)
	call("DisableRule", `{"Name":"r"}`)
	call("DeleteArchive", `{"ArchiveName":"a1"}`)
	call("DeleteConnection", `{"Name":"c1"}`)
	call("DeleteApiDestination", `{"Name":"d1"}`)
	call("DeleteEndpoint", `{"Name":"e1"}`)
	call("DeleteEventBus", `{"Name":"custom"}`)
}

func TestEventsHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 57 {
		t.Fatalf("events Operations() %d want 57", n)
	}
}

func TestBootedServerEventsExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.events"}
	cfg.Seed = "ev-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/events/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSEvents."+op)
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
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSEvents."+op)
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
	created := hard("CreateEventBus", `{"Name":"bootbus"}`)
	if !strings.Contains(created, "bootbus") {
		t.Fatalf("create bus %s", created)
	}
	got := hard("DescribeEventBus", `{"Name":"bootbus"}`)
	if !strings.Contains(got, "bootbus") {
		t.Fatalf("describe bus %s", got)
	}
	hard("DeleteEventBus", `{"Name":"bootbus"}`)
	listed := hard("ListEventBuses", `{}`)
	if strings.Contains(listed, `"Name":"bootbus"`) {
		t.Fatalf("bus still present %s", listed)
	}
	payload := `{"Name":"bootbus","ArchiveName":"a1","ReplayName":"r1","EventBusName":"bootbus","ResourceARN":"arn:r","StatementId":"s1","TargetArn":"arn:t","EventPattern":"{}","Event":"{}","Entries":[]}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
