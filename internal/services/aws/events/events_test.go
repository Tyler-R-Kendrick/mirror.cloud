package events

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	defer p.Close()
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
	defer p.Close()
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

func TestPutEventsRetriesAndDeadLettersTargets(t *testing.T) {
	var calls, succeed, retryDelay atomic.Int32
	retryDelay.Store(3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if succeed.Load() == 0 {
			w.Header().Set("Retry-After", toString(retryDelay.Load()))
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	queue := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	connection := invoke(p, "CreateConnection", map[string]any{
		"Name": "retry", "AuthorizationType": "API_KEY",
		"AuthParameters": map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyName": "X-Key", "ApiKeyValue": "value"}},
	})
	destination := invoke(p, "CreateApiDestination", map[string]any{
		"Name": "retry", "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": server.URL, "HttpMethod": "POST",
	})
	destinationARN := str(destination.Output["ApiDestinationArn"])
	invoke(p, "PutRule", map[string]any{"Name": "retry", "EventPattern": `{"source":["retry"]}`})
	invoke(p, "PutTargets", map[string]any{"Rule": "retry", "Targets": []any{map[string]any{
		"Id": "destination", "Arn": destinationARN, "RetryPolicy": map[string]any{"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 1},
	}}})
	invoke(p, "PutEvents", map[string]any{"Entries": []any{map[string]any{"Source": "retry", "DetailType": "test", "Detail": `{}`}}})
	retries := deps.Store.Scope(id.Account, id.Region).Collection("event-retries")
	if stored, _, _ := retries.List(ctx, "", "", 0); len(stored) != 1 || calls.Load() != 1 {
		t.Fatalf("persisted retries=%d calls=%d", len(stored), calls.Load())
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	succeed.Store(1)
	p = New(deps)
	_ = deps.Clock.Advance(2 * time.Second)
	select {
	case <-time.After(20 * time.Millisecond):
		if calls.Load() != 1 {
			t.Fatalf("Retry-After ignored: calls=%d", calls.Load())
		}
	}
	_ = deps.Clock.Advance(time.Second)
	eventuallyEvent(t, func() bool {
		stored, _, _ := retries.List(ctx, "", "", 0)
		return calls.Load() == 2 && len(stored) == 0
	})

	invoke(queue, "CreateQueue", map[string]any{"QueueName": "failures"})
	succeed.Store(0)
	invoke(p, "PutRule", map[string]any{"Name": "dead", "EventPattern": `{"source":["dead"]}`})
	invoke(p, "PutTargets", map[string]any{"Rule": "dead", "Targets": []any{map[string]any{
		"Id": "destination", "Arn": destinationARN,
		"RetryPolicy":      map[string]any{"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 0},
		"DeadLetterConfig": map[string]any{"Arn": "arn:aws:sqs:us-east-1:1:failures"},
	}}})
	invoke(p, "PutEvents", map[string]any{"Entries": []any{map[string]any{"Source": "dead", "DetailType": "test", "Detail": `{}`}}})
	dead := invoke(queue, "ReceiveMessage", map[string]any{"QueueName": "failures", "MaxNumberOfMessages": 1, "MessageAttributeNames": []any{"All"}}).Output["Messages"].([]any)
	if len(dead) != 1 {
		t.Fatalf("dead letters %#v", dead)
	}
	attributes := dead[0].(map[string]any)["MessageAttributes"].(map[string]any)
	attribute := func(name string) string { return str(attributes[name].(map[string]any)["StringValue"]) }
	if attribute("ERROR_CODE") != "ERROR_FROM_TARGET" || attribute("EXHAUSTED_RETRY_CONDITION") != "MaximumRetryAttempts" || attribute("RETRY_ATTEMPTS") != "0" || !strings.HasSuffix(attribute("RULE_ARN"), ":rule/dead") || attribute("TARGET_ARN") != destinationARN {
		t.Fatalf("dead-letter attributes %#v", attributes)
	}

	invoke(p, "PutRule", map[string]any{"Name": "permanent", "EventPattern": `{"source":["permanent"]}`})
	invoke(queue, "CreateQueue", map[string]any{"QueueName": "permanent-failures"})
	invoke(p, "PutTargets", map[string]any{"Rule": "permanent", "Targets": []any{map[string]any{
		"Id": "missing", "Arn": "arn:aws:sqs:us-east-1:1:missing", "DeadLetterConfig": map[string]any{"Arn": "arn:aws:sqs:us-east-1:1:permanent-failures"},
	}}})
	invoke(p, "PutEvents", map[string]any{"Entries": []any{map[string]any{"Source": "permanent", "DetailType": "test", "Detail": `{}`}}})
	permanent := invoke(queue, "ReceiveMessage", map[string]any{"QueueName": "permanent-failures", "MaxNumberOfMessages": 1, "MessageAttributeNames": []any{"All"}}).Output["Messages"].([]any)
	if len(permanent) != 1 {
		t.Fatalf("permanent dead letters %#v", permanent)
	}
	attributes = permanent[0].(map[string]any)["MessageAttributes"].(map[string]any)
	if str(attributes["ERROR_CODE"].(map[string]any)["StringValue"]) != "NO_RESOURCE" || attributes["EXHAUSTED_RETRY_CONDITION"] != nil {
		t.Fatalf("permanent attributes %#v", attributes)
	}

	retryDelay.Store(120)
	beforeAge := calls.Load()
	invoke(queue, "CreateQueue", map[string]any{"QueueName": "aged-failures"})
	invoke(p, "PutRule", map[string]any{"Name": "aged", "EventPattern": `{"source":["aged"]}`})
	invoke(p, "PutTargets", map[string]any{"Rule": "aged", "Targets": []any{map[string]any{
		"Id": "destination", "Arn": destinationARN,
		"RetryPolicy":      map[string]any{"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 185},
		"DeadLetterConfig": map[string]any{"Arn": "arn:aws:sqs:us-east-1:1:aged-failures"},
	}}})
	invoke(p, "PutEvents", map[string]any{"Entries": []any{map[string]any{"Source": "aged", "DetailType": "test", "Detail": `{}`}}})
	_ = deps.Clock.Advance(time.Minute)
	eventuallyEvent(t, func() bool {
		messages := invoke(queue, "ReceiveMessage", map[string]any{"QueueName": "aged-failures", "MaxNumberOfMessages": 1, "MessageAttributeNames": []any{"All"}}).Output["Messages"].([]any)
		if len(messages) == 0 {
			return false
		}
		attributes = messages[0].(map[string]any)["MessageAttributes"].(map[string]any)
		return str(attributes["EXHAUSTED_RETRY_CONDITION"].(map[string]any)["StringValue"]) == "MaximumEventAgeInSeconds"
	})
	if calls.Load() != beforeAge+1 {
		t.Fatalf("expired event retried: calls before=%d after=%d", beforeAge, calls.Load())
	}
}

func TestPutTargetsValidatesReliability(t *testing.T) {
	p := New(spitest.Deps(t))
	defer p.Close()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	ctx := context.Background()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutRule", Input: map[string]any{"Name": "rule", "EventPattern": `{}`}}); err != nil {
		t.Fatal(err)
	}
	for _, target := range []map[string]any{
		{"Id": "age", "Arn": "arn:aws:sqs:us-east-1:1:q", "RetryPolicy": map[string]any{"MaximumEventAgeInSeconds": 59}},
		{"Id": "attempts", "Arn": "arn:aws:sqs:us-east-1:1:q", "RetryPolicy": map[string]any{"MaximumRetryAttempts": 186}},
		{"Id": "fifo", "Arn": "arn:aws:sqs:us-east-1:1:q", "DeadLetterConfig": map[string]any{"Arn": "arn:aws:sqs:us-east-1:1:failures.fifo"}},
	} {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutTargets", Input: map[string]any{"Rule": "rule", "Targets": []any{target}}})
		if err != nil || response.Output["FailedEntryCount"] != 1 {
			t.Fatalf("target %s response=%#v err=%v", target["Id"], response, err)
		}
	}
	negative := &spi.Fault{Fields: map[string]any{"RetryAfter": "-1"}}
	if at, specified := targetRetryAfter(negative, time.Unix(0, 0)); !specified || !at.IsZero() {
		t.Fatalf("negative Retry-After at=%v specified=%v", at, specified)
	}
}

func eventuallyEvent(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("condition not met")
		default:
			time.Sleep(time.Millisecond)
		}
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
	standard, err := machine.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateStateMachine", Input: map[string]any{
		"name": "async-target", "roleArn": "arn:aws:iam::1:role/states",
		"definition": `{"StartAt":"Done","States":{"Done":{"Type":"Pass","End":true}}}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := DeliverTarget(context.Background(), deps, id, str(standard.Output["stateMachineArn"]), map[string]any{}, []byte(`{}`)); err != nil {
		t.Fatalf("default async: %v", err)
	}
	if err := DeliverTarget(context.Background(), deps, id, str(standard.Output["stateMachineArn"]), map[string]any{"StateMachineParameters": map[string]any{"InvocationType": "REQUEST_RESPONSE"}}, []byte(`{}`)); err == nil {
		t.Fatal("synchronous Standard invocation succeeded")
	}
	executions, err := machine.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "ListExecutions", Input: map[string]any{}})
	if err != nil || len(executions.Output["executions"].([]any)) != 3 {
		t.Fatalf("executions %#v err=%v", executions, err)
	}
	if err := DeliverTarget(context.Background(), deps, id, arn, map[string]any{"StateMachineParameters": map[string]any{"InvocationType": "INVALID"}}, []byte(`{}`)); err == nil {
		t.Fatal("invalid invocation type succeeded")
	}
}

func TestInvokeAPIDestinationBasicAuth(t *testing.T) {
	seen := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		seen <- ok && username == "user" && password == "pass" && r.URL.Path == "/items/widget" && r.URL.Query().Get("source") == "connection" && r.Header.Get("X-Test") == "connection" && body["value"] == "connection" && body["target"] == "yes" && body["connection"] == "yes"
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	connection, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateConnection", Input: map[string]any{
		"Name": "basic", "AuthorizationType": "BASIC", "AuthParameters": map[string]any{
			"BasicAuthParameters": map[string]any{"Username": "user", "Password": "pass"},
			"InvocationHttpParameters": map[string]any{
				"HeaderParameters":      []any{map[string]any{"Key": "X-Test", "Value": "connection"}},
				"QueryStringParameters": []any{map[string]any{"Key": "source", "Value": "connection"}},
				"BodyParameters": []any{
					map[string]any{"Key": "value", "Value": "connection"},
					map[string]any{"Key": "connection", "Value": "yes"},
				},
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateApiDestination", Input: map[string]any{
		"Name": "basic", "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": server.URL + "/items/*", "HttpMethod": "POST",
	}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := InvokeAPIDestination(context.Background(), deps, id, str(destination.Output["ApiDestinationArn"]), map[string]any{
		"PathParameterValues": []any{"widget"}, "QueryStringParameters": map[string]any{"source": "target"}, "HeaderParameters": map[string]any{"X-Test": "target"},
	}, []byte(`{"value":"target","target":"yes"}`))
	if err != nil || string(body) != `{"ok":true}` || !<-seen {
		t.Fatalf("API destination body=%s err=%v", body, err)
	}
	if _, err := mergeConnectionBody([]byte(`[]`), []any{map[string]any{"Key": "value", "Value": "connection"}}); err == nil {
		t.Fatal("connection body parameters merged into a non-object payload")
	}
	if _, err := mergeConnectionBody([]byte(`{}`), []any{map[string]any{"Key": "value", "Value": strings.Repeat("x", 64<<10)}}); err == nil {
		t.Fatal("connection body parameters exceeded the 64 KB payload limit")
	}
	if !apiDestinationRetryable(401) || !apiDestinationRetryable(500) || apiDestinationRetryable(400) || apiDestinationRetryable(302) {
		t.Fatal("API destination retry classification changed")
	}
}

func TestInvokeAPIDestinationOAuth(t *testing.T) {
	tokenCalls := 0
	destinationCalls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			tokenCalls++
			username, password, ok := r.BasicAuth()
			if !ok || username != "client" || password != "secret" || r.FormValue("grant_type") != "client_credentials" {
				t.Errorf("OAuth request auth=%v username=%q password=%q form=%v", ok, username, password, r.Form)
			}
			_, _ = w.Write([]byte(`{"access_token":"token-` + toString(tokenCalls) + `","token_type":"Bearer"}`))
		case "/unauthorized", "/proxy-auth-required":
			destinationCalls[r.URL.Path]++
			payload, _ := io.ReadAll(r.Body)
			if r.Header.Get("Authorization") != "Bearer token-"+toString(tokenCalls) || string(payload) != `{}` {
				t.Errorf("Authorization %q", r.Header.Get("Authorization"))
			}
			if destinationCalls[r.URL.Path] == 1 {
				status := http.StatusUnauthorized
				if r.URL.Path == "/proxy-auth-required" {
					status = http.StatusProxyAuthRequired
				}
				w.WriteHeader(status)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/retry":
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer server.Close()
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	connection, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateConnection", Input: map[string]any{
		"Name": "oauth", "AuthorizationType": "OAUTH_CLIENT_CREDENTIALS",
		"AuthParameters": map[string]any{"OAuthParameters": map[string]any{
			"AuthorizationEndpoint": server.URL + "/token", "HttpMethod": "POST",
			"ClientParameters":    map[string]any{"ClientID": "client", "ClientSecret": "secret"},
			"OAuthHttpParameters": map[string]any{"BodyParameters": []any{map[string]any{"Key": "grant_type", "Value": "client_credentials"}}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"unauthorized", "proxy-auth-required"} {
		destination, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateApiDestination", Input: map[string]any{
			"Name": "oauth-" + path, "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": server.URL + "/" + path, "HttpMethod": "POST",
		}})
		if err != nil {
			t.Fatal(err)
		}
		body, err := InvokeAPIDestination(context.Background(), deps, id, str(destination.Output["ApiDestinationArn"]), nil, []byte(`{}`))
		if err != nil || string(body) != `{"ok":true}` || destinationCalls["/"+path] != 2 {
			t.Fatalf("OAuth %s destination body=%s tokenCalls=%d destinationCalls=%v err=%v", path, body, tokenCalls, destinationCalls, err)
		}
	}
	if tokenCalls != 4 {
		t.Fatalf("OAuth tokenCalls=%d want 4", tokenCalls)
	}
	retryDestination, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateApiDestination", Input: map[string]any{
		"Name": "oauth-retry", "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": server.URL + "/retry", "HttpMethod": "POST",
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = InvokeAPIDestination(context.Background(), deps, id, str(retryDestination.Output["ApiDestinationArn"]), nil, []byte(`{}`))
	fault, ok := err.(*spi.Fault)
	if !ok || fault.HTTPStatus != http.StatusTooManyRequests || fault.Fields["RetryAfter"] != "3" {
		t.Fatalf("retryable fault=%#v err=%v", fault, err)
	}
}

func TestAPIDestinationControlPlane(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	wantFault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := invoke(operation, input)
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != code {
			t.Fatalf("%s fault=%v want %s", operation, err, code)
		}
	}

	wantFault("CreateConnection", map[string]any{"Name": "missing-auth"}, "ValidationException")
	wantFault("CreateConnection", map[string]any{"Name": "wrong-auth", "AuthorizationType": "BASIC", "AuthParameters": map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyName": "X-Key", "ApiKeyValue": "secret"}}}, "ValidationException")
	connection, err := invoke("CreateConnection", map[string]any{
		"Name": "control", "Description": "original", "AuthorizationType": "API_KEY",
		"AuthParameters": map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyName": "X-Key", "ApiKeyValue": "secret"}},
	})
	if err != nil || connection.Output["ConnectionState"] != "AUTHORIZED" || !strings.Contains(str(connection.Output["ConnectionArn"]), ":connection/control/") {
		t.Fatalf("create connection %#v err=%v", connection, err)
	}
	wantFault("CreateConnection", map[string]any{
		"Name": "control", "AuthorizationType": "API_KEY",
		"AuthParameters": map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyName": "X-Key", "ApiKeyValue": "secret"}},
	}, "ResourceAlreadyExistsException")
	described, err := invoke("DescribeConnection", map[string]any{"Name": "control"})
	encoded, _ := json.Marshal(described.Output)
	if err != nil || described.Output["ConnectionState"] != "AUTHORIZED" || strings.Contains(string(encoded), "secret") {
		t.Fatalf("describe connection %s err=%v", encoded, err)
	}
	listed, err := invoke("ListConnections", map[string]any{})
	encoded, _ = json.Marshal(listed.Output)
	if err != nil || strings.Contains(string(encoded), "AuthParameters") || strings.Contains(string(encoded), "secret") {
		t.Fatalf("list connections %s err=%v", encoded, err)
	}
	updated, err := invoke("UpdateConnection", map[string]any{
		"Name": "control", "Description": "updated",
		"AuthParameters": map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyValue": "replacement"}},
	})
	storedConnection, _ := p.load(ctx, &spi.Request{Identity: id}, "connections", "control")
	storedAuth := storedConnection["AuthParameters"].(map[string]any)["ApiKeyAuthParameters"].(map[string]any)
	if err != nil || updated.Output["ConnectionState"] != "AUTHORIZED" || storedConnection["Description"] != "updated" || storedAuth["ApiKeyName"] != "X-Key" || storedAuth["ApiKeyValue"] != "replacement" {
		t.Fatalf("update connection %#v stored=%#v err=%v", updated, storedConnection, err)
	}

	wantFault("CreateApiDestination", map[string]any{"Name": "missing", "ConnectionArn": "arn:aws:events:us-east-1:1:connection/missing/id", "InvocationEndpoint": "https://example.test", "HttpMethod": "POST"}, "ResourceNotFoundException")
	wantFault("CreateApiDestination", map[string]any{"Name": "invalid", "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": "https://example.test", "HttpMethod": "TRACE"}, "ValidationException")
	destination, err := invoke("CreateApiDestination", map[string]any{
		"Name": "control", "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": "https://example.test/original", "HttpMethod": "POST",
	})
	if err != nil || destination.Output["ApiDestinationState"] != "ACTIVE" || !strings.Contains(str(destination.Output["ApiDestinationArn"]), ":api-destination/control/") {
		t.Fatalf("create API destination %#v err=%v", destination, err)
	}
	if _, err := invoke("UpdateApiDestination", map[string]any{"Name": "control", "Description": "updated"}); err != nil {
		t.Fatal(err)
	}
	storedDestination, _ := p.load(ctx, &spi.Request{Identity: id}, "apidest", "control")
	if storedDestination["Description"] != "updated" || storedDestination["InvocationEndpoint"] != "https://example.test/original" || storedDestination["HttpMethod"] != "POST" || storedDestination["InvocationRateLimitPerSecond"] != float64(300) {
		t.Fatalf("update API destination %#v", storedDestination)
	}
	wantFault("DescribeApiDestination", map[string]any{"Name": "missing"}, "ResourceNotFoundException")
	wantFault("UpdateApiDestination", map[string]any{"Name": "missing", "Description": "updated"}, "ResourceNotFoundException")

	deauthorized, err := invoke("DeauthorizeConnection", map[string]any{"Name": "control"})
	storedConnection, _ = p.load(ctx, &spi.Request{Identity: id}, "connections", "control")
	if err != nil || deauthorized.Output["ConnectionState"] != "DEAUTHORIZED" || storedConnection["AuthParameters"] != nil {
		t.Fatalf("deauthorize connection %#v stored=%#v err=%v", deauthorized, storedConnection, err)
	}
	_, err = InvokeAPIDestination(ctx, deps, id, str(destination.Output["ApiDestinationArn"]), nil, []byte(`{}`))
	if fault, ok := err.(*spi.Fault); !ok || fault.Code != "ConnectionFailure" {
		t.Fatalf("deauthorized invocation fault=%v", err)
	}
	wantFault("UpdateConnection", map[string]any{"Name": "missing", "Description": "updated"}, "ResourceNotFoundException")
}

func TestAPIDestinationRateLimit(t *testing.T) {
	requests := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests <- struct{}{} }))
	defer server.Close()
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	connection, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateConnection", Input: map[string]any{
		"Name": "rate", "AuthorizationType": "API_KEY", "AuthParameters": map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyName": "X-Key", "ApiKeyValue": "value"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateApiDestination", Input: map[string]any{
		"Name": "rate", "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": server.URL, "HttpMethod": "POST", "InvocationRateLimitPerSecond": 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(destination.Output["ApiDestinationArn"])
	if _, err := InvokeAPIDestination(ctx, deps, id, arn, nil, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	<-requests
	done := make(chan error, 1)
	go func() {
		_, err := InvokeAPIDestination(ctx, deps, id, arn, nil, []byte(`{}`))
		done <- err
	}()
	reserved := false
	timeout := time.After(time.Second)
	for !reserved {
		select {
		case <-timeout:
			t.Fatal("second invocation did not reserve the next rate window")
		default:
		}
		body, ok, _ := deps.Store.Scope(id.Account, id.Region).Collection("apidest-rate").Get(ctx, "rate")
		var state apiDestinationRateState
		_ = json.Unmarshal(body, &state)
		if ok && state.Window == int64(time.Second) {
			reserved = true
		}
		runtime.Gosched()
	}
	select {
	case err := <-done:
		t.Fatalf("rate-limited invocation completed early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	_ = deps.Clock.Advance(time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("rate-limited invocation did not resume")
	}
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("rate-limited request was not sent")
	}
}

func TestTargetsUpsertRemoveAndEventBusIsolation(t *testing.T) {
	deps := spitest.Deps(t)
	p, sp := New(deps), sqs.New(deps)
	defer p.Close()
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
	defer p.Close()
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
	_, connection := call("CreateConnection", `{"Name":"c1","AuthorizationType":"API_KEY","AuthParameters":{"ApiKeyAuthParameters":{"ApiKeyName":"X-Key","ApiKeyValue":"value"}}}`)
	call("DescribeConnection", `{"Name":"c1"}`)
	call("ListConnections", `{}`)
	call("CreateApiDestination", `{"Name":"d1","ConnectionArn":"`+str(connection["ConnectionArn"])+`","InvocationEndpoint":"https://example.test","HttpMethod":"POST"}`)
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
	defer p.Close()
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
