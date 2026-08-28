package sns

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestTopicSubscribePublish(t *testing.T) {
	deps := spitest.Deps(t)
	p := &Pack{deps: deps}
	qp := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	_, _ = qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "q"}})
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "t"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := created.Output["TopicArn"].(string)
	got := [][]byte{}
	cancel := deps.Bus.Subscribe("sns:"+arn, func(_ context.Context, b []byte) { got = append(got, append([]byte(nil), b...)) })
	defer cancel()
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "q"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": arn, "Message": "hi"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0]) != "hi" {
		t.Fatalf("bus %q", got)
	}
	list, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSubscriptionsByTopic", Input: map[string]any{"TopicArn": arn}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Output["Subscriptions"].([]any)) != 1 {
		t.Fatalf("%v", list.Output)
	}
}

func TestPublishFilterAndSQSDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	qp := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	_, _ = qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "q"}})
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp
	}
	arn := str(inv("CreateTopic", map[string]any{"Name": "t"}).Output["TopicArn"])
	inv("Subscribe", map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:q", "FilterPolicy": `{"event":["order"]}`, "RawMessageDelivery": "true"})
	inv("Publish", map[string]any{"TopicArn": arn, "Message": "nope", "MessageAttributes": map[string]any{"event": map[string]any{"Type": "String", "Value": "other"}}})
	inv("Publish", map[string]any{"TopicArn": arn, "Message": "yes", "MessageAttributes": map[string]any{"event": map[string]any{"Type": "String", "Value": "order"}}})
	inv("Publish", map[string]any{"TopicArn": arn, "Message": "again", "MessageAttributes": map[string]any{"event": map[string]any{"Type": "String", "Value": "order"}}})
	kvs, _, _ := deps.Store.Scope("1", "us-east-1").Collection("msgs:q").List(ctx, "", "", 0)
	if len(kvs) != 2 {
		t.Fatalf("sqs delivery %d", len(kvs))
	}
	bodies := map[string]bool{}
	sequences := map[float64]bool{}
	for _, kv := range kvs {
		var msg map[string]any
		_ = json.Unmarshal(kv.Value, &msg)
		bodies[str(msg["body"])] = true
		sequences[msg["seq"].(float64)] = true
	}
	if !bodies["yes"] || !bodies["again"] || len(sequences) != 2 {
		t.Fatalf("raw bodies %v sequences %v", bodies, sequences)
	}
}

func TestLambdaSubscriptionDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	p, lp := New(deps), lambda.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	code := base64.StdEncoding.EncodeToString([]byte("def lambda_handler(event, context):\n return event['Records'][0]['Sns']\n"))
	_, err := lp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "notify", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": map[string]any{"ZipFile": code},
	}})
	if err != nil {
		t.Fatal(err)
	}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "alerts"}})
	if err != nil {
		t.Fatal(err)
	}
	topic := str(created.Output["TopicArn"])
	subscribed, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic, "Protocol": "lambda", "Endpoint": "arn:aws:lambda:us-east-1:1:function:notify",
	}})
	if err != nil {
		t.Fatal(err)
	}
	sub := map[string]any{
		"SubscriptionArn": subscribed.Output["SubscriptionArn"], "TopicArn": topic,
		"Endpoint": "arn:aws:lambda:us-east-1:1:function:notify",
	}
	req := &spi.Request{Identity: id, Input: map[string]any{"Subject": "warning"}}
	event := p.lambdaNotification(req, sub, "hello", "message-1", map[string]any{"severity": "high"})
	got := event["Records"].([]any)[0].(map[string]any)["Sns"].(map[string]any)
	if got["Message"] != "hello" || got["TopicArn"] != topic || got["Subject"] != "warning" {
		t.Fatalf("lambda SNS event %#v", got)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": topic, "Message": "hello", "Subject": "warning"}}); err != nil {
		t.Fatal(err)
	}
}

func TestSNSControlPlaneOperations(t *testing.T) {
	ctx := context.Background()
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
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
	if p.ServiceID() != "aws.sns" || p.Tier() != model.TierEmulate || len(p.Operations()) != 42 {
		t.Fatalf("SNS metadata %s %s %d", p.ServiceID(), p.Tier(), len(p.Operations()))
	}
	topic := str(must("CreateTopic", map[string]any{"Name": "events"}).Output["TopicArn"])
	if topics := must("ListTopics", nil).Output["Topics"].([]any); len(topics) != 1 {
		t.Fatalf("topics %#v", topics)
	}
	must("SetTopicAttributes", map[string]any{"TopicArn": topic, "AttributeName": "DisplayName", "AttributeValue": "Events"})
	attributes := must("GetTopicAttributes", map[string]any{"TopicArn": topic}).Output["Attributes"].(map[string]any)
	if attributes["DisplayName"] != "Events" || attributes["TopicArn"] != topic {
		t.Fatalf("topic attributes %#v", attributes)
	}
	if _, err := call("GetTopicAttributes", map[string]any{"TopicArn": topic + "-missing"}); err == nil {
		t.Fatal("found missing topic")
	}

	if _, err := call("AddPermission", map[string]any{"TopicArn": topic}); err == nil {
		t.Fatal("added permission without label")
	}
	must("AddPermission", map[string]any{
		"TopicArn": topic, "Label": "publish", "AWSAccountIds.member.1": "111111111111", "ActionNames.member.1": "Publish",
	})
	if !strings.Contains(str(must("GetTopicAttributes", map[string]any{"TopicArn": topic}).Output["Attributes"].(map[string]any)["Policy"]), "publish") {
		t.Fatal("topic permission was not stored")
	}
	if _, err := call("RemovePermission", map[string]any{"TopicArn": topic, "Label": "missing"}); err == nil {
		t.Fatal("removed missing permission")
	}
	must("RemovePermission", map[string]any{"TopicArn": topic, "Label": "publish"})

	tags := []any{map[string]any{"Key": "env", "Value": "test"}}
	must("TagResource", map[string]any{"ResourceArn": topic, "Tags": tags})
	if listed := must("ListTagsForResource", map[string]any{"ResourceArn": topic}).Output["Tags"].([]any); len(listed) != 1 {
		t.Fatalf("tags %#v", listed)
	}
	must("UntagResource", map[string]any{"ResourceArn": topic})
	if listed := must("ListTagsForResource", map[string]any{"ResourceArn": topic}).Output["Tags"].([]any); len(listed) != 0 {
		t.Fatalf("untagged resource %#v", listed)
	}
	must("TagResource", map[string]any{
		"ResourceArn": topic + ":flat", "Tags.member.1.Key": "team", "Tags.member.1.Value": "platform",
	})
	must("PutDataProtectionPolicy", map[string]any{"TopicArn": topic, "DataProtectionPolicy": `{"Name":"policy"}`})
	if got := must("GetDataProtectionPolicy", map[string]any{"ResourceArn": topic}).Output["DataProtectionPolicy"]; got != `{"Name":"policy"}` {
		t.Fatalf("data protection policy %q", got)
	}
	if got := must("GetDataProtectionPolicy", map[string]any{"ResourceArn": topic + ":missing"}).Output["DataProtectionPolicy"]; got != "" {
		t.Fatalf("missing data protection policy %q", got)
	}

	confirmed := make(chan map[string]any, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		payload := map[string]any{}
		_ = json.NewDecoder(request.Body).Decode(&payload)
		confirmed <- payload
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	pending := must("Subscribe", map[string]any{"TopicArn": topic, "Protocol": "http", "Endpoint": server.URL})
	if pending.Output["SubscriptionArn"] != "pending confirmation" {
		t.Fatalf("HTTP subscription %#v", pending.Output)
	}
	confirmation := <-confirmed
	token := str(confirmation["Token"])
	must("Publish", map[string]any{"TopicArn": topic, "Message": "before-confirmation"})
	select {
	case notification := <-confirmed:
		t.Fatalf("delivered unconfirmed HTTP subscription %#v", notification)
	default:
	}
	httpSub := str(must("ConfirmSubscription", map[string]any{"Token": token}).Output["SubscriptionArn"])
	if got := must("ConfirmSubscription", map[string]any{"Token": "missing"}).Output["SubscriptionArn"]; got != "missing" {
		t.Fatalf("missing confirmation %#v", got)
	}
	must("Publish", map[string]any{"TopicArn": topic, "Message": "notify"})
	if notification := <-confirmed; notification["Type"] != "Notification" || notification["Message"] != "notify" {
		t.Fatalf("HTTP notification %#v", notification)
	}
	allSubscriptions := must("ListSubscriptions", nil).Output["Subscriptions"].([]any)
	if len(allSubscriptions) != 1 {
		t.Fatalf("subscriptions %#v", allSubscriptions)
	}
	must("SetSubscriptionAttributes", map[string]any{"SubscriptionArn": httpSub, "AttributeName": "RawMessageDelivery", "AttributeValue": "true"})
	subAttributes := must("GetSubscriptionAttributes", map[string]any{"SubscriptionArn": httpSub}).Output["Attributes"].(map[string]any)
	if subAttributes["RawMessageDelivery"] != "true" || subAttributes["ConfirmationWasAuthenticated"] != "true" {
		t.Fatalf("subscription attributes %#v", subAttributes)
	}
	must("Unsubscribe", map[string]any{"SubscriptionArn": httpSub})

	batch := must("PublishBatch", map[string]any{"TopicArn": topic, "Entries": []any{
		map[string]any{"Id": "one", "Message": "first", "MessageAttributes": map[string]any{"kind": map[string]any{"Value": "a"}}},
		map[string]any{"Id": "two", "Message": "second", "MessageAttributes.entry.1.Name": "kind", "MessageAttributes.entry.1.Value.StringValue": "b"},
	}})
	if successful := batch.Output["Successful"].([]any); len(successful) != 2 {
		t.Fatalf("batch %#v", successful)
	}
	if response := must("PublishBatch", map[string]any{"TopicArn": topic, "Message": "fallback"}); response.Output["MessageId"] == nil {
		t.Fatalf("fallback batch %#v", response.Output)
	}

	app := str(must("CreatePlatformApplication", map[string]any{"Name": "mobile", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"}}).Output["PlatformApplicationArn"])
	if apps := must("ListPlatformApplications", nil).Output["PlatformApplications"].([]any); len(apps) != 1 {
		t.Fatalf("platform apps %#v", apps)
	}
	must("SetPlatformApplicationAttributes", map[string]any{
		"PlatformApplicationArn": app, "Attributes.entry.1.key": "Enabled", "Attributes.entry.1.value": "true",
	})
	if attrs := must("GetPlatformApplicationAttributes", map[string]any{"PlatformApplicationArn": app}).Output["Attributes"].(map[string]any); attrs["Enabled"] != "true" {
		t.Fatalf("platform app attributes %#v", attrs)
	}
	endpoint := str(must("CreatePlatformEndpoint", map[string]any{"PlatformApplicationArn": app, "Token": "device", "CustomUserData": "user"}).Output["EndpointArn"])
	if endpoints := must("ListEndpointsByPlatformApplication", map[string]any{"PlatformApplicationArn": app}).Output["Endpoints"].([]any); len(endpoints) != 1 {
		t.Fatalf("platform endpoints %#v", endpoints)
	}
	must("SetEndpointAttributes", map[string]any{"EndpointArn": endpoint, "Attributes": map[string]any{"Enabled": "false"}})
	if attrs := must("GetEndpointAttributes", map[string]any{"EndpointArn": endpoint}).Output["Attributes"].(map[string]any); attrs["Enabled"] != "false" {
		t.Fatalf("endpoint attributes %#v", attrs)
	}
	must("DeleteEndpoint", map[string]any{"EndpointArn": endpoint})
	if _, err := call("GetEndpointAttributes", map[string]any{"EndpointArn": endpoint}); err == nil {
		t.Fatal("found deleted platform endpoint")
	}
	must("DeletePlatformApplication", map[string]any{"PlatformApplicationArn": app})
	if _, err := call("GetPlatformApplicationAttributes", map[string]any{"PlatformApplicationArn": app}); err == nil {
		t.Fatal("found deleted platform application")
	}

	must("SetSMSAttributes", map[string]any{"Attributes.entry.1.key": "DefaultSenderID", "Attributes.entry.1.value": "Mirror"})
	if attrs := must("GetSMSAttributes", nil).Output["Attributes"].(map[string]any); attrs["DefaultSenderID"] != "Mirror" {
		t.Fatalf("SMS attributes %#v", attrs)
	}
	phone := "+15555550100"
	_ = p.col(&spi.Request{Identity: id}, "smsopt").Put(ctx, phone, []byte("true"))
	if must("CheckIfPhoneNumberIsOptedOut", map[string]any{"PhoneNumber": phone}).Output["isOptedOut"] != true {
		t.Fatal("phone was not opted out")
	}
	if numbers := must("ListPhoneNumbersOptedOut", nil).Output["phoneNumbers"].([]any); len(numbers) != 1 {
		t.Fatalf("opted-out numbers %#v", numbers)
	}
	must("OptInPhoneNumber", map[string]any{"phoneNumber": phone})
	if must("CheckIfPhoneNumberIsOptedOut", map[string]any{"PhoneNumber": phone}).Output["isOptedOut"] != false {
		t.Fatal("phone remained opted out")
	}
	must("CreateSMSSandboxPhoneNumber", map[string]any{"PhoneNumber": phone})
	must("VerifySMSSandboxPhoneNumber", map[string]any{"PhoneNumber": phone})
	if phones := must("ListSMSSandboxPhoneNumbers", nil).Output["PhoneNumbers"].([]any); len(phones) != 1 {
		t.Fatalf("sandbox phones %#v", phones)
	}
	if status := must("GetSMSSandboxAccountStatus", nil).Output; status["Verified"] != 1 || status["IsInSandbox"] != true {
		t.Fatalf("sandbox status %#v", status)
	}
	must("DeleteSMSSandboxPhoneNumber", map[string]any{"PhoneNumber": phone})
	_ = p.col(&spi.Request{Identity: id}, "orig").Put(ctx, phone, mustJSON(map[string]any{"PhoneNumber": phone}))
	if numbers := must("ListOriginationNumbers", nil).Output["PhoneNumbers"].([]any); len(numbers) != 1 {
		t.Fatalf("origination numbers %#v", numbers)
	}

	if _, err := call("Unknown", nil); err == nil {
		t.Fatal("implemented unknown SNS operation")
	}
	must("DeleteTopic", map[string]any{"TopicArn": topic})
	if topics := must("ListTopics", nil).Output["Topics"].([]any); len(topics) != 0 {
		t.Fatalf("deleted topics %#v", topics)
	}
}
