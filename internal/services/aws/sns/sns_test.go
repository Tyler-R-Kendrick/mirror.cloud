package sns

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
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
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:q"}})
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
	must("SetTopicAttributes", map[string]any{"TopicArn": topic, "AttributeName": "DeliveryPolicy", "AttributeValue": `{"http":{"defaultHealthyRetryPolicy":{"numRetries":1}}}`})
	if got := must("GetTopicAttributes", map[string]any{"TopicArn": topic}).Output["Attributes"].(map[string]any)["DeliveryPolicy"]; got == nil {
		t.Fatal("delivery policy was not stored")
	}
	must("SetTopicAttributes", map[string]any{"TopicArn": topic, "AttributeName": "DeliveryPolicy", "AttributeValue": ""})
	if _, found := must("GetTopicAttributes", map[string]any{"TopicArn": topic}).Output["Attributes"].(map[string]any)["DeliveryPolicy"]; found {
		t.Fatal("empty delivery policy was retained")
	}
	if _, err := call("GetTopicAttributes", map[string]any{"TopicArn": topic + "-missing"}); err == nil {
		t.Fatal("found missing topic")
	}
	for _, arn := range []string{"randomstring", "arn:aws:sns:us-east-1:random", "arn:aws:sns:us-east-1:111111111111:random"} {
		if _, err := call("Unsubscribe", map[string]any{"SubscriptionArn": arn}); err == nil {
			t.Fatalf("accepted invalid subscription ARN %q", arn)
		}
	}
	if _, err := call("ConfirmSubscription", map[string]any{"TopicArn": topic, "Token": "random-token"}); err == nil {
		t.Fatal("accepted unknown confirmation token")
	}
	if _, err := call("ConfirmSubscription", map[string]any{"TopicArn": topic + "-missing", "Token": "random-token"}); err == nil {
		t.Fatal("accepted confirmation for missing topic")
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

func TestTopicValidationAndPublishTargetCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, *spi.Fault) {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err == nil {
			return response, nil
		}
		fault, _ := err.(*spi.Fault)
		return nil, fault
	}
	_, invalidName := call("CreateTopic", map[string]any{"Name": "bad.topic"})
	_, invalidFIFO := call("CreateTopic", map[string]any{"Name": "fifo-topic", "Attributes": map[string]any{"FifoTopic": "true"}})
	created, createFault := call("CreateTopic", map[string]any{
		"Name":       "characterized-topic",
		"Attributes": map[string]any{"DisplayName": "before"},
		"Tags":       []any{map[string]any{"Key": "env", "Value": "test"}},
	})
	if createFault != nil {
		t.Fatal(createFault)
	}
	arn := str(created.Output["TopicArn"])
	duplicate, duplicateFault := call("CreateTopic", map[string]any{
		"Name": "characterized-topic", "Attributes": map[string]any{"DisplayName": "before"},
	})
	_, differentFault := call("CreateTopic", map[string]any{
		"Name": "characterized-topic", "Attributes": map[string]any{"DisplayName": "after"},
	})
	_, targetFault := call("Publish", map[string]any{"TargetArn": arn, "Message": "target"})
	_, malformedFault := call("Publish", map[string]any{"TopicArn": "randomstring", "Message": "bad"})
	_, missingFault := call("Publish", map[string]any{"TopicArn": arn + "-missing", "Message": "bad"})
	_, missingPermissionFault := call("AddPermission", map[string]any{"TopicArn": arn + "-missing", "Label": "missing"})
	_, missingAttributeFault := call("SetTopicAttributes", map[string]any{"TopicArn": arn + "-missing", "AttributeName": "DisplayName", "AttributeValue": "bad"})
	_, missingSubscriptionFault := call("Subscribe", map[string]any{"TopicArn": arn + "-missing", "Protocol": "sqs", "Endpoint": "q"})
	_, missingTagFault := call("TagResource", map[string]any{"ResourceArn": arn + "-missing", "Tags": []any{map[string]any{"Key": "a", "Value": "b"}}})
	_, _ = call("CreateTopic", map[string]any{"Name": "untagged-topic"})
	_, moreTagsFault := call("CreateTopic", map[string]any{
		"Name": "untagged-topic", "Tags": []any{map[string]any{"Key": "new", "Value": "tag"}},
	})
	if duplicateFault != nil || str(duplicate.Output["TopicArn"]) != arn || differentFault == nil || targetFault != nil || malformedFault == nil || missingFault == nil || missingPermissionFault == nil || missingAttributeFault == nil || missingSubscriptionFault == nil || missingTagFault == nil || moreTagsFault == nil {
		t.Fatalf("duplicate=%#v/%v different=%#v target=%#v malformed=%#v missing=%#v missingPermission=%#v missingAttribute=%#v missingSubscription=%#v missingTag=%#v moreTags=%#v", duplicate, duplicateFault, differentFault, targetFault, malformedFault, missingFault, missingPermissionFault, missingAttributeFault, missingSubscriptionFault, missingTagFault, moreTagsFault)
	}
	if _, fault := call("DeleteTopic", map[string]any{"TopicArn": arn}); fault != nil {
		t.Fatal(fault)
	}
	recreated, fault := call("CreateTopic", map[string]any{"Name": "characterized-topic"})
	if fault != nil {
		t.Fatal(fault)
	}
	listed, fault := call("ListTagsForResource", map[string]any{"ResourceArn": str(recreated.Output["TopicArn"])})
	if fault != nil {
		t.Fatal(fault)
	}
	_, fault = call("TagResource", map[string]any{"ResourceArn": str(recreated.Output["TopicArn"]), "Tags": []any{
		map[string]any{"Key": "a", "Value": "1"}, map[string]any{"Key": "b", "Value": "2"},
	}})
	if fault != nil {
		t.Fatal(fault)
	}
	_, fault = call("TagResource", map[string]any{"ResourceArn": str(recreated.Output["TopicArn"]), "Tags": []any{map[string]any{"Key": "c", "Value": "3"}}})
	if fault != nil {
		t.Fatal(fault)
	}
	_, fault = call("UntagResource", map[string]any{"ResourceArn": str(recreated.Output["TopicArn"]), "TagKeys": []any{"a"}})
	if fault != nil {
		t.Fatal(fault)
	}
	selective, fault := call("ListTagsForResource", map[string]any{"ResourceArn": str(recreated.Output["TopicArn"])})
	if fault != nil {
		t.Fatal(fault)
	}
	golden.AssertJSON(t, map[string]any{
		"invalidName":             map[string]any{"Code": invalidName.Code, "Message": invalidName.Message},
		"invalidFIFO":             map[string]any{"Code": invalidFIFO.Code, "Message": invalidFIFO.Message},
		"different":               map[string]any{"Code": differentFault.Code, "Message": differentFault.Message},
		"malformed":               map[string]any{"Code": malformedFault.Code, "Message": malformedFault.Message},
		"missing":                 map[string]any{"Code": missingFault.Code, "Message": missingFault.Message},
		"missingPermission":       map[string]any{"Code": missingPermissionFault.Code, "Message": missingPermissionFault.Message},
		"missingAttribute":        map[string]any{"Code": missingAttributeFault.Code, "Message": missingAttributeFault.Message},
		"missingSubscription":     map[string]any{"Code": missingSubscriptionFault.Code, "Message": missingSubscriptionFault.Message},
		"missingTag":              map[string]any{"Code": missingTagFault.Code, "Message": missingTagFault.Message},
		"moreTags":                map[string]any{"Code": moreTagsFault.Code, "Message": moreTagsFault.Message},
		"tagsAfterDelete":         listed.Output,
		"tagsAfterSelectiveUntag": selective.Output,
	})
}

func FuzzTopicNameValidation(f *testing.F) {
	for _, seed := range []string{"topic", "topic_name-1", "topic.fifo", "bad.topic", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		valid := validTopicName(name)
		if valid && strings.HasSuffix(name, ".fifo") {
			if !validTopicChars(strings.TrimSuffix(name, ".fifo")) {
				t.Fatalf("accepted invalid FIFO topic %q", name)
			}
		}
	})
}

func TestSNSListPagination(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	for i := 0; i < 101; i++ {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": fmt.Sprintf("paging-%03d", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListTopics", Input: map[string]any{}})
	if err != nil || len(asSlice(first.Output["Topics"])) != 100 || str(first.Output["NextToken"]) == "" {
		t.Fatalf("first topic page=%#v err=%v", first, err)
	}
	second, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListTopics", Input: map[string]any{"NextToken": first.Output["NextToken"]}})
	if err != nil || len(asSlice(second.Output["Topics"])) != 1 || second.Output["NextToken"] != nil {
		t.Fatalf("second topic page=%#v err=%v", second, err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListTopics", Input: map[string]any{"NextToken": "bad"}}); err == nil {
		t.Fatal("invalid topic token succeeded")
	}

	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "subscription-page"}})
	if err != nil {
		t.Fatal(err)
	}
	topic := str(created.Output["TopicArn"])
	for i := 0; i < 101; i++ {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": topic, "Protocol": "email", "Endpoint": fmt.Sprintf("user-%03d@example.com", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	first, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSubscriptionsByTopic", Input: map[string]any{"TopicArn": topic}})
	if err != nil || len(asSlice(first.Output["Subscriptions"])) != 100 || str(first.Output["NextToken"]) == "" {
		t.Fatalf("first subscription page=%#v err=%v", first, err)
	}
	second, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSubscriptionsByTopic", Input: map[string]any{"TopicArn": topic, "NextToken": first.Output["NextToken"]}})
	if err != nil || len(asSlice(second.Output["Subscriptions"])) != 1 || second.Output["NextToken"] != nil {
		t.Fatalf("second subscription page=%#v err=%v", second, err)
	}
}

func TestSNSFilterOperators(t *testing.T) {
	tests := []struct {
		name   string
		policy any
		attrs  map[string]any
		want   bool
	}{
		{"numeric-range", map[string]any{"n": []any{map[string]any{"numeric": []any{">", 1, "<=", 3}}}}, map[string]any{"n": map[string]any{"Value": "4"}}, false},
		{"numeric-range-valid", map[string]any{"n": []any{map[string]any{"numeric": []any{">", 1, "<=", 3}}}}, map[string]any{"n": map[string]any{"Value": "2"}}, true},
		{"exists-present", map[string]any{"n": []any{map[string]any{"exists": true}}}, map[string]any{"n": "2"}, true},
		{"exists-missing", map[string]any{"n": []any{map[string]any{"exists": false}}}, map[string]any{}, true},
		{"anything-but-list", map[string]any{"n": []any{map[string]any{"anything-but": []any{"1", "2"}}}}, map[string]any{"n": "3"}, true},
		{"suffix", map[string]any{"n": []any{map[string]any{"suffix": "-prod"}}}, map[string]any{"n": "api-prod"}, true},
		{"equals-ignore-case", map[string]any{"n": []any{map[string]any{"equals-ignore-case": "PROD"}}}, map[string]any{"n": "prod"}, true},
		{"nested-body", map[string]any{"detail": map[string]any{"type": []any{"order"}}}, map[string]any{"detail": map[string]any{"type": "order"}}, true},
		{"nested-body-or", map[string]any{"$or": []any{map[string]any{"type": []any{"order"}}, map[string]any{"type": []any{"refund"}}}}, map[string]any{"type": "refund"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchFilter(tc.policy, tc.attrs); got != tc.want {
				t.Fatalf("matchFilter(%#v, %#v) = %v, want %v", tc.policy, tc.attrs, got, tc.want)
			}
		})
	}
}

func TestSNSFIFOPublishValidationAndTopicDeduplication(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	create := func(name string, attrs map[string]any) string {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": name, "Attributes": attrs}})
		if err != nil {
			t.Fatal(err)
		}
		return str(response.Output["TopicArn"])
	}
	fifo := create("events.fifo", map[string]any{"FifoTopic": "true", "ContentBasedDeduplication": "true"})
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": fifo, "Message": "missing-group"}}); err == nil {
		t.Fatal("FIFO publish without MessageGroupId succeeded")
	}
	seen := 0
	cancel := deps.Bus.Subscribe("sns:"+fifo, func(context.Context, []byte) { seen++ })
	defer cancel()
	publish := func(input map[string]any) string {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return str(response.Output["MessageId"])
	}
	first := publish(map[string]any{"TopicArn": fifo, "Message": "same", "MessageGroupId": "g"})
	second := publish(map[string]any{"TopicArn": fifo, "Message": "same", "MessageGroupId": "other"})
	if first == "" || first != second || seen != 1 {
		t.Fatalf("topic deduplication first=%q second=%q deliveries=%d", first, second, seen)
	}
	noCBD := create("without-cbd.fifo", map[string]any{"FifoTopic": "true"})
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": noCBD, "Message": "missing-dedup", "MessageGroupId": "g"}}); err == nil {
		t.Fatal("FIFO publish without deduplication ID succeeded")
	}
	batch, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PublishBatch", Input: map[string]any{
		"TopicArn": noCBD,
		"Entries": []any{
			map[string]any{"Id": "one", "Message": "one", "MessageGroupId": "g", "MessageDeduplicationId": "d1"},
			map[string]any{"Id": "two", "Message": "two", "MessageGroupId": "g", "MessageDeduplicationId": "d2"},
		},
	}})
	if err != nil || len(batch.Output["Successful"].([]any)) != 2 {
		t.Fatalf("FIFO batch response=%#v err=%v", batch, err)
	}
}

func TestSNSMessageStructureAndSizeValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	qp := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "structured"}}); err != nil {
		t.Fatal(err)
	}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "structured-topic"}})
	if err != nil {
		t.Fatal(err)
	}
	topic := str(created.Output["TopicArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": topic, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:structured"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic, "MessageStructure": "json", "Message": `{"default":"default field","sqs":"sqs field"}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	kvs, _, _ := deps.Store.Scope("1", "us-east-1").Collection("msgs:structured").List(ctx, "", "", 0)
	if len(kvs) != 1 {
		t.Fatalf("structured delivery count %d", len(kvs))
	}
	var message map[string]any
	_ = json.Unmarshal(kvs[0].Value, &message)
	var envelope map[string]any
	_ = json.Unmarshal([]byte(str(message["body"])), &envelope)
	if envelope["Message"] != "sqs field" {
		t.Fatalf("structured payload %#v", envelope)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic, "MessageStructure": "json", "Message": `{"default":"fallback","sqs":{"ignored":"object"}}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	kvs, _, _ = deps.Store.Scope("1", "us-east-1").Collection("msgs:structured").List(ctx, "", "", 0)
	if len(kvs) != 2 {
		t.Fatalf("structured fallback delivery count %d", len(kvs))
	}
	_ = json.Unmarshal(kvs[1].Value, &message)
	_ = json.Unmarshal([]byte(str(message["body"])), &envelope)
	if envelope["Message"] != "fallback" {
		t.Fatalf("structured fallback payload %#v", envelope)
	}
	for _, input := range []map[string]any{
		{"TopicArn": topic, "Message": ""},
		{"TopicArn": topic, "MessageStructure": "json", "Message": `{"sqs":"missing default"}`},
		{"TopicArn": topic, "MessageStructure": "json", "Message": `{"default": {}}`},
	} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: input}); err == nil {
			t.Fatalf("invalid publish succeeded: %#v", input)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": topic, "Message": strings.Repeat("x", 262145)}}); err == nil {
		t.Fatal("oversized publish succeeded")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PublishBatch", Input: map[string]any{"TopicArn": topic}}); err == nil {
		t.Fatal("empty publish batch succeeded")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PublishBatch", Input: map[string]any{
		"TopicArn": topic,
		"Entries":  []any{map[string]any{"Id": "bad.id", "Message": "x"}},
	}}); err == nil {
		t.Fatal("invalid batch entry ID succeeded")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PublishBatch", Input: map[string]any{
		"TopicArn": topic,
		"Entries": []any{
			map[string]any{"Id": "one", "Message": strings.Repeat("x", 131073)},
			map[string]any{"Id": "two", "Message": strings.Repeat("y", 131072)},
		},
	}}); err == nil {
		t.Fatal("oversized publish batch succeeded")
	}
}

func TestSNSSubscriptionProtocolAndQueueValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	qp := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	create := func(operation string, input map[string]any) {
		t.Helper()
		if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input}); err != nil {
			t.Fatal(err)
		}
	}
	create("CreateQueue", map[string]any{"QueueName": "regular"})
	create("CreateQueue", map[string]any{"QueueName": "fifo.fifo", "Attributes": map[string]any{"FifoQueue": "true"}})
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "topic"}})
	if err != nil {
		t.Fatal(err)
	}
	topic := str(created.Output["TopicArn"])
	for _, input := range []map[string]any{
		{"TopicArn": topic, "Protocol": "invalid", "Endpoint": "endpoint"},
		{"TopicArn": topic, "Protocol": "sqs", "Endpoint": "unknown"},
		{"TopicArn": topic, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:fifo.fifo"},
	} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: input}); err == nil {
			t.Fatalf("invalid subscription succeeded: %#v", input)
		}
	}
	for _, endpoint := range []string{"+15--551234567", "NAA+15551234567", "+15551234567.", "/+15551234567"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": topic, "Protocol": "sms", "Endpoint": endpoint}}); err == nil {
			t.Fatalf("invalid SMS endpoint accepted: %s", endpoint)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": topic, "Protocol": "sms", "Endpoint": "+1234567890"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": topic, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:regular"}}); err != nil {
		t.Fatal(err)
	}
}

func TestSNSPlatformEndpointLifecycleValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreatePlatformEndpoint", map[string]any{"PlatformApplicationArn": "arn:aws:sns:us-east-1:1:app/GCM/missing", "Token": "token"}); err == nil {
		t.Fatal("created endpoint for missing application")
	}
	app, err := call("CreatePlatformApplication", map[string]any{"Name": "mobile", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	appARN := str(app.Output["PlatformApplicationArn"])
	first, err := call("CreatePlatformEndpoint", map[string]any{"PlatformApplicationArn": appARN, "Token": "token"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := call("CreatePlatformEndpoint", map[string]any{"PlatformApplicationArn": appARN, "Token": "token"})
	if err != nil || str(first.Output["EndpointArn"]) != str(second.Output["EndpointArn"]) {
		t.Fatalf("endpoint idempotency first=%#v second=%#v err=%v", first, second, err)
	}
	if _, err := call("SetEndpointAttributes", map[string]any{"EndpointArn": appARN + "/endpoint/missing"}); err == nil {
		t.Fatal("set attributes for missing endpoint")
	}
	if _, err := call("SetPlatformApplicationAttributes", map[string]any{"PlatformApplicationArn": appARN + "/missing"}); err == nil {
		t.Fatal("set attributes for missing application")
	}
	if _, err := call("DeletePlatformApplication", map[string]any{"PlatformApplicationArn": appARN}); err != nil {
		t.Fatal(err)
	}
	if endpoints, err := call("ListEndpointsByPlatformApplication", map[string]any{"PlatformApplicationArn": appARN}); err != nil || len(asSlice(endpoints.Output["Endpoints"])) != 0 {
		t.Fatalf("deleted application endpoints=%#v err=%v", endpoints, err)
	}
}

func TestSNSPlatformApplicationValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	for name, input := range map[string]map[string]any{
		"invalid name":       {"Name": "bad.name", "Platform": "GCM"},
		"invalid platform":   {"Name": "mobile", "Platform": "INVALID"},
		"missing credential": {"Name": "mobile", "Platform": "GCM", "Attributes": map[string]any{"PlatformPrincipal": "principal"}},
		"unknown attribute":  {"Name": "mobile", "Platform": "GCM", "Attributes": map[string]any{"Unknown": "value", "PlatformCredential": "secret"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: input}); err == nil {
				t.Fatalf("accepted invalid platform application: %#v", input)
			}
		})
	}
	for _, operation := range []string{"GetPlatformApplicationAttributes", "SetPlatformApplicationAttributes"} {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: map[string]any{
			"PlatformApplicationArn": "invalid-arn", "Attributes": map[string]any{},
		}})
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "InvalidParameter" {
			t.Fatalf("malformed application ARN for %s returned %#v", operation, err)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: map[string]any{
		"Name": "mobile", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"},
	}}); err != nil {
		t.Fatal(err)
	}
}

func TestSNSPublishDisabledPlatformEndpoint(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	app, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: map[string]any{
		"Name": "publish-endpoint", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformEndpoint", Input: map[string]any{
		"PlatformApplicationArn": app.Output["PlatformApplicationArn"], "Token": "token",
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpointARN := str(endpoint.Output["EndpointArn"])
	delivered := 0
	cancel := deps.Bus.Subscribe("sns:"+endpointARN, func(_ context.Context, body []byte) {
		if string(body) == "message" {
			delivered++
		}
	})
	defer cancel()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TargetArn": endpointARN, "Message": "message",
	}}); err != nil {
		t.Fatalf("enabled endpoint publish: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("enabled endpoint deliveries=%d", delivered)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetEndpointAttributes", Input: map[string]any{
		"EndpointArn": endpointARN, "Attributes": map[string]any{"Enabled": "false"},
	}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TargetArn": endpointARN, "Message": "message",
	}})
	fault, ok := err.(*spi.Fault)
	if !ok || fault.Code != "EndpointDisabled" || fault.Message != "Endpoint is disabled" {
		t.Fatalf("disabled endpoint publish fault=%#v err=%v", fault, err)
	}
}

func TestSNSPlatformEndpointSubscriptionDispatch(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	app, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: map[string]any{
		"Name": "application-dispatch", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformEndpoint", Input: map[string]any{
		"PlatformApplicationArn": app.Output["PlatformApplicationArn"], "Token": "token",
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpointARN := str(endpoint.Output["EndpointArn"])
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "application-dispatch"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "application", "Endpoint": endpointARN,
	}}); err != nil {
		t.Fatal(err)
	}
	var delivered string
	cancel := deps.Bus.Subscribe("sns:"+endpointARN, func(_ context.Context, body []byte) { delivered = string(body) })
	defer cancel()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "MessageStructure": "json",
		"Message": `{"default":"default","GCM":"platform"}`,
	}}); err != nil {
		t.Fatal(err)
	}
	if delivered != "platform" {
		t.Fatalf("platform endpoint message=%q", delivered)
	}
}

func TestSNSPhoneNumberPublish(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	phone := "+15555550101"
	delivered := ""
	cancel := deps.Bus.Subscribe("sns:sms:"+phone, func(_ context.Context, body []byte) { delivered = string(body) })
	defer cancel()
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"PhoneNumber": phone, "Message": "sms message",
	}})
	if err != nil || str(response.Output["MessageId"]) == "" || delivered != "sms message" {
		t.Fatalf("sms publish response=%#v err=%v delivered=%q", response, err, delivered)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"PhoneNumber": "not-a-phone", "Message": "sms message",
	}}); err == nil {
		t.Fatal("invalid phone publish succeeded")
	}
}

func TestSNSSMSSubscriptionDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	phone := "+15555550102"
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "sms-subscription"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "sms", "Endpoint": phone,
	}}); err != nil {
		t.Fatal(err)
	}
	delivered := ""
	cancel := deps.Bus.Subscribe("sns:sms:"+phone, func(_ context.Context, body []byte) { delivered = string(body) })
	defer cancel()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Message": "topic sms",
	}}); err != nil {
		t.Fatal(err)
	}
	if delivered != "topic sms" {
		t.Fatalf("sms subscription delivery=%q", delivered)
	}
}

func TestSNSOptInPhoneValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	for _, key := range []string{"PhoneNumber", "phoneNumber"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "OptInPhoneNumber", Input: map[string]any{key: "invalid"}}); err == nil {
			t.Fatalf("accepted invalid phone key %s", key)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "OptInPhoneNumber", Input: map[string]any{"PhoneNumber": "+15555550103"}}); err != nil {
		t.Fatal(err)
	}
}

func TestSNSSandboxPhoneValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSMSSandboxPhoneNumber", Input: map[string]any{"PhoneNumber": "invalid"}}); err == nil {
		t.Fatal("accepted invalid sandbox phone")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "VerifySMSSandboxPhoneNumber", Input: map[string]any{"PhoneNumber": "+15555550999"}}); err == nil {
		t.Fatal("verified an unregistered sandbox phone")
	}
}

func TestSNSSMSAttributeValidationAndSelection(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	for _, attrs := range []map[string]any{
		{"InvalidAttribute": "invalid"},
		{"DefaultSenderID": strings.Repeat("x", 12)},
		{"DefaultSenderID": "123456789"},
		{"DefaultSMSType": "invalid"},
	} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSMSAttributes", Input: map[string]any{"Attributes": attrs}}); err == nil {
			t.Fatalf("accepted invalid SMS attributes %#v", attrs)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSMSAttributes", Input: map[string]any{"Attributes": map[string]any{
		"DeliveryStatusSuccessSamplingRate": "100", "DefaultSenderID": "Mirror", "DefaultSMSType": "Promotional",
	}}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSMSAttributes", Input: map[string]any{"Attributes": []any{"DefaultSenderID", "DefaultSMSType"}}})
	if err != nil {
		t.Fatal(err)
	}
	attrs := asMap(response.Output["Attributes"])
	if len(attrs) != 2 || attrs["DefaultSenderID"] != "Mirror" || attrs["DefaultSMSType"] != "Promotional" {
		t.Fatalf("filtered SMS attributes %#v", attrs)
	}
}

func TestSNSPlatformEndpointAttributeValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	app, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: map[string]any{
		"Name": "attribute-validation", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	appARN := app.Output["PlatformApplicationArn"]
	for _, attrs := range []map[string]any{
		{"InvalidKey": "value"},
		{"Enabled": "maybe"},
		{"CustomUserData": strings.Repeat("x", 2049)},
	} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformEndpoint", Input: map[string]any{
			"PlatformApplicationArn": appARN, "Token": "token-" + str(attrs["Enabled"]), "Attributes": attrs,
		}}); err == nil {
			t.Fatalf("accepted invalid endpoint attributes %#v", attrs)
		}
	}
	endpoint, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformEndpoint", Input: map[string]any{
		"PlatformApplicationArn": appARN, "Token": "valid-token",
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, attrs := range []map[string]any{{"InvalidKey": "value"}, {"Enabled": "maybe"}, {"CustomUserData": strings.Repeat("x", 2049)}} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetEndpointAttributes", Input: map[string]any{
			"EndpointArn": endpoint.Output["EndpointArn"], "Attributes": attrs,
		}}); err == nil {
			t.Fatalf("accepted invalid set attributes %#v", attrs)
		}
	}
}

func TestSNSFilterPolicyScopeCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "q"}}); err != nil {
		t.Fatal(err)
	}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "scope"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(created.Output["TopicArn"])
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:q"}})
	if err != nil {
		t.Fatal(err)
	}
	subARN := str(sub.Output["SubscriptionArn"])
	set := func(name, value string) *spi.Fault {
		_, callErr := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": subARN, "AttributeName": name, "AttributeValue": value}})
		fault, _ := callErr.(*spi.Fault)
		return fault
	}
	if set("FilterPolicyScope", "invalid") == nil {
		t.Fatal("accepted invalid filter policy scope")
	}
	if fault := set("FilterPolicyScope", "MessageBody"); fault != nil {
		t.Fatal(fault)
	}
	attrs, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": subARN}})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := attrs.Output["Attributes"].(map[string]any)["FilterPolicyScope"]; found {
		t.Fatal("exposed scope without filter policy")
	}
	if set("FilterPolicy", "invalid-json") == nil {
		t.Fatal("accepted invalid filter policy")
	}
	for _, tc := range []struct{ name, value string }{
		{"FakeAttribute", "test-value"},
		{"RawMessageDelivery", "test-value"},
		{"RedrivePolicy", `{"deadLetterTargetArn":"fake-arn"}`},
	} {
		if set(tc.name, tc.value) == nil {
			t.Fatalf("accepted invalid subscription attribute %s", tc.name)
		}
	}
	if fault := set("FilterPolicy", `{"n":["x"]}`); fault != nil {
		t.Fatal(fault)
	}
	attrs, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": subARN}})
	if err != nil {
		t.Fatal(err)
	}
	if got := attrs.Output["Attributes"].(map[string]any)["FilterPolicyScope"]; got != "MessageBody" {
		t.Fatalf("scope %#v", got)
	}
	if fault := set("FilterPolicy", ""); fault != nil {
		t.Fatal(fault)
	}
	attrs, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": subARN}})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := attrs.Output["Attributes"].(map[string]any)["FilterPolicy"]; found {
		t.Fatal("retained cleared filter policy")
	}
}

func TestSNSMessageBodyFilterDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "body-filter"}}); err != nil {
		t.Fatal(err)
	}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "body-filter"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(created.Output["TopicArn"])
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:body-filter"}})
	if err != nil {
		t.Fatal(err)
	}
	subARN := str(sub.Output["SubscriptionArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": subARN, "AttributeName": "FilterPolicyScope", "AttributeValue": "MessageBody"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": subARN, "AttributeName": "FilterPolicy", "AttributeValue": `{"detail":{"type":["order"]}}`}}); err != nil {
		t.Fatal(err)
	}
	publish := func(body string) {
		t.Helper()
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": arn, "Message": body}}); err != nil {
			t.Fatal(err)
		}
	}
	publish(`{"detail":{"type":"refund"}}`)
	publish(`{"detail":{"type":"order"}}`)
	msgs, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "body-filter", "VisibilityTimeout": 0, "MaxNumberOfMessages": 10}})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(asSlice(msgs.Output["Messages"])); got != 1 {
		t.Fatalf("body filter delivered %d messages, want 1", got)
	}
}

func TestSNSSubscribeIdempotency(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "q"}}); err != nil {
		t.Fatal(err)
	}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "idempotent"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(created.Output["TopicArn"])
	subscribe := func(attrs map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:q", "Attributes": attrs}})
	}
	first, err := subscribe(map[string]any{"RawMessageDelivery": "True"})
	if err != nil {
		t.Fatal(err)
	}
	gotAttrs, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": first.Output["SubscriptionArn"]}})
	if err != nil || gotAttrs.Output["Attributes"].(map[string]any)["RawMessageDelivery"] != "true" {
		t.Fatalf("raw delivery normalization attrs=%#v err=%v", gotAttrs, err)
	}
	for _, attrs := range []map[string]any{{"RawMessageDelivery": "true", "FilterPolicyScope": "MessageAttributes"}, nil, {}} {
		got, callErr := subscribe(attrs)
		if callErr != nil || str(got.Output["SubscriptionArn"]) != str(first.Output["SubscriptionArn"]) {
			t.Fatalf("idempotent subscribe attrs=%#v got=%#v err=%v", attrs, got, callErr)
		}
	}
	if _, err := subscribe(map[string]any{"RawMessageDelivery": "false", "FilterPolicyScope": "MessageBody"}); err == nil {
		t.Fatal("accepted subscription with differing attributes")
	}
}

func TestSNSConcurrentPublishChaos(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "chaos"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(created.Output["TopicArn"])
	received := make(chan []byte, 32)
	cancel := deps.Bus.Subscribe("sns:"+arn, func(_ context.Context, body []byte) { received <- append([]byte(nil), body...) })
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, callErr := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": arn, "Message": fmt.Sprintf("body-%d", i)}})
			if callErr != nil {
				errCh <- callErr
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for callErr := range errCh {
		t.Fatal(callErr)
	}
	if len(received) != 32 {
		t.Fatalf("received %d messages, want 32", len(received))
	}
}
