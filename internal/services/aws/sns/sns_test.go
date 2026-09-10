package sns

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestSNSSQSRawDeliveryPreservesMessageAttributes(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	snsCall := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	sqsCall := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	sqsCall("CreateQueue", map[string]any{"QueueName": "raw-attrs"})
	topic := str(snsCall("CreateTopic", map[string]any{"Name": "raw-attrs"}).Output["TopicArn"])
	snsCall("Subscribe", map[string]any{"TopicArn": topic, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:raw-attrs", "RawMessageDelivery": "true"})
	attrs := map[string]any{
		"text":   map[string]any{"Type": "String", "Value": "value"},
		"binary": map[string]any{"DataType": "Binary", "BinaryValue": base64.StdEncoding.EncodeToString([]byte{2, 3, 4})},
	}
	snsCall("Publish", map[string]any{"TopicArn": topic, "Message": "raw", "MessageAttributes": attrs})
	received := sqsCall("ReceiveMessage", map[string]any{"QueueName": "raw-attrs", "MessageAttributeNames": []any{"All"}})
	messages := asSlice(received.Output["Messages"])
	if len(messages) != 1 {
		t.Fatalf("messages %#v", received.Output)
	}
	message := asMap(messages[0])
	messageAttrs := asMap(message["MessageAttributes"])
	if message["Body"] != "raw" || len(messageAttrs) != 2 ||
		str(asMap(messageAttrs["text"])["StringValue"]) != "value" ||
		str(asMap(messageAttrs["binary"])["BinaryValue"]) != base64.StdEncoding.EncodeToString([]byte{2, 3, 4}) {
		t.Fatalf("raw delivery %#v", message)
	}
}

func TestSNSFlattenedBinaryMessageAttributeDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "flattened-binary"}}); err != nil {
		t.Fatal(err)
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "flattened-binary"}})
	if err != nil {
		t.Fatal(err)
	}
	topicARN := str(topic.Output["TopicArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topicARN, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:flattened-binary", "RawMessageDelivery": "true",
	}}); err != nil {
		t.Fatal(err)
	}
	binaryValue := base64.StdEncoding.EncodeToString([]byte{7, 8, 9})
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topicARN, "Message": "flattened", "MessageAttributes.entry.1.Name": "binary",
		"MessageAttributes.entry.1.Value.DataType": "Binary", "MessageAttributes.entry.1.Value.BinaryValue": binaryValue,
	}}); err != nil {
		t.Fatal(err)
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "flattened-binary", "MessageAttributeNames": []any{"All"}}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("flattened binary delivery=%#v err=%v", received, err)
	}
	message := asMap(asSlice(received.Output["Messages"])[0])
	if str(asMap(asMap(message["MessageAttributes"])["binary"])["BinaryValue"]) != binaryValue {
		t.Fatalf("flattened binary attributes=%#v", message)
	}
}

func TestSNSSQSNotificationPreservesMessageAttributes(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "envelope-attrs"}}); err != nil {
		t.Fatal(err)
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "envelope-attrs"}})
	if err != nil {
		t.Fatal(err)
	}
	topicARN := str(topic.Output["TopicArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topicARN, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:envelope-attrs",
	}}); err != nil {
		t.Fatal(err)
	}
	attrs := map[string]any{"kind": map[string]any{"DataType": "String", "StringValue": "event"}}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topicARN, "Message": "envelope", "Subject": "subject", "MessageAttributes": attrs,
	}}); err != nil {
		t.Fatal(err)
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "envelope-attrs"}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("envelope delivery=%#v err=%v", received, err)
	}
	message := asMap(asSlice(received.Output["Messages"])[0])
	var envelope map[string]any
	if json.Unmarshal([]byte(str(message["Body"])), &envelope) != nil || envelope["Subject"] != "subject" || str(envelope["Timestamp"]) == "" || !validTestSNSNotificationSignature(envelope) || !strings.Contains(str(envelope["SigningCertURL"]), "SimpleNotificationService.pem") || !strings.Contains(str(envelope["UnsubscribeURL"]), "Action=Unsubscribe") || str(asMap(envelope["MessageAttributes"])["kind"].(map[string]any)["StringValue"]) != "event" {
		t.Fatalf("notification envelope=%#v", envelope)
	}
}

func TestSNSSQSNotificationSignatureVersionTwo(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "signature-v2"}}); err != nil {
		t.Fatal(err)
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{
		"Name": "signature-v2", "Attributes": map[string]any{"SignatureVersion": "2"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(topic.Output["TopicArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:signature-v2",
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": arn, "Message": "signed-v2"}}); err != nil {
		t.Fatal(err)
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "signature-v2"}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("signature v2 delivery=%#v err=%v", received, err)
	}
	var envelope map[string]any
	if json.Unmarshal([]byte(str(asMap(asSlice(received.Output["Messages"])[0])["Body"])), &envelope) != nil || str(envelope["SignatureVersion"]) != "2" || !validTestSNSNotificationSignature(envelope) {
		t.Fatalf("signature v2 envelope=%#v", envelope)
	}
}

func validTestSNSNotificationSignature(values map[string]any) bool {
	block, _ := pem.Decode(signingCertificate())
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}
	var canonical strings.Builder
	fields := []string{"Message", "MessageId"}
	if typ := str(values["Type"]); typ == "SubscriptionConfirmation" || typ == "UnsubscribeConfirmation" {
		fields = append(fields, "SubscribeURL", "Subject", "Timestamp", "Token", "TopicArn", "Type")
	} else {
		fields = append(fields, "Subject", "Timestamp", "TopicArn", "Type")
	}
	for _, field := range fields {
		if value, ok := values[field]; ok {
			canonical.WriteString(field + "\n" + str(value) + "\n")
		}
	}
	signature, err := base64.StdEncoding.DecodeString(str(values["Signature"]))
	if err != nil {
		return false
	}
	var hash crypto.Hash
	var digest []byte
	if str(values["SignatureVersion"]) == "2" {
		hash = crypto.SHA256
		sum := sha256.Sum256([]byte(canonical.String()))
		digest = sum[:]
	} else {
		hash = crypto.SHA1
		sum := sha1.Sum([]byte(canonical.String()))
		digest = sum[:]
	}
	public, ok := cert.PublicKey.(*rsa.PublicKey)
	return ok && rsa.VerifyPKCS1v15(public, hash, digest, signature) == nil
}

func TestSNSHTTPSubscriptionConfirmationSignature(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "confirmation-signature"}})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("confirmation payload: %v", err)
			return
		}
		received <- payload
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "http", "Endpoint": server.URL,
	}}); err != nil {
		t.Fatal(err)
	}
	payload := <-received
	if got := payload["Type"]; got != "SubscriptionConfirmation" {
		t.Fatalf("confirmation type %v", got)
	}
	if str(payload["Type"]) != "SubscriptionConfirmation" || str(payload["SigningCertURL"]) == "" || str(payload["Signature"]) == "" || !validTestSNSNotificationSignature(payload) {
		t.Fatalf("unsigned confirmation %#v", payload)
	}
	topicARN := str(topic.Output["TopicArn"])
	wantURL := "http://127.0.0.1:4566/?Action=ConfirmSubscription&TopicArn=" + topicARN + "&Token=" + str(payload["Token"])
	if str(payload["SubscribeURL"]) != wantURL {
		t.Fatalf("confirmation URL %q", payload["SubscribeURL"])
	}
}

func TestSNSHTTPUnsubscribeConfirmation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "unsubscribe-confirmation"}})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan map[string]any, 2)
	headers := make(chan http.Header, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("SNS confirmation payload: %v", err)
			return
		}
		if got := request.Header.Get("Content-Type"); got != "text/plain; charset=UTF-8" {
			t.Errorf("content type %q", got)
		}
		headers <- request.Header.Clone()
		received <- payload
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "http", "Endpoint": server.URL, "ReturnSubscriptionArn": true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	confirmation := <-received
	<-headers
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ConfirmSubscription", Input: map[string]any{"Token": confirmation["Token"]}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Unsubscribe", Input: map[string]any{"SubscriptionArn": sub.Output["SubscriptionArn"]}}); err != nil {
		t.Fatal(err)
	}
	unsubscribed := <-received
	unsubscribeHeaders := <-headers
	if str(unsubscribed["Type"]) != "UnsubscribeConfirmation" || str(unsubscribed["Signature"]) == "" || !validTestSNSNotificationSignature(unsubscribed) {
		t.Fatalf("unsigned unsubscribe confirmation %#v", unsubscribed)
	}
	if got := str(unsubscribed["Message"]); !strings.Contains(got, str(sub.Output["SubscriptionArn"])) {
		t.Fatalf("unsubscribe message %q", got)
	}
	if got := unsubscribeHeaders.Get("x-amz-sns-subscription-arn"); got != str(sub.Output["SubscriptionArn"]) {
		t.Fatalf("unsubscribe subscription header %q", got)
	}
}

func TestSNSSQSDeliveryPropagatesTraceHeader(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "trace-queue"}}); err != nil {
		t.Fatal(err)
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "trace-topic"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:trace-queue",
	}}); err != nil {
		t.Fatal(err)
	}
	trace := "Root=1-3152b799-8954dae64eda91bc9a23a7e8;Parent=7fa8c0f79203be72;Sampled=1"
	if _, err := p.Invoke(ctx, &spi.Request{
		Identity: id, Operation: "Publish", HTTP: &http.Request{Header: http.Header{"X-Amzn-Trace-Id": []string{trace}}},
		Input: map[string]any{"TopicArn": topic.Output["TopicArn"], "Message": "trace"},
	}); err != nil {
		t.Fatal(err)
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{
		"QueueName": "trace-queue", "AttributeNames": []any{"AWSTraceHeader"},
	}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("trace delivery=%#v err=%v", received, err)
	}
	attrs := asMap(asMap(asSlice(received.Output["Messages"])[0])["Attributes"])
	if attrs["AWSTraceHeader"] != trace {
		t.Fatalf("trace attributes=%#v", attrs)
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
	if got["Message"] != "hello" || got["TopicArn"] != topic || got["Subject"] != "warning" || !strings.Contains(str(got["SigningCertUrl"]), "SimpleNotificationService.pem") || !strings.Contains(str(got["UnsubscribeURL"]), "Action=Unsubscribe") {
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
	if attributes["DisplayName"] != "Events" || attributes["TopicArn"] != topic || attributes["Owner"] != id.Account || attributes["SubscriptionsConfirmed"] != "0" || attributes["SubscriptionsPending"] != "0" || attributes["EffectiveDeliveryPolicy"] == nil {
		t.Fatalf("topic attributes %#v", attributes)
	}
	must("SetTopicAttributes", map[string]any{"TopicArn": topic, "AttributeName": "DeliveryPolicy", "AttributeValue": `{"http":{"defaultHealthyRetryPolicy":{"numRetries":1}}}`})
	if got := asMap(must("GetTopicAttributes", map[string]any{"TopicArn": topic}).Output["Attributes"].(map[string]any)["EffectiveDeliveryPolicy"])["http"]; got == nil {
		t.Fatal("effective delivery policy was not projected")
	}
	must("SetTopicAttributes", map[string]any{"TopicArn": topic, "AttributeName": "DeliveryPolicy", "AttributeValue": ""})
	if _, found := must("GetTopicAttributes", map[string]any{"TopicArn": topic}).Output["Attributes"].(map[string]any)["DeliveryPolicy"]; found {
		t.Fatal("raw delivery policy was exposed")
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
	if got := must("GetTopicAttributes", map[string]any{"TopicArn": topic}).Output["Attributes"].(map[string]any)["DataProtectionPolicy"]; got != `{"Name":"policy"}` {
		t.Fatalf("topic data protection attribute %q", got)
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
	must("SetSubscriptionAttributes", map[string]any{"SubscriptionArn": httpSub, "AttributeName": "DeliveryPolicy", "AttributeValue": `{"requestPolicy":{"headerContentType":"text/plain"}}`})
	if got := must("GetSubscriptionAttributes", map[string]any{"SubscriptionArn": httpSub}).Output["Attributes"].(map[string]any)["DeliveryPolicy"]; got == nil {
		t.Fatal("subscription delivery policy was not stored")
	}
	must("SetSubscriptionAttributes", map[string]any{"SubscriptionArn": httpSub, "AttributeName": "DeliveryPolicy", "AttributeValue": ""})
	if _, found := must("GetSubscriptionAttributes", map[string]any{"SubscriptionArn": httpSub}).Output["Attributes"].(map[string]any)["DeliveryPolicy"]; found {
		t.Fatal("empty subscription delivery policy was retained")
	}
	must("Unsubscribe", map[string]any{"SubscriptionArn": httpSub})

	batch := must("PublishBatch", map[string]any{"TopicArn": topic, "Entries": []any{
		map[string]any{"Id": "one", "Message": "first", "MessageAttributes": map[string]any{"kind": map[string]any{"Type": "String", "Value": "a"}}},
		map[string]any{"Id": "two", "Message": "second", "MessageAttributes.entry.1.Name": "kind", "MessageAttributes.entry.1.Value.DataType": "String", "MessageAttributes.entry.1.Value.StringValue": "b"},
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

func TestSNSPartitionARNs(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-gov-west-1"}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "partition-topic"}})
	if err != nil {
		t.Fatal(err)
	}
	wantTopic := "arn:aws-us-gov:sns:us-gov-west-1:1:partition-topic"
	if got := str(topic.Output["TopicArn"]); got != wantTopic {
		t.Fatalf("topic ARN %q, want %q", got, wantTopic)
	}
	app, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: map[string]any{
		"Name": "partition-app", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := str(app.Output["PlatformApplicationArn"]); !strings.HasPrefix(got, "arn:aws-us-gov:sns:us-gov-west-1:1:app/") {
		t.Fatalf("platform application ARN %q", got)
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
	_, malformedPermissionARNFault := call("AddPermission", map[string]any{"TopicArn": "characterized-topic", "Label": "malformed"})
	_, crossScopePermissionARNFault := call("AddPermission", map[string]any{"TopicArn": "arn:aws:sns:us-west-2:123456789012:characterized-topic", "Label": "cross-scope"})
	_, missingAttributeFault := call("SetTopicAttributes", map[string]any{"TopicArn": arn + "-missing", "AttributeName": "DisplayName", "AttributeValue": "bad"})
	_, malformedAttributeARNFault := call("SetTopicAttributes", map[string]any{"TopicArn": "characterized-topic", "AttributeName": "DisplayName", "AttributeValue": "bad"})
	_, crossScopeAttributeARNFault := call("SetTopicAttributes", map[string]any{"TopicArn": "arn:aws:sns:us-west-2:123456789012:characterized-topic", "AttributeName": "DisplayName", "AttributeValue": "bad"})
	_, malformedDeleteARNFault := call("DeleteTopic", map[string]any{"TopicArn": "characterized-topic"})
	_, crossScopeDeleteARNFault := call("DeleteTopic", map[string]any{"TopicArn": "arn:aws:sns:us-west-2:123456789012:characterized-topic"})
	_, missingSubscriptionFault := call("Subscribe", map[string]any{"TopicArn": arn + "-missing", "Protocol": "sqs", "Endpoint": "q"})
	_, malformedSubscriptionARNFault := call("Subscribe", map[string]any{"TopicArn": "characterized-topic", "Protocol": "sqs", "Endpoint": "q"})
	_, crossScopeSubscriptionARNFault := call("Subscribe", map[string]any{"TopicArn": "arn:aws:sns:us-west-2:123456789012:characterized-topic", "Protocol": "sqs", "Endpoint": "q"})
	_, malformedConfirmTopicARNFault := call("ConfirmSubscription", map[string]any{"TopicArn": "characterized-topic", "Token": "random-token"})
	_, crossScopeConfirmTopicARNFault := call("ConfirmSubscription", map[string]any{"TopicArn": "arn:aws:sns:us-west-2:123456789012:characterized-topic", "Token": "random-token"})
	_, missingTagFault := call("TagResource", map[string]any{"ResourceArn": arn + "-missing", "Tags": []any{map[string]any{"Key": "a", "Value": "b"}}})
	_, _ = call("CreateTopic", map[string]any{"Name": "untagged-topic"})
	_, moreTagsFault := call("CreateTopic", map[string]any{
		"Name": "untagged-topic", "Tags": []any{map[string]any{"Key": "new", "Value": "tag"}},
	})
	if duplicateFault != nil || str(duplicate.Output["TopicArn"]) != arn || differentFault == nil || targetFault != nil || malformedFault == nil || missingFault == nil || missingPermissionFault == nil || malformedPermissionARNFault == nil || crossScopePermissionARNFault == nil || missingAttributeFault == nil || malformedAttributeARNFault == nil || crossScopeAttributeARNFault == nil || malformedDeleteARNFault == nil || crossScopeDeleteARNFault == nil || missingSubscriptionFault == nil || malformedSubscriptionARNFault == nil || crossScopeSubscriptionARNFault == nil || malformedConfirmTopicARNFault == nil || crossScopeConfirmTopicARNFault == nil || missingTagFault == nil || moreTagsFault == nil {
		t.Fatalf("duplicate=%#v/%v different=%#v target=%#v malformed=%#v missing=%#v missingPermission=%#v malformedPermissionARN=%#v crossScopePermissionARN=%#v missingAttribute=%#v malformedAttributeARN=%#v crossScopeAttributeARN=%#v malformedDeleteARN=%#v crossScopeDeleteARN=%#v missingSubscription=%#v malformedSubscriptionARN=%#v crossScopeSubscriptionARN=%#v malformedConfirmTopicARN=%#v crossScopeConfirmTopicARN=%#v missingTag=%#v moreTags=%#v", duplicate, duplicateFault, differentFault, targetFault, malformedFault, missingFault, missingPermissionFault, malformedPermissionARNFault, crossScopePermissionARNFault, missingAttributeFault, malformedAttributeARNFault, crossScopeAttributeARNFault, malformedDeleteARNFault, crossScopeDeleteARNFault, missingSubscriptionFault, malformedSubscriptionARNFault, crossScopeSubscriptionARNFault, malformedConfirmTopicARNFault, crossScopeConfirmTopicARNFault, missingTagFault, moreTagsFault)
	}
	attrs, fault := call("GetTopicAttributes", map[string]any{"TopicArn": arn})
	if fault != nil || str(asMap(attrs.Output["Attributes"])["DisplayName"]) != "before" {
		t.Fatalf("malformed SetTopicAttributes mutated topic: %#v/%v", attrs, fault)
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

func TestSNSFIFOTopicToSQSWithoutQueueDeduplication(t *testing.T) {
	deps := spitest.Deps(t)
	p, qp := New(deps), sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{
		"QueueName": "sns-fifo-no-dedup.fifo", "Attributes": map[string]any{"FifoQueue": "true"},
	}}); err != nil {
		t.Fatal(err)
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{
		"Name": "sns-fifo-no-dedup.fifo", "Attributes": map[string]any{"FifoTopic": "true", "ContentBasedDeduplication": "true"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	topicARN := str(topic.Output["TopicArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topicARN, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:sns-fifo-no-dedup.fifo", "RawMessageDelivery": "true",
	}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
			"TopicArn": topicARN, "Message": "deduplicated", "MessageGroupId": "group-1",
		}}); err != nil {
			t.Fatal(err)
		}
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{
		"QueueName": "sns-fifo-no-dedup.fifo", "AttributeNames": []any{"All"},
	}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("FIFO SNS delivery=%#v err=%v", received, err)
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
	if _, err := call("CreatePlatformEndpoint", map[string]any{
		"PlatformApplicationArn": appARN, "Token": "token", "Attributes": map[string]any{"Enabled": "false"},
	}); err == nil {
		t.Fatal("accepted endpoint idempotency request with different attributes")
	}
	if _, err := call("SetEndpointAttributes", map[string]any{"EndpointArn": appARN + "/endpoint/missing"}); err == nil {
		t.Fatal("set attributes for missing endpoint")
	}
	endpointARN := str(first.Output["EndpointArn"])
	if _, err := call("SetEndpointAttributes", map[string]any{"EndpointArn": endpointARN, "Attributes": map[string]any{}}); err == nil {
		t.Fatal("accepted empty endpoint attributes")
	}
	if _, err := call("SetPlatformApplicationAttributes", map[string]any{"PlatformApplicationArn": appARN + "/missing"}); err == nil {
		t.Fatal("set attributes for missing application")
	}
	if _, err := call("DeletePlatformApplication", map[string]any{"PlatformApplicationArn": appARN}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("ListEndpointsByPlatformApplication", map[string]any{"PlatformApplicationArn": appARN}); err == nil {
		t.Fatal("listed endpoints for deleted application")
	}
}

func TestSNSPlatformEndpointAttributesProjection(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	app, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: map[string]any{
		"Name": "endpoint-attributes", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformEndpoint", Input: map[string]any{
		"PlatformApplicationArn": app.Output["PlatformApplicationArn"], "Token": "token", "CustomUserData": "custom",
	}})
	if err != nil {
		t.Fatal(err)
	}
	get, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetEndpointAttributes", Input: map[string]any{"EndpointArn": endpoint.Output["EndpointArn"]}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"Enabled": "true", "Token": "token", "CustomUserData": "custom"}
	if got := asMap(get.Output["Attributes"]); !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoint attributes %#v, want %#v", got, want)
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
		"missing attributes": {"Name": "mobile", "Platform": "ADM", "Attributes": map[string]any{}},
		"missing credential": {"Name": "mobile", "Platform": "GCM", "Attributes": map[string]any{"PlatformPrincipal": "principal"}},
		"missing principal":  {"Name": "mobile", "Platform": "ADM", "Attributes": map[string]any{"PlatformCredential": "secret"}},
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

func TestSNSPlatformEndpointDeletionCleansSubscriptions(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	app, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreatePlatformApplication", Input: map[string]any{
		"Name": "endpoint-delete", "Platform": "GCM", "Attributes": map[string]any{"PlatformCredential": "secret"},
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
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "endpoint-delete"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "application", "Endpoint": endpoint.Output["EndpointArn"],
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteEndpoint", Input: map[string]any{"EndpointArn": endpoint.Output["EndpointArn"]}}); err != nil {
		t.Fatal(err)
	}
	listed, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSubscriptionsByTopic", Input: map[string]any{"TopicArn": topic.Output["TopicArn"]}})
	if err != nil {
		t.Fatal(err)
	}
	if len(asSlice(listed.Output["Subscriptions"])) != 0 {
		t.Fatalf("endpoint subscription remained: %#v", listed.Output)
	}
}

func TestSNSTopicAttributesIncludeDefaultPolicy(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "default-policy"}})
	if err != nil {
		t.Fatal(err)
	}
	attrs, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetTopicAttributes", Input: map[string]any{"TopicArn": created.Output["TopicArn"]}})
	if err != nil {
		t.Fatal(err)
	}
	policy := str(asMap(attrs.Output["Attributes"])["Policy"])
	var decoded map[string]any
	if json.Unmarshal([]byte(policy), &decoded) != nil || decoded["Version"] != "2008-10-17" || decoded["Id"] != "__default_policy_ID" || !strings.Contains(policy, "__default_statement_ID") || !strings.Contains(policy, created.Output["TopicArn"].(string)) {
		t.Fatalf("default topic policy=%q", policy)
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

func TestSNSSMSOptOutSuppressesDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	phone := "+15555550123"
	received := make(chan []byte, 1)
	cancel := deps.Bus.Subscribe("sns:sms:"+phone, func(_ context.Context, body []byte) { received <- body })
	defer cancel()
	if err := p.col(&spi.Request{Identity: id}, "smsopt").Put(ctx, phone, []byte("true")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"PhoneNumber": phone, "Message": "blocked",
	}}); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-received:
		t.Fatalf("opted-out SMS was delivered: %q", body)
	default:
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
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CheckIfPhoneNumberIsOptedOut", Input: map[string]any{"PhoneNumber": "invalid"}}); err == nil {
		t.Fatal("checked invalid opt-out phone")
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

func TestSNSTagValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "duplicate-tag"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "TagResource", Input: map[string]any{
		"ResourceArn": created.Output["TopicArn"],
		"Tags":        []any{map[string]any{"Key": "duplicate", "Value": "one"}, map[string]any{"Key": "duplicate", "Value": "two"}},
	}})
	if err == nil {
		t.Fatal("accepted duplicate tag keys")
	}
}

func TestSNSTopicFIFOAttributeIsImmutable(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{
		"Name": "standard-fifo-attribute", "Attributes": map[string]any{"FifoTopic": "false"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetTopicAttributes", Input: map[string]any{
		"TopicArn": created.Output["TopicArn"], "AttributeName": "FifoTopic", "AttributeValue": "false",
	}}); err == nil {
		t.Fatal("modified FifoTopic after creation")
	}
}

func TestSNSPermissionValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "permission-validation"}})
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"TopicArn": created.Output["TopicArn"], "Label": "duplicate", "AWSAccountId": []any{"1"}, "ActionName": []any{"Publish"}}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "AddPermission", Input: input}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "AddPermission", Input: input}); err == nil {
		t.Fatal("accepted duplicate permission label")
	}
	input["Label"] = "invalid-action"
	input["ActionName"] = []any{"InvalidAction"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "AddPermission", Input: input}); err == nil {
		t.Fatal("accepted invalid permission action")
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

func TestSNSDefaultSMSAttributes(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	response, err := p.Invoke(context.Background(), &spi.Request{Identity: spi.Identity{Account: "1", Region: "us-west-2"}, Operation: "GetSMSAttributes"})
	if err != nil {
		t.Fatal(err)
	}
	attrs := asMap(response.Output["Attributes"])
	if attrs["MonthlySpendLimit"] != "1" {
		t.Fatalf("default SMS attributes %#v", attrs)
	}
}

func TestSNSUnsubscribeDeletedTopic(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "deleted-sub"}})
	if err != nil {
		t.Fatal(err)
	}
	topic := str(created.Output["TopicArn"])
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic, "Protocol": "email", "Endpoint": "user@example.com",
	}})
	if err != nil {
		t.Fatal(err)
	}
	subARN := str(sub.Output["SubscriptionArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteTopic", Input: map[string]any{"TopicArn": topic}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Unsubscribe", Input: map[string]any{"SubscriptionArn": subARN}}); err == nil {
		t.Fatal("unsubscribed from a deleted topic")
	}
}

func TestSNSListSubscriptionsByTopicARNValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "list-subs-validation"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(created.Output["TopicArn"])
	for _, topicARN := range []string{
		"list-subs-validation",
		strings.Replace(arn, ":us-east-1:", ":us-west-2:", 1),
		arn + "-missing",
	} {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSubscriptionsByTopic", Input: map[string]any{"TopicArn": topicARN}})
		if err == nil {
			t.Fatalf("accepted invalid or missing TopicArn %q", topicARN)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSubscriptionsByTopic", Input: map[string]any{"TopicArn": arn}}); err != nil {
		t.Fatalf("valid TopicArn rejected: %v", err)
	}
}

func TestSNSSubscriptionAttributesARNValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "sub-attrs-validation"}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": created.Output["TopicArn"], "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:1:q",
	}})
	if err != nil {
		t.Fatal(err)
	}
	arn := str(sub.Output["SubscriptionArn"])
	for _, operation := range []string{"GetSubscriptionAttributes", "SetSubscriptionAttributes"} {
		for _, tc := range []struct {
			subscriptionARN string
			code            string
		}{
			{"malformed-subscription", "InvalidParameter"},
			{strings.Replace(arn, ":us-east-1:", ":us-west-2:", 1), "NotFound"},
		} {
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: map[string]any{
				"SubscriptionArn": tc.subscriptionARN, "AttributeName": "RawMessageDelivery", "AttributeValue": "true",
			}})
			fault, _ := err.(*spi.Fault)
			if fault == nil || fault.Code != tc.code {
				t.Fatalf("%s SubscriptionArn %q fault=%v", operation, tc.subscriptionARN, err)
			}
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": arn}}); err != nil {
		t.Fatalf("valid SubscriptionArn rejected: %v", err)
	}
}

func TestSNSPendingEmailSubscription(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "pending-email"}})
	if err != nil {
		t.Fatal(err)
	}
	topic := str(created.Output["TopicArn"])
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic, "Protocol": "email", "Endpoint": "user@example.com",
	}})
	if err != nil || str(sub.Output["SubscriptionArn"]) != "pending confirmation" {
		t.Fatalf("email subscription=%#v err=%v", sub, err)
	}
	listed, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSubscriptions", Input: map[string]any{}})
	if err != nil || len(asSlice(listed.Output["Subscriptions"])) != 1 || str(asMap(asSlice(listed.Output["Subscriptions"])[0])["SubscriptionArn"]) != "PendingConfirmation" {
		t.Fatalf("pending email listing=%#v err=%v", listed, err)
	}
	withARN, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic, "Protocol": "email", "Endpoint": "other@example.com", "ReturnSubscriptionArn": true,
	}})
	if err != nil || !validSubscriptionARN(str(withARN.Output["SubscriptionArn"])) {
		t.Fatalf("email subscription with ARN=%#v err=%v", withARN, err)
	}
	attrs, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSubscriptionAttributes", Input: map[string]any{"SubscriptionArn": withARN.Output["SubscriptionArn"]}})
	if err != nil || attrs.Output["Attributes"].(map[string]any)["PendingConfirmation"] != "true" || attrs.Output["Attributes"].(map[string]any)["ConfirmationWasAuthenticated"] != "false" {
		t.Fatalf("pending subscription attributes=%#v err=%v", attrs, err)
	}
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer httpServer.Close()
	httpSub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic, "Protocol": "http", "Endpoint": httpServer.URL, "ReturnSubscriptionArn": true,
	}})
	if err != nil || !validSubscriptionARN(str(httpSub.Output["SubscriptionArn"])) {
		t.Fatalf("HTTP subscription with ARN=%#v err=%v", httpSub, err)
	}
	httpAgain, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic, "Protocol": "http", "Endpoint": httpServer.URL,
	}})
	if err != nil || str(httpAgain.Output["SubscriptionArn"]) != "pending confirmation" {
		t.Fatalf("HTTP idempotent pending subscription=%#v err=%v", httpAgain, err)
	}
}

func TestSNSHTTPSubscriptionRedrive(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	qp := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	queue, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "sns-http-dlq"}})
	if err != nil {
		t.Fatal(err)
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "sns-http-redrive"}})
	if err != nil {
		t.Fatal(err)
	}
	token := make(chan string, 1)
	messageType := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(request.Body).Decode(&payload)
		if str(payload["Type"]) == "SubscriptionConfirmation" {
			token <- str(payload["Token"])
			messageType <- request.Header.Get("x-amz-sns-message-type")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "http", "Endpoint": server.URL, "ReturnSubscriptionArn": true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-messageType; got != "SubscriptionConfirmation" {
		t.Fatalf("confirmation message type header=%q", got)
	}
	confirmationToken := <-token
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ConfirmSubscription", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Token": confirmationToken,
	}}); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSubscriptionAttributes", Input: map[string]any{
		"SubscriptionArn": sub.Output["SubscriptionArn"], "AttributeName": "RedrivePolicy",
		"AttributeValue": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:sns-http-dlq"}`,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Message": "redrive",
	}}); err != nil {
		t.Fatal(err)
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueUrl": queue.Output["QueueUrl"]}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("DLQ messages=%#v err=%v", received, err)
	}
}

func TestSNSSQSSubscriptionRedrive(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	qp := sqs.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	for _, name := range []string{"sns-sqs-source", "sns-sqs-dlq"} {
		if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": name}}); err != nil {
			t.Fatal(err)
		}
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "sns-sqs-redrive"}})
	if err != nil {
		t.Fatal(err)
	}
	sourceARN := "arn:aws:sqs:us-east-1:1:sns-sqs-source"
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "sqs", "Endpoint": sourceARN,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSubscriptionAttributes", Input: map[string]any{
		"SubscriptionArn": sub.Output["SubscriptionArn"], "AttributeName": "RedrivePolicy",
		"AttributeValue": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:sns-sqs-dlq"}`,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteQueue", Input: map[string]any{"QueueUrl": "http://localhost:4566/1/sns-sqs-source"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Message": "redrive-sqs",
	}}); err != nil {
		t.Fatal(err)
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueUrl": "http://localhost:4566/1/sns-sqs-dlq"}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("SQS DLQ messages=%#v err=%v", received, err)
	}
}

func TestSNSLambdaSubscriptionRedrive(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	qp := sqs.New(deps)
	lp := lambda.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "sns-lambda-dlq"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := lp.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{"FunctionName": "sns-redrive-lambda"}}); err != nil {
		t.Fatal(err)
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "sns-lambda-redrive"}})
	if err != nil {
		t.Fatal(err)
	}
	functionARN := "arn:aws:lambda:us-east-1:1:function:sns-redrive-lambda"
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "lambda", "Endpoint": functionARN,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSubscriptionAttributes", Input: map[string]any{
		"SubscriptionArn": sub.Output["SubscriptionArn"], "AttributeName": "RedrivePolicy",
		"AttributeValue": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:sns-lambda-dlq"}`,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := lp.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteFunction", Input: map[string]any{"FunctionName": "sns-redrive-lambda"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Message": "redrive-lambda",
	}}); err != nil {
		t.Fatal(err)
	}
	received, err := qp.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueUrl": "http://localhost:4566/1/sns-lambda-dlq"}})
	if err != nil || len(asSlice(received.Output["Messages"])) != 1 {
		t.Fatalf("Lambda DLQ messages=%#v err=%v", received, err)
	}
}

func TestSNSHTTPDeliveryPolicy(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	snsCall := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	topic, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "sns-http-policy"}})
	if err != nil {
		t.Fatal(err)
	}
	token := make(chan string, 1)
	bodyCh := make(chan string, 1)
	contentTypeCh := make(chan string, 1)
	messageTypeCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		if incoming.Header.Get("x-amz-sns-message-type") == "SubscriptionConfirmation" {
			var payload map[string]any
			_ = json.NewDecoder(incoming.Body).Decode(&payload)
			token <- str(payload["Token"])
		} else {
			body, _ := io.ReadAll(incoming.Body)
			bodyCh <- string(body)
			contentTypeCh <- incoming.Header.Get("Content-Type")
			messageTypeCh <- incoming.Header.Get("x-amz-sns-message-type")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	sub, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Protocol": "http", "Endpoint": server.URL, "ReturnSubscriptionArn": true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ConfirmSubscription", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Token": <-token,
	}}); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"DeliveryPolicy": `{"requestPolicy":{"headerContentType":"text/csv"}}`, "RawMessageDelivery": "true"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetSubscriptionAttributes", Input: map[string]any{
			"SubscriptionArn": sub.Output["SubscriptionArn"], "AttributeName": name, "AttributeValue": value,
		}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{
		"TopicArn": topic.Output["TopicArn"], "Message": "raw-http-message",
	}}); err != nil {
		t.Fatal(err)
	}
	if body := <-bodyCh; body != "raw-http-message" {
		t.Fatalf("raw HTTP body=%q", body)
	}
	if got := <-contentTypeCh; got != "text/csv" {
		t.Fatalf("HTTP content type=%q", got)
	}
	if got := <-messageTypeCh; got != "Notification" {
		t.Fatalf("SNS message type=%q", got)
	}
	snsCall("SetSubscriptionAttributes", map[string]any{"SubscriptionArn": sub.Output["SubscriptionArn"], "AttributeName": "DeliveryPolicy", "AttributeValue": ""})
	snsCall("SetSubscriptionAttributes", map[string]any{"SubscriptionArn": sub.Output["SubscriptionArn"], "AttributeName": "RawMessageDelivery", "AttributeValue": "false"})
	snsCall("SetTopicAttributes", map[string]any{"TopicArn": topic.Output["TopicArn"], "AttributeName": "DeliveryPolicy", "AttributeValue": `{"http":{"defaultRequestPolicy":{"headerContentType":"text/plain"}}}`})
	snsCall("Publish", map[string]any{"TopicArn": topic.Output["TopicArn"], "Message": "topic-policy-message"})
	if got := <-contentTypeCh; got != "text/plain" {
		t.Fatalf("topic HTTP content type=%q", got)
	}
}

func TestSNSCreateTopicIdempotencyPreservesAttributes(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{
		"Name": "idempotent-attributes",
	}})
	if err != nil {
		t.Fatal(err)
	}
	topic := str(created.Output["TopicArn"])
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetTopicAttributes", Input: map[string]any{
		"TopicArn": topic, "AttributeName": "DisplayName", "AttributeValue": "AlreadySet",
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{
		"Name": "idempotent-attributes",
	}}); err != nil {
		t.Fatal(err)
	}
	attrs, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetTopicAttributes", Input: map[string]any{"TopicArn": topic}})
	if err != nil || str(asMap(attrs.Output["Attributes"])["DisplayName"]) != "AlreadySet" {
		t.Fatalf("idempotent attributes=%#v err=%v", attrs, err)
	}
}

func TestSNSTopicAttributeARNValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "attrs-arn"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "GetTopicAttributes", Input: map[string]any{"TopicArn": "attrs-arn"}})
	fault, _ := err.(*spi.Fault)
	if fault == nil || fault.Code != "InvalidParameter" {
		t.Fatalf("malformed TopicArn fault=%v", err)
	}
}

func TestSNSTopicAttributeARNScope(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "scope-arn"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := strings.Replace(str(created.Output["TopicArn"]), ":us-east-1:", ":us-west-2:", 1)
	_, err = p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "GetTopicAttributes", Input: map[string]any{"TopicArn": arn}})
	fault, _ := err.(*spi.Fault)
	if fault == nil || fault.Code != "NotFound" {
		t.Fatalf("cross-region TopicArn fault=%v", err)
	}
}

func TestSNSStandardTopicFalseFIFOIsIdempotent(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "standard-idempotent"}})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{
		"Name": "standard-idempotent", "Attributes": map[string]any{"FifoTopic": "false"},
	}})
	if err != nil || duplicate.Output["TopicArn"] != created.Output["TopicArn"] {
		t.Fatalf("explicit false was not idempotent: %#v %v", duplicate, err)
	}
}

func TestSNSMessageAttributeValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTopic", Input: map[string]any{"Name": "message-attributes"}})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []map[string]any{
		{"missing": map[string]any{"DataType": "String", "StringValue": ""}},
		{"missing-type": map[string]any{"StringValue": "value"}},
		{"binary-as-string": map[string]any{"DataType": "String", "BinaryValue": "123"}},
		{"string-as-binary": map[string]any{"DataType": "Binary", "StringValue": "123"}},
		{"invalid-type": map[string]any{"DataType": "InvalidType", "StringValue": "123"}},
		{strings.Repeat("a", 257): map[string]any{"DataType": "String", "StringValue": "123"}},
		{"a^*?": map[string]any{"DataType": "String", "StringValue": "123"}},
		{".abc": map[string]any{"DataType": "String", "StringValue": "123"}},
		{"abc.": map[string]any{"DataType": "String", "StringValue": "123"}},
		{"a..bc": map[string]any{"DataType": "String", "StringValue": "123"}},
		{"attr": map[string]any{"DataType": "String.", "StringValue": "123"}},
	}
	for _, attrs := range invalid {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": created.Output["TopicArn"], "Message": "message", "MessageAttributes": attrs}}); err == nil {
			t.Fatalf("accepted invalid message attributes %#v", attrs)
		}
	}
	for _, dataType := range []string{"String.prefixed", "String.  prefixed.", "Number"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Publish", Input: map[string]any{"TopicArn": created.Output["TopicArn"], "Message": "message", "MessageAttributes": map[string]any{"attr": map[string]any{"DataType": dataType, "StringValue": "123"}}}}); err != nil {
			t.Fatalf("rejected valid message attribute type %q: %v", dataType, err)
		}
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
