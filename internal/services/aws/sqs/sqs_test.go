package sqs

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

type observedClock struct {
	spi.Clock
	after chan time.Duration
}

func (c *observedClock) After(delay time.Duration) <-chan time.Time {
	result := c.Clock.After(delay)
	c.after <- delay
	return result
}

func TestCreateSendReceiveDelete(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	ctx := context.Background()

	created, err := p.Invoke(ctx, &spi.Request{
		ServiceID: "aws.sqs",
		Operation: "CreateQueue",
		Input:     map[string]any{"QueueName": "q"},
		Identity:  id,
	})
	if err != nil {
		t.Fatal(err)
	}
	url, _ := created.Output["QueueUrl"].(string)
	if url == "" || !strings.Contains(url, "q") {
		t.Fatalf("queue url: %q", url)
	}

	sent, err := p.Invoke(ctx, &spi.Request{
		ServiceID: "aws.sqs",
		Operation: "SendMessage",
		Input:     map[string]any{"QueueUrl": url, "QueueName": "q", "MessageBody": "hello"},
		Identity:  id,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sent.Output["MessageId"] == nil {
		t.Fatal("missing MessageId")
	}
	if sent.Output["MD5OfMessageBody"] != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatalf("md5 %v", sent.Output["MD5OfMessageBody"])
	}

	got, err := p.Invoke(ctx, &spi.Request{
		ServiceID: "aws.sqs",
		Operation: "ReceiveMessage",
		Input:     map[string]any{"QueueUrl": url},
		Identity:  id,
	})
	if err != nil {
		t.Fatal(err)
	}
	msgs, _ := got.Output["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("receive: %+v", got.Output)
	}
	msg, _ := msgs[0].(map[string]any)
	if msg["Body"] != "hello" {
		t.Fatalf("body: %v", msg["Body"])
	}
	if msg["MD5OfBody"] != sent.Output["MD5OfMessageBody"] {
		t.Fatalf("receive md5 %v", msg["MD5OfBody"])
	}
	handle, _ := msg["ReceiptHandle"].(string)
	if handle == "" {
		t.Fatal("missing ReceiptHandle")
	}

	if _, err := p.Invoke(ctx, &spi.Request{
		ServiceID: "aws.sqs",
		Operation: "DeleteMessage",
		Input:     map[string]any{"QueueUrl": url, "ReceiptHandle": handle},
		Identity:  id,
	}); err != nil {
		t.Fatal(err)
	}
	empty, err := p.Invoke(ctx, &spi.Request{
		ServiceID: "aws.sqs",
		Operation: "ReceiveMessage",
		Input:     map[string]any{"QueueUrl": url},
		Identity:  id,
	})
	if err != nil {
		t.Fatal(err)
	}
	left, _ := empty.Output["Messages"].([]any)
	if len(left) != 0 {
		t.Fatalf("message survived delete: %+v", left)
	}
}

func TestSendReceiveCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	call("CreateQueue", map[string]any{"QueueName": "roundtrip"})
	sent := call("SendMessage", map[string]any{"QueueName": "roundtrip", "MessageBody": "message"})
	received := call("ReceiveMessage", map[string]any{"QueueName": "roundtrip", "VisibilityTimeout": 0})
	golden.AssertJSON(t, map[string]any{"sent": sent.Output, "received": received.Output})
}

func TestEmptyMessageCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "empty-body"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "empty-body", "MessageBody": ""}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("empty message fault %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestReceiveMessageMaxNumberValidation(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "max-messages"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "max-messages", "MaxNumberOfMessages": 11}})
	fault, ok := err.(*spi.Fault)
	if !ok || fault.Code != "InvalidParameterValue" || fault.Message != "Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided." || fault.HTTPStatus != 400 {
		t.Fatalf("max messages fault %#v", err)
	}
}

func TestReceiveMessageMaxNumberCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "max-messages"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "max-messages", "MaxNumberOfMessages": 11}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("max messages fault %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestListQueuesPrefixAndPagination(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	for _, name := range []string{"a-0", "a-1", "b-0"} {
		call("CreateQueue", map[string]any{"QueueName": name})
	}
	if queues := call("ListQueues", map[string]any{"QueueNamePrefix": "a-"}).Output["QueueUrls"].([]any); len(queues) != 2 {
		t.Fatalf("prefix queues %#v", queues)
	}
	first := call("ListQueues", map[string]any{"QueueNamePrefix": "a-", "MaxResults": 1}).Output
	urls := first["QueueUrls"].([]any)
	if len(urls) != 1 {
		t.Fatalf("first page %#v", first)
	}
	wantToken := base64.StdEncoding.EncodeToString([]byte(urls[0].(string)))
	if first["NextToken"] != wantToken {
		t.Fatalf("first page %#v", first)
	}
	second := call("ListQueues", map[string]any{"QueueNamePrefix": "a-", "MaxResults": 10, "NextToken": first["NextToken"]}).Output
	if urls := second["QueueUrls"].([]any); len(urls) != 1 || !strings.HasSuffix(urls[0].(string), "/a-1") || second["NextToken"] != nil {
		t.Fatalf("second page %#v", second)
	}
	if missing := call("ListQueues", map[string]any{"QueueNamePrefix": "missing"}).Output; missing["QueueUrls"] != nil {
		t.Fatalf("empty prefix %#v", missing)
	}
}

func TestCreateQueueMetadataAttributes(t *testing.T) {
	deps := spitest.Deps(t)
	wantCreated := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := deps.Clock.Advance(wantCreated.Sub(deps.Clock.Now())); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "metadata"}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{
		"QueueUrl": created.Output["QueueUrl"], "AttributeNames": []any{"QueueArn", "CreatedTimestamp", "VisibilityTimeout"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	attrs := response.Output["Attributes"].(map[string]any)
	if len(attrs) != 3 || attrs["QueueArn"] != "arn:aws:sqs:us-east-1:123456789012:metadata" ||
		attrs["CreatedTimestamp"] != "1577934245" || attrs["VisibilityTimeout"] != "30" {
		t.Fatalf("attributes %#v", attrs)
	}
	empty, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueUrl": created.Output["QueueUrl"]}})
	if err != nil || len(empty.Output["Attributes"].(map[string]any)) != 0 {
		t.Fatalf("omitted attributes %#v, %v", empty, err)
	}
}

func TestQueueCannotBeRecreatedUntilDeleteWindowExpires(t *testing.T) {
	clk := clock.NewControllable()
	if err := clk.Advance(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC).Sub(clk.Now())); err != nil {
		t.Fatal(err)
	}
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	name := "deleted.fifo"
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	created, err := call("CreateQueue", map[string]any{"QueueName": name, "Attributes": map[string]any{"DelaySeconds": "5"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := call("TagQueue", map[string]any{"QueueName": name, "Tags": map[string]any{"old": "tag"}}); err != nil {
		t.Fatal(err)
	}
	firstSent, err := call("SendMessage", map[string]any{"QueueName": name, "MessageBody": "old", "MessageGroupId": "group", "MessageDeduplicationId": "dedup"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := call("DeleteQueue", map[string]any{"QueueName": name}); err != nil {
		t.Fatal(err)
	}
	for _, collection := range []string{"qattrs", "qtags"} {
		if _, ok, err := p.col(&spi.Request{Identity: id}, collection).Get(ctx, name); err != nil || ok {
			t.Fatalf("%s survived delete: found=%v err=%v", collection, ok, err)
		}
	}
	for _, collection := range []string{"msgs:" + name, "dedup:" + name} {
		if records, _, err := p.col(&spi.Request{Identity: id}, collection).List(ctx, "", "", 0); err != nil || len(records) != 0 {
			t.Fatalf("%s survived delete: records=%d err=%v", collection, len(records), err)
		}
	}
	_, err = call("CreateQueue", map[string]any{"QueueName": name})
	fault, ok := err.(*spi.Fault)
	if !ok || fault.Code != "AWS.SimpleQueueService.QueueDeletedRecently" || fault.Message != "You must wait 60 seconds after deleting a queue before you can create another with the same name." {
		t.Fatalf("recently deleted fault %#v", err)
	}
	if err := clk.Advance(time.Minute); err != nil {
		t.Fatal(err)
	}
	recreated, err := call("CreateQueue", map[string]any{"QueueName": name})
	if err != nil || recreated.Output["QueueUrl"] != created.Output["QueueUrl"] {
		t.Fatalf("recreate %#v, %v", recreated, err)
	}
	secondSent, sendErr := call("SendMessage", map[string]any{"QueueName": name, "MessageBody": "old", "MessageGroupId": "group", "MessageDeduplicationId": "dedup"})
	attrs, attrErr := call("GetQueueAttributes", map[string]any{"QueueName": name, "AttributeNames": []any{"All"}})
	tags, tagErr := call("ListQueueTags", map[string]any{"QueueName": name})
	messages, receiveErr := call("ReceiveMessage", map[string]any{"QueueName": name})
	if attrErr != nil || tagErr != nil || receiveErr != nil {
		t.Fatalf("recreated queue reads: %v, %v, %v", attrErr, tagErr, receiveErr)
	}
	if sendErr != nil || secondSent.Output["MessageId"] == firstSent.Output["MessageId"] || attrs.Output["Attributes"].(map[string]any)["DelaySeconds"] != nil ||
		len(tags.Output["Tags"].(map[string]any)) != 0 || len(messages.Output["Messages"].([]any)) != 1 {
		t.Fatalf("deleted state survived: attrs=%#v tags=%#v messages=%#v", attrs.Output, tags.Output, messages.Output)
	}
}

func TestQueueMetadataCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	if err := deps.Clock.Advance(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC).Sub(deps.Clock.Now())); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{
		"QueueName": "metadata", "Attributes": map[string]any{"DelaySeconds": "5"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{
		"QueueUrl": created.Output["QueueUrl"], "AttributeNames": []any{"QueueArn", "CreatedTimestamp", "VisibilityTimeout"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	all, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{
		"QueueUrl": created.Output["QueueUrl"], "AttributeNames": []any{"All"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{"selected": selected.Output, "all": all.Output})
}

func TestQueueRecentlyDeletedCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	if err := clk.Advance(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC).Sub(clk.Now())); err != nil {
		t.Fatal(err)
	}
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateQueue", map[string]any{"QueueName": "deleted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("DeleteQueue", map[string]any{"QueueName": "deleted"}); err != nil {
		t.Fatal(err)
	}
	attempt := func() map[string]any {
		response, err := call("CreateQueue", map[string]any{"QueueName": "deleted"})
		if err == nil {
			return response.Output
		}
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("create fault %#v", err)
		}
		return map[string]any{"Code": fault.Code, "Message": fault.Message}
	}
	immediate := attempt()
	_ = clk.Advance(59 * time.Second)
	beforeBoundary := attempt()
	_ = clk.Advance(time.Second)
	golden.AssertJSON(t, map[string]any{"immediate": immediate, "beforeBoundary": beforeBoundary, "atBoundary": attempt()})
}

func TestListQueuesCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output
	}
	for _, name := range []string{"a-0", "a-1", "a-2", "b-0"} {
		call("CreateQueue", map[string]any{"QueueName": name})
	}
	first := call("ListQueues", map[string]any{"QueueNamePrefix": "a-", "MaxResults": 2})
	second := call("ListQueues", map[string]any{"QueueNamePrefix": "a-", "MaxResults": 2, "NextToken": first["NextToken"]})
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListQueues", Input: map[string]any{"NextToken": "not-base64"}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("invalid token fault %#v", err)
	}
	golden.AssertJSON(t, map[string]any{
		"all": call("ListQueues", nil), "prefix": call("ListQueues", map[string]any{"QueueNamePrefix": "a-"}),
		"first": first, "second": second, "empty": call("ListQueues", map[string]any{"QueueNamePrefix": "missing"}),
		"invalidToken": map[string]any{"code": fault.Code, "message": fault.Message},
	})
}

func TestQueueScopedOperationsRejectMissingQueue(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	for _, operation := range []string{"GetQueueUrl", "SendMessage", "ReceiveMessage", "DeleteQueue", "GetQueueAttributes", "TagQueue"} {
		_, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: map[string]any{"QueueName": "missing"}})
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "AWS.SimpleQueueService.NonExistentQueue" {
			t.Fatalf("%s error %#v", operation, err)
		}
	}
}

func TestSendValidationAndDelay(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	invoke := func(op string, in map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
	}
	_, _ = invoke("CreateQueue", map[string]any{"QueueName": "strict.fifo"})
	if _, err := invoke("SendMessage", map[string]any{"QueueName": "strict.fifo", "MessageBody": "x"}); faultCode(err) != "MissingParameter" {
		t.Fatalf("missing group error %v", err)
	}
	if _, err := invoke("SendMessage", map[string]any{"QueueName": "strict.fifo", "MessageBody": "x", "MessageGroupId": "g"}); faultCode(err) != "InvalidParameterValue" {
		t.Fatalf("missing dedup error %v", err)
	}
	batch, err := invoke("SendMessageBatch", map[string]any{"QueueName": "strict.fifo", "Entries": []any{
		map[string]any{"Id": "bad", "MessageBody": "x"},
		map[string]any{"Id": "ok", "MessageBody": "x", "MessageGroupId": "g", "MessageDeduplicationId": "d"},
	}})
	if err != nil || len(batch.Output["Successful"].([]any)) != 1 || len(batch.Output["Failed"].([]any)) != 1 {
		t.Fatalf("batch response %#v error %v", batch, err)
	}
	_, _ = invoke("CreateQueue", map[string]any{"QueueName": "delayed", "Attributes": map[string]any{"DelaySeconds": "10"}})
	if _, err := invoke("SendMessage", map[string]any{"QueueName": "delayed", "MessageBody": ""}); err != nil {
		fault, _ := err.(*spi.Fault)
		if fault == nil || fault.Code != "MissingParameter" || fault.Message != "The request must contain the parameter MessageBody." {
			t.Fatalf("empty message error %v", err)
		}
	} else {
		t.Fatal("empty message accepted")
	}
	if _, err := invoke("SendMessage", map[string]any{"QueueName": "delayed", "MessageBody": "later"}); err != nil {
		t.Fatal(err)
	}
	before, _ := invoke("ReceiveMessage", map[string]any{"QueueName": "delayed"})
	if len(before.Output["Messages"].([]any)) != 0 {
		t.Fatalf("delayed message visible early %#v", before.Output)
	}
	_ = clk.Advance(10 * time.Second)
	after, _ := invoke("ReceiveMessage", map[string]any{"QueueName": "delayed"})
	if len(after.Output["Messages"].([]any)) != 1 {
		t.Fatalf("delayed message missing %#v", after.Output)
	}
	if _, err := invoke("SendMessage", map[string]any{"QueueName": "delayed", "MessageBody": "x", "DelaySeconds": 901}); faultCode(err) != "InvalidParameterValue" {
		t.Fatalf("invalid delay error %v", err)
	}
}

func faultCode(err error) string {
	fault, _ := err.(*spi.Fault)
	if fault == nil {
		return ""
	}
	return fault.Code
}

func TestFIFODedupDLQLongPoll(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := &Pack{deps: deps}
	id := spi.Identity{Account: "1", Region: "us-east-1"}
	ctx := context.Background()
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp
	}
	inv("CreateQueue", map[string]any{"QueueName": "dlq"})
	inv("CreateQueue", map[string]any{"QueueName": "q.fifo", "Attributes": map[string]any{
		"ContentBasedDeduplication": "true",
		"RedrivePolicy":             `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq","maxReceiveCount":"1"}`,
		"VisibilityTimeout":         "0",
	}})
	inv("SendMessage", map[string]any{"QueueName": "q.fifo", "MessageBody": "g1a", "MessageGroupId": "g1", "MessageDeduplicationId": "d1"})
	inv("SendMessage", map[string]any{"QueueName": "q.fifo", "MessageBody": "g1a", "MessageGroupId": "g1", "MessageDeduplicationId": "d1"})
	inv("SendMessage", map[string]any{"QueueName": "q.fifo", "MessageBody": "g1b", "MessageGroupId": "g1", "MessageDeduplicationId": "d2"})
	inv("SendMessage", map[string]any{"QueueName": "q.fifo", "MessageBody": "g2a", "MessageGroupId": "g2", "MessageDeduplicationId": "d3"})
	queued, _, err := p.col(&spi.Request{Identity: id}, "msgs:q.fifo").List(ctx, "", "", 0)
	if err != nil || len(queued) != 3 {
		t.Fatalf("dedup queue size %d, %v", len(queued), err)
	}
	got := inv("ReceiveMessage", map[string]any{"QueueName": "q.fifo", "MaxNumberOfMessages": 10, "VisibilityTimeout": 0})
	msgs, _ := got.Output["Messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("fifo+dedup receive %d %v", len(msgs), got.Output)
	}
	bodies := map[string]bool{}
	for _, m := range msgs {
		bodies[m.(map[string]any)["Body"].(string)] = true
	}
	if !bodies["g1a"] || !bodies["g2a"] || bodies["g1b"] {
		t.Fatalf("fifo order/dedup %v", bodies)
	}

	inv("CreateQueue", map[string]any{"QueueName": "src", "Attributes": map[string]any{
		"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq","maxReceiveCount":"1"}`, "VisibilityTimeout": "0",
	}})
	inv("SendMessage", map[string]any{"QueueName": "src", "MessageBody": "poison"})
	inv("ReceiveMessage", map[string]any{"QueueName": "src", "VisibilityTimeout": 0})
	inv("ReceiveMessage", map[string]any{"QueueName": "src", "VisibilityTimeout": 0})
	dlq := inv("ReceiveMessage", map[string]any{"QueueName": "dlq"})
	dmsgs, _ := dlq.Output["Messages"].([]any)
	if len(dmsgs) != 1 || dmsgs[0].(map[string]any)["Body"] != "poison" {
		t.Fatalf("dlq %v", dlq.Output)
	}

	inv("CreateQueue", map[string]any{"QueueName": "empty"})
	after := make(chan time.Duration, 1)
	p.deps.Clock = &observedClock{Clock: clk, after: after}
	done := make(chan *spi.Response, 1)
	go func() {
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "empty", "WaitTimeSeconds": 1}})
		if err != nil {
			t.Errorf("long poll %v", err)
		}
		done <- resp
	}()
	if delay := <-after; delay != time.Second {
		t.Fatalf("long poll delay %v", delay)
	}
	_ = clk.Advance(time.Second)
	resp := <-done
	msgs, _ = resp.Output["Messages"].([]any)
	if len(msgs) != 0 {
		t.Fatalf("long poll msgs %v", resp.Output)
	}
}

func FuzzListQueuesPagination(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3}, uint8(2))
	f.Add([]byte("queues"), uint8(10))
	f.Fuzz(func(t *testing.T, raw []byte, pageSize uint8) {
		if len(raw) > 64 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		want := 0
		for index, value := range raw {
			prefix := "other-"
			if value%2 == 0 {
				prefix = "wanted-"
				want++
			}
			_, _ = call("CreateQueue", map[string]any{"QueueName": fmt.Sprintf("%s%02x-%d", prefix, value, index)})
		}
		max := int(pageSize%10) + 1
		seen := map[string]bool{}
		next := ""
		for {
			response, err := call("ListQueues", map[string]any{"QueueNamePrefix": "wanted-", "MaxResults": max, "NextToken": next})
			if err != nil {
				t.Fatal(err)
			}
			urls, _ := response.Output["QueueUrls"].([]any)
			for _, rawURL := range urls {
				url := str(rawURL)
				if seen[url] || !strings.Contains(url, "/wanted-") {
					t.Fatalf("invalid queue page %#v", response.Output)
				}
				seen[url] = true
			}
			next = str(response.Output["NextToken"])
			if next == "" {
				break
			}
		}
		if len(seen) != want {
			t.Fatalf("listed %d queues want %d", len(seen), want)
		}
	})
}

func FuzzQueueMetadataAttributeSelection(f *testing.F) {
	f.Add([]byte{0, 1, 2})
	f.Add([]byte{4})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "metadata"}}); err != nil {
			t.Fatal(err)
		}
		choices := []string{"QueueArn", "CreatedTimestamp", "VisibilityTimeout", "missing", "All"}
		names := make([]any, 0, len(raw))
		want := map[string]bool{}
		for _, value := range raw {
			name := choices[int(value)%len(choices)]
			names = append(names, name)
			if name != "missing" && name != "All" {
				want[name] = true
			}
			if name == "All" {
				want = map[string]bool{"ApproximateNumberOfMessages": true, "QueueArn": true, "CreatedTimestamp": true, "VisibilityTimeout": true}
				break
			}
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueName": "metadata", "AttributeNames": names}})
		if err != nil {
			t.Fatal(err)
		}
		attrs := response.Output["Attributes"].(map[string]any)
		if len(attrs) != len(want) {
			t.Fatalf("attributes %#v want %#v", attrs, want)
		}
		for name := range attrs {
			if !want[name] {
				t.Fatalf("unrequested attribute %s in %#v", name, attrs)
			}
		}
	})
}

func FuzzQueueDeletionWindow(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(59))
	f.Add(uint8(60))
	f.Fuzz(func(t *testing.T, raw uint8) {
		seconds := int(raw) % 121
		deps := spitest.Deps(t)
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := call("CreateQueue", map[string]any{"QueueName": "deleted", "Attributes": map[string]any{"DelaySeconds": "5"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := call("DeleteQueue", map[string]any{"QueueName": "deleted"}); err != nil {
			t.Fatal(err)
		}
		if err := deps.Clock.Advance(time.Duration(seconds) * time.Second); err != nil {
			t.Fatal(err)
		}
		_, err := call("CreateQueue", map[string]any{"QueueName": "deleted"})
		if seconds < 60 {
			if fault, ok := err.(*spi.Fault); !ok || fault.Code != "AWS.SimpleQueueService.QueueDeletedRecently" {
				t.Fatalf("%ds recreate fault %#v", seconds, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("%ds recreate %v", seconds, err)
		}
		response, err := call("GetQueueAttributes", map[string]any{"QueueName": "deleted", "AttributeNames": []any{"All"}})
		if err != nil || response.Output["Attributes"].(map[string]any)["DelaySeconds"] != nil {
			t.Fatalf("%ds stale attributes %#v, %v", seconds, response, err)
		}
	})
}

func FuzzSendReceiveMessageDigest(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("message"))
	f.Add([]byte{0, 1, 2, 255})
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1024 || !utf8.Valid(body) {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := call("CreateQueue", map[string]any{"QueueName": "roundtrip"}); err != nil {
			t.Fatal(err)
		}
		sent, err := call("SendMessage", map[string]any{"QueueName": "roundtrip", "MessageBody": string(body)})
		if len(body) == 0 {
			fault, _ := err.(*spi.Fault)
			if fault == nil || fault.Code != "MissingParameter" {
				t.Fatalf("empty body fault %#v", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		received, err := call("ReceiveMessage", map[string]any{"QueueName": "roundtrip", "VisibilityTimeout": 0})
		if err != nil {
			t.Fatal(err)
		}
		message := received.Output["Messages"].([]any)[0].(map[string]any)
		want := fmt.Sprintf("%x", md5.Sum(body))
		if message["Body"] != string(body) || message["MD5OfBody"] != want || sent.Output["MD5OfMessageBody"] != want {
			t.Fatalf("sent=%#v received=%#v want=%s", sent.Output, message, want)
		}
	})
}
