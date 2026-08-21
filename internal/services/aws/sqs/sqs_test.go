package sqs

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

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
	got := inv("ReceiveMessage", map[string]any{"QueueName": "q.fifo", "MaxNumberOfMessages": 11, "VisibilityTimeout": 0})
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
	done := make(chan *spi.Response, 1)
	go func() {
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "empty", "WaitTimeSeconds": 1}})
		if err != nil {
			t.Errorf("long poll %v", err)
		}
		done <- resp
	}()
	for i := 0; i < 10000; i++ {
		select {
		case resp := <-done:
			msgs, _ := resp.Output["Messages"].([]any)
			if len(msgs) != 0 {
				t.Fatalf("long poll msgs %v", resp.Output)
			}
			return
		default:
			runtime.Gosched()
			_ = clk.Advance(time.Second)
		}
	}
	t.Fatal("long poll did not return on clock advance")
}
