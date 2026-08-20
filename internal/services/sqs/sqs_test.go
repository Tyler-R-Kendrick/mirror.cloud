package sqs

import (
	"context"
	"strings"
	"testing"

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
