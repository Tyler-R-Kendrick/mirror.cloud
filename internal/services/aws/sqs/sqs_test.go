package sqs

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net/http/httptest"
	"strconv"
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

func TestInvalidReceiptHandleCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "invalid-receipt"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ChangeMessageVisibility", Input: map[string]any{"QueueName": "invalid-receipt", "ReceiptHandle": "garbage", "VisibilityTimeout": 60}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("invalid receipt handle error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
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

func TestInvalidMessageContentsCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "invalid-contents"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "invalid-contents", "MessageBody": "Invalid-\x00"}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("invalid contents error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestMessageRetentionCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "retention", "Attributes": map[string]any{"MessageRetentionPeriod": "2"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "retention", "MessageBody": "expires"}}); err != nil {
		t.Fatal(err)
	}
	if err := clk.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "retention"}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, response.Output)
}

func TestApproximateMessageStatesCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
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
	call("CreateQueue", map[string]any{"QueueName": "states"})
	call("SendMessage", map[string]any{"QueueName": "states", "MessageBody": "visible"})
	call("SendMessage", map[string]any{"QueueName": "states", "MessageBody": "delayed-1", "DelaySeconds": 2})
	call("SendMessage", map[string]any{"QueueName": "states", "MessageBody": "delayed-2", "DelaySeconds": 2})
	states := map[string]any{}
	states["initial"] = call("GetQueueAttributes", map[string]any{"QueueName": "states", "AttributeNames": []any{"All"}}).Output["Attributes"]
	call("ReceiveMessage", map[string]any{"QueueName": "states", "VisibilityTimeout": 5})
	states["inFlight"] = call("GetQueueAttributes", map[string]any{"QueueName": "states", "AttributeNames": []any{"All"}}).Output["Attributes"]
	if err := clk.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	states["released"] = call("GetQueueAttributes", map[string]any{"QueueName": "states", "AttributeNames": []any{"All"}}).Output["Attributes"]
	golden.AssertJSON(t, states)
}

func TestReceiptHandleRotatesAfterVisibilityTimeout(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
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
	call("CreateQueue", map[string]any{"QueueName": "rotate"})
	call("SendMessage", map[string]any{"QueueName": "rotate", "MessageBody": "message"})
	first := call("ReceiveMessage", map[string]any{"QueueName": "rotate", "VisibilityTimeout": 1}).Output["Messages"].([]any)[0].(map[string]any)
	if err := clk.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	second := call("ReceiveMessage", map[string]any{"QueueName": "rotate", "VisibilityTimeout": 0}).Output["Messages"].([]any)[0].(map[string]any)
	firstHandle, _ := first["ReceiptHandle"].(string)
	secondHandle, _ := second["ReceiptHandle"].(string)
	if firstHandle == "" || secondHandle == "" || firstHandle == secondHandle {
		t.Fatalf("receipt handles did not rotate: %q %q", firstHandle, secondHandle)
	}
	golden.AssertJSON(t, map[string]any{"firstHandleLength": len(firstHandle), "secondHandleLength": len(secondHandle), "rotated": firstHandle != secondHandle})
}

func TestPriorReceiptHandleRemainsUsable(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
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
	call("CreateQueue", map[string]any{"QueueName": "prior-handle"})
	call("SendMessage", map[string]any{"QueueName": "prior-handle", "MessageBody": "message"})
	first := call("ReceiveMessage", map[string]any{"QueueName": "prior-handle", "VisibilityTimeout": 1}).Output["Messages"].([]any)[0].(map[string]any)
	if err := clk.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	call("ReceiveMessage", map[string]any{"QueueName": "prior-handle", "VisibilityTimeout": 5})
	oldHandle := first["ReceiptHandle"].(string)
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ChangeMessageVisibility", Input: map[string]any{"QueueName": "prior-handle", "ReceiptHandle": oldHandle, "VisibilityTimeout": 0}}); err != nil {
		t.Fatal(err)
	}
	third := call("ReceiveMessage", map[string]any{"QueueName": "prior-handle", "VisibilityTimeout": 5}).Output["Messages"].([]any)
	if len(third) != 1 {
		t.Fatalf("old receipt handle did not restore visibility: %#v", third)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessage", Input: map[string]any{"QueueName": "prior-handle", "ReceiptHandle": oldHandle}}); err != nil {
		t.Fatal(err)
	}
	left := call("ReceiveMessage", map[string]any{"QueueName": "prior-handle", "VisibilityTimeout": 0}).Output["Messages"]
	golden.AssertJSON(t, map[string]any{"oldHandleLength": len(oldHandle), "deletedWithOldHandle": left == nil})
}

func TestFIFODeleteAfterVisibilityTimeoutCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
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
	call("CreateQueue", map[string]any{"QueueName": "expired.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": "1"}})
	call("SendMessage", map[string]any{"QueueName": "expired.fifo", "MessageBody": "message", "MessageGroupId": "group"})
	received := call("ReceiveMessage", map[string]any{"QueueName": "expired.fifo"}).Output["Messages"].([]any)[0].(map[string]any)
	if err := clk.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessage", Input: map[string]any{"QueueName": "expired.fifo", "ReceiptHandle": received["ReceiptHandle"]}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("expired FIFO receipt error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestFIFOEmptyMessageGroupReuseCharacterization(t *testing.T) {
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
	call("CreateQueue", map[string]any{"QueueName": "reuse-group.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
	call("SendMessage", map[string]any{"QueueName": "reuse-group.fifo", "MessageBody": "first", "MessageGroupId": "g1"})
	first := call("ReceiveMessage", map[string]any{"QueueName": "reuse-group.fifo"}).Output["Messages"].([]any)[0].(map[string]any)
	call("DeleteMessage", map[string]any{"QueueName": "reuse-group.fifo", "ReceiptHandle": first["ReceiptHandle"]})
	empty := call("ReceiveMessage", map[string]any{"QueueName": "reuse-group.fifo"}).Output
	call("SendMessage", map[string]any{"QueueName": "reuse-group.fifo", "MessageBody": "second", "MessageGroupId": "g1"})
	final := call("ReceiveMessage", map[string]any{"QueueName": "reuse-group.fifo"}).Output
	golden.AssertJSON(t, map[string]any{"empty": empty, "finalBody": final["Messages"].([]any)[0].(map[string]any)["Body"]})
}

func TestFIFOMessageGroupVisibilityAfterTerminateCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
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
	call("CreateQueue", map[string]any{"QueueName": "partial-group.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": "30"}})
	call("SendMessage", map[string]any{"QueueName": "partial-group.fifo", "MessageBody": "g1-m1", "MessageGroupId": "g1"})
	call("SendMessage", map[string]any{"QueueName": "partial-group.fifo", "MessageBody": "g1-m2", "MessageGroupId": "g1"})
	call("SendMessage", map[string]any{"QueueName": "partial-group.fifo", "MessageBody": "g2-m1", "MessageGroupId": "g2"})
	first := call("ReceiveMessage", map[string]any{"QueueName": "partial-group.fifo", "MaxNumberOfMessages": 2})
	firstMessages := first["Messages"].([]any)
	if len(firstMessages) != 2 {
		t.Fatalf("first receive %#v", first)
	}
	call("ChangeMessageVisibility", map[string]any{"QueueName": "partial-group.fifo", "ReceiptHandle": asMap(firstMessages[0])["ReceiptHandle"], "VisibilityTimeout": 0})
	second := call("ReceiveMessage", map[string]any{"QueueName": "partial-group.fifo", "MaxNumberOfMessages": 3})
	secondMessages := second["Messages"].([]any)
	if len(secondMessages) != 2 {
		t.Fatalf("second receive %#v", second)
	}
	golden.AssertJSON(t, map[string]any{
		"first":  []any{asMap(firstMessages[0])["Body"], asMap(firstMessages[1])["Body"]},
		"second": []any{asMap(secondMessages[0])["Body"], asMap(secondMessages[1])["Body"]},
	})
}

func TestFIFOMessageGroupVisibilityAfterDeleteCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	create := func(name string) {
		t.Helper()
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{
			"QueueName": name, "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	send := func(name, body, group string) {
		t.Helper()
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{
			"QueueName": name, "MessageBody": body, "MessageGroupId": group,
		}}); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(name string, max int) []any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": name, "MaxNumberOfMessages": max, "VisibilityTimeout": 30}})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output["Messages"].([]any)
	}
	delete := func(name string, messages []any) {
		t.Helper()
		for _, raw := range messages {
			message := raw.(map[string]any)
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessage", Input: map[string]any{"QueueName": name, "ReceiptHandle": message["ReceiptHandle"]}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	bodies := func(messages []any) []string {
		out := make([]string, 0, len(messages))
		for _, raw := range messages {
			out = append(out, raw.(map[string]any)["Body"].(string))
		}
		return out
	}
	populate := func(name string) {
		for _, message := range []struct{ body, group string }{{"g1-m1", "g1"}, {"g2-m1", "g2"}, {"g1-m2", "g1"}, {"g2-m2", "g2"}, {"g1-m3", "g1"}, {"g1-m4", "g1"}, {"g3-m1", "g3"}} {
			send(name, message.body, message.group)
		}
	}
	full := "fifo-delete-order.fifo"
	create(full)
	populate(full)
	first := receive(full, 2)
	delete(full, first)
	fullRemaining := bodies(receive(full, 10))

	partial := "fifo-partial-delete-order.fifo"
	create(partial)
	populate(partial)
	partialFirst := receive(partial, 2)
	delete(partial, partialFirst[:1])
	partialRemaining := bodies(receive(partial, 10))
	golden.AssertJSON(t, map[string]any{"fullDelete": fullRemaining, "partialDelete": partialRemaining})
}

func FuzzFIFOMessageGroupDeleteVisibility(f *testing.F) {
	f.Add(false)
	f.Add(true)
	f.Fuzz(func(t *testing.T, partial bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		name := "fuzz-delete-order.fifo"
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": name, "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}}); err != nil {
			t.Fatal(err)
		}
		for _, message := range []struct{ body, group string }{{"g1-m1", "g1"}, {"g2-m1", "g2"}, {"g1-m2", "g1"}, {"g2-m2", "g2"}, {"g3-m1", "g3"}} {
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": name, "MessageBody": message.body, "MessageGroupId": message.group}}); err != nil {
				t.Fatal(err)
			}
		}
		first, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": name, "MaxNumberOfMessages": 2}})
		if err != nil || len(first.Output["Messages"].([]any)) != 2 {
			t.Fatalf("first %#v error %v", first, err)
		}
		messages := first.Output["Messages"].([]any)
		limit := 2
		if partial {
			limit = 1
		}
		for _, raw := range messages[:limit] {
			message := raw.(map[string]any)
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessage", Input: map[string]any{"QueueName": name, "ReceiptHandle": message["ReceiptHandle"]}}); err != nil {
				t.Fatal(err)
			}
		}
		remaining, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": name, "MaxNumberOfMessages": 10}})
		if err != nil {
			t.Fatal(err)
		}
		for _, raw := range remaining.Output["Messages"].([]any) {
			body := raw.(map[string]any)["Body"]
			if body == "g1-m1" || body == "g1-m2" {
				t.Fatalf("deleted or blocked FIFO message resurfaced partial=%v output=%#v", partial, remaining.Output)
			}
		}
	})
}

func TestSuccessivePurgeCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "purge"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PurgeQueue", Input: map[string]any{"QueueName": "purge"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PurgeQueue", Input: map[string]any{"QueueName": "purge"}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("purge error %#v", err)
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

func TestReceiveEmptyQueueOmitsMessages(t *testing.T) {
	deps := spitest.Deps(t)
	deps.Clock = clock.Real{}
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "empty"}}); err != nil {
		t.Fatal(err)
	}
	for _, input := range []map[string]any{
		{"QueueName": "empty", "MaxNumberOfMessages": 1},
		{"QueueName": "empty", "MaxNumberOfMessages": 1, "WaitTimeSeconds": 1},
	} {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: input})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := response.Output["Messages"]; ok {
			t.Fatalf("empty receive %#v", response.Output)
		}
	}
}

func TestReceiveEmptyQueueCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	deps.Clock = clock.Real{}
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "empty"}}); err != nil {
		t.Fatal(err)
	}
	short, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "empty", "MaxNumberOfMessages": 1}})
	if err != nil {
		t.Fatal(err)
	}
	long, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "empty", "MaxNumberOfMessages": 1, "WaitTimeSeconds": 1}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{"short": short.Output, "long": long.Output})
}

func TestReceiveMessageWaitTimeValidation(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateQueue", map[string]any{"QueueName": "wait-time"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := call("SendMessage", map[string]any{"QueueName": "wait-time", "MessageBody": "message"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []int{-1, 21} {
		_, err := call("ReceiveMessage", map[string]any{"QueueName": "wait-time", "WaitTimeSeconds": value})
		fault, ok := err.(*spi.Fault)
		want := fmt.Sprintf("Value %d for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided.", value)
		if !ok || fault.Code != "InvalidParameterValue" || fault.Message != want || fault.HTTPStatus != 400 {
			t.Fatalf("wait=%d fault %#v", value, err)
		}
	}
	for _, input := range []map[string]any{
		{"QueueName": "wait-time"},
		{"QueueName": "wait-time", "WaitTimeSeconds": 0},
	} {
		response, err := call("ReceiveMessage", input)
		if err != nil {
			t.Fatal(err)
		}
		messages, _ := response.Output["Messages"].([]any)
		if len(messages) != 1 || messages[0].(map[string]any)["Body"] != "message" {
			t.Fatalf("short poll %#v", response)
		}
	}
}

func TestReceiveMessageWaitTimeCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateQueue", map[string]any{"QueueName": "wait-time"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := call("SendMessage", map[string]any{"QueueName": "wait-time", "MessageBody": "message"}); err != nil {
			t.Fatal(err)
		}
	}
	result := map[string]any{}
	for _, value := range []int{-1, 21} {
		_, err := call("ReceiveMessage", map[string]any{"QueueName": "wait-time", "WaitTimeSeconds": value})
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("wait=%d fault %#v", value, err)
		}
		result[fmt.Sprintf("wait%d", value)] = map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault}
	}
	for _, tc := range []struct {
		name  string
		input map[string]any
	}{
		{"default", map[string]any{"QueueName": "wait-time"}},
		{"explicit", map[string]any{"QueueName": "wait-time", "WaitTimeSeconds": 0}},
	} {
		response, err := call("ReceiveMessage", tc.input)
		if err != nil {
			t.Fatal(err)
		}
		result[tc.name] = response.Output
	}
	golden.AssertJSON(t, result)
}

func TestQueueReceiveWaitTimeCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	after := make(chan time.Duration, 1)
	deps.Clock = &observedClock{Clock: clk, after: after}
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "queue-wait", "Attributes": map[string]any{"ReceiveMessageWaitTimeSeconds": "2"}}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan *spi.Response, 1)
	go func() {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "queue-wait"}})
		if err != nil {
			t.Errorf("queue wait: %v", err)
		}
		done <- response
	}()
	if delay := <-after; delay != 2*time.Second {
		t.Fatalf("queue wait delay %v", delay)
	}
	if err := clk.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, (<-done).Output)
}

func FuzzQueueReceiveWaitTime(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Fuzz(func(t *testing.T, raw uint8) {
		queueWait := int(raw % 2)
		clk := clock.NewControllable()
		deps := spitest.Deps(t)
		after := make(chan time.Duration, 1)
		deps.Clock = &observedClock{Clock: clk, after: after}
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-queue-wait", "Attributes": map[string]any{"ReceiveMessageWaitTimeSeconds": fmt.Sprintf("%d", queueWait)}}}); err != nil {
			t.Fatal(err)
		}
		if queueWait == 0 {
			response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-queue-wait"}})
			if err != nil || response.Output["Messages"] != nil {
				t.Fatalf("zero queue wait %#v error %v", response, err)
			}
			return
		}
		done := make(chan *spi.Response, 1)
		go func() {
			response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-queue-wait"}})
			if err != nil {
				t.Errorf("queue wait: %v", err)
			}
			done <- response
		}()
		if delay := <-after; delay != time.Second {
			t.Fatalf("queue wait delay %v", delay)
		}
		if err := clk.Advance(time.Second); err != nil {
			t.Fatal(err)
		}
		if response := <-done; response.Output["Messages"] != nil {
			t.Fatalf("queue wait response %#v", response.Output)
		}
	})
}

func FuzzInvalidReceiptHandle(f *testing.F) {
	f.Add("garbage")
	f.Add("")
	f.Add(strings.Repeat("a", 64))
	f.Fuzz(func(t *testing.T, handle string) {
		if len(handle) > 128 || !utf8.ValidString(handle) {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-invalid-receipt"}}); err != nil {
			t.Fatal(err)
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ChangeMessageVisibility", Input: map[string]any{"QueueName": "fuzz-invalid-receipt", "ReceiptHandle": handle, "VisibilityTimeout": 60}})
		fault, ok := err.(*spi.Fault)
		if validReceiptHandle(handle) {
			if !ok || fault.Code != "InvalidParameterValue" {
				t.Fatalf("valid-shaped missing handle %q error %#v", handle, err)
			}
		} else if !ok || fault.Code != "ReceiptHandleIsInvalid" {
			t.Fatalf("invalid handle %q error %#v", handle, err)
		}
	})
}

func TestReceiveMessageTimestampAttributes(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateQueue", map[string]any{"QueueName": "timestamps"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("SendMessage", map[string]any{"QueueName": "timestamps", "MessageBody": "message"}); err != nil {
		t.Fatal(err)
	}
	if err := deps.Clock.Advance(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	response, err := call("ReceiveMessage", map[string]any{"QueueName": "timestamps", "MessageSystemAttributeNames": []any{"All"}})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := response.Output["Messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("receive %#v", response.Output)
	}
	attributes, _ := messages[0].(map[string]any)["Attributes"].(map[string]any)
	if attributes["SentTimestamp"] != "0" || attributes["ApproximateFirstReceiveTimestamp"] != "1" || attributes["ApproximateReceiveCount"] != "1" {
		t.Fatalf("timestamp attributes %#v", attributes)
	}
}

func TestReceiveMessageTimestampsCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateQueue", map[string]any{"QueueName": "timestamps"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("SendMessage", map[string]any{"QueueName": "timestamps", "MessageBody": "message"}); err != nil {
		t.Fatal(err)
	}
	if err := deps.Clock.Advance(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	response, err := call("ReceiveMessage", map[string]any{"QueueName": "timestamps", "AttributeNames": []any{"All"}})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := response.Output["Messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("receive %#v", response.Output)
	}
	golden.AssertJSON(t, messages[0].(map[string]any)["Attributes"])
}

func TestFIFOMessageAttributesCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "fifo-attrs.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": "0"}})
	call("SendMessage", map[string]any{"QueueName": "fifo-attrs.fifo", "MessageBody": "message-body-1", "MessageGroupId": "group-1", "MessageDeduplicationId": "dedup-1", "MessageAttributes": map[string]any{
		"kind": map[string]any{"DataType": "String", "StringValue": "fifo"},
	}})
	first := call("ReceiveMessage", map[string]any{"QueueName": "fifo-attrs.fifo", "AttributeNames": []any{"All"}, "MessageAttributeNames": []any{"All"}, "WaitTimeSeconds": 0})
	second := call("ReceiveMessage", map[string]any{"QueueName": "fifo-attrs.fifo", "AttributeNames": []any{"All"}, "MessageAttributeNames": []any{"All"}, "WaitTimeSeconds": 0})
	golden.AssertJSON(t, map[string]any{"first": first, "second": second})
}

func TestMessageAttributeDigestCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "attribute-digest"}}); err != nil {
		t.Fatal(err)
	}
	attrs := map[string]any{
		"binary": map[string]any{"DataType": "Binary", "BinaryValue": base64.StdEncoding.EncodeToString([]byte{0, 1, 2})},
		"string": map[string]any{"DataType": "String", "StringValue": "value"},
	}
	sent, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "attribute-digest", "MessageBody": "message", "MessageAttributes": attrs}})
	if err != nil {
		t.Fatal(err)
	}
	digest, ok := sent.Output["MD5OfMessageAttributes"].(string)
	if !ok || len(digest) != 32 {
		t.Fatalf("send digest %#v", sent.Output)
	}
	received, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "attribute-digest", "MessageAttributeNames": []any{"All"}}})
	if err != nil {
		t.Fatal(err)
	}
	message := received.Output["Messages"].([]any)[0].(map[string]any)
	if message["MD5OfMessageAttributes"] != digest || len(asMap(message["MessageAttributes"])) != 2 {
		t.Fatalf("receive attributes %#v", message)
	}
	golden.AssertJSON(t, map[string]any{"digest": digest, "attributes": message["MessageAttributes"]})
}

func TestMessageAttributeValidationCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "attribute-validation"}}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		attrs map[string]any
	}{
		{"empty-string", map[string]any{"ErrorDetails": map[string]any{"StringValue": "", "DataType": "String"}}},
		{"control-value", map[string]any{"attr1": map[string]any{"StringValue": "Invalid-\b", "DataType": "String"}}},
		{"reserved-prefix", map[string]any{"aWs.Invalid": map[string]any{"StringValue": "Valid", "DataType": "String"}}},
		{"illegal-name", map[string]any{"Invalid!attr": map[string]any{"StringValue": "Valid", "DataType": "String"}}},
		{"invalid-type", map[string]any{"Attribute_name": map[string]any{"StringValue": "Valid", "DataType": "Invalid"}}},
		{"empty-custom-type", map[string]any{"Attribute_name": map[string]any{"StringValue": "Valid", "DataType": "Number."}}},
	}
	results := map[string]any{}
	for _, tc := range cases {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "attribute-validation", "MessageBody": "test", "MessageAttributes": tc.attrs}})
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("%s error %#v", tc.name, err)
		}
		results[tc.name] = map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault}
	}
	valid, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "attribute-validation", "MessageBody": "test", "MessageAttributes": map[string]any{"attr.1øßä": map[string]any{"StringValue": "Valid", "DataType": "String"}}}})
	if err != nil {
		t.Fatal("valid unicode attribute", err)
	}
	results["valid"] = valid.Output
	golden.AssertJSON(t, results)
}

func TestMessageAttributeNameFiltersCharacterization(t *testing.T) {
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
	call("CreateQueue", map[string]any{"QueueName": "attribute-filters", "Attributes": map[string]any{"VisibilityTimeout": "0"}})
	call("SendMessage", map[string]any{"QueueName": "attribute-filters", "MessageBody": "message", "MessageAttributes": map[string]any{
		"Help.Me": map[string]any{"DataType": "String", "StringValue": "Me"},
		"Hello":   map[string]any{"DataType": "String", "StringValue": "There"},
		"General": map[string]any{"DataType": "String", "StringValue": "Kenobi"},
	}})
	outputs := map[string]any{}
	for _, tc := range []struct {
		name   string
		filter []any
	}{
		{"empty", []any{}},
		{"exact", []any{"Hello"}},
		{"prefix", []any{"Hel.*"}},
		{"all", []any{"*"}},
	} {
		response := call("ReceiveMessage", map[string]any{"QueueName": "attribute-filters", "MessageAttributeNames": tc.filter})
		outputs[tc.name] = response.Output
	}
	golden.AssertJSON(t, outputs)
}

func FuzzFIFOMessageAttributes(f *testing.F) {
	f.Add("fifo")
	f.Add("")
	f.Fuzz(func(t *testing.T, value string) {
		if value == "" || len(value) > 1024 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-fifo-attrs.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": "0"}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-fifo-attrs.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "MessageDeduplicationId": "dedup-1", "MessageAttributes": map[string]any{"kind": map[string]any{"DataType": "String", "StringValue": value}}}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-fifo-attrs.fifo", "MessageAttributeNames": []any{"All"}}})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Output["Messages"].([]any)) != 1 {
			t.Fatalf("messages %#v", response.Output)
		}
		message := asMap(response.Output["Messages"].([]any)[0])
		if str(asMap(asMap(message["MessageAttributes"])["kind"])["StringValue"]) != value {
			t.Fatalf("attributes %#v", message["MessageAttributes"])
		}
	})
}

func TestFIFOApproximateMessageCountCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "fifo-count.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
	for _, message := range []string{"g1-m1", "g1-m2", "g1-m3", "g2-m1", "g3-m1"} {
		call("SendMessage", map[string]any{"QueueName": "fifo-count.fifo", "MessageBody": message, "MessageGroupId": strings.Split(message, "-")[0]})
	}
	before := call("GetQueueAttributes", map[string]any{"QueueName": "fifo-count.fifo", "AttributeNames": []any{"ApproximateNumberOfMessages"}})
	received := call("ReceiveMessage", map[string]any{"QueueName": "fifo-count.fifo", "MaxNumberOfMessages": 4, "WaitTimeSeconds": 0})
	after := call("GetQueueAttributes", map[string]any{"QueueName": "fifo-count.fifo", "AttributeNames": []any{"ApproximateNumberOfMessages"}})
	golden.AssertJSON(t, map[string]any{"before": before, "received": received, "after": after})
}

func TestFIFOContentBasedDeduplicationStrategyCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "dedup-strategy.fifo", "Attributes": map[string]any{"FifoQueue": "true", "SqsManagedSseEnabled": "true", "ContentBasedDeduplication": "true"}})
	before := call("GetQueueAttributes", map[string]any{"QueueName": "dedup-strategy.fifo", "AttributeNames": []any{"All"}})
	call("SetQueueAttributes", map[string]any{"QueueName": "dedup-strategy.fifo", "Attributes": map[string]any{"ContentBasedDeduplication": "false"}})
	after := call("GetQueueAttributes", map[string]any{"QueueName": "dedup-strategy.fifo", "AttributeNames": []any{"All"}})
	golden.AssertJSON(t, map[string]any{"before": before, "after": after})
}

func TestRedrivePolicyClearingCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "redrive-policy"}}); err != nil {
		t.Fatal(err)
	}
	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dlq","maxReceiveCount":"42"}`
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": "redrive-policy", "Attributes": map[string]any{"RedrivePolicy": policy, "Policy": policy}}}); err != nil {
		t.Fatal(err)
	}
	set, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueName": "redrive-policy", "AttributeNames": []any{"All"}}})
	if err != nil {
		t.Fatal(err)
	}
	if asMap(set.Output["Attributes"])["RedrivePolicy"] != policy || asMap(set.Output["Attributes"])["Policy"] != policy {
		t.Fatalf("set policy %#v", set.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": "redrive-policy", "Attributes": map[string]any{"RedrivePolicy": "", "Policy": ""}}}); err != nil {
		t.Fatal(err)
	}
	cleared, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueName": "redrive-policy", "AttributeNames": []any{"All"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, redrivePresent := asMap(cleared.Output["Attributes"])["RedrivePolicy"]
	_, policyPresent := asMap(cleared.Output["Attributes"])["Policy"]
	golden.AssertJSON(t, map[string]any{"setRedrive": asMap(set.Output["Attributes"])["RedrivePolicy"], "setPolicy": asMap(set.Output["Attributes"])["Policy"], "clearedRedrive": redrivePresent, "clearedPolicy": policyPresent})
}

func TestListDeadLetterSourceQueuesCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	for _, name := range []string{"dead-letter", "source-a", "source-b"} {
		attrs := map[string]any{}
		if name != "dead-letter" {
			attrs["RedrivePolicy"] = `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dead-letter","maxReceiveCount":"42"}`
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": name, "Attributes": attrs}}); err != nil {
			t.Fatal(err)
		}
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListDeadLetterSourceQueues", Input: map[string]any{"QueueName": "dead-letter"}})
	if err != nil {
		t.Fatal(err)
	}
	urls := response.Output["QueueUrls"].([]any)
	if len(urls) != 2 || !strings.Contains(str(urls[0]), "source-") || !strings.Contains(str(urls[1]), "source-") {
		t.Fatalf("dead-letter sources %#v", response.Output)
	}
	golden.AssertJSON(t, map[string]any{"count": len(urls), "containsSourceA": strings.Contains(str(urls[0]), "source-a") || strings.Contains(str(urls[1]), "source-a"), "containsSourceB": strings.Contains(str(urls[0]), "source-b") || strings.Contains(str(urls[1]), "source-b")})
}

func TestSetFifoAttributeValidationCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	for _, name := range []string{"standard-attribute", "fifo-attribute.fifo"} {
		attrs := map[string]any{}
		if strings.HasSuffix(name, ".fifo") {
			attrs["FifoQueue"] = "true"
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": name, "Attributes": attrs}}); err != nil {
			t.Fatal(err)
		}
	}
	faultFor := func(name, value string) map[string]any {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": name, "Attributes": map[string]any{"FifoQueue": value}}})
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("fifo attribute %s=%s error %#v", name, value, err)
		}
		return map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": "fifo-attribute.fifo", "Attributes": map[string]any{"FifoQueue": "true"}}}); err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{"standardTrue": faultFor("standard-attribute", "true"), "standardFalse": faultFor("standard-attribute", "false"), "fifoFalse": faultFor("fifo-attribute.fifo", "false")})
}

func FuzzFIFOContentBasedDeduplicationStrategy(f *testing.F) {
	f.Add(true)
	f.Add(false)
	f.Fuzz(func(t *testing.T, enabled bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-dedup-strategy.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}}); err != nil {
			t.Fatal(err)
		}
		value := strconv.FormatBool(enabled)
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": "fuzz-dedup-strategy.fifo", "Attributes": map[string]any{"ContentBasedDeduplication": value}}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueName": "fuzz-dedup-strategy.fifo", "AttributeNames": []any{"ContentBasedDeduplication"}}})
		if err != nil || asMap(response.Output["Attributes"])["ContentBasedDeduplication"] != value {
			t.Fatalf("enabled=%v response=%#v error=%v", enabled, response.Output, err)
		}
	})
}

func FuzzRedrivePolicyClearing(f *testing.F) {
	f.Add(true)
	f.Add(false)
	f.Fuzz(func(t *testing.T, clear bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-redrive-policy"}}); err != nil {
			t.Fatal(err)
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dlq","maxReceiveCount":"42"}`
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": "fuzz-redrive-policy", "Attributes": map[string]any{"RedrivePolicy": policy, "Policy": policy}}}); err != nil {
			t.Fatal(err)
		}
		value := policy
		if clear {
			value = ""
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": "fuzz-redrive-policy", "Attributes": map[string]any{"RedrivePolicy": value, "Policy": value}}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueName": "fuzz-redrive-policy", "AttributeNames": []any{"All"}}})
		if err != nil {
			t.Fatal(err)
		}
		_, redrivePresent := asMap(response.Output["Attributes"])["RedrivePolicy"]
		_, policyPresent := asMap(response.Output["Attributes"])["Policy"]
		if redrivePresent == clear || policyPresent == clear {
			t.Fatalf("clear=%v response=%#v", clear, response.Output)
		}
	})
}

func FuzzListDeadLetterSourceQueues(f *testing.F) {
	f.Add(1)
	f.Add(2)
	f.Fuzz(func(t *testing.T, sourceCount int) {
		if sourceCount < 1 || sourceCount > 2 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-dead-letter"}}); err != nil {
			t.Fatal(err)
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:fuzz-dead-letter","maxReceiveCount":"42"}`
		for i := range sourceCount {
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": fmt.Sprintf("fuzz-source-%d", i), "Attributes": map[string]any{"RedrivePolicy": policy}}}); err != nil {
				t.Fatal(err)
			}
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListDeadLetterSourceQueues", Input: map[string]any{"QueueName": "fuzz-dead-letter"}})
		if err != nil || len(response.Output["QueueUrls"].([]any)) != sourceCount {
			t.Fatalf("sourceCount=%d response=%#v error=%v", sourceCount, response.Output, err)
		}
	})
}

func FuzzSetFifoAttributeValidation(f *testing.F) {
	f.Add(false, false)
	f.Add(false, true)
	f.Add(true, false)
	f.Add(true, true)
	f.Fuzz(func(t *testing.T, fifo, enabled bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		name := "fuzz-fifo-attribute"
		attrs := map[string]any{}
		if fifo {
			name += ".fifo"
			attrs["FifoQueue"] = "true"
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": name, "Attributes": attrs}}); err != nil {
			t.Fatal(err)
		}
		value := "false"
		if enabled {
			value = "true"
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": name, "Attributes": map[string]any{"FifoQueue": value}}})
		if !fifo || !enabled {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "InvalidAttributeName" {
				t.Fatalf("fifo=%v enabled=%v error %#v", fifo, enabled, err)
			}
		} else if err != nil {
			t.Fatalf("fifo=%v enabled=%v error %v", fifo, enabled, err)
		}
	})
}

func FuzzMessageAttributeDigest(f *testing.F) {
	f.Add("value")
	f.Add("")
	f.Fuzz(func(t *testing.T, value string) {
		if value == "" || len(value) > 256 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-attribute-digest"}}); err != nil {
			t.Fatal(err)
		}
		attrs := map[string]any{"kind": map[string]any{"DataType": "String", "StringValue": value}}
		sent, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-attribute-digest", "MessageBody": "message", "MessageAttributes": attrs}})
		if err != nil || str(sent.Output["MD5OfMessageAttributes"]) == "" {
			t.Fatalf("send %#v error %v", sent.Output, err)
		}
		received, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-attribute-digest", "MessageAttributeNames": []any{"All"}}})
		if err != nil || str(received.Output["Messages"].([]any)[0].(map[string]any)["MD5OfMessageAttributes"]) != str(sent.Output["MD5OfMessageAttributes"]) {
			t.Fatalf("receive %#v error %v", received.Output, err)
		}
	})
}

func FuzzMessageSystemAttributeDigest(f *testing.F) {
	f.Add("trace")
	f.Add("Root=1-5759e988")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 256 || !validMessageContents(value) {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-system-attribute-digest"}}); err != nil {
			t.Fatal(err)
		}
		without, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-system-attribute-digest", "MessageBody": "message", "MessageAttributes": map[string]any{"kind": map[string]any{"DataType": "String", "StringValue": "value"}}}})
		if err != nil {
			t.Fatal(err)
		}
		with, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-system-attribute-digest", "MessageBody": "message", "MessageAttributes": map[string]any{"kind": map[string]any{"DataType": "String", "StringValue": "value"}}, "MessageSystemAttributes": map[string]any{"AWSTraceHeader": map[string]any{"DataType": "String", "StringValue": value}}}})
		if err != nil || with.Output["MD5OfMessageSystemAttributes"] == nil || with.Output["MD5OfMessageAttributes"] != without.Output["MD5OfMessageAttributes"] {
			t.Fatalf("system digest without=%#v with=%#v error=%v", without.Output, with.Output, err)
		}
	})
}

func FuzzTraceHeaderPropagation(f *testing.F) {
	f.Add("Root=1-5759e988")
	f.Add("trace-parent")
	f.Fuzz(func(t *testing.T, trace string) {
		if len(trace) > 256 || trace == "" || !validMessageContents(trace) {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-trace-header"}}); err != nil {
			t.Fatal(err)
		}
		httpRequest := httptest.NewRequest("POST", "http://queue", nil)
		httpRequest.Header.Set("X-Amzn-Trace-Id", trace)
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, HTTP: httpRequest, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-trace-header", "MessageBody": "message"}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-trace-header", "AttributeNames": []any{"AWSTraceHeader"}}})
		if err != nil || len(response.Output["Messages"].([]any)) != 1 || asMap(response.Output["Messages"].([]any)[0].(map[string]any)["Attributes"])["AWSTraceHeader"] != trace {
			t.Fatalf("trace %#v error %v", response.Output, err)
		}
	})
}

func FuzzFIFODeduplicationScope(f *testing.F) {
	f.Add(uint8(2))
	f.Add(uint8(5))
	f.Fuzz(func(t *testing.T, raw uint8) {
		count := int(raw%5) + 2
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-dedup-scope.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false", "DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}}}); err != nil {
			t.Fatal(err)
		}
		for index := 0; index < count; index++ {
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-dedup-scope.fifo", "MessageBody": fmt.Sprintf("message-%d", index), "MessageGroupId": fmt.Sprintf("group-%d", index), "MessageDeduplicationId": "same-dedup"}}); err != nil {
				t.Fatal(err)
			}
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-dedup-scope.fifo", "MaxNumberOfMessages": 10}})
		if err != nil {
			t.Fatal(err)
		}
		if messages, _ := response.Output["Messages"].([]any); len(messages) != count {
			t.Fatalf("dedup scope response %#v", response.Output)
		}
	})
}

func FuzzMessageAttributeValidation(f *testing.F) {
	for _, seed := range []string{"", "aWs.Invalid", "Invalid!attr", "attr.1øßä"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if len([]rune(name)) > 256 {
			return
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-attribute-validation"}}); err != nil {
			t.Fatal(err)
		}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-attribute-validation", "MessageBody": "test", "MessageAttributes": map[string]any{name: map[string]any{"StringValue": "value", "DataType": "String"}}}})
	})
}

func FuzzMessageAttributeNameFilters(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Add(uint8(2))
	f.Add(uint8(3))
	f.Fuzz(func(t *testing.T, raw uint8) {
		filters := [][]any{{}, {"Hello"}, {"Hel.*"}, {"*"}}
		filter := filters[int(raw)%len(filters)]
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := call("CreateQueue", map[string]any{"QueueName": "fuzz-attribute-filters", "Attributes": map[string]any{"VisibilityTimeout": "0"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := call("SendMessage", map[string]any{"QueueName": "fuzz-attribute-filters", "MessageBody": "message", "MessageAttributes": map[string]any{"Help.Me": map[string]any{"DataType": "String", "StringValue": "Me"}, "Hello": map[string]any{"DataType": "String", "StringValue": "There"}, "General": map[string]any{"DataType": "String", "StringValue": "Kenobi"}}}); err != nil {
			t.Fatal(err)
		}
		response, err := call("ReceiveMessage", map[string]any{"QueueName": "fuzz-attribute-filters", "MessageAttributeNames": filter})
		if err != nil {
			t.Fatal(err)
		}
		messages := response.Output["Messages"].([]any)
		attrs := messages[0].(map[string]any)["MessageAttributes"].(map[string]any)
		want := []int{0, 1, 2, 3}[int(raw)%len(filters)]
		if len(attrs) != want {
			t.Fatalf("filter %#v attrs %#v", filter, attrs)
		}
	})
}

func FuzzInvalidMessageContents(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Fuzz(func(t *testing.T, raw uint8) {
		body := "valid"
		if raw%2 == 1 {
			body += "\x00"
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-invalid-contents"}}); err != nil {
			t.Fatal(err)
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-invalid-contents", "MessageBody": body}})
		if raw%2 == 1 {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "InvalidMessageContents" {
				t.Fatalf("invalid body error %#v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzMessageRetention(f *testing.F) {
	f.Add(uint8(1))
	f.Add(uint8(2))
	f.Fuzz(func(t *testing.T, raw uint8) {
		retention := int(raw%3) + 1
		clk := clock.NewControllable()
		deps := spitest.Deps(t)
		deps.Clock = clk
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-retention", "Attributes": map[string]any{"MessageRetentionPeriod": fmt.Sprintf("%d", retention)}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-retention", "MessageBody": "expires"}}); err != nil {
			t.Fatal(err)
		}
		if err := clk.Advance(time.Duration(retention) * time.Second); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-retention", "WaitTimeSeconds": 0}})
		if err != nil || response.Output["Messages"] != nil {
			t.Fatalf("retention=%d response %#v error %v", retention, response, err)
		}
	})
}

func FuzzSuccessivePurge(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Fuzz(func(t *testing.T, raw uint8) {
		clk := clock.NewControllable()
		deps := spitest.Deps(t)
		deps.Clock = clk
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-purge"}}); err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PurgeQueue", Input: map[string]any{"QueueName": "fuzz-purge"}})
			if attempt == 0 && err != nil {
				t.Fatal(err)
			}
			if attempt == 1 {
				fault, ok := err.(*spi.Fault)
				if !ok || fault.Code != "AWS.SimpleQueueService.PurgeQueueInProgress" {
					t.Fatalf("raw=%d purge error %#v", raw, err)
				}
			}
		}
	})
}

func FuzzApproximateMessageStates(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(2))
	f.Fuzz(func(t *testing.T, delay uint8) {
		if delay > 10 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-states"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-states", "MessageBody": "message", "DelaySeconds": int(delay)}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueName": "fuzz-states", "AttributeNames": []any{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesDelayed"}}})
		if err != nil {
			t.Fatal(err)
		}
		attrs := asMap(response.Output["Attributes"])
		wantVisible, wantDelayed := "1", "0"
		if delay > 0 {
			wantVisible, wantDelayed = "0", "1"
		}
		if attrs["ApproximateNumberOfMessages"] != wantVisible || attrs["ApproximateNumberOfMessagesDelayed"] != wantDelayed {
			t.Fatalf("delay=%d attributes=%#v", delay, attrs)
		}
	})
}

func FuzzReceiptHandleRotation(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(2))
	f.Fuzz(func(t *testing.T, timeout uint8) {
		if timeout > 10 {
			t.Skip()
		}
		clk := clock.NewControllable()
		deps := spitest.Deps(t)
		deps.Clock = clk
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-rotate"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-rotate", "MessageBody": "message"}}); err != nil {
			t.Fatal(err)
		}
		first, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-rotate", "VisibilityTimeout": int(timeout)}})
		if err != nil {
			t.Fatal(err)
		}
		if err := clk.Advance(time.Duration(timeout) * time.Second); err != nil {
			t.Fatal(err)
		}
		second, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-rotate", "VisibilityTimeout": 0}})
		if err != nil {
			t.Fatal(err)
		}
		firstHandle := first.Output["Messages"].([]any)[0].(map[string]any)["ReceiptHandle"]
		secondHandle := second.Output["Messages"].([]any)[0].(map[string]any)["ReceiptHandle"]
		if firstHandle == secondHandle {
			t.Fatalf("timeout=%d handle did not rotate", timeout)
		}
	})
}

func FuzzFIFODeleteAfterVisibilityTimeout(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(2))
	f.Fuzz(func(t *testing.T, timeout uint8) {
		if timeout > 10 {
			t.Skip()
		}
		clk := clock.NewControllable()
		deps := spitest.Deps(t)
		deps.Clock = clk
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-expired.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": strconv.Itoa(int(timeout))}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-expired.fifo", "MessageBody": "message", "MessageGroupId": "group"}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-expired.fifo"}})
		if err != nil {
			t.Fatal(err)
		}
		handle := response.Output["Messages"].([]any)[0].(map[string]any)["ReceiptHandle"]
		if err := clk.Advance(time.Duration(timeout) * time.Second); err != nil {
			t.Fatal(err)
		}
		_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessage", Input: map[string]any{"QueueName": "fuzz-expired.fifo", "ReceiptHandle": handle}})
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "InvalidParameterValue" || !strings.Contains(fault.Message, "receipt handle has expired") {
			t.Fatalf("timeout=%d error %#v", timeout, err)
		}
	})
}

func FuzzFIFOMessageGroupReuse(f *testing.F) {
	f.Add("second")
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > 256 || body == "" {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		invoke := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := invoke("CreateQueue", map[string]any{"QueueName": "fuzz-reuse-group.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := invoke("SendMessage", map[string]any{"QueueName": "fuzz-reuse-group.fifo", "MessageBody": "first", "MessageGroupId": "g1"}); err != nil {
			t.Fatal(err)
		}
		received, err := invoke("ReceiveMessage", map[string]any{"QueueName": "fuzz-reuse-group.fifo"})
		if err != nil {
			t.Fatal(err)
		}
		message := received.Output["Messages"].([]any)[0].(map[string]any)
		if _, err := invoke("DeleteMessage", map[string]any{"QueueName": "fuzz-reuse-group.fifo", "ReceiptHandle": message["ReceiptHandle"]}); err != nil {
			t.Fatal(err)
		}
		if _, err := invoke("SendMessage", map[string]any{"QueueName": "fuzz-reuse-group.fifo", "MessageBody": body, "MessageGroupId": "g1", "MessageDeduplicationId": "second"}); err != nil {
			t.Fatal(err)
		}
		final, err := invoke("ReceiveMessage", map[string]any{"QueueName": "fuzz-reuse-group.fifo"})
		if err != nil || len(final.Output["Messages"].([]any)) != 1 || final.Output["Messages"].([]any)[0].(map[string]any)["Body"] != body {
			t.Fatalf("body=%q response=%#v error=%v", body, final.Output, err)
		}
	})
}

func FuzzFIFOMessageGroupVisibilityAfterTerminate(f *testing.F) {
	f.Add(uint8(1))
	f.Add(uint8(3))
	f.Fuzz(func(t *testing.T, timeout uint8) {
		if timeout == 0 || timeout > 30 {
			t.Skip()
		}
		clk := clock.NewControllable()
		deps := spitest.Deps(t)
		deps.Clock = clk
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		invoke := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := invoke("CreateQueue", map[string]any{"QueueName": "fuzz-partial-group.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": int(timeout)}}); err != nil {
			t.Fatal(err)
		}
		for _, message := range []struct{ body, group string }{{"g1-m1", "g1"}, {"g1-m2", "g1"}, {"g2-m1", "g2"}} {
			if _, err := invoke("SendMessage", map[string]any{"QueueName": "fuzz-partial-group.fifo", "MessageBody": message.body, "MessageGroupId": message.group}); err != nil {
				t.Fatal(err)
			}
		}
		first, err := invoke("ReceiveMessage", map[string]any{"QueueName": "fuzz-partial-group.fifo", "MaxNumberOfMessages": 2})
		if err != nil || len(first.Output["Messages"].([]any)) != 2 {
			t.Fatalf("first %#v error %v", first, err)
		}
		firstMessage := first.Output["Messages"].([]any)[0].(map[string]any)
		if _, err := invoke("ChangeMessageVisibility", map[string]any{"QueueName": "fuzz-partial-group.fifo", "ReceiptHandle": firstMessage["ReceiptHandle"], "VisibilityTimeout": 0}); err != nil {
			t.Fatal(err)
		}
		second, err := invoke("ReceiveMessage", map[string]any{"QueueName": "fuzz-partial-group.fifo", "MaxNumberOfMessages": 3})
		if err != nil || len(second.Output["Messages"].([]any)) != 2 {
			t.Fatalf("second %#v error %v", second, err)
		}
		messages := second.Output["Messages"].([]any)
		if asMap(messages[0])["Body"] != "g2-m1" || asMap(messages[1])["Body"] != "g1-m1" {
			t.Fatalf("partial visibility ordering %#v", second.Output)
		}
	})
}

func FuzzFIFOApproximateMessageCount(f *testing.F) {
	f.Add(1)
	f.Add(5)
	f.Fuzz(func(t *testing.T, count int) {
		if count < 1 || count > 10 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-fifo-count.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}}); err != nil {
			t.Fatal(err)
		}
		for i := range count {
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-fifo-count.fifo", "MessageBody": fmt.Sprintf("message-%d", i), "MessageGroupId": fmt.Sprintf("group-%d", i)}}); err != nil {
				t.Fatal(err)
			}
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueueAttributes", Input: map[string]any{"QueueName": "fuzz-fifo-count.fifo", "AttributeNames": []any{"ApproximateNumberOfMessages"}}})
		if err != nil || asMap(response.Output["Attributes"])["ApproximateNumberOfMessages"] != strconv.Itoa(count) {
			t.Fatalf("count=%d response=%#v error=%v", count, response.Output, err)
		}
	})
}

func TestMessagesRemainQueueScoped(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	for _, name := range []string{"queue-0", "queue-1"} {
		if _, err := call("CreateQueue", map[string]any{"QueueName": name}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := call("SendMessage", map[string]any{"QueueName": "queue-0", "MessageBody": "message"}); err != nil {
		t.Fatal(err)
	}
	empty, err := call("ReceiveMessage", map[string]any{"QueueName": "queue-1"})
	if err != nil || empty.Output["Messages"] != nil {
		t.Fatalf("queue-1 %#v error %v", empty, err)
	}
	received, err := call("ReceiveMessage", map[string]any{"QueueName": "queue-0"})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := received.Output["Messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["Body"] != "message" {
		t.Fatalf("queue-0 %#v", received.Output)
	}
}

func TestMessagesRemainQueueScopedCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	for _, name := range []string{"queue-0", "queue-1"} {
		if _, err := call("CreateQueue", map[string]any{"QueueName": name}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := call("SendMessage", map[string]any{"QueueName": "queue-0", "MessageBody": "message"}); err != nil {
		t.Fatal(err)
	}
	empty, err := call("ReceiveMessage", map[string]any{"QueueName": "queue-1"})
	if err != nil {
		t.Fatal(err)
	}
	received, err := call("ReceiveMessage", map[string]any{"QueueName": "queue-0"})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{"queue-1": empty.Output, "queue-0": received.Output})
}

func TestEncodedMessageContentRoundTrips(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateQueue", map[string]any{"QueueName": "encoded"}); err != nil {
		t.Fatal(err)
	}
	want := `"&quot;&quot;` + "\r"
	if _, err := call("SendMessage", map[string]any{"QueueName": "encoded", "MessageBody": want}); err != nil {
		t.Fatal(err)
	}
	response, err := call("ReceiveMessage", map[string]any{"QueueName": "encoded"})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := response.Output["Messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["Body"] != want {
		t.Fatalf("encoded receive %#v", response.Output)
	}
}

func TestEncodedMessageContentCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateQueue", map[string]any{"QueueName": "encoded"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("SendMessage", map[string]any{"QueueName": "encoded", "MessageBody": `"&quot;&quot;` + "\r"}); err != nil {
		t.Fatal(err)
	}
	response, err := call("ReceiveMessage", map[string]any{"QueueName": "encoded"})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, response.Output)
}

func TestSendMessageBatchCharacterization(t *testing.T) {
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
	call("CreateQueue", map[string]any{"QueueName": "batch"})
	batch := call("SendMessageBatch", map[string]any{"QueueName": "batch", "Entries": []any{
		map[string]any{"Id": "1", "MessageBody": "message-0"},
		map[string]any{"Id": "2", "MessageBody": "message-1"},
	}})
	first := call("ReceiveMessage", map[string]any{"QueueName": "batch"})
	second := call("ReceiveMessage", map[string]any{"QueueName": "batch"})
	empty := call("ReceiveMessage", map[string]any{"QueueName": "batch"})
	golden.AssertJSON(t, map[string]any{"batch": batch.Output, "first": first.Output, "second": second.Output, "empty": empty.Output})
}

func TestInvalidBatchEntryIDCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "invalid-batch-id"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "invalid-batch-id", "Entries": []any{map[string]any{"Id": "message:invalid", "MessageBody": "message"}}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("invalid batch id error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestFIFOBatchMissingDeduplicationIDCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-missing-dedup.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false"}}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-missing-dedup.fifo", "Entries": []any{
		map[string]any{"Id": "message-1", "MessageBody": "message-1", "MessageGroupId": "test-group", "MessageDeduplicationId": "dedup-1"},
		map[string]any{"Id": "message-2", "MessageBody": "message-2", "MessageGroupId": "test-group"},
	}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("missing deduplication id error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestFIFOBatchMissingMessageGroupIDCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-missing-group.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false"}}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-missing-group.fifo", "Entries": []any{
		map[string]any{"Id": "message-1", "MessageBody": "message-1", "MessageGroupId": "test-group", "MessageDeduplicationId": "dedup-1"},
		map[string]any{"Id": "message-2", "MessageBody": "message-2", "MessageDeduplicationId": "dedup-2"},
	}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("missing message group id error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestTooManyBatchEntriesCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "too-many-batch"}}); err != nil {
		t.Fatal(err)
	}
	entries := make([]any, 20)
	for i := range entries {
		entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "MessageBody": "message"}
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "too-many-batch", "Entries": entries}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("too many entries error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestDeleteMessageBatchInvalidEntryIDCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "delete-invalid-batch-id"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessageBatch", Input: map[string]any{"QueueName": "delete-invalid-batch-id", "Entries": []any{map[string]any{"Id": "message:invalid", "ReceiptHandle": "handle"}}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("invalid delete batch id error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestDeleteMessageBatchTooManyEntriesCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "delete-too-many-batch"}}); err != nil {
		t.Fatal(err)
	}
	entries := make([]any, 20)
	for i := range entries {
		entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "ReceiptHandle": "handle"}
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessageBatch", Input: map[string]any{"QueueName": "delete-too-many-batch", "Entries": entries}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("too many delete batch entries error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestDeleteMessageBatchEmptyCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "delete-empty-batch"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessageBatch", Input: map[string]any{"QueueName": "delete-empty-batch", "Entries": []any{}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("empty delete batch error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestSendMessageBatchInvalidContentsPartialFailureCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-invalid-contents"}}); err != nil {
		t.Fatal(err)
	}
	entries := make([]any, 10)
	for i := range entries {
		entries[i] = map[string]any{"Id": strconv.Itoa(i), "MessageBody": strconv.Itoa(i)}
	}
	entries[9] = map[string]any{"Id": "9", "MessageBody": "\x01"}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-invalid-contents", "Entries": entries}})
	if err != nil {
		t.Fatal(err)
	}
	successful := response.Output["Successful"].([]any)
	failed := response.Output["Failed"].([]any)
	if len(successful) != 9 || len(failed) != 1 {
		t.Fatalf("batch response %#v", response.Output)
	}
	failure := failed[0].(map[string]any)
	golden.AssertJSON(t, map[string]any{"successful": len(successful), "failed": failure})
}

func TestChangeMessageVisibilityBatchTooManyEntriesCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "visibility-too-many"}}); err != nil {
		t.Fatal(err)
	}
	entries := make([]any, 20)
	for i := range entries {
		entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "ReceiptHandle": "handle", "VisibilityTimeout": 123}
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ChangeMessageVisibilityBatch", Input: map[string]any{"QueueName": "visibility-too-many", "Entries": entries}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("too many visibility entries error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestSendBatchReceiveMultipleCharacterization(t *testing.T) {
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
	call("CreateQueue", map[string]any{"QueueName": "batch-mixed"})
	call("SendMessageBatch", map[string]any{"QueueName": "batch-mixed", "Entries": []any{
		map[string]any{"Id": "1", "MessageBody": "message-0"},
		map[string]any{"Id": "2", "MessageBody": "message-1"},
	}})
	call("SendMessage", map[string]any{"QueueName": "batch-mixed", "MessageBody": "message-2"})
	received := call("ReceiveMessage", map[string]any{"QueueName": "batch-mixed", "MaxNumberOfMessages": 3})
	bodies := map[string]bool{}
	for _, raw := range received.Output["Messages"].([]any) {
		bodies[raw.(map[string]any)["Body"].(string)] = true
	}
	if len(bodies) != 3 || !bodies["message-0"] || !bodies["message-1"] || !bodies["message-2"] {
		t.Fatalf("mixed batch receive %#v", received.Output)
	}
	golden.AssertJSON(t, received.Output)
}

func TestPublishGetDeleteMessageBatchCharacterization(t *testing.T) {
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
	call("CreateQueue", map[string]any{"QueueName": "publish-get-delete-batch"})
	entries := make([]any, 10)
	for i := range entries {
		entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "MessageBody": fmt.Sprintf("messageBody-%d", i)}
	}
	sent := call("SendMessageBatch", map[string]any{"QueueName": "publish-get-delete-batch", "Entries": entries})
	received := call("ReceiveMessage", map[string]any{"QueueName": "publish-get-delete-batch", "MaxNumberOfMessages": 10})
	messages := received.Output["Messages"].([]any)
	deleteEntries := make([]any, len(messages))
	for i, raw := range messages {
		message := raw.(map[string]any)
		deleteEntries[i] = map[string]any{"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
	}
	deleted := call("DeleteMessageBatch", map[string]any{"QueueName": "publish-get-delete-batch", "Entries": deleteEntries})
	remaining := call("ReceiveMessage", map[string]any{"QueueName": "publish-get-delete-batch", "MaxNumberOfMessages": 10})
	if len(sent.Output["Successful"].([]any)) != 10 || len(messages) != 10 || len(deleted.Output["Successful"].([]any)) != 10 {
		t.Fatalf("batch lifecycle sent=%#v received=%#v deleted=%#v", sent.Output, received.Output, deleted.Output)
	}
	remainingCount := 0
	if raw, ok := remaining.Output["Messages"].([]any); ok {
		remainingCount = len(raw)
	}
	golden.AssertJSON(t, map[string]any{"sent": len(sent.Output["Successful"].([]any)), "received": len(messages), "deleted": len(deleted.Output["Successful"].([]any)), "remaining": remainingCount})
}

func TestSendMessageBatchEmptyCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "empty-batch"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "empty-batch", "Entries": []any{}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("empty batch fault %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestSendOversizedMessageCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "oversized"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "oversized", "MessageBody": strings.Repeat("a", (1<<20)-7), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("oversized message fault %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestSendMessageUpdatedMaximumSizeCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	ctx := context.Background()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "maximum", "Attributes": map[string]any{"MaximumMessageSize": "1024"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "maximum", "MessageBody": strings.Repeat("a", 1024)}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "maximum", "MessageBody": strings.Repeat("a", 1017), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("updated maximum fault %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestSendMessageBatchOversizedCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	ctx := context.Background()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-size"}}); err != nil {
		t.Fatal(err)
	}
	attrs := map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-size", "Entries": []any{
		map[string]any{"Id": "1", "MessageBody": strings.Repeat("a", (1<<20)-8), "MessageAttributes": attrs},
		map[string]any{"Id": "2", "MessageBody": "a"},
	}}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("oversized batch fault %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-size-valid"}}); err != nil {
		t.Fatal(err)
	}
	valid, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-size-valid", "Entries": []any{
		map[string]any{"Id": "1", "MessageBody": strings.Repeat("a", 600000)},
		map[string]any{"Id": "2", "MessageBody": "a"},
	}}})
	if err != nil || len(valid.Output["Successful"].([]any)) != 2 {
		t.Fatalf("valid batch %#v error %v", valid, err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestSendMessageBatchUpdatedMaximumSizeCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	ctx := context.Background()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-maximum", "Attributes": map[string]any{"MaximumMessageSize": "2048"}}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-maximum", "Entries": []any{
		map[string]any{"Id": "1", "MessageBody": strings.Repeat("a", 2040), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}},
		map[string]any{"Id": "2", "MessageBody": "a"},
	}}})
	if err != nil || len(response.Output["Successful"].([]any)) != 2 {
		t.Fatalf("updated batch %#v error %v", response, err)
	}
	failed := 0
	if entries, ok := response.Output["Failed"].([]any); ok {
		failed = len(entries)
	}
	golden.AssertJSON(t, map[string]any{"successful": len(response.Output["Successful"].([]any)), "failed": failed})
}

func TestSendMessageBatchPerEntryMaximumSizeCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	ctx := context.Background()
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-entry-maximum", "Attributes": map[string]any{"MaximumMessageSize": "1024"}}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-entry-maximum", "Entries": []any{
		map[string]any{"Id": "valid", "MessageBody": strings.Repeat("a", 1024)},
		map[string]any{"Id": "oversized", "MessageBody": strings.Repeat("a", 1025)},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	successful := response.Output["Successful"].([]any)
	failed := response.Output["Failed"].([]any)
	if len(successful) != 1 || len(failed) != 1 {
		t.Fatalf("batch response %#v", response.Output)
	}
	golden.AssertJSON(t, map[string]any{"successful": successful, "failed": failed})
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
	created, err := call("CreateQueue", map[string]any{"QueueName": name, "Attributes": map[string]any{"FifoQueue": "true", "DelaySeconds": "5"}})
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
	recreated, err := call("CreateQueue", map[string]any{"QueueName": name, "Attributes": map[string]any{"FifoQueue": "true"}})
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
	emptyTags := len(tags.Output) == 0 || len(asMap(tags.Output["Tags"])) == 0
	if sendErr != nil || secondSent.Output["MessageId"] == firstSent.Output["MessageId"] || attrs.Output["Attributes"].(map[string]any)["DelaySeconds"] != nil ||
		!emptyTags || len(messages.Output["Messages"].([]any)) != 1 {
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
	_, _ = invoke("CreateQueue", map[string]any{"QueueName": "strict.fifo", "Attributes": map[string]any{"FifoQueue": "true"}})
	if _, err := invoke("SendMessage", map[string]any{"QueueName": "strict.fifo", "MessageBody": "x"}); faultCode(err) != "MissingParameter" {
		t.Fatalf("missing group error %v", err)
	}
	if _, err := invoke("SendMessage", map[string]any{"QueueName": "strict.fifo", "MessageBody": "x", "MessageGroupId": "g"}); faultCode(err) != "InvalidParameterValue" {
		t.Fatalf("missing dedup error %v", err)
	}
	batch, err := invoke("SendMessageBatch", map[string]any{"QueueName": "strict.fifo", "Entries": []any{
		map[string]any{"Id": "bad", "MessageBody": "", "MessageGroupId": "g", "MessageDeduplicationId": "bad-d"},
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
	if _, ok := before.Output["Messages"]; ok {
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

func TestStandardMessageGroupIDCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(group string) map[string]any {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{
			"QueueName": "standard", "MessageBody": "message", "MessageGroupId": group,
		}})
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("group %q error %#v", group, err)
		}
		return map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "standard"}}); err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{
		"empty": call(""), "tooLong": call(strings.Repeat("a", 129)), "spaces": call("group 123"),
	})
}

func TestQueueTagCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "tagged"})
	call("TagQueue", map[string]any{"QueueName": "tagged", "Tags": map[string]any{"tag1": "value1", "tag2": "value2", "tag3": ""}})
	first := call("ListQueueTags", map[string]any{"QueueName": "tagged"})
	call("UntagQueue", map[string]any{"QueueName": "tagged", "TagKeys": []any{"tag1", "tag3"}})
	second := call("ListQueueTags", map[string]any{"QueueName": "tagged"})
	call("UntagQueue", map[string]any{"QueueName": "tagged", "TagKeys": []any{"tag2"}})
	final := call("ListQueueTags", map[string]any{"QueueName": "tagged"})
	golden.AssertJSON(t, map[string]any{"first": first, "second": second, "final": final})
}

func TestQueueTagOverwriteCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "tag-overwrite"})
	call("TagQueue", map[string]any{"QueueName": "tag-overwrite", "Tags": map[string]any{"tag1": "value1", "tag2": "value2"}})
	call("TagQueue", map[string]any{"QueueName": "tag-overwrite", "Tags": map[string]any{"tag1": "VALUE1", "tag3": "value3"}})
	golden.AssertJSON(t, call("ListQueueTags", map[string]any{"QueueName": "tag-overwrite"}))
}

func TestCreateQueueTagsCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "create-tags", "Tags": map[string]any{"tag1": "value1", "tag2": "value2"}}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListQueueTags", Input: map[string]any{"QueueName": "create-tags"}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, response.Output)
}

func FuzzCreateQueueTags(f *testing.F) {
	f.Add("tag", "value")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, key, value string) {
		if len(key) > 256 || len(value) > 1024 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "create-tags", "Tags": map[string]any{key: value}}})
		if err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListQueueTags", Input: map[string]any{"QueueName": "create-tags"}})
		if err != nil || str(asMap(response.Output["Tags"])[key]) != value {
			t.Fatalf("tags %#v error %v", response.Output, err)
		}
	})
}

func FuzzInvalidBatchEntryID(f *testing.F) {
	f.Add("message-1")
	f.Add("message:invalid")
	f.Add(strings.Repeat("a", 81))
	f.Fuzz(func(t *testing.T, entryID string) {
		if len(entryID) > 256 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-batch-id"}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "fuzz-batch-id", "Entries": []any{map[string]any{"Id": entryID, "MessageBody": "message"}}}})
		if validBatchEntryID(entryID) {
			if err != nil || len(response.Output["Successful"].([]any)) != 1 {
				t.Fatalf("valid id %q response %#v error %v", entryID, response.Output, err)
			}
			return
		}
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "AWS.SimpleQueueService.InvalidBatchEntryId" {
			t.Fatalf("invalid id %q error %#v", entryID, err)
		}
	})
}

func FuzzSendMessageBatchInvalidContents(f *testing.F) {
	f.Add("\x01")
	f.Add("ok")
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > 256 || body == "" {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-batch-contents"}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "fuzz-batch-contents", "Entries": []any{map[string]any{"Id": "1", "MessageBody": body}}}})
		if validMessageContents(body) {
			if err != nil || len(response.Output["Successful"].([]any)) != 1 {
				t.Fatalf("valid body %q response %#v error %v", body, response.Output, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("invalid body should be per-entry failure: %q error %v", body, err)
		}
		failed := response.Output["Failed"].([]any)
		if len(failed) != 1 || asMap(failed[0])["Code"] != "InvalidMessageContents" {
			t.Fatalf("invalid body %q response %#v", body, response.Output)
		}
	})
}

func FuzzDeleteMessageBatchEntryID(f *testing.F) {
	f.Add("message-1")
	f.Add("message:invalid")
	f.Add(strings.Repeat("a", 81))
	f.Fuzz(func(t *testing.T, entryID string) {
		if len(entryID) > 256 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-delete-batch-id"}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessageBatch", Input: map[string]any{"QueueName": "fuzz-delete-batch-id", "Entries": []any{map[string]any{"Id": entryID, "ReceiptHandle": "handle"}}}})
		if validBatchEntryID(entryID) {
			if err != nil || len(response.Output["Successful"].([]any)) != 1 {
				t.Fatalf("valid id %q response %#v error %v", entryID, response.Output, err)
			}
			return
		}
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "AWS.SimpleQueueService.InvalidBatchEntryId" {
			t.Fatalf("invalid id %q error %#v", entryID, err)
		}
	})
}

func FuzzDeleteMessageBatchEmpty(f *testing.F) {
	f.Add(false)
	f.Add(true)
	f.Fuzz(func(t *testing.T, nilEntries bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-delete-empty-batch"}}); err != nil {
			t.Fatal(err)
		}
		entries := []any{}
		if nilEntries {
			entries = nil
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessageBatch", Input: map[string]any{"QueueName": "fuzz-delete-empty-batch", "Entries": entries}})
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "AWS.SimpleQueueService.EmptyBatchRequest" {
			t.Fatalf("nil=%v error %#v", nilEntries, err)
		}
	})
}

func FuzzChangeMessageVisibilityBatchSize(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(10))
	f.Add(uint8(11))
	f.Fuzz(func(t *testing.T, count uint8) {
		if count > 32 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-visibility-batch"}}); err != nil {
			t.Fatal(err)
		}
		entries := make([]any, int(count))
		for i := range entries {
			entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "ReceiptHandle": "handle", "VisibilityTimeout": 123}
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ChangeMessageVisibilityBatch", Input: map[string]any{"QueueName": "fuzz-visibility-batch", "Entries": entries}})
		if count > 10 {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "AWS.SimpleQueueService.TooManyEntriesInBatchRequest" {
				t.Fatalf("count=%d error %#v", count, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("count=%d error %v", count, err)
		}
	})
}

func FuzzFIFOBatchDeduplicationPresence(f *testing.F) {
	f.Add(true)
	f.Add(false)
	f.Fuzz(func(t *testing.T, provided bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-batch-dedup.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false"}}}); err != nil {
			t.Fatal(err)
		}
		entry := map[string]any{"Id": "message-1", "MessageBody": "message", "MessageGroupId": "group-1"}
		if provided {
			entry["MessageDeduplicationId"] = "dedup-1"
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "fuzz-batch-dedup.fifo", "Entries": []any{entry}}})
		if provided {
			if err != nil || len(response.Output["Successful"].([]any)) != 1 {
				t.Fatalf("provided response %#v error %v", response.Output, err)
			}
		} else {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "InvalidParameterValue" {
				t.Fatalf("missing response %#v error %#v", response.Output, err)
			}
		}
	})
}

func FuzzFIFOBatchMessageGroupPresence(f *testing.F) {
	f.Add(true)
	f.Add(false)
	f.Fuzz(func(t *testing.T, provided bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-batch-group.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false"}}}); err != nil {
			t.Fatal(err)
		}
		entry := map[string]any{"Id": "message-1", "MessageBody": "message", "MessageDeduplicationId": "dedup-1"}
		if provided {
			entry["MessageGroupId"] = "group-1"
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "fuzz-batch-group.fifo", "Entries": []any{entry}}})
		if provided {
			if err != nil || len(response.Output["Successful"].([]any)) != 1 {
				t.Fatalf("provided response %#v error %v", response.Output, err)
			}
		} else {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "MissingParameter" || fault.Message != "MessageGroupId" {
				t.Fatalf("missing response error %#v", err)
			}
		}
	})
}

func FuzzSendMessageBatchPerEntryMaximumSize(f *testing.F) {
	f.Add(false)
	f.Add(true)
	f.Fuzz(func(t *testing.T, oversized bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-batch-entry-maximum", "Attributes": map[string]any{"MaximumMessageSize": "1024"}}}); err != nil {
			t.Fatal(err)
		}
		secondBody := strings.Repeat("a", 1024)
		if oversized {
			secondBody = strings.Repeat("a", 1025)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "fuzz-batch-entry-maximum", "Entries": []any{
			map[string]any{"Id": "valid", "MessageBody": strings.Repeat("a", 1024)},
			map[string]any{"Id": "second", "MessageBody": secondBody},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		successful := response.Output["Successful"].([]any)
		failed, _ := response.Output["Failed"].([]any)
		if oversized && (len(successful) != 1 || len(failed) != 1) {
			t.Fatalf("oversized response %#v", response.Output)
		}
		if !oversized && (len(successful) != 2 || len(failed) != 0) {
			t.Fatalf("valid response %#v", response.Output)
		}
	})
}

func FuzzPublishGetDeleteMessageBatch(f *testing.F) {
	f.Add(1)
	f.Add(10)
	f.Fuzz(func(t *testing.T, count int) {
		if count < 1 || count > 10 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-publish-get-delete"}}); err != nil {
			t.Fatal(err)
		}
		entries := make([]any, count)
		for i := range entries {
			entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "MessageBody": fmt.Sprintf("body-%d", i)}
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "fuzz-publish-get-delete", "Entries": entries}}); err != nil {
			t.Fatal(err)
		}
		received, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-publish-get-delete", "MaxNumberOfMessages": 10}})
		if err != nil || len(received.Output["Messages"].([]any)) != count {
			t.Fatalf("receive %#v error %v", received.Output, err)
		}
		deleteEntries := make([]any, count)
		for i, raw := range received.Output["Messages"].([]any) {
			message := raw.(map[string]any)
			deleteEntries[i] = map[string]any{"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
		}
		deleted, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessageBatch", Input: map[string]any{"QueueName": "fuzz-publish-get-delete", "Entries": deleteEntries}})
		if err != nil || len(deleted.Output["Successful"].([]any)) != count {
			t.Fatalf("delete %#v error %v", deleted.Output, err)
		}
	})
}

func FuzzFIFODeduplicationID(f *testing.F) {
	f.Add("")
	f.Add("dedup-1")
	f.Add(strings.Repeat("a", 129))
	f.Add("group 123")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 256 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{
			"QueueName": "fuzz-dedup.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false"},
		}}); err != nil {
			t.Fatal(err)
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{
			"QueueName": "fuzz-dedup.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "MessageDeduplicationId": value,
		}})
		if validMessageGroupID(value) {
			if err != nil {
				t.Fatalf("valid id %q error %v", value, err)
			}
			return
		}
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "InvalidParameterValue" || !strings.Contains(fault.Message, "MessageDeduplicationId can only include alphanumeric and punctuation characters") {
			t.Fatalf("invalid id %q error %#v", value, err)
		}
	})
}

func FuzzFIFODelayZeroUsesQueueDelay(f *testing.F) {
	f.Add(1)
	f.Add(2)
	f.Fuzz(func(t *testing.T, delay int) {
		if delay < 1 || delay > 20 {
			t.Skip()
		}
		clk := clock.NewControllable()
		deps := spitest.Deps(t)
		deps.Clock = clk
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{
			"QueueName": "fuzz-delay.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "DelaySeconds": strconv.Itoa(delay)},
		}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{
			"QueueName": "fuzz-delay.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "DelaySeconds": 0,
		}}); err != nil {
			t.Fatal(err)
		}
		before, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-delay.fifo"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := before.Output["Messages"]; ok {
			t.Fatalf("delay %d visible early %#v", delay, before.Output)
		}
		if err := clk.Advance(time.Duration(delay) * time.Second); err != nil {
			t.Fatal(err)
		}
		after, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-delay.fifo"}})
		if err != nil || len(after.Output["Messages"].([]any)) != 1 {
			t.Fatalf("delay %d after %#v error %v", delay, after.Output, err)
		}
	})
}

func FuzzFIFOPerMessageDelay(f *testing.F) {
	f.Add(1)
	f.Add(900)
	f.Fuzz(func(t *testing.T, delay int) {
		if delay < 1 || delay > 900 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-invalid-delay.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}}); err != nil {
			t.Fatal(err)
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-invalid-delay.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "DelaySeconds": delay}})
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "InvalidParameterValue" || !strings.Contains(fault.Message, "not valid for this queue type") {
			t.Fatalf("delay %d error %#v", delay, err)
		}
	})
}

func TestQueueTagKeysAreCaseSensitive(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "tag-case"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "TagQueue", Input: map[string]any{"QueueName": "tag-case", "Tags": map[string]any{"MyTag": "value1", "mytag": "value2"}}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListQueueTags", Input: map[string]any{"QueueName": "tag-case"}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, response.Output)
}

func TestCreateQueueIdempotencyAndAttributeValidation(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(input map[string]any) (map[string]any, error) {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: input})
		if err != nil {
			if fault, ok := err.(*spi.Fault); ok {
				return map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus}, nil
			}
		}
		if err != nil {
			return nil, err
		}
		return response.Output, nil
	}
	first, err := call(map[string]any{"QueueName": "idempotent", "Attributes": map[string]any{"VisibilityTimeout": "69"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := call(map[string]any{"QueueName": "idempotent"})
	if err != nil || second["QueueUrl"] != first["QueueUrl"] {
		t.Fatalf("idempotent first=%#v second=%#v err=%v", first, second, err)
	}
	conflict, err := call(map[string]any{"QueueName": "idempotent", "Attributes": map[string]any{"VisibilityTimeout": "70"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetQueueAttributes", Input: map[string]any{"QueueName": "idempotent", "Attributes": map[string]any{"VisibilityTimeout": "70"}}}); err != nil {
		t.Fatal(err)
	}
	updated, err := call(map[string]any{"QueueName": "idempotent", "Attributes": map[string]any{"VisibilityTimeout": "70"}})
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := call(map[string]any{"QueueName": "standard-invalid", "Attributes": map[string]any{"FifoQueue": "false"}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{"conflict": conflict, "updated": updated, "invalid": invalid})
}

func TestFIFOQueueNameValidationCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(input map[string]any) map[string]any {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: input})
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("CreateQueue %#v error %#v", input, err)
		}
		return map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault}
	}
	golden.AssertJSON(t, map[string]any{
		"fifoMissingAttribute":  call(map[string]any{"QueueName": "missing-attribute.fifo"}),
		"fifoFalseAttribute":    call(map[string]any{"QueueName": "false-attribute.fifo", "Attributes": map[string]any{"FifoQueue": "false"}}),
		"standardFIFOAttribute": call(map[string]any{"QueueName": "standard-with-fifo", "Attributes": map[string]any{"FifoQueue": "true"}}),
	})
}

func TestFIFODeduplicationIDCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{
		"QueueName": "dedup-invalid.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false"},
	}}); err != nil {
		t.Fatal(err)
	}
	call := func(value string) map[string]any {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{
			"QueueName": "dedup-invalid.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "MessageDeduplicationId": value,
		}})
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("deduplication id %q error %#v", value, err)
		}
		return map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault}
	}
	golden.AssertJSON(t, map[string]any{
		"empty": call(""), "tooLong": call(strings.Repeat("a", 129)), "spaces": call("group 123"),
	})
}

func TestFIFODelayZeroUsesQueueDelayCharacterization(t *testing.T) {
	clk := clock.NewControllable()
	deps := spitest.Deps(t)
	deps.Clock = clk
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{
		"QueueName": "delay-zero.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "DelaySeconds": "2"},
	}}); err != nil {
		t.Fatal(err)
	}
	sent, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{
		"QueueName": "delay-zero.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "DelaySeconds": 0,
	}})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "delay-zero.fifo", "WaitTimeSeconds": 0}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := initial.Output["Messages"]; ok {
		t.Fatalf("message visible before queue delay: %#v", initial.Output)
	}
	if err := clk.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	after, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "delay-zero.fifo", "WaitTimeSeconds": 0}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{"sent": sent.Output, "initial": initial.Output, "after": after.Output})
}

func TestFIFOPerMessageDelayCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "delay-invalid.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "delay-invalid.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "DelaySeconds": 2}})
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("FIFO per-message delay error %#v", err)
	}
	golden.AssertJSON(t, map[string]any{"Code": fault.Code, "Message": fault.Message, "HTTPStatus": fault.HTTPStatus, "Fault": fault.Fault})
}

func TestFIFOSequenceNumberCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "sequence.fifo", "Attributes": map[string]any{"FifoQueue": "true"}})
	sequences := []string{}
	for index := 1; index <= 3; index++ {
		output := call("SendMessage", map[string]any{"QueueName": "sequence.fifo", "MessageBody": fmt.Sprintf("message-%d", index), "MessageGroupId": "group", "MessageDeduplicationId": fmt.Sprintf("dedup-%d", index)})
		sequence, ok := output["SequenceNumber"].(string)
		if !ok || sequence == "" {
			t.Fatalf("FIFO sequence output %#v", output)
		}
		sequences = append(sequences, sequence)
	}
	duplicate := call("SendMessage", map[string]any{"QueueName": "sequence.fifo", "MessageBody": "message-1", "MessageGroupId": "group", "MessageDeduplicationId": "dedup-1"})
	standard := call("CreateQueue", map[string]any{"QueueName": "sequence-standard"})
	_ = standard
	standardSend := call("SendMessage", map[string]any{"QueueName": "sequence-standard", "MessageBody": "message"})
	if _, present := standardSend["SequenceNumber"]; present {
		t.Fatalf("standard sequence output %#v", standardSend)
	}
	golden.AssertJSON(t, map[string]any{"sequences": sequences, "duplicate": duplicate, "standard": standardSend})
}

func TestMessageSystemAttributeDigestCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "system-attribute-digest"}}); err != nil {
		t.Fatal(err)
	}
	messageAttributes := map[string]any{"timestamp": map[string]any{"StringValue": "1493147359900", "DataType": "Number"}}
	systemAttributes := map[string]any{"AWSTraceHeader": map[string]any{"StringValue": "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1", "DataType": "String"}}
	without, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "system-attribute-digest", "MessageBody": "test", "MessageAttributes": messageAttributes}})
	if err != nil {
		t.Fatal(err)
	}
	with, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "system-attribute-digest", "MessageBody": "test", "MessageAttributes": messageAttributes, "MessageSystemAttributes": systemAttributes}})
	if err != nil {
		t.Fatal(err)
	}
	if without.Output["MD5OfMessageSystemAttributes"] != nil || with.Output["MD5OfMessageAttributes"] != without.Output["MD5OfMessageAttributes"] || with.Output["MD5OfMessageSystemAttributes"] != "5ae4d5d7636402d80f4eb6d213245a88" {
		t.Fatalf("system digest without=%#v with=%#v", without.Output, with.Output)
	}
	golden.AssertJSON(t, map[string]any{"without": without.Output, "with": with.Output})
}

func TestFIFODeduplicationScopeCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "dedup-scope.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false", "DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}})
	first := call("SendMessage", map[string]any{"QueueName": "dedup-scope.fifo", "MessageBody": "group-1", "MessageGroupId": "group-1", "MessageDeduplicationId": "same-dedup"})
	second := call("SendMessage", map[string]any{"QueueName": "dedup-scope.fifo", "MessageBody": "group-2", "MessageGroupId": "group-2", "MessageDeduplicationId": "same-dedup"})
	duplicate := call("SendMessage", map[string]any{"QueueName": "dedup-scope.fifo", "MessageBody": "duplicate", "MessageGroupId": "group-1", "MessageDeduplicationId": "same-dedup"})
	received := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope.fifo", "MaxNumberOfMessages": 10})
	messages, _ := received["Messages"].([]any)
	if len(messages) != 2 || asMap(messages[0])["MessageId"] != first["MessageId"] || asMap(messages[1])["MessageId"] != second["MessageId"] || duplicate["MessageId"] != first["MessageId"] || duplicate["MD5OfMessageBody"] != "24f1b0a79473250c195c7fb84e393392" {
		t.Fatalf("scope first=%#v second=%#v duplicate=%#v received=%#v", first, second, duplicate, received)
	}
	golden.AssertJSON(t, map[string]any{"first": first, "second": second, "duplicate": duplicate, "received": received})
}

func TestFIFOMessageGroupScopeWithoutThroughputCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "dedup-scope-default.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "true", "DeduplicationScope": "messageGroup"}})
	first := call("SendMessage", map[string]any{"QueueName": "dedup-scope-default.fifo", "MessageBody": "Test", "MessageGroupId": "group-1"})
	firstReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-default.fifo", "MaxNumberOfMessages": 1})
	second := call("SendMessage", map[string]any{"QueueName": "dedup-scope-default.fifo", "MessageBody": "Test", "MessageGroupId": "group-2"})
	secondReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-default.fifo", "MaxNumberOfMessages": 1})
	messages, _ := secondReceive["Messages"].([]any)
	if len(messages) != 1 || asMap(messages[0])["MessageId"] != second["MessageId"] || first["MessageId"] == second["MessageId"] || len(asAnySlice(firstReceive["Messages"])) != 1 {
		t.Fatalf("message-group scope first=%#v firstReceive=%#v second=%#v secondReceive=%#v", first, firstReceive, second, secondReceive)
	}
	golden.AssertJSON(t, map[string]any{"first": first, "firstReceive": firstReceive, "second": second, "secondReceive": secondReceive})
}

func TestFIFODeduplicationScopeUpdateCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response.Output
	}
	call("CreateQueue", map[string]any{"QueueName": "dedup-scope-update.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false", "DeduplicationScope": "queue"}})
	first := call("SendMessage", map[string]any{"QueueName": "dedup-scope-update.fifo", "MessageBody": "Test1", "MessageGroupId": "group-1", "MessageDeduplicationId": "same-dedup"})
	firstReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-update.fifo", "MaxNumberOfMessages": 1})
	call("DeleteMessage", map[string]any{"QueueName": "dedup-scope-update.fifo", "ReceiptHandle": asMap(asAnySlice(firstReceive["Messages"])[0])["ReceiptHandle"]})
	queueDuplicate := call("SendMessage", map[string]any{"QueueName": "dedup-scope-update.fifo", "MessageBody": "Test2", "MessageGroupId": "group-2", "MessageDeduplicationId": "same-dedup"})
	queueReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-update.fifo", "MaxNumberOfMessages": 1, "VisibilityTimeout": 0})
	call("SetQueueAttributes", map[string]any{"QueueName": "dedup-scope-update.fifo", "Attributes": map[string]any{"DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}})
	updated := call("SendMessage", map[string]any{"QueueName": "dedup-scope-update.fifo", "MessageBody": "Test3", "MessageGroupId": "group-3", "MessageDeduplicationId": "same-dedup"})
	updatedReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-update.fifo", "MaxNumberOfMessages": 1, "VisibilityTimeout": 0})
	if first["MessageId"] != queueDuplicate["MessageId"] || len(asAnySlice(queueReceive["Messages"])) != 0 || first["MessageId"] != updated["MessageId"] || len(asAnySlice(updatedReceive["Messages"])) != 0 {
		t.Fatalf("queue scope update first=%#v queueDuplicate=%#v queueReceive=%#v updated=%#v updatedReceive=%#v", first, queueDuplicate, queueReceive, updated, updatedReceive)
	}

	call("CreateQueue", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "Attributes": map[string]any{"FifoQueue": "true", "ContentBasedDeduplication": "false", "DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}})
	highFirst := call("SendMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "MessageBody": "Test1", "MessageGroupId": "group-1", "MessageDeduplicationId": "same-dedup"})
	highFirstReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "MaxNumberOfMessages": 1})
	call("DeleteMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "ReceiptHandle": asMap(asAnySlice(highFirstReceive["Messages"])[0])["ReceiptHandle"]})
	highSecond := call("SendMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "MessageBody": "Test2", "MessageGroupId": "group-2", "MessageDeduplicationId": "same-dedup"})
	highSecondReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "MaxNumberOfMessages": 1})
	call("DeleteMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "ReceiptHandle": asMap(asAnySlice(highSecondReceive["Messages"])[0])["ReceiptHandle"]})
	call("SetQueueAttributes", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "Attributes": map[string]any{"DeduplicationScope": "queue", "FifoThroughputLimit": "perQueue"}})
	highUpdated := call("SendMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "MessageBody": "Test3", "MessageGroupId": "group-3", "MessageDeduplicationId": "same-dedup"})
	highUpdatedReceive := call("ReceiveMessage", map[string]any{"QueueName": "dedup-scope-update-high.fifo", "MaxNumberOfMessages": 1})
	if highFirst["MessageId"] == highSecond["MessageId"] || len(asAnySlice(highSecondReceive["Messages"])) != 1 || highSecond["MessageId"] == highUpdated["MessageId"] || len(asAnySlice(highUpdatedReceive["Messages"])) != 1 {
		t.Fatalf("message-group scope update first=%#v second=%#v secondReceive=%#v updated=%#v updatedReceive=%#v", highFirst, highSecond, highSecondReceive, highUpdated, highUpdatedReceive)
	}
	golden.AssertJSON(t, map[string]any{"queueDuplicate": queueDuplicate, "queueReceive": queueReceive, "updated": updated, "updatedReceive": updatedReceive, "highSecond": highSecond, "highSecondReceive": highSecondReceive, "highUpdated": highUpdated, "highUpdatedReceive": highUpdatedReceive})
}

func TestTraceHeaderPropagationCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "trace-header"}}); err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest("POST", "http://queue", nil)
	httpRequest.Header.Set("X-Amzn-Trace-Id", "Root=1-3152b799-8954dae64eda91bc9a23a7e8;Parent=7fa8c0f79203be72;Sampled=1")
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, HTTP: httpRequest, Operation: "SendMessage", Input: map[string]any{"QueueName": "trace-header", "MessageBody": "test", "MessageAttributes": map[string]any{"timestamp": map[string]any{"StringValue": "1493147359900", "DataType": "Number"}}}})
	if err != nil {
		t.Fatal(err)
	}
	received, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "trace-header", "AttributeNames": []any{"AWSTraceHeader"}, "MessageAttributeNames": []any{"All"}}})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := received.Output["Messages"].([]any)
	if len(messages) != 1 || asMap(asMap(messages[0])["Attributes"])["AWSTraceHeader"] == nil {
		t.Fatalf("trace header %#v", received.Output)
	}
	golden.AssertJSON(t, received.Output)
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
		"FifoQueue":                 "true",
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
	if len(msgs) != 3 {
		t.Fatalf("fifo+dedup receive %d %v", len(msgs), got.Output)
	}
	bodies := map[string]bool{}
	for _, m := range msgs {
		bodies[m.(map[string]any)["Body"].(string)] = true
	}
	if !bodies["g1a"] || !bodies["g1b"] || !bodies["g2a"] {
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

func TestDeadLetterQueueMaxReceiveCountCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(operation, err)
		}
		return response
	}
	call("CreateQueue", map[string]any{"QueueName": "max-receive-dlq"})
	call("CreateQueue", map[string]any{"QueueName": "max-receive-source", "Attributes": map[string]any{
		"RedrivePolicy":     `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:max-receive-dlq","maxReceiveCount":"1"}`,
		"VisibilityTimeout": "0",
	}})
	sent := call("SendMessage", map[string]any{"QueueName": "max-receive-source", "MessageBody": "poison"})
	first := call("ReceiveMessage", map[string]any{"QueueName": "max-receive-source", "VisibilityTimeout": 0})
	second := call("ReceiveMessage", map[string]any{"QueueName": "max-receive-source", "VisibilityTimeout": 0})
	dlq := call("ReceiveMessage", map[string]any{"QueueName": "max-receive-dlq"})
	firstMessages, _ := first.Output["Messages"].([]any)
	secondMessages, _ := second.Output["Messages"].([]any)
	dlqMessages, _ := dlq.Output["Messages"].([]any)
	if len(firstMessages) != 1 || len(secondMessages) != 0 || len(dlqMessages) != 1 {
		t.Fatalf("receive counts first=%#v second=%#v dlq=%#v", first.Output, second.Output, dlq.Output)
	}
	if asMap(dlqMessages[0])["MessageId"] != sent.Output["MessageId"] || asMap(dlqMessages[0])["Body"] != "poison" {
		t.Fatalf("dead letter message %#v sent %#v", dlqMessages[0], sent.Output)
	}
	golden.AssertJSON(t, map[string]any{"first": first.Output, "second": second.Output, "dlq": dlq.Output})
}

func FuzzDeadLetterQueueMaxReceiveCount(f *testing.F) {
	f.Add(uint8(1))
	f.Add(uint8(2))
	f.Add(uint8(5))
	f.Fuzz(func(t *testing.T, raw uint8) {
		maxReceiveCount := int(raw%5) + 1
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-dlq"}}); err != nil {
			t.Fatal(err)
		}
		policy := fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:fuzz-dlq","maxReceiveCount":"%d"}`, maxReceiveCount)
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-source", "Attributes": map[string]any{"RedrivePolicy": policy, "VisibilityTimeout": "0"}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-source", "MessageBody": "poison"}}); err != nil {
			t.Fatal(err)
		}
		for range maxReceiveCount + 1 {
			if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-source", "VisibilityTimeout": 0}}); err != nil {
				t.Fatal(err)
			}
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "fuzz-dlq"}})
		if err != nil {
			t.Fatal(err)
		}
		messages, _ := response.Output["Messages"].([]any)
		if len(messages) != 1 || asMap(messages[0])["Body"] != "poison" {
			t.Fatalf("dead-letter response %#v", response.Output)
		}
	})
}

func FuzzFIFOSequenceNumbers(f *testing.F) {
	f.Add(uint8(1))
	f.Add(uint8(3))
	f.Add(uint8(8))
	f.Fuzz(func(t *testing.T, raw uint8) {
		count := int(raw%8) + 1
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "fuzz-sequence.fifo", "Attributes": map[string]any{"FifoQueue": "true"}}}); err != nil {
			t.Fatal(err)
		}
		previous := 0
		for index := 0; index < count; index++ {
			response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "fuzz-sequence.fifo", "MessageBody": "message", "MessageGroupId": "group", "MessageDeduplicationId": fmt.Sprintf("dedup-%d", index)}})
			if err != nil {
				t.Fatal(err)
			}
			sequence, _ := strconv.Atoi(str(response.Output["SequenceNumber"]))
			if sequence <= previous {
				t.Fatalf("sequence %d after %d", sequence, previous)
			}
			previous = sequence
		}
	})
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
				want = map[string]bool{"ApproximateNumberOfMessages": true, "ApproximateNumberOfMessagesDelayed": true, "ApproximateNumberOfMessagesNotVisible": true, "QueueArn": true, "CreatedTimestamp": true, "VisibilityTimeout": true}
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
	f.Add([]byte(`"&quot;&quot;` + "\r"))
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
		received, err := call("ReceiveMessage", map[string]any{"QueueName": "roundtrip", "VisibilityTimeout": 0, "MessageSystemAttributeNames": []any{"All"}})
		if err != nil {
			t.Fatal(err)
		}
		message := received.Output["Messages"].([]any)[0].(map[string]any)
		want := fmt.Sprintf("%x", md5.Sum(body))
		attributes := message["Attributes"].(map[string]any)
		sentAt, sentErr := strconv.ParseInt(attributes["SentTimestamp"].(string), 10, 64)
		first, firstErr := strconv.ParseInt(attributes["ApproximateFirstReceiveTimestamp"].(string), 10, 64)
		if message["Body"] != string(body) || message["MD5OfBody"] != want || sent.Output["MD5OfMessageBody"] != want || sentErr != nil || firstErr != nil || first < sentAt {
			t.Fatalf("sent=%#v received=%#v want=%s", sent.Output, message, want)
		}
	})
}

func FuzzStandardMessageGroupID(f *testing.F) {
	f.Add("group")
	f.Add("")
	f.Add("group 123")
	f.Add(strings.Repeat("a", 128))
	f.Add(strings.Repeat("a", 129))
	f.Fuzz(func(t *testing.T, group string) {
		if len(group) > 256 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "group"}}); err != nil {
			t.Fatal(err)
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "group", "MessageBody": "message", "MessageGroupId": group}})
		if validMessageGroupID(group) {
			if err != nil {
				t.Fatalf("valid group %q rejected: %v", group, err)
			}
			return
		}
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "InvalidParameterValue" {
			t.Fatalf("invalid group %q error %#v", group, err)
		}
	})
}

func FuzzQueueTags(f *testing.F) {
	f.Add("tag", "value")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, key, value string) {
		if len(key) > 256 || len(value) > 1024 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "tags"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "TagQueue", Input: map[string]any{"QueueName": "tags", "Tags": map[string]any{key: value}}}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListQueueTags", Input: map[string]any{"QueueName": "tags"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Output) != 1 || str(asMap(response.Output["Tags"])[key]) != value {
			t.Fatalf("tags %#v", response.Output)
		}
	})
}

func FuzzReceiveMessageMaxNumber(f *testing.F) {
	for _, seed := range []uint8{0, 1, 10, 11, 255} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw uint8) {
		max := int(raw)
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := call("CreateQueue", map[string]any{"QueueName": "max-messages"}); err != nil {
			t.Fatal(err)
		}
		_, err := call("ReceiveMessage", map[string]any{"QueueName": "max-messages", "MaxNumberOfMessages": max})
		if max < 1 || max > 10 {
			fault, _ := err.(*spi.Fault)
			if fault == nil || fault.Code != "InvalidParameterValue" {
				t.Fatalf("max=%d fault %#v", max, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("max=%d: %v", max, err)
		}
	})
}

func FuzzEmptyReceiveOmitsMessages(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(9))
	f.Add(uint8(255))
	f.Fuzz(func(t *testing.T, raw uint8) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := call("CreateQueue", map[string]any{"QueueName": "empty"}); err != nil {
			t.Fatal(err)
		}
		response, err := call("ReceiveMessage", map[string]any{"QueueName": "empty", "MaxNumberOfMessages": int(raw)%10 + 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := response.Output["Messages"]; ok {
			t.Fatalf("empty receive %#v", response.Output)
		}
	})
}

func FuzzReceiveMessageWaitTime(f *testing.F) {
	for _, seed := range []int8{-1, 0, 20, 21, 127} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw int8) {
		wait := int(raw)
		p := New(spitest.Deps(t))
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "wait-time"}}); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if wait > 0 && wait <= 20 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ReceiveMessage", Input: map[string]any{"QueueName": "wait-time", "WaitTimeSeconds": wait}})
		if wait < 0 || wait > 20 {
			fault, _ := err.(*spi.Fault)
			if fault == nil || fault.Code != "InvalidParameterValue" {
				t.Fatalf("wait=%d fault %#v", wait, err)
			}
			return
		}
		if err != nil || response.Output["Messages"] != nil {
			t.Fatalf("wait=%d response %#v error %v", wait, response, err)
		}
	})
}

func FuzzMessagesRemainQueueScoped(f *testing.F) {
	f.Add(false, []byte("message"))
	f.Add(true, []byte("other"))
	f.Fuzz(func(t *testing.T, second bool, body []byte) {
		if len(body) == 0 || len(body) > 1024 || !utf8.Valid(body) {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		for _, name := range []string{"queue-0", "queue-1"} {
			if _, err := call("CreateQueue", map[string]any{"QueueName": name}); err != nil {
				t.Fatal(err)
			}
		}
		target, other := "queue-0", "queue-1"
		if second {
			target, other = other, target
		}
		if _, err := call("SendMessage", map[string]any{"QueueName": target, "MessageBody": string(body)}); err != nil {
			t.Fatal(err)
		}
		empty, err := call("ReceiveMessage", map[string]any{"QueueName": other})
		if err != nil || empty.Output["Messages"] != nil {
			t.Fatalf("other=%s response %#v error %v", other, empty, err)
		}
		received, err := call("ReceiveMessage", map[string]any{"QueueName": target})
		if err != nil {
			t.Fatal(err)
		}
		messages, _ := received.Output["Messages"].([]any)
		if len(messages) != 1 || messages[0].(map[string]any)["Body"] != string(body) {
			t.Fatalf("target=%s response %#v", target, received.Output)
		}
	})
}

func FuzzSendMessageBatchBodies(f *testing.F) {
	f.Add([]byte("batch"))
	f.Add([]byte{0, 1, 2})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := call("CreateQueue", map[string]any{"QueueName": "batch"}); err != nil {
			t.Fatal(err)
		}
		bodies := []string{fmt.Sprintf("%x-0", raw), fmt.Sprintf("%x-1", raw)}
		response, err := call("SendMessageBatch", map[string]any{"QueueName": "batch", "Entries": []any{
			map[string]any{"Id": "0", "MessageBody": bodies[0]},
			map[string]any{"Id": "1", "MessageBody": bodies[1]},
		}})
		if err != nil || len(response.Output["Successful"].([]any)) != 2 {
			t.Fatalf("batch %#v error %v", response.Output, err)
		}
		received, err := call("ReceiveMessage", map[string]any{"QueueName": "batch", "MaxNumberOfMessages": 10, "VisibilityTimeout": 0})
		if err != nil || len(received.Output["Messages"].([]any)) != 2 {
			t.Fatalf("receive %#v error %v", received.Output, err)
		}
		seen := map[string]bool{}
		for _, rawMessage := range received.Output["Messages"].([]any) {
			seen[rawMessage.(map[string]any)["Body"].(string)] = true
		}
		if !seen[bodies[0]] || !seen[bodies[1]] {
			t.Fatalf("batch bodies %#v want %#v", seen, bodies)
		}
	})
}

func FuzzSendMessageBatchEntryCount(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Add(uint8(2))
	f.Add(uint8(11))
	f.Add(uint8(20))
	f.Fuzz(func(t *testing.T, raw uint8) {
		count := int(raw % 21)
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-count"}}); err != nil {
			t.Fatal(err)
		}
		entries := make([]any, count)
		for index := range entries {
			entries[index] = map[string]any{"Id": fmt.Sprintf("%d", index), "MessageBody": fmt.Sprintf("message-%d", index)}
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-count", "Entries": entries}})
		if count == 0 {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "AWS.SimpleQueueService.EmptyBatchRequest" {
				t.Fatalf("empty batch error %#v", err)
			}
			return
		}
		if count > 10 {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "AWS.SimpleQueueService.TooManyEntriesInBatchRequest" || !strings.Contains(fault.Message, fmt.Sprintf("You have sent %d.", count)) {
				t.Fatalf("oversized batch count=%d error %#v", count, err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzMessageSizeBoundary(f *testing.F) {
	f.Add(uint8(1))
	f.Add(uint8(56))
	f.Add(uint8(63))
	f.Fuzz(func(t *testing.T, raw uint8) {
		bodyLength := int(raw%64) + 1
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "size", "Attributes": map[string]any{"MaximumMessageSize": "64"}}}); err != nil {
			t.Fatal(err)
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessage", Input: map[string]any{"QueueName": "size", "MessageBody": strings.Repeat("a", bodyLength), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}}})
		if bodyLength+8 > 64 {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "InvalidParameterValue" {
				t.Fatalf("size=%d error %#v", bodyLength, err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzSendMessageBatchSizeBoundary(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Fuzz(func(t *testing.T, raw uint8) {
		delta := int(raw % 2)
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "batch-size"}}); err != nil {
			t.Fatal(err)
		}
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SendMessageBatch", Input: map[string]any{"QueueName": "batch-size", "Entries": []any{map[string]any{
			"Id": "1", "MessageBody": strings.Repeat("a", (1<<20)-8+delta), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}},
		}}}})
		if delta == 1 {
			fault, ok := err.(*spi.Fault)
			if !ok || fault.Code != "AWS.SimpleQueueService.BatchRequestTooLong" {
				t.Fatalf("batch delta=%d error %#v", delta, err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	})
}
