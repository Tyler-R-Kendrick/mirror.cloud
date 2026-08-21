package sns

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestTopicSubscribePublish(t *testing.T) {
	deps := spitest.Deps(t)
	p := &Pack{deps: deps}
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
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
	ctx := context.Background()
	id := spi.Identity{Account: "1", Region: "us-east-1"}
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
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Subscribe", Input: map[string]any{"TopicArn": arn, "Protocol": "lambda", "Endpoint": "fn"}}); err == nil {
		t.Fatal("lambda should 501")
	}
}
