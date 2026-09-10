package engine_test

import (
	"context"
	"testing"
	"time"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// These exercise what a recorded trace cannot: behavior that depends on time
// passing. The recording proves the bundle answers like the pack for a fixed
// sequence at one instant; these prove the deadlines it arms actually fire, and
// fire when they should.
//
// aws.sqs is a shadow bundle -- gated but not yet serving, because six of its
// operations are not expressed yet -- so it is built directly rather than
// through the registry.

const sqsID = "aws.sqs"

type sqsFixture struct {
	pack  spi.BehaviorPack
	clock *clock.Controllable
	t     *testing.T
}

func newSQS(t *testing.T) *sqsFixture {
	t.Helper()
	svc, err := generated.Model(sqsID)
	if err != nil {
		t.Fatal(err)
	}
	ir, err := behaviors.Load(sqsID, svc)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	deps := spitest.Deps(t)
	clk, ok := deps.Clock.(*clock.Controllable)
	if !ok {
		t.Fatalf("want a controllable clock, got %T", deps.Clock)
	}
	pack, err := engine.New(deps, ir, svc)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return &sqsFixture{pack: pack, clock: clk, t: t}
}

func (f *sqsFixture) call(op string, in map[string]any) (map[string]any, error) {
	f.t.Helper()
	resp, err := f.pack.Invoke(context.Background(), &spi.Request{
		ServiceID: sqsID,
		Operation: op,
		Input:     in,
		Identity:  spi.Identity{Account: "000000000000", Region: "us-east-1"},
	})
	if err != nil {
		return nil, err
	}
	return resp.Output, nil
}

func (f *sqsFixture) must(op string, in map[string]any) map[string]any {
	f.t.Helper()
	out, err := f.call(op, in)
	if err != nil {
		f.t.Fatalf("%s: %v", op, err)
	}
	return out
}

func (f *sqsFixture) queue(name string, attrs map[string]any) string {
	f.t.Helper()
	in := map[string]any{"QueueName": name}
	if attrs != nil {
		in["Attributes"] = attrs
	}
	return f.must("CreateQueue", in)["QueueUrl"].(string)
}

func (f *sqsFixture) receive(url string, max int) []any {
	f.t.Helper()
	out := f.must("ReceiveMessage", map[string]any{"QueueUrl": url, "MaxNumberOfMessages": max})
	items, _ := out["Messages"].([]any)
	return items
}

// TestVisibilityTimeoutExpiresLazily is the point of storing deadlines instead
// of scheduling callbacks: nothing runs while the message is invisible, and the
// next observation after the deadline is what makes it visible again.
func TestVisibilityTimeoutExpiresLazily(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q", map[string]any{"VisibilityTimeout": "30"})
	f.must("SendMessage", map[string]any{"QueueUrl": url, "MessageBody": "hello"})

	if got := f.receive(url, 10); len(got) != 1 {
		t.Fatalf("first receive got %d messages, want 1", len(got))
	}
	if got := f.receive(url, 10); len(got) != 0 {
		t.Fatalf("the message is invisible; got %d", len(got))
	}

	if err := f.clock.Advance(29 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := f.receive(url, 10); len(got) != 0 {
		t.Fatalf("visible one second early; got %d", len(got))
	}

	if err := f.clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	got := f.receive(url, 10)
	if len(got) != 1 {
		t.Fatalf("the timeout expired but the message did not return; got %d", len(got))
	}
	// The redelivery is counted, which is what a redrive policy acts on.
	attrs := got[0].(map[string]any)["Attributes"].(map[string]any)
	if attrs["ApproximateReceiveCount"] != "2" {
		t.Fatalf("receive count %v, want 2", attrs["ApproximateReceiveCount"])
	}
}

// TestDelaySecondsHoldsAMessageBack: a delayed message is born invisible with
// its reappearance already armed, rather than needing anything to deliver it.
func TestDelaySecondsHoldsAMessageBack(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q", nil)
	f.must("SendMessage", map[string]any{"QueueUrl": url, "MessageBody": "later", "DelaySeconds": 10})

	if got := f.receive(url, 10); len(got) != 0 {
		t.Fatalf("a delayed message was delivered immediately: %v", got)
	}
	if err := f.clock.Advance(11 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := f.receive(url, 10); len(got) != 1 {
		t.Fatalf("the delay expired but the message did not appear; got %d", len(got))
	}
}

// TestChangeMessageVisibilityRearmsTheDeadline covers the transition that
// re-arms an existing timer rather than starting a new lifecycle.
func TestChangeMessageVisibilityRearmsTheDeadline(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q", map[string]any{"VisibilityTimeout": "30"})
	f.must("SendMessage", map[string]any{"QueueUrl": url, "MessageBody": "hello"})
	got := f.receive(url, 10)
	handle := got[0].(map[string]any)["ReceiptHandle"].(string)

	f.must("ChangeMessageVisibility", map[string]any{
		"QueueUrl": url, "ReceiptHandle": handle, "VisibilityTimeout": 5,
	})
	if err := f.clock.Advance(6 * time.Second); err != nil {
		t.Fatal(err)
	}
	if n := len(f.receive(url, 10)); n != 1 {
		t.Fatalf("a shortened visibility timeout did not expire; got %d", n)
	}
}

// TestRedriveMovesToTheDeadLetterQueue exercises the move action: the message
// leaves one collection and arrives in another with a fresh receipt handle, in
// one declared step.
func TestRedriveMovesToTheDeadLetterQueue(t *testing.T) {
	f := newSQS(t)
	dlqURL := f.queue("dlq", nil)
	srcURL := f.queue("src", map[string]any{
		"VisibilityTimeout": "0",
		"RedrivePolicy": `{"maxReceiveCount":2,` +
			`"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`,
	})
	f.must("SendMessage", map[string]any{"QueueUrl": srcURL, "MessageBody": "poison"})

	// Two deliveries are within the policy. The third exceeds it, and is the
	// one that moves the message -- the bundle transcribes the pack, which
	// decides redrive at receive and still returns the message it is moving.
	// Real SQS moves it without delivering it again; the bundle records that
	// disagreement as a quirk rather than silently correcting it here.
	for i := 1; i <= 3; i++ {
		if n := len(f.receive(srcURL, 10)); n != 1 {
			t.Fatalf("delivery %d got %d messages, want 1", i, n)
		}
	}
	if n := len(f.receive(srcURL, 10)); n != 0 {
		t.Fatalf("the message stayed in the source queue after exceeding maxReceiveCount; got %d", n)
	}
	moved := f.receive(dlqURL, 10)
	if len(moved) != 1 {
		t.Fatalf("the message did not arrive in the dead-letter queue; got %d", len(moved))
	}
	if body := moved[0].(map[string]any)["Body"]; body != "poison" {
		t.Fatalf("dead-letter message body %v", body)
	}
}

// TestFifoGroupIsExclusiveWhileInFlight: ordering within a group means the
// second message waits for the first to settle, while another group is free to
// be delivered in the same batch.
func TestFifoGroupIsExclusiveWhileInFlight(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q.fifo", map[string]any{
		"ContentBasedDeduplication": "true", "VisibilityTimeout": "30",
	})
	for _, m := range []struct{ body, group string }{
		{"a1", "g1"}, {"a2", "g1"}, {"b1", "g2"},
	} {
		f.must("SendMessage", map[string]any{
			"QueueUrl": url, "MessageBody": m.body, "MessageGroupId": m.group,
		})
	}

	first := f.receive(url, 10)
	if len(first) != 2 {
		t.Fatalf("want one message per group, got %d", len(first))
	}
	bodies := map[string]bool{}
	for _, m := range first {
		bodies[m.(map[string]any)["Body"].(string)] = true
	}
	if !bodies["a1"] || !bodies["b1"] {
		t.Fatalf("want the head of each group, got %v", bodies)
	}
	if n := len(f.receive(url, 10)); n != 0 {
		t.Fatalf("both groups have a message in flight; got %d", n)
	}

	// Deleting g1's head opens the group; g2's is still in flight.
	handle := ""
	for _, m := range first {
		rec := m.(map[string]any)
		if rec["Body"] == "a1" {
			handle = rec["ReceiptHandle"].(string)
		}
	}
	f.must("DeleteMessage", map[string]any{"QueueUrl": url, "ReceiptHandle": handle})
	next := f.receive(url, 10)
	if len(next) != 1 || next[0].(map[string]any)["Body"] != "a2" {
		t.Fatalf("want a2 once g1 was released, got %v", next)
	}
}

// TestDeduplicationWindowLapses: a duplicate inside the window replays the
// first answer and writes nothing; past it, the message is accepted.
func TestDeduplicationWindowLapses(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q.fifo", map[string]any{"ContentBasedDeduplication": "true"})
	send := func() string {
		f.t.Helper()
		out := f.must("SendMessage", map[string]any{
			"QueueUrl": url, "MessageBody": "same", "MessageGroupId": "g",
		})
		return out["MessageId"].(string)
	}
	first := send()
	if second := send(); second != first {
		t.Fatalf("a duplicate got a new MessageId: %s then %s", first, second)
	}
	if n := len(f.receive(url, 10)); n != 1 {
		t.Fatalf("the duplicate was enqueued; the queue holds %d messages", n)
	}

	if err := f.clock.Advance(6 * time.Minute); err != nil {
		t.Fatal(err)
	}
	if third := send(); third == first {
		t.Fatal("the deduplication window did not lapse")
	}
}

// TestLongPollReturnsEarlyWhenTimeAdvances: the wait re-observes rather than
// sleeping blindly, so a deadline that expires during the poll is picked up by
// the poll itself.
func TestLongPollReturnsEarlyWhenTimeAdvances(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q", map[string]any{"VisibilityTimeout": "5"})
	f.must("SendMessage", map[string]any{"QueueUrl": url, "MessageBody": "hello"})
	f.receive(url, 10) // now invisible for five seconds

	done := make(chan int, 1)
	go func() {
		out, err := f.call("ReceiveMessage", map[string]any{
			"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 20,
		})
		if err != nil {
			done <- -1
			return
		}
		items, _ := out["Messages"].([]any)
		done <- len(items)
	}()

	// Let the poll park, then step past the visibility timeout.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case n := <-done:
			if n != 1 {
				t.Fatalf("long poll returned %d messages, want 1", n)
			}
			return
		case <-deadline:
			t.Fatal("long poll did not return after the visibility timeout expired")
		default:
			if err := f.clock.Advance(time.Second); err != nil {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestSendMessageBatchIsSendMessage: the batch shape delegates rather than
// reimplementing, so a per-entry failure lands in Failed while the rest succeed.
func TestSendMessageBatchIsSendMessage(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q.fifo", map[string]any{"ContentBasedDeduplication": "true"})
	out := f.must("SendMessageBatch", map[string]any{
		"QueueUrl": url,
		"Entries": []any{
			map[string]any{"Id": "ok", "MessageBody": "a", "MessageGroupId": "g"},
			// No group on a FIFO queue: the singular operation rejects it, and
			// the batch reports that entry rather than failing as a whole.
			map[string]any{"Id": "bad", "MessageBody": "b"},
		},
	})
	successful, _ := out["Successful"].([]any)
	failed, _ := out["Failed"].([]any)
	if len(successful) != 1 || len(failed) != 1 {
		t.Fatalf("want one success and one failure, got %d and %d", len(successful), len(failed))
	}
	if id := successful[0].(map[string]any)["Id"]; id != "ok" {
		t.Fatalf("successful entry %v", id)
	}
	bad := failed[0].(map[string]any)
	if bad["Id"] != "bad" || bad["Code"] != "MissingParameter" || bad["SenderFault"] != true {
		t.Fatalf("failed entry %v", bad)
	}
}

// TestPurgeQueueEmptiesEverything covers driving a chart over a whole
// selection, including messages that are currently invisible.
func TestPurgeQueueEmptiesEverything(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q", map[string]any{"VisibilityTimeout": "300"})
	for _, body := range []string{"a", "b", "c"} {
		f.must("SendMessage", map[string]any{"QueueUrl": url, "MessageBody": body})
	}
	f.receive(url, 1) // one is now invisible

	f.must("PurgeQueue", map[string]any{"QueueUrl": url})
	attrs := f.must("GetQueueAttributes", map[string]any{"QueueUrl": url})["Attributes"].(map[string]any)
	if attrs["ApproximateNumberOfMessages"] != "0" {
		t.Fatalf("purge left %v messages", attrs["ApproximateNumberOfMessages"])
	}
}
