package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	looks *lookCounter
	t     *testing.T
}

// lookCounter counts the times a queue's messages are listed: once by every
// selection, and once each time a wait reads their deadlines.
type lookCounter struct {
	spi.Store
	mu sync.Mutex
	n  int
}

func (c *lookCounter) Scope(account, region string) spi.Scope {
	return lookScope{Scope: c.Store.Scope(account, region), c: c}
}

// mark starts the count again.
func (c *lookCounter) mark() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n = 0
}

func (c *lookCounter) since() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

type lookScope struct {
	spi.Scope
	c *lookCounter
}

func (s lookScope) Collection(name string) spi.Collection {
	col := s.Scope.Collection(name)
	if !strings.HasPrefix(name, "msgs:") {
		return col
	}
	return lookCollection{Collection: col, c: s.c}
}

type lookCollection struct {
	spi.Collection
	c *lookCounter
}

func (l lookCollection) List(ctx context.Context, prefix, after string, limit int) ([]spi.KV, bool, error) {
	l.c.mu.Lock()
	l.c.n++
	l.c.mu.Unlock()
	return l.Collection.List(ctx, prefix, after, limit)
}

func newSQS(t *testing.T, around ...func(*clock.Controllable) spi.Clock) *sqsFixture {
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
	for _, wrap := range around {
		deps.Clock = wrap(clk)
	}
	looks := &lookCounter{Store: deps.Store}
	deps.Store = looks
	pack, err := engine.New(deps, ir, svc)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return &sqsFixture{pack: pack, clock: clk, looks: looks, t: t}
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
	if attrs == nil {
		attrs = map[string]any{}
	}
	if strings.HasSuffix(name, ".fifo") {
		if _, ok := attrs["FifoQueue"]; !ok {
			attrs["FifoQueue"] = "true"
		}
	}
	if len(attrs) > 0 {
		in["Attributes"] = attrs
	}
	return f.must("CreateQueue", in)["QueueUrl"].(string)
}

func (f *sqsFixture) receive(url string, max int) []any {
	f.t.Helper()
	out := f.must("ReceiveMessage", map[string]any{
		"QueueUrl": url, "MaxNumberOfMessages": max,
		"AttributeNames": []any{"All"},
	})
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

	// Two deliveries are within the policy. The third exceeds it and moves
	// the message without returning it -- pack parity (afterReceive skips).
	for i := 1; i <= 2; i++ {
		if n := len(f.receive(srcURL, 10)); n != 1 {
			t.Fatalf("delivery %d got %d messages, want 1", i, n)
		}
	}
	if n := len(f.receive(srcURL, 10)); n != 0 {
		t.Fatalf("redrive delivery should omit the message; got %d", n)
	}
	moved := f.receive(dlqURL, 10)
	if len(moved) != 1 {
		t.Fatalf("the message did not arrive in the dead-letter queue; got %d", len(moved))
	}
	if body := moved[0].(map[string]any)["Body"]; body != "poison" {
		t.Fatalf("dead-letter message body %v", body)
	}
}

// TestFifoGroupIsExclusiveWhileInFlight: a group with a message in flight is
// partial and sorts behind complete groups; within a group, consecutive
// visible messages drain up to the limit (pack FIFO receive).
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

	// Cap at 2: drain takes a1 then a2 from g1 before opening g2.
	first := f.receive(url, 2)
	if len(first) != 2 {
		t.Fatalf("want g1 drained to the limit, got %d", len(first))
	}
	if first[0].(map[string]any)["Body"] != "a1" || first[1].(map[string]any)["Body"] != "a2" {
		t.Fatalf("want a1 then a2, got %v", first)
	}
	// g2 is still complete and free; g1 is fully in flight.
	next := f.receive(url, 10)
	if len(next) != 1 || next[0].(map[string]any)["Body"] != "b1" {
		t.Fatalf("want b1 from the untouched group, got %v", next)
	}
	if n := len(f.receive(url, 10)); n != 0 {
		t.Fatalf("everything is in flight; got %d", n)
	}

	for _, m := range first {
		f.must("DeleteMessage", map[string]any{
			"QueueUrl": url, "ReceiptHandle": m.(map[string]any)["ReceiptHandle"],
		})
	}
	f.must("DeleteMessage", map[string]any{
		"QueueUrl": url, "ReceiptHandle": next[0].(map[string]any)["ReceiptHandle"],
	})
	if n := len(f.receive(url, 10)); n != 0 {
		t.Fatalf("queue should be empty, got %d", n)
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

// steppingClock moves the clock once, by step, inside the nth reading of it
// after arm: the instant passes between two things the engine does, which is
// where a wait can lose a deadline. parked is told each time something waits.
type steppingClock struct {
	*clock.Controllable
	mu     sync.Mutex
	left   int
	step   time.Duration
	onStep func() // called as the step is taken
	parked chan struct{}
}

func (c *steppingClock) arm(n int, step time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.left, c.step = n, step
}

// spent reports whether the step has been taken.
func (c *steppingClock) spent() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.left == 0
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	fire := c.left == 1
	if c.left > 0 {
		c.left--
	}
	c.mu.Unlock()
	if fire {
		c.onStep()
		_ = c.Controllable.Advance(c.step)
	}
	return c.Controllable.Now()
}

func (c *steppingClock) AfterTime(at time.Time) <-chan time.Time {
	ch := c.Controllable.AfterTime(at)
	select {
	case c.parked <- struct{}{}:
	default:
	}
	return ch
}

// TestLongPollDoesNotLoseADeadlineThatPassesWhileItLooks: a wait selects, finds
// nothing, and then works out when to wake. If the clock reaches a record's
// deadline between those two steps, the deadline is neither selected nor in the
// future, and a wait that only counted future deadlines parked on its own
// timeout with the message already visible: late on a real clock, for ever on
// a controllable one.
//
// The interleaving is forced, not raced for: the clock is stepped over the
// deadline inside the nth reading of it, for every n the poll makes before it
// parks. Left to the scheduler the window is hit a few times in a thousand.
func TestLongPollDoesNotLoseADeadlineThatPassesWhileItLooks(t *testing.T) {
	stepOverADeadlineInEveryReading(t, time.Millisecond, nil, 1, 999*time.Millisecond, 1)
}

// TestLongPollLooksOnceMoreWhenItsTimeoutPassesWithADeadline: the same window,
// with a step that carries the clock over the poll's own timeout as well as
// the record's deadline. The poll is out of time, and the message is
// selectable: it selects once more and answers it, where it used to see only
// that its time was up and answer empty. Once, not until something turns up:
// the times it looks at the messages after the step are counted, and are one
// reading of their deadlines and exactly one selection.
func TestLongPollLooksOnceMoreWhenItsTimeoutPassesWithADeadline(t *testing.T) {
	stepOverADeadlineInEveryReading(t, 3*time.Second, nil, 1, 999*time.Millisecond, 1)
}

// TestLongPollOutOfTimeLooksOnceAndNoMore: the deadline that comes due with
// the timeout makes nothing selectable -- the message reappears a second after
// the queue stopped retaining it. The one more selection finds nothing, and
// the poll answers empty there and then: it does not go round again.
func TestLongPollOutOfTimeLooksOnceAndNoMore(t *testing.T) {
	stepOverADeadlineInEveryReading(t, 3*time.Second, map[string]any{"MessageRetentionPeriod": "60"}, 61, 60*time.Second+999*time.Millisecond, 0)
}

// stepOverADeadlineInEveryReading receives a message for hidden seconds, waits
// lead -- a millisecond short of that -- and long-polls for two seconds,
// stepping the clock by step inside the nth reading the poll makes of it, for
// every n until the poll parks without one. Every poll answers want messages.
func stepOverADeadlineInEveryReading(t *testing.T, step time.Duration, attrs map[string]any, hidden int, lead time.Duration, want int) {
	t.Helper()
	var clk *steppingClock
	f := newSQS(t, func(c *clock.Controllable) spi.Clock {
		clk = &steppingClock{Controllable: c, parked: make(chan struct{}, 1)}
		return clk
	})
	clk.onStep = f.looks.mark
	// Rounds in which the step fell between a selection and the wait's next
	// reading of the deadlines: the window itself.
	reselected := 0
	for n := 1; ; n++ {
		// A queue a round, on one engine: building one costs more than the round.
		var queueAttrs map[string]any
		if attrs != nil {
			queueAttrs = map[string]any{}
			for k, v := range attrs {
				queueAttrs[k] = v
			}
		}
		url := f.queue(fmt.Sprintf("q%d", n), queueAttrs)
		select {
		case <-clk.parked:
		default:
		}
		f.must("SendMessage", map[string]any{"QueueUrl": url, "MessageBody": "hello"})
		if got := f.must("ReceiveMessage", map[string]any{"QueueUrl": url, "VisibilityTimeout": hidden})["Messages"].([]any); len(got) != 1 {
			t.Fatalf("receive got %d messages, want 1", len(got))
		}
		if err := f.clock.Advance(lead); err != nil {
			t.Fatal(err)
		}

		clk.arm(n, step)
		done := make(chan int, 1)
		go func() {
			out, err := f.call("ReceiveMessage", map[string]any{"QueueUrl": url, "WaitTimeSeconds": 2})
			if err != nil {
				done <- -1
				return
			}
			items, _ := out["Messages"].([]any)
			done <- len(items)
		}()
		hung := time.After(10 * time.Second)
		last, waited := false, false
		for got := false; !got; {
			select {
			case messages := <-done:
				if messages != want {
					t.Fatalf("clock stepped %s in reading %d: long poll returned %d messages, want %d", step, n, messages, want)
				}
				// A poll the step left with nothing to wait for looks at the
				// messages at most twice more: once for their deadlines, once
				// to select. A step inside one of the two leaves one look.
				after := f.looks.since()
				if waited {
					after = 0
				}
				if after > 2 {
					t.Fatalf("clock stepped %s in reading %d: the poll looked at the messages %d times after the step, want at most a deadline read and one selection", step, n, after)
				}
				if after == 2 {
					reselected++
				}
				got = true
			case <-clk.parked:
				// Parked with the step never taken: the poll reads the clock
				// fewer than n times before it waits, so every reading has
				// been tried. Take the step from outside, as a test would.
				if !clk.spent() {
					last = true
					clk.arm(0, 0)
					f.looks.mark()
					if err := f.clock.Advance(step); err != nil {
						t.Fatal(err)
					}
				} else {
					// Parked after the step: it fell before the poll set its
					// timeout, and there is nothing selectable to wake it.
					// Let the two seconds run out, as a clock would.
					waited = true
					if err := f.clock.Advance(2 * time.Second); err != nil {
						t.Fatal(err)
					}
				}
			case <-hung:
				t.Fatalf("clock stepped over the deadline in reading %d: the long poll never returned", n)
			}
		}
		if last {
			if n < 3 {
				t.Fatalf("the poll parked after %d readings of the clock; the step was never inside it", n-1)
			}
			if reselected == 0 {
				t.Fatal("no step fell between a selection and the deadlines' reading: the window was never entered")
			}
			return
		}
	}
}

// TestSendMessageBatchIsSendMessage: the batch shape delegates rather than
// reimplementing, so a per-entry failure lands in Failed while the rest succeed.
// FIFO missing-group is a whole-batch fault (pack); use a standard queue here.
func TestSendMessageBatchIsSendMessage(t *testing.T) {
	f := newSQS(t)
	url := f.queue("q", nil)
	out := f.must("SendMessageBatch", map[string]any{
		"QueueUrl": url,
		"Entries": []any{
			map[string]any{"Id": "ok", "MessageBody": "a"},
			// Empty body: the singular operation rejects it, and the batch
			// reports that entry rather than failing as a whole.
			map[string]any{"Id": "bad", "MessageBody": ""},
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
	attrs := f.must("GetQueueAttributes", map[string]any{"QueueUrl": url, "AttributeNames": []any{"All"}})["Attributes"].(map[string]any)
	if attrs["ApproximateNumberOfMessages"] != "0" {
		t.Fatalf("purge left %v messages", attrs["ApproximateNumberOfMessages"])
	}
}
