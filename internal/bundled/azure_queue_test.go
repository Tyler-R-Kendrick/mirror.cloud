package bundled_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func queuePack(t testing.TB) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New("azure.queue", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAzureQueueMessages(t *testing.T) {
	p := queuePack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res
	}
	fault := func(op string, in map[string]any, status int, code string) {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		f, ok := err.(*spi.Fault)
		if !ok || f.HTTPStatus != status || f.Code != code {
			t.Fatalf("%s: got %#v, want %d %s", op, err, status, code)
		}
	}

	inv("CreateQueue", map[string]any{"queue": "q", "metadata": map[string]any{"app": "demo"}})
	props := inv("GetQueueProperties", map[string]any{"queue": "q"})
	if md, _ := props.Output["metadata"].(map[string]any); fmt.Sprint(md["app"]) != "demo" || props.Output["approx_count"] != "0" {
		t.Fatalf("queue properties %#v", props.Output)
	}
	inv("SetQueueMetadata", map[string]any{"queue": "q", "metadata": map[string]any{"app": "v2"}})
	props = inv("GetQueueProperties", map[string]any{"queue": "q"})
	if md, _ := props.Output["metadata"].(map[string]any); fmt.Sprint(md["app"]) != "v2" {
		t.Fatalf("queue metadata %#v", props.Output)
	}
	fault("GetQueueProperties", map[string]any{"queue": "missing"}, 404, "QueueNotFound")
	inv("SetQueueAcl", map[string]any{"queue": "q", "acl": "<SignedIdentifiers><SignedIdentifier><Id>i1</Id></SignedIdentifier></SignedIdentifiers>"})
	if got := inv("GetQueueAcl", map[string]any{"queue": "q"}); !strings.Contains(fmt.Sprint(got.Output["acl"]), "<Id>i1</Id>") {
		t.Fatalf("queue acl %#v", got.Output)
	}

	// Service properties store/echo; stats is primary-400 like blobs.
	sp := inv("GetServiceProperties", map[string]any{})
	if fmt.Sprint(sp.Output["properties"]) != "" {
		t.Fatalf("default service properties %#v", sp.Output)
	}
	inv("SetServiceProperties", map[string]any{"body": "<StorageServiceProperties><Cors/></StorageServiceProperties>"})
	sp = inv("GetServiceProperties", map[string]any{})
	if !strings.Contains(fmt.Sprint(sp.Output["properties"]), "<Cors/>") {
		t.Fatalf("stored service properties %#v", sp.Output)
	}
	fault("GetServiceStats", map[string]any{}, 400, "InvalidQueryParameterValue")
	if got := inv("GetServiceStats", map[string]any{"secondary": true}); got.Output["geo_status"] != "live" {
		t.Fatalf("secondary stats %#v", got.Output)
	}

	// Enqueue two; peek shows the first without a receipt; dequeue flips it.
	m1 := inv("PutMessage", map[string]any{"queue": "q", "message": "m1"})
	if fmt.Sprint(m1.Output["id"]) == "" || fmt.Sprint(m1.Output["pop_receipt"]) == "" || fmt.Sprint(m1.Output["expires_at"]) != "604800" {
		t.Fatalf("enqueue output %#v", m1.Output)
	}
	putReceipt := fmt.Sprint(m1.Output["pop_receipt"])
	mid1 := fmt.Sprint(m1.Output["id"])
	inv("PutMessage", map[string]any{"queue": "q", "message": "m2"})

	props = inv("GetQueueProperties", map[string]any{"queue": "q"})
	if props.Output["approx_count"] != "2" {
		t.Fatalf("approx count %#v", props.Output)
	}
	peek := inv("PeekMessages", map[string]any{"queue": "q"})
	pitems, _ := peek.Output["_list"].([]any)
	if len(pitems) != 1 || pitems[0].(map[string]any)["message"] != "m1" {
		t.Fatalf("peek %#v", peek.Output)
	}
	if _, has := pitems[0].(map[string]any)["pop_receipt"]; has {
		t.Fatalf("peek carries a receipt %#v", pitems[0])
	}

	dq := inv("GetMessages", map[string]any{"queue": "q"})
	ditems, _ := dq.Output["_list"].([]any)
	if len(ditems) != 1 || ditems[0].(map[string]any)["message"] != "m1" || fmt.Sprint(ditems[0].(map[string]any)["dequeue_count"]) != "1" {
		t.Fatalf("dequeue %#v", dq.Output)
	}
	receipt := fmt.Sprint(ditems[0].(map[string]any)["pop_receipt"])
	if receipt == "" || receipt == putReceipt {
		t.Fatalf("receipt not rotated %#v", dq.Output)
	}
	// Invisible now: the next dequeue sees m2.
	dq = inv("GetMessages", map[string]any{"queue": "q"})
	ditems, _ = dq.Output["_list"].([]any)
	if len(ditems) != 1 || ditems[0].(map[string]any)["message"] != "m2" {
		t.Fatalf("second dequeue %#v", dq.Output)
	}
	// numofmessages=2 dequeues the remaining visibility window... m2 just went
	// invisible too, so this is empty.
	dq = inv("GetMessages", map[string]any{"queue": "q", "numofmessages": "2"})
	if n := len(dq.Output["_list"].([]any)); n != 0 {
		t.Fatalf("third dequeue %#v", dq.Output)
	}

	// Update: stale and wrong receipts 400, the current one works.
	fault("UpdateMessage", map[string]any{"queue": "q", "messageid": mid1, "popreceipt": putReceipt, "visibilitytimeout": "5", "message": "x"}, 400, "PopReceiptMismatch")
	upd := inv("UpdateMessage", map[string]any{"queue": "q", "messageid": mid1, "popreceipt": receipt, "visibilitytimeout": "5", "message": "changed"})
	if fmt.Sprint(upd.Output["pop_receipt"]) == "" || fmt.Sprint(upd.Output["visible_at"]) != "5" {
		t.Fatalf("update %#v", upd.Output)
	}
	fault("UpdateMessage", map[string]any{"queue": "q", "messageid": "nope", "popreceipt": "x", "visibilitytimeout": "5", "message": "x"}, 404, "MessageNotFound")

	// Delete: the pre-update receipt is stale now; the update one works.
	fault("DeleteMessage", map[string]any{"queue": "q", "messageid": mid1, "popreceipt": receipt}, 400, "PopReceiptMismatch")
	inv("DeleteMessage", map[string]any{"queue": "q", "messageid": mid1, "popreceipt": fmt.Sprint(upd.Output["pop_receipt"])})
	fault("DeleteMessage", map[string]any{"queue": "q", "messageid": mid1, "popreceipt": "x"}, 404, "MessageNotFound")

	// Clear empties the queue.
	inv("ClearMessages", map[string]any{"queue": "q"})
	peek = inv("PeekMessages", map[string]any{"queue": "q"})
	if n := len(peek.Output["_list"].([]any)); n != 0 {
		t.Fatalf("peek after clear %#v", peek.Output)
	}

	// Validation: ranges, sizes, malformed bodies.
	fault("PutMessage", map[string]any{"queue": "q", "message": "x", "visibilitytimeout": "691200"}, 400, "OutOfRangeQueryParameterValue")
	fault("PutMessage", map[string]any{"queue": "q", "message": "x", "visibilitytimeout": "-1"}, 400, "OutOfRangeQueryParameterValue")
	fault("PutMessage", map[string]any{"queue": "q", "message": "x", "messagettl": "0"}, 400, "OutOfRangeQueryParameterValue")
	_ = inv("PutMessage", map[string]any{"queue": "q", "message": "x", "messagettl": "-1"})
	fault("PutMessage", map[string]any{"queue": "q", "message": strings.Repeat("a", 65537)}, 400, "RequestBodyTooLarge")
	fault("GetMessages", map[string]any{"queue": "q", "numofmessages": "33"}, 400, "OutOfRangeQueryParameterValue")
	fault("PutMessage", map[string]any{"queue": "q", "body_invalid": true}, 400, "InvalidXmlDocument")
}

// A message TTL or a visibility timeout of N seconds lasts N seconds from the
// instant it was set, wherever in the clock second that instant fell. Whole
// seconds are only how the times are reported.
func TestAzureQueueLifetimesAreNotTruncated(t *testing.T) {
	deps := spitest.Deps(t)
	p, err := bundled.New("azure.queue", deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res
	}
	advance := func(d time.Duration) {
		t.Helper()
		if err := deps.Clock.Advance(d); err != nil {
			t.Fatal(err)
		}
	}
	peeked := func() string {
		t.Helper()
		var texts []string
		for _, m := range inv("PeekMessages", map[string]any{"queue": "q", "numofmessages": "32"}).Output["_list"].([]any) {
			texts = append(texts, fmt.Sprint(m.(map[string]any)["message"]))
		}
		return strings.Join(texts, ",")
	}
	count := func() string {
		t.Helper()
		return fmt.Sprint(inv("GetQueueProperties", map[string]any{"queue": "q"}).Output["approx_count"])
	}
	inv("CreateQueue", map[string]any{"queue": "q"})

	// 0.9 s into a second: everything below is set just before a boundary.
	advance(900 * time.Millisecond)
	short := inv("PutMessage", map[string]any{"queue": "q", "message": "short", "messagettl": "1"})
	if fmt.Sprint(short.Output["inserted_at"]) != "0" || fmt.Sprint(short.Output["expires_at"]) != "1" {
		t.Fatalf("reported times are whole seconds %#v", short.Output)
	}
	inv("PutMessage", map[string]any{"queue": "q", "message": "delayed", "visibilitytimeout": "1"})

	// 0.2 s later the clock second has turned; neither second has passed.
	advance(200 * time.Millisecond)
	if got := peeked(); got != "short" {
		t.Fatalf("0.2s after put: peek %q, want only the unexpired short message", got)
	}
	if got := count(); got != "2" {
		t.Fatalf("0.2s after put: approx count %s, want 2", got)
	}
	dq := inv("GetMessages", map[string]any{"queue": "q", "visibilitytimeout": "1"}).Output["_list"].([]any)
	if len(dq) != 1 || dq[0].(map[string]any)["message"] != "short" {
		t.Fatalf("0.2s after put: dequeue %#v", dq)
	}
	shortID, receipt := fmt.Sprint(dq[0].(map[string]any)["id"]), fmt.Sprint(dq[0].(map[string]any)["pop_receipt"])
	upd := inv("UpdateMessage", map[string]any{"queue": "q", "messageid": shortID, "popreceipt": receipt, "visibilitytimeout": "1", "message": "short"})

	// 0.999 s after the put: still a millisecond of TTL and of delay left.
	advance(799 * time.Millisecond)
	if got := peeked(); got != "" {
		t.Fatalf("0.999s after put: peek %q, want nothing visible yet", got)
	}
	if got := count(); got != "2" {
		t.Fatalf("0.999s after put: approx count %s, want 2", got)
	}

	// Exactly 1 s after the put: the TTL is spent and the delay is over.
	advance(time.Millisecond)
	if got := peeked(); got != "delayed" {
		t.Fatalf("1s after put: peek %q, want only the delayed message", got)
	}
	if got := count(); got != "1" {
		t.Fatalf("1s after put: approx count %s, want 1", got)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMessage", Input: map[string]any{"queue": "q", "messageid": shortID, "popreceipt": fmt.Sprint(upd.Output["pop_receipt"])}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "MessageNotFound" {
		t.Fatalf("delete of an expired message: %#v", err)
	}

	// A dequeue's visibility timeout, taken late in a second, also runs whole.
	inv("PutMessage", map[string]any{"queue": "q", "message": "again"})
	advance(950 * time.Millisecond) // 2.85 s
	if dq := inv("GetMessages", map[string]any{"queue": "q", "visibilitytimeout": "1", "numofmessages": "2"}).Output["_list"].([]any); len(dq) != 2 {
		t.Fatalf("dequeue both %#v", dq)
	}
	advance(999 * time.Millisecond)
	if got := peeked(); got != "" {
		t.Fatalf("0.999s after dequeue: peek %q, want nothing", got)
	}
	advance(time.Millisecond)
	if got := peeked(); got != "delayed,again" {
		t.Fatalf("1s after dequeue: peek %q", got)
	}

	// A record written before the millisecond deadlines existed (a restored
	// snapshot) is still served, on its whole-second times.
	inv("ClearMessages", map[string]any{"queue": "q"})
	old := inv("PutMessage", map[string]any{"queue": "q", "message": "old", "messagettl": "2"})
	col := deps.Store.Scope(id.Account, id.Region).Collection("azqmsg:q")
	raw, ok, err := col.Get(ctx, fmt.Sprint(old.Output["id"]))
	if err != nil || !ok {
		t.Fatalf("stored message: %v %v", ok, err)
	}
	var rec map[string]any
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	if rec["expires_ms"] == nil || rec["visible_ms"] == nil {
		t.Fatalf("stored message has no millisecond deadlines: %s", raw)
	}
	delete(rec, "expires_ms")
	delete(rec, "visible_ms")
	raw, _ = json.Marshal(rec)
	if err := col.Put(ctx, fmt.Sprint(old.Output["id"]), raw); err != nil {
		t.Fatal(err)
	}
	if got := peeked(); got != "old" {
		t.Fatalf("record without millisecond deadlines: peek %q", got)
	}
	advance(2 * time.Second)
	if got := peeked(); got != "" {
		t.Fatalf("record without millisecond deadlines after its TTL: peek %q", got)
	}
}
