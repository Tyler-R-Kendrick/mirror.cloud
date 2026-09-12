package bundled_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

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
