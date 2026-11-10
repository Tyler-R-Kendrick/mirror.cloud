package behavior

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// The 60 seconds after a DeleteQueue or a PurgeQueue run in full from the
// request, wherever in the clock second it fell.
func TestSQSCooldownsAreNotTruncated(t *testing.T) {
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.sqs", deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	code := func(operation string, input map[string]any) string {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err == nil {
			return ""
		}
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("%s: %v", operation, err)
		}
		return fault.Code
	}
	want := func(when, got, want string) {
		t.Helper()
		if got != want {
			t.Fatalf("%s: got %q, want %q", when, got, want)
		}
	}
	advance := func(d time.Duration) {
		t.Helper()
		if err := deps.Clock.Advance(d); err != nil {
			t.Fatal(err)
		}
	}
	const recently, purging = "AWS.SimpleQueueService.QueueDeletedRecently", "AWS.SimpleQueueService.PurgeQueueInProgress"
	create := func(name string) string { return code("CreateQueue", map[string]any{"QueueName": name}) }
	purge := func(name string) string { return code("PurgeQueue", map[string]any{"QueueName": name}) }

	want("create", create("gone"), "")
	want("create", create("kept"), "")
	// 0.9 s into a second.
	advance(900 * time.Millisecond)
	want("delete", code("DeleteQueue", map[string]any{"QueueName": "gone"}), "")
	want("purge", purge("kept"), "")

	// 59.2 s later the clock has turned sixty seconds; 0.8 s is still owed.
	advance(59*time.Second + 200*time.Millisecond)
	want("59.2s: recreate", create("gone"), recently)
	want("59.2s: purge", purge("kept"), purging)
	advance(799 * time.Millisecond)
	want("59.999s: recreate", create("gone"), recently)
	want("59.999s: purge", purge("kept"), purging)
	advance(time.Millisecond)
	want("60s: recreate", create("gone"), "")
	want("60s: purge", purge("kept"), "")

	// Records written before the millisecond stamps existed (a restored
	// snapshot) keep their whole-second window.
	advance(100 * time.Millisecond) // 61.0 s
	want("delete", code("DeleteQueue", map[string]any{"QueueName": "gone"}), "")
	advance(60 * time.Second)
	want("purge", purge("kept"), "")
	scope := deps.Store.Scope(id.Account, id.Region)
	strip := func(collection, key, field string) {
		t.Helper()
		col := scope.Collection(collection)
		raw, ok, err := col.Get(ctx, key)
		if err != nil || !ok {
			t.Fatalf("%s/%s: %v %v", collection, key, ok, err)
		}
		var rec map[string]any
		if err := json.Unmarshal(raw, &rec); err != nil {
			t.Fatal(err)
		}
		if rec[field] == nil {
			t.Fatalf("%s/%s has no %s: %s", collection, key, field, raw)
		}
		delete(rec, field)
		raw, _ = json.Marshal(rec)
		if err := col.Put(ctx, key, raw); err != nil {
			t.Fatal(err)
		}
	}
	strip("qdeleted", "gone", "atMs")
	strip("queues", "kept", "purgedAtMs")
	want("old records: recreate", create("gone"), "") // deleted 60 s ago
	want("old records: purge", purge("kept"), purging)
}
