package bundled_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func tablePack(t testing.TB) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New("azure.table", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAzureTableEntities(t *testing.T) {
	p := tablePack(t)
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
	entIn := func(pk, rk string, props map[string]any) map[string]any {
		body := map[string]any{"PartitionKey": pk, "RowKey": rk}
		for k, v := range props {
			body[k] = v
		}
		return map[string]any{"table": "t", "__entity": body, "PartitionKey": pk, "RowKey": rk}
	}

	inv("CreateTable", map[string]any{"table": "t"})
	fault("CreateTable", map[string]any{"table": "t"}, 409, "TableAlreadyExists")

	ins := inv("InsertEntity", entIn("p", "r", map[string]any{"Name": "ada", "Age": "36"}))
	stored, _ := ins.Output["entity"].(map[string]any)
	etag := fmt.Sprint(stored["etag"])
	if etag == "" || stored["Name"] != "ada" {
		t.Fatalf("insert %#v", ins.Output)
	}
	fault("InsertEntity", entIn("p", "r", map[string]any{"Name": "dup"}), 409, "EntityAlreadyExists")

	got := inv("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"})
	entity, _ := got.Output["entity"].(map[string]any)
	if entity["Name"] != "ada" || entity["Age"] != "36" || fmt.Sprint(entity["etag"]) != etag {
		t.Fatalf("get %#v", got.Output)
	}
	if _, leaked := entity["table"]; leaked {
		t.Fatalf("control member leaked %#v", entity)
	}
	fault("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "missing"}, 404, "EntityNotFound")

	q := inv("QueryEntities", map[string]any{"table": "t"})
	items, _ := q.Output["_list"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["Name"] != "ada" {
		t.Fatalf("query %#v", q.Output)
	}

	// Replace: old properties are dropped; the etag moves.
	inv("UpdateEntity", entIn("p", "r", map[string]any{"Name": "grace"}))
	got = inv("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"})
	entity, _ = got.Output["entity"].(map[string]any)
	if entity["Name"] != "grace" {
		t.Fatalf("replaced %#v", entity)
	}
	if _, dropped := entity["Age"]; dropped {
		t.Fatalf("replace kept old property %#v", entity)
	}
	etag2 := fmt.Sprint(entity["etag"])
	if etag2 == etag || etag2 == "" {
		t.Fatalf("etag did not move %#v", entity)
	}

	// Merge: unstated properties survive.
	inv("MergeEntity", entIn("p", "r", map[string]any{"City": "london"}))
	got = inv("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"})
	entity, _ = got.Output["entity"].(map[string]any)
	if entity["Name"] != "grace" || entity["City"] != "london" {
		t.Fatalf("merged %#v", entity)
	}
	_ = fmt.Sprint(entity["etag"])

	// Conditional writes: wildcard, match, mismatch, missing-with-condition.
	inv("UpdateEntity", withMatch(entIn("p", "r", map[string]any{"Name": "hopper"}), "*"))
	fault("UpdateEntity", withMatch(entIn("p", "r", map[string]any{"Name": "x"}), "stale-etag"), 412, "UpdateConditionNotSatisfied")
	fault("UpdateEntity", withMatch(entIn("p", "ghost", map[string]any{"Name": "x"}), "*"), 404, "EntityNotFound")
	got = inv("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"})
	entity, _ = got.Output["entity"].(map[string]any)
	if entity["Name"] != "hopper" {
		t.Fatalf("conditional update %#v", entity)
	}

	// No If-Match on a missing key is an upsert, for both PUT and PATCH.
	inv("UpdateEntity", entIn("p", "new", map[string]any{"Name": "upserted"}))
	inv("MergeEntity", entIn("p", "new2", map[string]any{"Name": "merge-upserted"}))
	got = inv("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "new2"})
	if got.Output["entity"].(map[string]any)["Name"] != "merge-upserted" {
		t.Fatalf("merge upsert %#v", got.Output)
	}

	// Delete honors the etag.
	fault("DeleteEntity", withMatch(map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"}, "stale"), 412, "UpdateConditionNotSatisfied")
	got = inv("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"})
	current := fmt.Sprint(got.Output["entity"].(map[string]any)["etag"])
	inv("DeleteEntity", withMatch(map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"}, current))
	fault("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": "r"}, 404, "EntityNotFound")

	// Empty RowKey is addressable.
	inv("InsertEntity", entIn("p", "", map[string]any{"Name": "emptyrk"}))
	got = inv("GetEntity", map[string]any{"table": "t", "PartitionKey": "p", "RowKey": ""})
	if got.Output["entity"].(map[string]any)["Name"] != "emptyrk" {
		t.Fatalf("empty rowkey %#v", got.Output)
	}
	fault("GetEntity", map[string]any{"table": "missing", "PartitionKey": "p", "RowKey": "r"}, 404, "TableNotFound")
}

func withMatch(in map[string]any, etag string) map[string]any {
	in["if_match_list"] = []any{etag}
	return in
}
