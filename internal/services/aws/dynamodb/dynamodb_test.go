package dynamodb

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestTablePutGet(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTable", Input: map[string]any{
		"TableName": "T",
		"KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	item := map[string]any{"id": map[string]any{"S": "1"}, "n": map[string]any{"N": "2"}}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutItem", Input: map[string]any{"TableName": "T", "Item": item}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetItem", Input: map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "1"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Output["Item"] == nil {
		t.Fatal("missing item — Put/Get keys must match")
	}
}

func TestQueryKeyConditionNotUnfilteredScan(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	must := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp
	}
	must("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "a"}, "n": map[string]any{"N": "1"}}})
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "b"}, "n": map[string]any{"N": "9"}}})
	q := must("Query", map[string]any{
		"TableName":                 "T",
		"KeyConditionExpression":    "id = :id",
		"ExpressionAttributeValues": map[string]any{":id": map[string]any{"S": "a"}},
	})
	items, _ := q.Output["Items"].([]any)
	if len(items) != 1 {
		t.Fatalf("query returned whole table: %v", q.Output)
	}
	scan := must("Scan", map[string]any{
		"TableName":                 "T",
		"FilterExpression":          "n < :n",
		"ExpressionAttributeValues": map[string]any{":n": map[string]any{"N": "5"}},
	})
	sitems, _ := scan.Output["Items"].([]any)
	if len(sitems) != 1 {
		t.Fatalf("scan filter dropped nothing: %v", scan.Output)
	}
	must("DeleteItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "a"}}})
	miss := must("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "a"}}})
	if miss.Output["Item"] != nil {
		t.Fatalf("delete left item %v", miss.Output)
	}
}
