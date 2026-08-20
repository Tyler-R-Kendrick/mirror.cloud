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
