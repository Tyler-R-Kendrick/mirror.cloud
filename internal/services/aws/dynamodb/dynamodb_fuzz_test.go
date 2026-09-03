package dynamodb

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzTableLifecycle(f *testing.F) {
	f.Add([]byte("table"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		name := hex.EncodeToString(raw)
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		table := map[string]any{"TableName": name, "Tags": []any{map[string]any{"Key": "fuzz", "Value": name}}}
		_, created := call("CreateTable", table)
		_, duplicate := call("CreateTable", table)
		arn := "arn:aws:dynamodb:us-east-1:000000000000:table/" + name
		listed, tagsErr := call("ListTagsOfResource", map[string]any{"ResourceArn": arn})
		_, deleted := call("DeleteTable", table)
		_, missing := call("DeleteTable", table)
		_, ttlDescribe := call("DescribeTimeToLive", table)
		_, ttlUpdate := call("UpdateTimeToLive", table)
		tags := asSlice(listed.Output["Tags"])
		if created != nil || duplicate == nil || tagsErr != nil || len(tags) != 1 || str(asMap(tags[0])["Value"]) != name || deleted != nil || missing == nil || ttlDescribe == nil || ttlUpdate == nil {
			t.Fatal("table lifecycle was not create/tag/conflict/delete/missing")
		}
	})
}
