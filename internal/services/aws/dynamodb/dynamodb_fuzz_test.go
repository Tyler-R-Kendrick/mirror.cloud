package dynamodb

import (
	"context"
	"encoding/hex"
	"strings"
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
		table := map[string]any{"TableName": name, "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "GlobalSecondaryIndexes": []any{map[string]any{"IndexName": "keys", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "Projection": map[string]any{"ProjectionType": "KEYS_ONLY"}}}, "Tags": []any{map[string]any{"Key": "fuzz", "Value": name}}}
		_, created := call("CreateTable", table)
		_, duplicate := call("CreateTable", table)
		arn := "arn:aws:dynamodb:us-east-1:000000000000:table/" + name
		listed, tagsErr := call("ListTagsOfResource", map[string]any{"ResourceArn": arn})
		value := strings.ToValidUTF8(string(raw), "�")
		_, putErr := call("PutItem", map[string]any{"TableName": name, "Item": map[string]any{"id": map[string]any{"S": "item"}, "data": map[string]any{"S": value}}})
		got, getErr := call("GetItem", map[string]any{"TableName": name, "Key": map[string]any{"id": map[string]any{"S": "item"}}})
		_, invalidProjection := call("Query", map[string]any{"TableName": name, "IndexName": "keys", "Select": "ALL_ATTRIBUTES"})
		_, ttlEnable := call("UpdateTimeToLive", map[string]any{"TableName": name, "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"}})
		_, expiredPut := call("PutItem", map[string]any{"TableName": name, "Item": map[string]any{"id": map[string]any{"S": "expired"}, "ttl": map[string]any{"N": "-1"}}})
		expiration, expirationErr := call("ExpireItems", nil)
		expiredItem, expiredGet := call("GetItem", map[string]any{"TableName": name, "Key": map[string]any{"id": map[string]any{"S": "expired"}}})
		_, deleted := call("DeleteTable", table)
		_, missing := call("DeleteTable", table)
		_, missingQuery := call("Query", map[string]any{"TableName": name})
		_, missingTransaction := call("TransactWriteItems", map[string]any{"TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": name, "Item": map[string]any{}}}}})
		_, ttlDescribe := call("DescribeTimeToLive", table)
		_, ttlUpdate := call("UpdateTimeToLive", table)
		tags := asSlice(listed.Output["Tags"])
		if created != nil || duplicate == nil || tagsErr != nil || len(tags) != 1 || str(asMap(tags[0])["Value"]) != name || putErr != nil || getErr != nil || str(asMap(asMap(got.Output["Item"])["data"])["S"]) != value || invalidProjection == nil || ttlEnable != nil || expiredPut != nil || expirationErr != nil || expiration.Output["ExpiredItems"] != 1 || expiredGet != nil || expiredItem.Output["Item"] != nil || deleted != nil || missing == nil || missingQuery == nil || missingTransaction == nil || ttlDescribe == nil || ttlUpdate == nil {
			t.Fatal("table lifecycle was not create/tag/conflict/delete/missing")
		}
	})
}
