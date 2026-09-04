package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestDynamoDBTransactionCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	create := func(table string, stream bool) map[string]any {
		input := map[string]any{"TableName": table, "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}}
		if stream {
			input["StreamSpecification"] = map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
		}
		return asMap(must("CreateTable", input).Output["TableDescription"])
	}
	table := create("T", true)
	create("Plain", false)
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "doomed"}}})

	success := must("TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"ConditionCheck": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "missing"}}, "ConditionExpression": "attribute_not_exists(id)"}},
		map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "test2"}, "binaryData": map[string]any{"B": "Zm9vYmFy"}}}},
		map[string]any{"Update": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "test3"}}, "UpdateExpression": "SET attr1 = :v1, attr2 = :v2", "ExpressionAttributeValues": map[string]any{":v1": map[string]any{"S": "value1"}, ":v2": map[string]any{"S": "value2"}}}},
		map[string]any{"Delete": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "doomed"}}}},
	}}).Output
	gets := must("TransactGetItems", map[string]any{"TransactItems": []any{
		map[string]any{"Get": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "test2"}}}},
		map[string]any{"Get": map[string]any{"TableName": table["TableArn"], "Key": map[string]any{"id": map[string]any{"S": "test3"}}, "ProjectionExpression": "id, attr2"}},
		map[string]any{"Get": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "missing"}}}},
	}}).Output

	_, canceledErr := call("TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"ConditionCheck": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "test2"}}, "ConditionExpression": "attribute_not_exists(id)", "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}},
		map[string]any{"Put": map[string]any{"TableName": "Plain", "Item": map[string]any{"id": map[string]any{"S": "blocked"}}}},
	}})
	canceled := canceledErr.(*spi.Fault)
	blocked := must("GetItem", map[string]any{"TableName": "Plain", "Key": map[string]any{"id": map[string]any{"S": "blocked"}}}).Output

	idempotent := map[string]any{"ClientRequestToken": "dedupe-token", "TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "idem"}, "name": map[string]any{"S": "same"}}}}}}
	must("TransactWriteItems", idempotent)
	must("TransactWriteItems", map[string]any{"ClientRequestToken": "dedupe-token", "TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"name": map[string]any{"S": "same"}, "id": map[string]any{"S": "idem"}}}}}})
	mismatch := cloneMap(idempotent)
	mismatch["TransactItems"] = []any{map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "different"}}}}}
	_, mismatchErr := call("TransactWriteItems", mismatch)
	if err := deps.Clock.Advance(10*time.Minute + time.Second); err != nil {
		t.Fatal(err)
	}
	must("TransactWriteItems", mismatch)
	must("TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"Put": map[string]any{"TableName": "Plain", "Item": map[string]any{"id": map[string]any{"S": "multi"}}}},
		map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "multi"}}}},
	}})
	// Exact duplicate transactional writes do not produce stream records.
	must("TransactWriteItems", map[string]any{"TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "test2"}, "binaryData": map[string]any{"B": "Zm9vYmFy"}}}}}})

	conditionalItem := map[string]any{"id": map[string]any{"S": "conditional"}, "price": map[string]any{"N": "650"}, "product": map[string]any{"S": "sporting goods"}}
	must("PutItem", map[string]any{"TableName": "Plain", "Item": conditionalItem})
	_, conditionalErr := call("DeleteItem", map[string]any{"TableName": "Plain", "Key": map[string]any{"id": map[string]any{"S": "conditional"}}, "ConditionExpression": "price BETWEEN :lo AND :hi", "ExpressionAttributeValues": map[string]any{":lo": map[string]any{"N": "500"}, ":hi": map[string]any{"N": "600"}}, "ReturnValuesOnConditionCheckFailure": "ALL_OLD"})
	conditional := conditionalErr.(*spi.Fault)
	_, conditionalWithoutReturnErr := call("DeleteItem", map[string]any{"TableName": "Plain", "Key": map[string]any{"id": map[string]any{"S": "conditional"}}, "ConditionExpression": "attribute_not_exists(id)"})
	conditionalWithoutReturn := conditionalWithoutReturnErr.(*spi.Fault)
	must("UpdateItem", map[string]any{"TableName": "Plain", "Key": map[string]any{"id": map[string]any{"S": "created-by-update"}}, "ConditionExpression": "attribute_not_exists(id)", "UpdateExpression": "SET value = :v", "ExpressionAttributeValues": map[string]any{":v": map[string]any{"S": "created"}}})
	createdByUpdate := must("GetItem", map[string]any{"TableName": "Plain", "Key": map[string]any{"id": map[string]any{"S": "created-by-update"}}}).Output["Item"]

	arn := str(table["LatestStreamArn"])
	iterator := must("GetShardIterator", map[string]any{"StreamArn": arn, "ShardId": "shardId-000000000000", "ShardIteratorType": "TRIM_HORIZON"}).Output["ShardIterator"]
	records := asSlice(must("GetRecords", map[string]any{"ShardIterator": iterator}).Output["Records"])
	stream := make([]any, 0, len(records))
	for _, raw := range records {
		record := asMap(raw)
		dynamodb := asMap(record["dynamodb"])
		stream = append(stream, map[string]any{"eventName": record["eventName"], "keys": dynamodb["Keys"], "newImage": dynamodb["NewImage"], "oldImage": dynamodb["OldImage"]})
	}
	mismatchFault := mismatchErr.(*spi.Fault)
	golden.AssertJSON(t, map[string]any{
		"success": success,
		"gets":    gets,
		"canceled": map[string]any{
			"code": canceled.Code, "message": canceled.Message, "reasons": canceled.Fields["CancellationReasons"], "blocked": blocked,
		},
		"idempotencyMismatch": map[string]any{"code": mismatchFault.Code, "message": mismatchFault.Message},
		"conditionalFailure":  map[string]any{"code": conditional.Code, "message": conditional.Message, "item": conditional.Fields["Item"]},
		"conditionalWithoutReturn": map[string]any{
			"code": conditionalWithoutReturn.Code, "item": conditionalWithoutReturn.Fields["Item"],
		},
		"createdByConditionalUpdate": createdByUpdate,
		"stream":                     stream,
	})
}
