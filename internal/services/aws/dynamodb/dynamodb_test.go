package dynamodb

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
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

func TestTableLifecycleFaults(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string) error {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: map[string]any{"TableName": "T"}})
		return err
	}
	if err := call("CreateTable"); err != nil {
		t.Fatal(err)
	}
	if fault, ok := call("CreateTable").(*spi.Fault); !ok || fault.Code != "ResourceInUseException" || fault.Message != "Table already exists: T" {
		t.Fatalf("duplicate create fault %#v", fault)
	}
	if err := call("DeleteTable"); err != nil {
		t.Fatal(err)
	}
	if fault, ok := call("DeleteTable").(*spi.Fault); !ok || fault.Code != "ResourceNotFoundException" || fault.Message != "Requested resource not found: Table: T not found" {
		t.Fatalf("missing delete fault %#v", fault)
	}
}

func TestTableLifecycleCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string) any {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: map[string]any{"TableName": "T"}})
		if err == nil {
			return "success"
		}
		fault := err.(*spi.Fault)
		return map[string]any{"code": fault.Code, "message": fault.Message}
	}
	golden.AssertJSON(t, map[string]any{
		"create": call("CreateTable"), "duplicateCreate": call("CreateTable"),
		"delete": call("DeleteTable"), "missingDelete": call("DeleteTable"),
	})
}

func TestDynamoDBTTLMissingTableFaults(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	for _, operation := range []string{"DescribeTimeToLive", "UpdateTimeToLive"} {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: map[string]any{
			"TableName": "missing", "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"},
		}})
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "ResourceNotFoundException" || fault.HTTPStatus != 400 {
			t.Fatalf("%s fault %#v", operation, fault)
		}
	}
}

func TestDynamoDBTTLDoesNotSurviveTableRecreation(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	table := map[string]any{"TableName": "T"}
	must("CreateTable", table)
	must("UpdateTimeToLive", map[string]any{"TableName": "T", "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"}})
	must("DeleteTable", table)
	must("CreateTable", table)
	description := asMap(must("DescribeTimeToLive", table).Output["TimeToLiveDescription"])
	if description["TimeToLiveStatus"] != "DISABLED" || description["AttributeName"] != nil {
		t.Fatalf("recreated table retained ttl %#v", description)
	}
}

func TestDynamoDBTTLCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			fault := err.(*spi.Fault)
			return map[string]any{"code": fault.Code, "message": fault.Message}
		}
		return response.Output
	}
	missing := map[string]any{"TableName": "missing", "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"}}
	created := map[string]any{"TableName": "T"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTable", Input: created}); err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{
		"missingDescribe": call("DescribeTimeToLive", missing),
		"missingUpdate":   call("UpdateTimeToLive", missing),
		"default":         call("DescribeTimeToLive", created),
		"enable":          call("UpdateTimeToLive", map[string]any{"TableName": "T", "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"}}),
		"enabled":         call("DescribeTimeToLive", created),
	})
}

func TestDynamoDBTTLExpiration(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	for _, table := range []map[string]any{
		{"TableName": "hash", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}},
		{"TableName": "range", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}, map[string]any{"AttributeName": "range", "KeyType": "RANGE"}}},
		{"TableName": "disabled", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}},
	} {
		must("CreateTable", table)
	}
	for _, table := range []string{"hash", "range"} {
		must("UpdateTimeToLive", map[string]any{"TableName": table, "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"}})
	}
	must("UpdateTimeToLive", map[string]any{"TableName": "disabled", "TimeToLiveSpecification": map[string]any{"Enabled": false, "AttributeName": "ttl"}})
	past, future := strconv.FormatInt(deps.Clock.Now().Unix()-10, 10), strconv.FormatInt(deps.Clock.Now().Unix()+120, 10)
	must("PutItem", map[string]any{"TableName": "hash", "Item": map[string]any{"id": map[string]any{"S": "expired"}, "ttl": map[string]any{"N": past}}})
	must("PutItem", map[string]any{"TableName": "hash", "Item": map[string]any{"id": map[string]any{"S": "future"}, "ttl": map[string]any{"N": future}}})
	must("PutItem", map[string]any{"TableName": "range", "Item": map[string]any{"id": map[string]any{"S": "expired"}, "range": map[string]any{"S": "one"}, "ttl": map[string]any{"N": past}}})
	must("PutItem", map[string]any{"TableName": "range", "Item": map[string]any{"id": map[string]any{"S": "future"}, "range": map[string]any{"S": "two"}, "ttl": map[string]any{"N": future}}})
	must("PutItem", map[string]any{"TableName": "disabled", "Item": map[string]any{"id": map[string]any{"S": "expired"}, "ttl": map[string]any{"N": past}}})
	if got := must("ExpireItems", nil).Output["ExpiredItems"]; got != 2 {
		t.Fatalf("expired count %v", got)
	}
	for _, tc := range []struct {
		table string
		key   map[string]any
		found bool
	}{
		{"hash", map[string]any{"id": map[string]any{"S": "expired"}}, false},
		{"hash", map[string]any{"id": map[string]any{"S": "future"}}, true},
		{"range", map[string]any{"id": map[string]any{"S": "expired"}, "range": map[string]any{"S": "one"}}, false},
		{"range", map[string]any{"id": map[string]any{"S": "future"}, "range": map[string]any{"S": "two"}}, true},
		{"disabled", map[string]any{"id": map[string]any{"S": "expired"}}, true},
	} {
		found := must("GetItem", map[string]any{"TableName": tc.table, "Key": tc.key}).Output["Item"] != nil
		if found != tc.found {
			t.Fatalf("%s %#v found=%t", tc.table, tc.key, found)
		}
	}
}

func TestDynamoDBTTLExpirationCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	must("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
	must("UpdateTimeToLive", map[string]any{"TableName": "T", "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"}})
	for _, item := range []map[string]any{
		{"id": map[string]any{"S": "expired"}, "ttl": map[string]any{"N": "-1"}},
		{"id": map[string]any{"S": "future"}, "ttl": map[string]any{"N": "9999999999"}},
		{"id": map[string]any{"S": "invalid"}, "ttl": map[string]any{"S": "yesterday"}},
		{"id": map[string]any{"S": "unset"}},
	} {
		must("PutItem", map[string]any{"TableName": "T", "Item": item})
	}
	golden.AssertJSON(t, map[string]any{
		"expiration": must("ExpireItems", nil).Output,
		"expired":    must("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "expired"}}}).Output,
		"remaining":  must("Scan", map[string]any{"TableName": "T"}).Output,
	})
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

func TestDynamoDBTagLifecycle(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	created := must("CreateTable", map[string]any{"TableName": "T", "Tags": []any{
		map[string]any{"Key": "env", "Value": "dev"}, map[string]any{"Key": "team", "Value": "platform"},
	}})
	arn := str(asMap(created.Output["TableDescription"])["TableArn"])
	if arn != "arn:aws:dynamodb:us-east-1:000000000000:table/T" {
		t.Fatalf("table arn %q", arn)
	}
	if tags := must("ListTagsOfResource", map[string]any{"ResourceArn": arn}).Output["Tags"].([]any); len(tags) != 2 || str(asMap(tags[0])["Value"]) != "dev" || str(asMap(tags[1])["Value"]) != "platform" {
		t.Fatalf("creation tags %#v", tags)
	}
	must("TagResource", map[string]any{"ResourceArn": arn, "Tags": []any{map[string]any{"Key": "env", "Value": "prod"}}})
	must("UntagResource", map[string]any{"ResourceArn": arn, "TagKeys": []any{"team"}})
	tags := must("ListTagsOfResource", map[string]any{"ResourceArn": arn}).Output["Tags"].([]any)
	if len(tags) != 1 || str(asMap(tags[0])["Key"]) != "env" || str(asMap(tags[0])["Value"]) != "prod" {
		t.Fatalf("tags %#v", tags)
	}
	must("DeleteTable", map[string]any{"TableName": "T"})
	if tags := must("ListTagsOfResource", map[string]any{"ResourceArn": arn}).Output["Tags"].([]any); len(tags) != 0 {
		t.Fatalf("deleted table tags %#v", tags)
	}
}

func TestDynamoDBTagCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	created := must("CreateTable", map[string]any{"TableName": "T", "Tags": []any{map[string]any{"Key": "Name", "Value": "test"}, map[string]any{"Key": "env", "Value": "dev"}}})
	arn := str(asMap(created.Output["TableDescription"])["TableArn"])
	initial := must("ListTagsOfResource", map[string]any{"ResourceArn": arn}).Output["Tags"]
	must("TagResource", map[string]any{"ResourceArn": arn, "Tags": []any{map[string]any{"Key": "env", "Value": "prod"}, map[string]any{"Key": "team", "Value": "storage"}}})
	updated := must("ListTagsOfResource", map[string]any{"ResourceArn": arn}).Output["Tags"]
	must("UntagResource", map[string]any{"ResourceArn": arn, "TagKeys": []any{"Name", "team"}})
	remaining := must("ListTagsOfResource", map[string]any{"ResourceArn": arn}).Output["Tags"]
	must("DeleteTable", map[string]any{"TableName": "T"})
	golden.AssertJSON(t, map[string]any{
		"tableArn": arn, "initial": initial, "updated": updated, "remaining": remaining,
		"afterDelete": must("ListTagsOfResource", map[string]any{"ResourceArn": arn}).Output["Tags"],
	})
}

func TestDynamoDBUnicodeAndLargeScan(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	must("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
	for i, value := range []string{"foobar123 ✓", "foobar123 £", "foobar123 ¢"} {
		id := "unicode-" + strconv.Itoa(i)
		must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": id}, "data": map[string]any{"S": value}}})
		got := asMap(must("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": id}}}).Output["Item"])
		if str(asMap(got["data"])["S"]) != value {
			t.Fatalf("unicode item %#v", got)
		}
	}
	for i := 0; i < 20; i++ {
		must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "large-" + strconv.Itoa(i)}, "data": map[string]any{"S": strings.Repeat("foobar123 ", 1000)}}})
	}
	scan := must("Scan", map[string]any{"TableName": "T"}).Output
	if scan["Count"] != 23 || scan["ScannedCount"] != 23 || len(asSlice(scan["Items"])) != 23 {
		t.Fatalf("large scan %#v", scan)
	}
}

func TestDynamoDBDataCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output
	}
	call("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
	for i, value := range []string{"✓", "£", "¢"} {
		call("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": strconv.Itoa(i)}, "data": map[string]any{"S": value}}})
	}
	golden.AssertJSON(t, map[string]any{
		"unicode": call("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "0"}}})["Item"],
		"scan":    call("Scan", map[string]any{"TableName": "T"}),
	})
}

func TestDynamoDBItemFaults(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	assertFault := func(operation string, input map[string]any, code string) {
		t.Helper()
		_, err := call(operation, input)
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != code || fault.HTTPStatus != 400 {
			t.Fatalf("%s fault %#v", operation, err)
		}
	}
	table := map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}, map[string]any{"AttributeName": "range", "KeyType": "RANGE"}}}
	if _, err := call("CreateTable", table); err != nil {
		t.Fatal(err)
	}
	assertFault("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "1"}}}, "ValidationException")
	assertFault("BatchWriteItem", map[string]any{"RequestItems": map[string]any{"T": []any{map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"nonKey": map[string]any{"S": "value"}}}}}}}, "ValidationException")
	if _, err := call("DeleteTable", map[string]any{"TableName": "T"}); err != nil {
		t.Fatal(err)
	}
	assertFault("Query", map[string]any{"TableName": "T"}, "ResourceNotFoundException")
	assertFault("TransactWriteItems", map[string]any{"TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": "missing", "Item": map[string]any{}}}}}, "ResourceNotFoundException")
}

func TestDynamoDBItemFaultCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err == nil {
			return response.Output
		}
		fault := err.(*spi.Fault)
		return map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
	}
	call("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}, map[string]any{"AttributeName": "sortKey", "KeyType": "RANGE"}}})
	invalid := call("BatchWriteItem", map[string]any{"RequestItems": map[string]any{"T": []any{map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"nonKey": map[string]any{"S": "value"}}}}}}})
	call("DeleteTable", map[string]any{"TableName": "T"})
	golden.AssertJSON(t, map[string]any{
		"invalidSchema":      invalid,
		"queryDeleted":       call("Query", map[string]any{"TableName": "T"}),
		"transactionMissing": call("TransactWriteItems", map[string]any{"TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": "missing", "Item": map[string]any{}}}}}),
	})
}

func TestDynamoDBExtendedOperations(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
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
	must("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
	must("ExecuteStatement", map[string]any{"Statement": "INSERT INTO T VALUE {'id': 'k1', 'n': 7}"})
	if item := must("ExecuteStatement", map[string]any{"Statement": "SELECT * FROM T WHERE id = 'k1'"}).Output["Item"]; item == nil {
		t.Fatal("PartiQL SELECT missed inserted item")
	}
	if responses := must("BatchExecuteStatement", map[string]any{"Statements": []any{
		map[string]any{"Statement": "SELECT * FROM T WHERE id = 'k1'"}, map[string]any{"Statement": "INSERT"},
	}}).Output["Responses"].([]any); len(responses) != 2 || asMap(responses[1])["Error"] == nil {
		t.Fatalf("batch statements %#v", responses)
	}
	if responses := must("ExecuteTransaction", map[string]any{"TransactStatements": []any{map[string]any{"Statement": "SELECT * FROM T"}}}).Output["Responses"].([]any); len(responses) != 1 {
		t.Fatalf("transaction %#v", responses)
	}
	must("ExecuteStatement", map[string]any{"Statement": "DELETE FROM T WHERE id = 'k1'"})

	must("CreateGlobalTable", map[string]any{"GlobalTableName": "gt", "ReplicationGroup": []any{map[string]any{"RegionName": "us-east-1"}}})
	must("UpdateGlobalTable", map[string]any{"GlobalTableName": "gt", "ReplicaUpdates": []any{map[string]any{"Create": map[string]any{"RegionName": "us-west-2"}}}})
	if group := must("DescribeGlobalTable", map[string]any{"GlobalTableName": "gt"}).Output["GlobalTableDescription"].(map[string]any)["ReplicationGroup"].([]any); len(group) != 2 {
		t.Fatalf("global replicas %#v", group)
	}
	if tables := must("ListGlobalTables", nil).Output["GlobalTables"].([]any); len(tables) != 1 {
		t.Fatalf("global tables %#v", tables)
	}
	if _, err := call("DescribeGlobalTable", map[string]any{"GlobalTableName": "missing"}); err == nil {
		t.Fatal("described missing global table")
	}
	if settings := must("DescribeGlobalTableSettings", map[string]any{"GlobalTableName": "gt"}).Output["ReplicaSettings"].([]any); len(settings) != 0 {
		t.Fatalf("default settings %#v", settings)
	}
	must("UpdateGlobalTableSettings", map[string]any{"GlobalTableName": "gt", "ReplicaSettings": []any{map[string]any{"RegionName": "us-east-1"}}})
	if settings := must("DescribeGlobalTableSettings", map[string]any{"GlobalTableName": "gt"}).Output["ReplicaSettings"].([]any); len(settings) != 1 {
		t.Fatalf("updated settings %#v", settings)
	}

	if status := must("DescribeContributorInsights", map[string]any{"TableName": "T"}).Output["ContributorInsightsStatus"]; status != "DISABLED" {
		t.Fatalf("default insights %v", status)
	}
	must("UpdateContributorInsights", map[string]any{"TableName": "T", "ContributorInsightsAction": "ENABLE"})
	if summaries := must("ListContributorInsights", nil).Output["ContributorInsightsSummaries"].([]any); len(summaries) != 1 {
		t.Fatalf("insight summaries %#v", summaries)
	}
	must("UpdateContributorInsights", map[string]any{"TableName": "T", "ContributorInsightsAction": "DISABLE"})

	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "vector-hit"}}})
	exported := must("ExportTableToPointInTime", map[string]any{"TableName": "T", "S3Bucket": "bucket"}).Output["ExportDescription"].(map[string]any)
	exportARN := str(exported["ExportArn"])
	if exported["ExportedItemCount"] != 1 || must("DescribeExport", map[string]any{"ExportArn": exportARN}).Output["ExportDescription"] == nil {
		t.Fatalf("export %#v", exported)
	}
	if exports := must("ListExports", nil).Output["ExportSummaries"].([]any); len(exports) != 1 {
		t.Fatalf("exports %#v", exports)
	}
	if _, err := call("DescribeExport", map[string]any{"ExportArn": "missing"}); err == nil {
		t.Fatal("described missing export")
	}

	imported := must("ImportTable", map[string]any{"TableCreationParameters": map[string]any{
		"TableName": "Imported", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}},
	}}).Output["ImportTableDescription"].(map[string]any)
	importARN := str(imported["ImportArn"])
	if must("DescribeImport", map[string]any{"ImportArn": importARN}).Output["ImportTableDescription"] == nil {
		t.Fatalf("import %#v", imported)
	}
	if imports := must("ListImports", nil).Output["ImportSummaryList"].([]any); len(imports) != 1 {
		t.Fatalf("imports %#v", imports)
	}
	if _, err := call("DescribeImport", map[string]any{"ImportArn": "missing"}); err == nil {
		t.Fatal("described missing import")
	}

	if _, err := call("RestoreTableToPointInTime", map[string]any{"SourceTableName": "missing", "TargetTableName": "Nope"}); err == nil {
		t.Fatal("restored missing table")
	}
	must("RestoreTableToPointInTime", map[string]any{"SourceTableName": "T", "TargetTableName": "Restored"})
	if item := must("GetItem", map[string]any{"TableName": "Restored", "Key": map[string]any{"id": map[string]any{"S": "vector-hit"}}}).Output["Item"]; item == nil {
		t.Fatal("point-in-time restore omitted items")
	}
	if description := must("DescribeTableReplicaAutoScaling", map[string]any{"TableName": "T"}).Output["TableAutoScalingDescription"].(map[string]any); description["TableName"] != "T" {
		t.Fatalf("default scaling %#v", description)
	}
	must("UpdateTableReplicaAutoScaling", map[string]any{"TableName": "T", "ProvisionedWriteCapacityAutoScalingSettingsUpdate": map[string]any{"MinimumUnits": 1}})
	if description := must("DescribeTableReplicaAutoScaling", map[string]any{"TableName": "T"}).Output["TableAutoScalingDescription"].(map[string]any); description["ProvisionedWriteCapacityAutoScalingSettingsUpdate"] == nil {
		t.Fatalf("updated scaling %#v", description)
	}
	if hits := must("SearchVectors", map[string]any{"TableName": "T", "Query": "vector-hit"}).Output["Items"].([]any); len(hits) != 1 {
		t.Fatalf("vector hits %#v", hits)
	}
	if hits := must("SearchVectors", map[string]any{"TableName": "T"}).Output["Items"].([]any); len(hits) != 1 {
		t.Fatalf("unfiltered vectors %#v", hits)
	}
	if destination := must("UpdateKinesisStreamingDestination", map[string]any{
		"TableName": "T", "StreamArn": "arn:aws:kinesis:us-east-1:000000000000:stream/s", "ApproximateCreationDateTimePrecision": "MICROSECOND",
	}).Output; destination["DestinationStatus"] != "ACTIVE" || destination["ApproximateCreationDateTimePrecision"] != "MICROSECOND" {
		t.Fatalf("kinesis destination %#v", destination)
	}
	if _, err := call("Unknown", nil); err == nil {
		t.Fatal("unknown operation succeeded")
	}
}
