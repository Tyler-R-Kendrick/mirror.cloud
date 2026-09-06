package dynamodb

import (
	"context"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestDynamoDBGlobalTableReplicas(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	identity := func(region string) spi.Identity {
		return spi.Identity{Account: "000000000000", Region: region}
	}
	call := func(region, operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: identity(region), Operation: operation, Input: input})
	}
	must := func(region, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := call(region, operation, input)
		if err != nil {
			t.Fatalf("%s %s: %v", region, operation, err)
		}
		return response
	}

	must("ap-south-1", "CreateTable", map[string]any{
		"TableName":           "songs",
		"KeySchema":           []any{map[string]any{"AttributeName": "Artist", "KeyType": "HASH"}, map[string]any{"AttributeName": "SongTitle", "KeyType": "RANGE"}},
		"StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
	})
	must("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Create": map[string]any{"RegionName": "us-east-1", "KMSMasterKeyId": "foo"}},
	}})
	must("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Create": map[string]any{"RegionName": "eu-west-1", "KMSMasterKeyId": "bar"}},
	}})

	for _, region := range []string{"ap-south-1", "us-east-1", "eu-west-1"} {
		table := must(region, "DescribeTable", map[string]any{"TableName": "songs"}).Output["Table"].(map[string]any)
		if replicas := asSlice(table["Replicas"]); len(replicas) != 2 || !hasReplica(replicas, "us-east-1") || !hasReplica(replicas, "eu-west-1") {
			t.Fatalf("%s replicas %#v", region, replicas)
		}
		if arn := str(table["LatestStreamArn"]); !strings.Contains(arn, ":"+region+":") {
			t.Fatalf("%s stream ARN %q", region, arn)
		}
		if tables := asSlice(must(region, "ListTables", nil).Output["TableNames"]); len(tables) != 1 || str(tables[0]) != "songs" {
			t.Fatalf("%s tables %#v", region, tables)
		}
	}

	item := map[string]any{"Artist": map[string]any{"S": "Queen"}, "SongTitle": map[string]any{"S": "Bohemian Rhapsody"}}
	must("ap-south-1", "PutItem", map[string]any{"TableName": "songs", "Item": item})
	for _, region := range []string{"us-east-1", "eu-west-1"} {
		if got := must(region, "GetItem", map[string]any{"TableName": "songs", "Key": item}).Output["Item"]; got == nil {
			t.Fatalf("%s missed replicated item", region)
		}
		streams := asSlice(must(region, "ListStreams", map[string]any{"TableName": "songs"}).Output["Streams"])
		arn := str(asMap(streams[0])["StreamArn"])
		iterator := str(must(region, "GetShardIterator", map[string]any{"StreamArn": arn, "ShardIteratorType": "TRIM_HORIZON"}).Output["ShardIterator"])
		if records := asSlice(must(region, "GetRecords", map[string]any{"ShardIterator": iterator}).Output["Records"]); len(records) != 1 {
			t.Fatalf("%s stream records %#v", region, records)
		}
	}

	must("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Delete": map[string]any{"RegionName": "eu-west-1"}},
	}})
	if _, err := call("eu-west-1", "GetItem", map[string]any{"TableName": "songs", "Key": item}); err == nil {
		t.Fatal("deleted replica remained readable")
	}
	_, err := call("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Delete": map[string]any{"RegionName": "eu-west-1"}},
	}})
	if err == nil || !strings.Contains(err.Error(), "replicas were not part of the global table") {
		t.Fatalf("missing replica delete: %v", err)
	}
	for _, region := range []string{"ap-south-1", "us-east-1"} {
		replicas := asSlice(must(region, "DescribeTable", map[string]any{"TableName": "songs"}).Output["Table"].(map[string]any)["Replicas"])
		if len(replicas) != 1 || !hasReplica(replicas, "us-east-1") {
			t.Fatalf("%s replicas after delete %#v", region, replicas)
		}
	}
	must("us-east-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Delete": map[string]any{"RegionName": "us-east-1"}},
	}})
	if table := must("ap-south-1", "DescribeTable", map[string]any{"TableName": "songs"}).Output["Table"].(map[string]any); table["Replicas"] != nil {
		t.Fatalf("last replica left global metadata %#v", table["Replicas"])
	}
}

func TestDynamoDBLegacyGlobalTableLifecycle(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	regions := []any{map[string]any{"RegionName": "us-east-1"}, map[string]any{"RegionName": "us-west-1"}, map[string]any{"RegionName": "eu-central-1"}}
	if _, err := call("CreateGlobalTable", map[string]any{"GlobalTableName": "songs", "ReplicationGroup": regions}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("CreateGlobalTable", map[string]any{"GlobalTableName": "songs", "ReplicationGroup": regions}); err == nil || !strings.Contains(err.Error(), "GlobalTableAlreadyExistsException") {
		t.Fatalf("duplicate create: %v", err)
	}
	updated, err := call("UpdateGlobalTable", map[string]any{"GlobalTableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Create": map[string]any{"RegionName": "us-east-2"}},
		map[string]any{"Create": map[string]any{"RegionName": "us-west-2"}},
		map[string]any{"Delete": map[string]any{"RegionName": "us-west-1"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if replicas := asSlice(asMap(updated.Output["GlobalTableDescription"])["ReplicationGroup"]); len(replicas) != 4 || hasReplica(replicas, "us-west-1") {
		t.Fatalf("updated replicas %#v", replicas)
	}
	if _, err := call("UpdateGlobalTable", map[string]any{"GlobalTableName": "missing"}); err == nil || !strings.Contains(err.Error(), "GlobalTableNotFoundException") {
		t.Fatalf("missing update: %v", err)
	}
}

func TestDynamoDBGlobalTableCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	call := func(region, operation string, input map[string]any) map[string]any {
		t.Helper()
		response, err := p.Invoke(ctx, &spi.Request{Identity: spi.Identity{Account: "000000000000", Region: region}, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output
	}
	fault := func(region, operation string, input map[string]any) map[string]any {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: spi.Identity{Account: "000000000000", Region: region}, Operation: operation, Input: input})
		got, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("%s fault: %#v", operation, err)
		}
		return map[string]any{"code": got.Code, "message": got.Message}
	}
	call("ap-south-1", "CreateTable", map[string]any{
		"TableName":           "songs",
		"KeySchema":           []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}},
		"StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"},
	})
	createdUS := call("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Create": map[string]any{"RegionName": "us-east-1", "KMSMasterKeyId": "foo"}},
	}})
	createdEU := call("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Create": map[string]any{"RegionName": "eu-west-1", "KMSMasterKeyId": "bar"}},
	}})
	call("ap-south-1", "PutItem", map[string]any{"TableName": "songs", "Item": map[string]any{"id": map[string]any{"S": "one"}}})
	streams := map[string]any{}
	items := map[string]any{}
	for _, region := range []string{"ap-south-1", "us-east-1", "eu-west-1"} {
		streams[region] = call(region, "ListStreams", map[string]any{"TableName": "songs"})
		items[region] = call(region, "GetItem", map[string]any{"TableName": "songs", "Key": map[string]any{"id": map[string]any{"S": "one"}}})
	}
	deletedEU := call("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Delete": map[string]any{"RegionName": "eu-west-1"}},
	}})
	missingDelete := fault("ap-south-1", "UpdateTable", map[string]any{"TableName": "songs", "ReplicaUpdates": []any{
		map[string]any{"Delete": map[string]any{"RegionName": "eu-west-1"}},
	}})
	regions := []any{map[string]any{"RegionName": "us-east-1"}, map[string]any{"RegionName": "us-west-1"}, map[string]any{"RegionName": "eu-central-1"}}
	legacyCreated := call("ap-south-1", "CreateGlobalTable", map[string]any{"GlobalTableName": "legacy", "ReplicationGroup": regions})
	legacyDuplicate := fault("ap-south-1", "CreateGlobalTable", map[string]any{"GlobalTableName": "legacy", "ReplicationGroup": regions})
	legacyUpdated := call("ap-south-1", "UpdateGlobalTable", map[string]any{"GlobalTableName": "legacy", "ReplicaUpdates": []any{
		map[string]any{"Create": map[string]any{"RegionName": "us-east-2"}},
		map[string]any{"Delete": map[string]any{"RegionName": "us-west-1"}},
	}})
	golden.AssertJSON(t, map[string]any{
		"createdUS": createdUS, "createdEU": createdEU, "streams": streams, "items": items,
		"deletedEU": deletedEU, "missingDelete": missingDelete,
		"legacyCreated": legacyCreated, "legacyDuplicate": legacyDuplicate, "legacyUpdated": legacyUpdated,
	})
}
