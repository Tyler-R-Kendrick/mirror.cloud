package behavior

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
)

func TestDynamoDBTableLifecycle(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.dynamodb"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	call := func(action, payload string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL, bytes.NewBufferString(payload))
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=00")
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, body
	}

	t.Run("Given an existing table When creating it again Then ResourceInUse is returned", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"T"}`); status != http.StatusOK {
			t.Fatalf("first create %d %s", status, body)
		}
		if status, body := call("CreateTable", `{"TableName":"T"}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ResourceInUseException")) || !bytes.Contains(body, []byte("Table already exists: T")) {
			t.Fatalf("duplicate create %d %s", status, body)
		}
	})

	t.Run("Given a deleted table When deleting it again Then ResourceNotFound is returned", func(t *testing.T) {
		if status, body := call("DeleteTable", `{"TableName":"T"}`); status != http.StatusOK {
			t.Fatalf("first delete %d %s", status, body)
		}
		if status, body := call("DeleteTable", `{"TableName":"T"}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ResourceNotFoundException")) || !bytes.Contains(body, []byte("Requested resource not found: Table: T not found")) {
			t.Fatalf("missing delete %d %s", status, body)
		}
	})

	t.Run("Given a missing table When reading or updating TTL Then ResourceNotFound is returned", func(t *testing.T) {
		for _, action := range []string{"DescribeTimeToLive", "UpdateTimeToLive"} {
			status, body := call(action, `{"TableName":"missing","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"ttl"}}`)
			if status != http.StatusBadRequest || !bytes.Contains(body, []byte("ResourceNotFoundException")) {
				t.Fatalf("%s %d %s", action, status, body)
			}
		}
	})

	t.Run("Given tags at table creation When tags change Then listing reflects the lifecycle", func(t *testing.T) {
		status, body := call("CreateTable", `{"TableName":"Tags","Tags":[{"Key":"Name","Value":"test"}]}`)
		var created map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &created) != nil {
			t.Fatalf("create tagged table %d %s", status, body)
		}
		arn := created["TableDescription"].(map[string]any)["TableArn"].(string)
		if status, body = call("TagResource", `{"ResourceArn":"`+arn+`","Tags":[{"Key":"env","Value":"test"}]}`); status != http.StatusOK {
			t.Fatalf("tag %d %s", status, body)
		}
		if status, body = call("ListTagsOfResource", `{"ResourceArn":"`+arn+`"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"Name"`)) || !bytes.Contains(body, []byte(`"env"`)) {
			t.Fatalf("list tags %d %s", status, body)
		}
		if status, body = call("UntagResource", `{"ResourceArn":"`+arn+`","TagKeys":["Name"]}`); status != http.StatusOK {
			t.Fatalf("untag %d %s", status, body)
		}
		if status, body = call("ListTagsOfResource", `{"ResourceArn":"`+arn+`"}`); status != http.StatusOK || bytes.Contains(body, []byte(`"Name"`)) || !bytes.Contains(body, []byte(`"env"`)) {
			t.Fatalf("list remaining tags %d %s", status, body)
		}
	})

	t.Run("Given a large table When scanning Then every item crosses the HTTP boundary", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"Large","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`); status != http.StatusOK {
			t.Fatalf("create large table %d %s", status, body)
		}
		for i := 0; i < 20; i++ {
			payload := fmt.Sprintf(`{"TableName":"Large","Item":{"id":{"S":"id%d"},"data":{"S":%q}}}`, i, strings.Repeat("foobar123 ", 1000))
			if status, body := call("PutItem", payload); status != http.StatusOK {
				t.Fatalf("put large item %d %s", status, body)
			}
		}
		status, body := call("Scan", `{"TableName":"Large"}`)
		var scan map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &scan) != nil || scan["Count"] != float64(20) || scan["ScannedCount"] != float64(20) || len(body) < 200000 {
			t.Fatalf("large scan %d bytes=%d %s", status, len(body), body[:min(len(body), 200)])
		}
	})

	t.Run("Given expired TTL items When sweeping Then only expired items are deleted", func(t *testing.T) {
		for _, step := range []struct{ action, payload string }{
			{"CreateTable", `{"TableName":"Expire","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`},
			{"UpdateTimeToLive", `{"TableName":"Expire","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"ttl"}}`},
			{"PutItem", `{"TableName":"Expire","Item":{"id":{"S":"expired"},"ttl":{"N":"-1"}}}`},
		} {
			if status, body := call(step.action, step.payload); status != http.StatusOK {
				t.Fatalf("%s %d %s", step.action, status, body)
			}
		}
		if status, body := call("PutItem", `{"TableName":"Expire","Item":{"id":{"S":"future"},"ttl":{"N":"9999999999"}}}`); status != http.StatusOK {
			t.Fatalf("put future %d %s", status, body)
		}
		req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/_aws/dynamodb/expired", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"ExpiredItems":1`)) {
			t.Fatalf("expiration %d %s", res.StatusCode, body)
		}
		if status, body := call("GetItem", `{"TableName":"Expire","Key":{"id":{"S":"expired"}}}`); status != http.StatusOK || bytes.Contains(body, []byte(`"Item"`)) {
			t.Fatalf("expired item %d %s", status, body)
		}
		if status, body := call("GetItem", `{"TableName":"Expire","Key":{"id":{"S":"future"}}}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"Item"`)) {
			t.Fatalf("future item %d %s", status, body)
		}
	})

	t.Run("Given invalid item targets When writing or querying Then modeled faults are returned", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"Faults","KeySchema":[{"AttributeName":"id","KeyType":"HASH"},{"AttributeName":"sortKey","KeyType":"RANGE"}]}`); status != http.StatusOK {
			t.Fatalf("create faults table %d %s", status, body)
		}
		if status, body := call("BatchWriteItem", `{"RequestItems":{"Faults":[{"PutRequest":{"Item":{"nonKey":{"S":"value"}}}}]}}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ValidationException")) {
			t.Fatalf("invalid batch schema %d %s", status, body)
		}
		if status, body := call("DeleteTable", `{"TableName":"Faults"}`); status != http.StatusOK {
			t.Fatalf("delete faults table %d %s", status, body)
		}
		if status, body := call("Query", `{"TableName":"Faults"}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ResourceNotFoundException")) {
			t.Fatalf("query deleted table %d %s", status, body)
		}
		if status, body := call("TransactWriteItems", `{"TransactItems":[{"Put":{"TableName":"missing","Item":{}}}]}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ResourceNotFoundException")) {
			t.Fatalf("transaction missing table %d %s", status, body)
		}
	})

	t.Run("Given projected indexes When selecting all attributes Then projection rules are enforced", func(t *testing.T) {
		create := `{"TableName":"Indexes","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"GlobalSecondaryIndexes":[{"IndexName":"keys","KeySchema":[{"AttributeName":"fieldA","KeyType":"HASH"}],"Projection":{"ProjectionType":"KEYS_ONLY"}},{"IndexName":"all","KeySchema":[{"AttributeName":"fieldB","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}]}`
		if status, body := call("CreateTable", create); status != http.StatusOK {
			t.Fatalf("create indexes %d %s", status, body)
		}
		if status, body := call("PutItem", `{"TableName":"Indexes","Item":{"id":{"S":"1"},"fieldA":{"S":"a"},"fieldB":{"S":"b"},"data":{"S":"value"}}}`); status != http.StatusOK {
			t.Fatalf("put indexed item %d %s", status, body)
		}
		if status, body := call("Query", `{"TableName":"Indexes","IndexName":"keys","KeyConditionExpression":"fieldA = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Select":"ALL_ATTRIBUTES"}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ValidationException")) {
			t.Fatalf("invalid projection %d %s", status, body)
		}
		if status, body := call("Query", `{"TableName":"Indexes","IndexName":"all","KeyConditionExpression":"fieldB = :v","ExpressionAttributeValues":{":v":{"S":"b"}},"Select":"ALL_ATTRIBUTES"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"data":{"S":"value"}`)) {
			t.Fatalf("all projection %d %s", status, body)
		}
	})

	t.Run("Given two SET clauses When updating an item Then both attributes persist", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"Updates","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`); status != http.StatusOK {
			t.Fatalf("create updates table %d %s", status, body)
		}
		if status, body := call("PutItem", `{"TableName":"Updates","Item":{"id":{"S":"1"}}}`); status != http.StatusOK {
			t.Fatalf("put update item %d %s", status, body)
		}
		payload := `{"TableName":"Updates","Key":{"id":{"S":"1"}},"UpdateExpression":"SET attr1 = :v1, attr2 = :v2","ExpressionAttributeValues":{":v1":{"S":"value1"},":v2":{"S":"value2"}}}`
		if status, body := call("UpdateItem", payload); status != http.StatusOK {
			t.Fatalf("update item %d %s", status, body)
		}
		if status, body := call("GetItem", `{"TableName":"Updates","Key":{"id":{"S":"1"}}}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"attr1":{"S":"value1"}`)) || !bytes.Contains(body, []byte(`"attr2":{"S":"value2"}`)) {
			t.Fatalf("updated item %d %s", status, body)
		}
	})

	t.Run("Given PutItem ALL_OLD When replacing an item Then only existing attributes are returned", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"Returns","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`); status != http.StatusOK {
			t.Fatalf("create returns table %d %s", status, body)
		}
		first := `{"TableName":"Returns","Item":{"id":{"S":"1"},"data":{"S":"foobar"}},"ReturnValues":"ALL_OLD"}`
		if status, body := call("PutItem", first); status != http.StatusOK || bytes.Contains(body, []byte(`"Attributes"`)) {
			t.Fatalf("first all-old %d %s", status, body)
		}
		second := `{"TableName":"Returns","Item":{"id":{"S":"1"},"data":{"S":"barfoo"}},"ReturnValues":"ALL_OLD"}`
		if status, body := call("PutItem", second); status != http.StatusOK || !bytes.Contains(body, []byte(`"Attributes":{"data":{"S":"foobar"}`)) {
			t.Fatalf("replacement all-old %d %s", status, body)
		}
	})

	t.Run("Given empty and binary values When writing single and batch items Then bytes round trip", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"BinaryValues","KeySchema":[{"AttributeName":"PK","KeyType":"HASH"},{"AttributeName":"SK","KeyType":"RANGE"}]}`); status != http.StatusOK {
			t.Fatalf("create binary table %d %s", status, body)
		}
		if status, body := call("PutItem", `{"TableName":"BinaryValues","Item":{"PK":{"S":"empty"},"SK":{"S":"item"},"data":{"S":""}}}`); status != http.StatusOK || !bytes.Equal(bytes.TrimSpace(body), []byte("{}")) {
			t.Fatalf("put empty value %d %s", status, body)
		}
		batch := `{"RequestItems":{"BinaryValues":[{"PutRequest":{"Item":{"PK":{"S":"binary"},"SK":{"S":"one"},"data":{"B":"kA=="}}}},{"PutRequest":{"Item":{"PK":{"S":"binary"},"SK":{"S":"two"},"data":{"B":"dGVzdCDAIN0="}}}}]}}`
		if status, body := call("BatchWriteItem", batch); status != http.StatusOK || !bytes.Contains(body, []byte(`"UnprocessedItems":{}`)) {
			t.Fatalf("batch binary values %d %s", status, body)
		}
		if status, body := call("GetItem", `{"TableName":"BinaryValues","Key":{"PK":{"S":"binary"},"SK":{"S":"two"}}}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"B":"dGVzdCDAIN0="`)) {
			t.Fatalf("get binary value %d %s", status, body)
		}
	})

	t.Run("Given a table class When creating and updating a table Then the class summary persists", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"TableClass","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"TableClass":"STANDARD"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"TableClassSummary":{"TableClass":"STANDARD"}`)) {
			t.Fatalf("create table class %d %s", status, body)
		}
		if status, body := call("UpdateTable", `{"TableName":"TableClass","TableClass":"STANDARD_INFREQUENT_ACCESS"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"TableClassSummary":{"TableClass":"STANDARD_INFREQUENT_ACCESS"}`)) {
			t.Fatalf("update table class %d %s", status, body)
		}
		if status, body := call("DescribeTable", `{"TableName":"TableClass"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"TableClassSummary":{"TableClass":"STANDARD_INFREQUENT_ACCESS"}`)) {
			t.Fatalf("describe table class %d %s", status, body)
		}
	})
}
