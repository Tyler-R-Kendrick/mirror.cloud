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
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
)

func TestDynamoDBTableLifecycle(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.dynamodb", "aws.kms"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	request := func(target, authorization, action, payload string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL, bytes.NewBufferString(payload))
		req.Header.Set("Authorization", authorization)
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", target+"."+action)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, body
	}
	call := func(action, payload string) (int, []byte) {
		return request("DynamoDB_20120810", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=00", action, payload)
	}
	streamCall := func(action, payload string) (int, []byte) {
		return request("DynamoDBStreams_20120810", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/streams/aws4_request, SignedHeaders=host, Signature=00", action, payload)
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
		get := `{"RequestItems":{"BinaryValues":{"Keys":[{"PK":{"S":"binary"},"SK":{"S":"one"}},{"PK":{"S":"missing"},"SK":{"S":"item"}}]}}}`
		if status, body := call("BatchGetItem", get); status != http.StatusOK || !bytes.Contains(body, []byte(`"B":"kA=="`)) || !bytes.Contains(body, []byte(`"UnprocessedKeys":{}`)) {
			t.Fatalf("batch get values %d %s", status, body)
		}
		change := `{"RequestItems":{"BinaryValues":[{"DeleteRequest":{"Key":{"PK":{"S":"binary"},"SK":{"S":"one"}}}},{"PutRequest":{"Item":{"PK":{"S":"binary"},"SK":{"S":"three"}}}}]}}`
		if status, body := call("BatchWriteItem", change); status != http.StatusOK || !bytes.Contains(body, []byte(`"UnprocessedItems":{}`)) {
			t.Fatalf("batch delete and put %d %s", status, body)
		}
		if status, body := call("GetItem", `{"TableName":"BinaryValues","Key":{"PK":{"S":"binary"},"SK":{"S":"one"}}}`); status != http.StatusOK || bytes.Contains(body, []byte(`"Item"`)) {
			t.Fatalf("batch delete %d %s", status, body)
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

	t.Run("Given billing encryption indexes and warm capacity When creating tables Then metadata matches AWS", func(t *testing.T) {
		invalid := `{"TableName":"InvalidBilling","BillingMode":"PAY_PER_REQUEST","ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}}`
		if status, body := call("CreateTable", invalid); status != http.StatusBadRequest || !bytes.Contains(body, []byte("Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST")) {
			t.Fatalf("invalid billing metadata %d %s", status, body)
		}
		encrypted := `{"TableName":"EncryptedMetadata","SSESpecification":{"Enabled":true,"SSEType":"KMS","KMSMasterKeyId":"key-id"}}`
		if status, body := call("CreateTable", encrypted); status != http.StatusOK || !bytes.Contains(body, []byte(`"SSEDescription":{"KMSMasterKeyArn":"arn:aws:kms:us-east-1:000000000000:key/key-id","SSEType":"KMS","Status":"ENABLED"}`)) {
			t.Fatalf("encrypted metadata %d %s", status, body)
		}
		onDemand := `{"TableName":"MetadataBDD","BillingMode":"PAY_PER_REQUEST","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"GlobalSecondaryIndexes":[{"IndexName":"by-value","KeySchema":[{"AttributeName":"value","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}],"WarmThroughput":{"ReadUnitsPerSecond":1000,"WriteUnitsPerSecond":1200}}`
		if status, body := call("CreateTable", onDemand); status != http.StatusOK || !bytes.Contains(body, []byte(`"BillingModeSummary":{"BillingMode":"PAY_PER_REQUEST"}`)) || !bytes.Contains(body, []byte(`"IndexStatus":"CREATING"`)) || !bytes.Contains(body, []byte(`"Status":"UPDATING"`)) {
			t.Fatalf("create on-demand metadata %d %s", status, body)
		}
		if status, body := call("DescribeTable", `{"TableName":"MetadataBDD"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"IndexStatus":"ACTIVE"`)) || !bytes.Contains(body, []byte(`"Status":"ACTIVE"`)) {
			t.Fatalf("describe on-demand metadata %d %s", status, body)
		}
		provisioned := `{"TableName":"ProvisionedMetadataBDD","ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5},"GlobalSecondaryIndexes":[{"IndexName":"by-value","ProvisionedThroughput":{"ReadCapacityUnits":1,"WriteCapacityUnits":1}}]}`
		if status, body := call("CreateTable", provisioned); status != http.StatusOK || !bytes.Contains(body, []byte(`"ProvisionedThroughput":{"NumberOfDecreasesToday":0,"ReadCapacityUnits":1,"WriteCapacityUnits":1}`)) {
			t.Fatalf("provisioned metadata %d %s", status, body)
		}
	})

	t.Run("Given partial server-side encryption When tables and metadata change Then one default KMS key is preserved", func(t *testing.T) {
		create := `{"TableName":"DefaultEncryptedBDD","ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5},"SSESpecification":{"Enabled":true}}`
		status, body := call("CreateTable", create)
		var created map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &created) != nil {
			t.Fatalf("create default encryption %d %s", status, body)
		}
		keyARN := created["TableDescription"].(map[string]any)["SSEDescription"].(map[string]any)["KMSMasterKeyArn"].(string)
		kmsPayload := `{"KeyId":` + fmt.Sprintf("%q", keyARN) + `}`
		if status, body := request("TrentService", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/kms/aws4_request, SignedHeaders=host, Signature=00", "DescribeKey", kmsPayload); status != http.StatusOK || !bytes.Contains(body, []byte(`"KeyManager":"AWS"`)) || !bytes.Contains(body, []byte(`"Description":"Default key that protects my DynamoDB data when no other key is defined"`)) {
			t.Fatalf("describe default encryption key %d %s", status, body)
		}
		if status, body := call("CreateTable", `{"TableName":"AlsoEncryptedBDD","SSESpecification":{"Enabled":true}}`); status != http.StatusOK || !bytes.Contains(body, []byte(keyARN)) {
			t.Fatalf("reuse default encryption key %d %s", status, body)
		}
		if status, body := call("UpdateTable", `{"TableName":"DefaultEncryptedBDD","SSESpecification":{"Enabled":false}}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"Status":"UPDATING"`)) {
			t.Fatalf("disable default encryption %d %s", status, body)
		}
		if status, body := call("UpdateTable", `{"TableName":"DefaultEncryptedBDD","BillingMode":"PAY_PER_REQUEST"}`); status != http.StatusOK || !bytes.Contains(body, []byte(keyARN)) || !bytes.Contains(body, []byte(`"Status":"ENABLED"`)) {
			t.Fatalf("preserve default encryption %d %s", status, body)
		}
	})

	t.Run("Given PartiQL statements When batching transactions and missing predicates Then AWS responses are preserved", func(t *testing.T) {
		if status, body := call("CreateTable", `{"TableName":"PartiQL","KeySchema":[{"AttributeName":"Username","KeyType":"HASH"}]}`); status != http.StatusOK {
			t.Fatalf("create PartiQL table %d %s", status, body)
		}
		if status, body := call("PutItem", `{"TableName":"PartiQL","Item":{"Username":{"S":"user02"}}}`); status != http.StatusOK {
			t.Fatalf("put PartiQL item %d %s", status, body)
		}
		batch := `{"Statements":[{"Statement":"INSERT INTO PartiQL VALUE {'Username': 'user01', 'FirstName': 'Alice'}"},{"Statement":"UPDATE PartiQL SET Age=20 WHERE Username='user02'"}]}`
		if status, body := call("BatchExecuteStatement", batch); status != http.StatusOK || bytes.Count(body, []byte(`"TableName":"PartiQL"`)) != 2 {
			t.Fatalf("batch PartiQL %d %s", status, body)
		}
		transaction := `{"TransactStatements":[{"Statement":"INSERT INTO PartiQL VALUE {'Username': 'user03'}"},{"Statement":"INSERT INTO PartiQL VALUE {'Username': 'user04'}"}]}`
		if status, body := call("ExecuteTransaction", transaction); status != http.StatusOK || !bytes.Contains(body, []byte(`"Responses":[]`)) {
			t.Fatalf("transaction PartiQL %d %s", status, body)
		}
		if status, body := call("ExecuteStatement", `{"Statement":"SELECT * FROM PartiQL WHERE FirstName IS NOT MISSING"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"FirstName":{"S":"Alice"}`)) || bytes.Contains(body, []byte(`"user02"`)) {
			t.Fatalf("PartiQL not missing %d %s", status, body)
		}
		if status, body := call("ExecuteStatement", `{"Statement":"SELECT * FROM PartiQL WHERE FirstName IS MISSING"}`); status != http.StatusOK || bytes.Contains(body, []byte(`"user01"`)) || !bytes.Contains(body, []byte(`"user02"`)) {
			t.Fatalf("PartiQL missing %d %s", status, body)
		}
		if status, body := call("ExecuteStatement", `{"Statement":"SELECT * FROM PartiQL","Parameters":[]}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("Member must have length greater than or equal to 1")) {
			t.Fatalf("PartiQL empty parameters %d %s", status, body)
		}
	})

	t.Run("Given transaction items When writing and reading Then commits are atomic", func(t *testing.T) {
		status, body := call("CreateTable", `{"TableName":"TxnBDD","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`)
		var created map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &created) != nil {
			t.Fatalf("create transaction table %d %s", status, body)
		}
		arn := created["TableDescription"].(map[string]any)["TableArn"].(string)
		write := `{"ClientRequestToken":"bdd-token","TransactItems":[{"ConditionCheck":{"TableName":"TxnBDD","Key":{"id":{"S":"missing"}},"ConditionExpression":"attribute_not_exists(id)"}},{"Put":{"TableName":"TxnBDD","Item":{"id":{"S":"binary"},"data":{"B":"kA=="}}}},{"Update":{"TableName":"` + arn + `","Key":{"id":{"S":"updated"}},"UpdateExpression":"SET value = :v","ExpressionAttributeValues":{":v":{"S":"yes"}}}}]}`
		if status, body := call("TransactWriteItems", write); status != http.StatusOK {
			t.Fatalf("write transaction %d %s", status, body)
		}
		if status, body := call("TransactWriteItems", write); status != http.StatusOK {
			t.Fatalf("replay transaction %d %s", status, body)
		}
		get := `{"TransactItems":[{"Get":{"TableName":"` + arn + `","Key":{"id":{"S":"binary"}},"ProjectionExpression":"id, data"}},{"Get":{"TableName":"TxnBDD","Key":{"id":{"S":"updated"}}}}]}`
		if status, body := call("TransactGetItems", get); status != http.StatusOK || !bytes.Contains(body, []byte(`"B":"kA=="`)) || !bytes.Contains(body, []byte(`"value":{"S":"yes"}`)) {
			t.Fatalf("get transaction %d %s", status, body)
		}
		cancel := `{"TransactItems":[{"ConditionCheck":{"TableName":"TxnBDD","Key":{"id":{"S":"binary"}},"ConditionExpression":"attribute_not_exists(id)","ReturnValuesOnConditionCheckFailure":"ALL_OLD"}},{"Put":{"TableName":"TxnBDD","Item":{"id":{"S":"blocked"}}}}]}`
		if status, body := call("TransactWriteItems", cancel); status != http.StatusBadRequest || !bytes.Contains(body, []byte("TransactionCanceledException")) || !bytes.Contains(body, []byte(`"B":"kA=="`)) {
			t.Fatalf("cancel transaction %d %s", status, body)
		}
		if status, body := call("GetItem", `{"TableName":"TxnBDD","Key":{"id":{"S":"blocked"}}}`); status != http.StatusOK || bytes.Contains(body, []byte(`"Item"`)) {
			t.Fatalf("transaction rollback %d %s", status, body)
		}
	})

	t.Run("Given a DynamoDB stream When reading its shard Then metadata records and iterators match AWS", func(t *testing.T) {
		create := `{"TableName":"StreamBDD","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"}}`
		status, body := call("CreateTable", create)
		var created map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &created) != nil {
			t.Fatalf("create stream table %d %s", status, body)
		}
		arn := created["TableDescription"].(map[string]any)["LatestStreamArn"].(string)
		item := `{"TableName":"StreamBDD","Item":{"id":{"S":"one"},"data":{"B":"kA=="}}}`
		if status, body := call("PutItem", item); status != http.StatusOK {
			t.Fatalf("put stream item %d %s", status, body)
		}
		if status, body := call("PutItem", item); status != http.StatusOK {
			t.Fatalf("repeat stream item %d %s", status, body)
		}
		status, body = streamCall("DescribeStream", `{"StreamArn":"`+arn+`"}`)
		var described map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &described) != nil || !bytes.Contains(body, []byte(`"StreamViewType":"NEW_IMAGE"`)) {
			t.Fatalf("describe stream %d %s", status, body)
		}
		shard := described["StreamDescription"].(map[string]any)["Shards"].([]any)[0].(map[string]any)["ShardId"].(string)
		if status, body := streamCall("DescribeStream", `{"StreamArn":"`+arn+`","ExclusiveStartShardId":"`+shard+`"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"Shards":[]`)) {
			t.Fatalf("exclusive stream shard %d %s", status, body)
		}
		status, body = streamCall("GetShardIterator", `{"StreamArn":"`+arn+`","ShardId":"`+shard+`","ShardIteratorType":"TRIM_HORIZON"}`)
		var iterator map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &iterator) != nil || !strings.HasPrefix(iterator["ShardIterator"].(string), arn+"|") {
			t.Fatalf("stream iterator %d %s", status, body)
		}
		payload := `{"ShardIterator":` + fmt.Sprintf("%q", iterator["ShardIterator"]) + `}`
		if status, body := streamCall("GetRecords", payload); status != http.StatusOK || bytes.Count(body, []byte(`"eventName":"INSERT"`)) != 1 || !bytes.Contains(body, []byte(`"SizeBytes":15`)) || !bytes.Contains(body, []byte(`"B":"kA=="`)) {
			t.Fatalf("stream records %d %s", status, body)
		}
	})
}
