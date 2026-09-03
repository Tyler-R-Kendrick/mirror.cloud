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
}
