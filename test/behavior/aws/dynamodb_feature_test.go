package behavior

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
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
	call := func(action string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL, bytes.NewBufferString(`{"TableName":"T"}`))
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
		if status, body := call("CreateTable"); status != http.StatusOK {
			t.Fatalf("first create %d %s", status, body)
		}
		if status, body := call("CreateTable"); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ResourceInUseException")) || !bytes.Contains(body, []byte("Table already exists: T")) {
			t.Fatalf("duplicate create %d %s", status, body)
		}
	})

	t.Run("Given a deleted table When deleting it again Then ResourceNotFound is returned", func(t *testing.T) {
		if status, body := call("DeleteTable"); status != http.StatusOK {
			t.Fatalf("first delete %d %s", status, body)
		}
		if status, body := call("DeleteTable"); status != http.StatusBadRequest || !bytes.Contains(body, []byte("ResourceNotFoundException")) || !bytes.Contains(body, []byte("Requested resource not found: Table: T not found")) {
			t.Fatalf("missing delete %d %s", status, body)
		}
	})
}
