package s3tables

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestS3TablesHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 13 {
		t.Fatalf("s3tables Operations() %d want 13", n)
	}
}

func TestS3TablesRowMutations(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	identity := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateTableBucket", Input: map[string]any{"name": "warehouse"}}); err != nil {
		t.Fatal(err)
	}
	if err := p.CreateTable(ctx, identity, "warehouse", "analytics", "events", []string{"id", "name"}); err != nil {
		t.Fatal(err)
	}
	if err := p.ApplyRows(ctx, identity, "warehouse", "analytics", "events", []RowMutation{
		{Values: map[string]any{"id": "1", "name": "alice"}},
		{Values: map[string]any{"id": "2", "name": "bob"}},
		{Operation: "update", Values: map[string]any{"id": "2", "name": "robert"}, UniqueKeys: []string{"id"}},
		{Operation: "update", Values: map[string]any{"id": "3", "name": "carol"}, UniqueKeys: []string{"id"}},
		{Operation: "delete", Values: map[string]any{"id": "1"}, UniqueKeys: []string{"id"}},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := p.TableRows(ctx, identity, "warehouse", "analytics", "events")
	want := []map[string]any{{"id": "2", "name": "robert"}, {"id": "3", "name": "carol"}}
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("S3 table rows %#v, %v", rows, err)
	}
	for name, mutation := range map[string]RowMutation{
		"operation": {Operation: "merge", Values: map[string]any{"id": "4"}},
		"column":    {Values: map[string]any{"missing": true}},
		"keys":      {Operation: "update", Values: map[string]any{"id": "4"}},
		"key":       {Operation: "delete", Values: map[string]any{"id": "4"}, UniqueKeys: []string{"missing"}},
	} {
		if err := p.ApplyRows(ctx, identity, "warehouse", "analytics", "events", []RowMutation{mutation}); err == nil {
			t.Errorf("accepted invalid S3 table mutation %s", name)
		}
	}
}

func TestBootedServerS3TablesCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3tables"}
	cfg.Seed = "s3t-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3tables/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "S3Tables."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreateTableBucket", `{"name":"tb1"}`)
	if created["arn"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetTableBucket", `{"name":"tb1"}`)
	if got["name"] != "tb1" {
		t.Fatalf("get %v", got)
	}
	call("DeleteTableBucket", `{"name":"tb1"}`)
	listed := call("ListTableBuckets", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"tb1"`) {
		t.Fatalf("still present %s", raw)
	}
}
