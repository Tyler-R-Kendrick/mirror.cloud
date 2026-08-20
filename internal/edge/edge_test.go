package edge_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sts"
)

func TestS3PutGetAndForeignService501(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := edge.New(cfg, deps, reg, "test")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	mkb, err := http.NewRequest(http.MethodPut, ts.URL+"/mybucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	mkb.Header.Set("Authorization", auth)
	bres, err := http.DefaultClient.Do(mkb)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, bres.Body)
	bres.Body.Close()
	if bres.StatusCode >= 300 {
		t.Fatalf("create bucket status %d", bres.StatusCode)
	}

	put, err := http.NewRequest(http.MethodPut, ts.URL+"/mybucket/hello.txt", bytes.NewReader([]byte("hi")))
	if err != nil {
		t.Fatal(err)
	}
	put.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("put status %d", res.StatusCode)
	}

	get, _ := http.NewRequest(http.MethodGet, ts.URL+"/mybucket/hello.txt", nil)
	get.Header.Set("Authorization", put.Header.Get("Authorization"))
	gres, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(gres.Body)
	gres.Body.Close()
	if gres.StatusCode != 200 {
		t.Fatalf("get status %d body %s", gres.StatusCode, body)
	}
	if string(body) != "hi" {
		t.Fatalf("got %q", body)
	}

	ddb, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{}`))
	ddb.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	ddb.Header.Set("Content-Type", "application/x-amz-json-1.0")
	dres, err := http.DefaultClient.Do(ddb)
	if err != nil {
		t.Fatal(err)
	}
	db, _ := io.ReadAll(dres.Body)
	dres.Body.Close()
	if dres.StatusCode != 501 {
		t.Fatalf("expected 501 for dynamodb on s3-only boot, got %d %s", dres.StatusCode, db)
	}
	if dres.Header.Get("x-mirror-not-implemented") == "" && !strings.Contains(string(db), "MirrorNotImplemented") {
		t.Fatalf("not a §4.11 error: %s %v", db, dres.Header)
	}
}

func TestHealth(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	reg, err := registry.New(deps, []string{"aws.s3"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	res, err := http.Get(ts.URL + "/_mirror/health")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal(res.Status)
	}
}

func TestAWSChunkedDeframe(t *testing.T) {
	// framed: size;chunk-signature=x\r\npayload\r\n0;chunk-signature=x\r\n\r\n
	payload := "hello-chunk"
	framed := "b;chunk-signature=abc\r\n" + payload + "\r\n0;chunk-signature=abc\r\n\r\n"
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	mkb, _ := http.NewRequest(http.MethodPut, ts.URL+"/bkt", nil)
	mkb.Header.Set("Authorization", auth)
	bres, err := http.DefaultClient.Do(mkb)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, bres.Body)
	bres.Body.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/bkt/obj", strings.NewReader(framed))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", "11")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	get, _ := http.NewRequest(http.MethodGet, ts.URL+"/bkt/obj", nil)
	get.Header.Set("Authorization", req.Header.Get("Authorization"))
	gres, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(gres.Body)
	gres.Body.Close()
	if string(body) != payload {
		t.Fatalf("stored framing? got %q", body)
	}
}
