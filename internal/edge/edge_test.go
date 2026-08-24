package edge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
)

type recordingAuthorizer struct{ checks []string }

func (a *recordingAuthorizer) Authorize(_ context.Context, _ spi.Identity, _, operation, resource string) error {
	a.checks = append(a.checks, operation+" "+resource)
	return nil
}

func (a *recordingAuthorizer) AuthorizeRequest(_ context.Context, req *spi.Request, resource string) error {
	a.checks = append(a.checks, req.Operation+" "+resource)
	return nil
}

func TestS3CopyObjectAuthorizationChecks(t *testing.T) {
	deps := spitest.Deps(t)
	authorizer := &recordingAuthorizer{}
	deps.Authorizer = authorizer
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	do := func(path string, body []byte, headers map[string]string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, ts.URL+path, bytes.NewReader(body))
		req.Header.Set("Authorization", auth)
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			t.Fatalf("PUT %s: %s", path, resp.Status)
		}
	}
	do("/source", nil, nil)
	do("/destination", nil, nil)
	do("/source/key", []byte("body"), nil)
	authorizer.checks = nil
	do("/destination/copy", nil, map[string]string{"x-amz-copy-source": "/source/key"})
	want := "GetObject arn:aws:s3:::source/key\nPutObject arn:aws:s3:::destination/copy"
	if got := strings.Join(authorizer.checks, "\n"); got != want {
		t.Fatalf("copy checks:\n%s\nwant:\n%s", got, want)
	}
	authorizer.checks = nil
	do("/destination/tagged", nil, map[string]string{"x-amz-copy-source": "/source/key", "x-amz-tagging-directive": "REPLACE", "x-amz-tagging": "team=data"})
	if got := strings.Join(authorizer.checks, "\n"); !strings.HasSuffix(got, "PutObjectTagging arn:aws:s3:::destination/tagged") {
		t.Fatalf("replace-tagging checks:\n%s", got)
	}
}

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

	unknown, _ := http.NewRequest(http.MethodPost, ts.URL+"/", nil)
	unknown.Header.Set("X-Amz-Target", "UnknownService.UnknownOperation")
	ures, err := http.DefaultClient.Do(unknown)
	if err != nil {
		t.Fatal(err)
	}
	ubody, _ := io.ReadAll(ures.Body)
	ures.Body.Close()
	if ures.StatusCode != http.StatusNotImplemented || !strings.Contains(string(ubody), "unknown service") {
		t.Fatalf("unknown service %d %s", ures.StatusCode, ubody)
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

func TestDiagnostics(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	for _, path := range []string{"/_mirror/services", "/_mirror/journal?service=aws.s3", "/_mirror/model/aws.s3"} {
		response, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || !json.Valid(body) {
			t.Fatalf("GET %s: %d %s", path, response.StatusCode, body)
		}
	}
	response, err := http.Post(ts.URL+"/_mirror/clock/advance", "application/json", strings.NewReader(`{"duration":"1s"}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("advance clock: %s", response.Status)
	}
	response, err = http.Get(ts.URL + "/_mirror/missing")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing diagnostic: %s", response.Status)
	}
	response, err = http.Post(ts.URL+"/_mirror/reset", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("reset: %s", response.Status)
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
