package spine

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestBootedServerAzureBlob(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"azure.blobs"}
	cfg.Seed = "az-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body string, hdr map[string]string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "acct.blob.core.windows.net"
		req.Header.Set("Authorization", "Bearer test")
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}
	code, raw, _ := do(http.MethodPut, "/ctr?restype=container", "", nil)
	if code != 201 {
		t.Fatalf("create %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/?comp=list", "", nil)
	if code != 200 || !strings.Contains(string(raw), "EnumerationResults") || !strings.Contains(string(raw), "<Name>ctr</Name>") {
		t.Fatalf("list %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr?restype=container", "", nil)
	if code != 200 {
		t.Fatalf("get %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/o", "hello-azure", map[string]string{"x-ms-blob-type": "BlockBlob", "Content-Type": "text/plain"})
	if code != 201 {
		t.Fatalf("put %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/o", "", nil)
	if code != 200 || string(raw) != "hello-azure" {
		t.Fatalf("get blob %d %s", code, raw)
	}
	code, _, _ = do(http.MethodDelete, "/ctr/o", "", nil)
	if code != 202 {
		t.Fatalf("delete blob %d", code)
	}
	code, raw, h := do(http.MethodGet, "/ctr/o", "", nil)
	if code != 404 || !strings.Contains(string(raw), "<Code>BlobNotFound</Code>") || h.Get("x-ms-error-code") != "BlobNotFound" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing blob %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/missing?restype=container", "", nil)
	if code != 404 || !strings.Contains(string(raw), "<Code>ContainerNotFound</Code>") || h.Get("x-ms-error-code") != "ContainerNotFound" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing container %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodDelete, "/ctr/nope", "", nil)
	if code != 404 || !strings.Contains(string(raw), "<Code>BlobNotFound</Code>") || h.Get("x-ms-error-code") != "BlobNotFound" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing blob %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodDelete, "/missing?restype=container", "", nil)
	if code != 404 || !strings.Contains(string(raw), "<Code>ContainerNotFound</Code>") || h.Get("x-ms-error-code") != "ContainerNotFound" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing container %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr?restype=container", "", nil)
	if code != 201 && code != 409 {
		t.Fatalf("recreate %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/part?comp=block&blockid=YQ==", "A", map[string]string{"x-ms-blob-type": "BlockBlob"})
	if code >= 300 {
		t.Fatalf("put block %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/part?comp=block&blockid=Yg==", "B", nil)
	if code >= 300 {
		t.Fatalf("put block 2 %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/part?comp=blocklist", "", nil)
	if code != 200 || !strings.Contains(string(raw), "BlockList") {
		t.Fatalf("get block list %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/part?comp=blocklist", `<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>YQ==</Latest><Latest>Yg==</Latest></BlockList>`, nil)
	if code >= 300 {
		t.Fatalf("put block list %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/part", "", nil)
	if code != 200 || string(raw) != "AB" {
		t.Fatalf("get assembled %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/part?comp=blocklist", `<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>Yg==</Latest><Latest>YQ==</Latest></BlockList>`, nil)
	if code >= 300 {
		t.Fatalf("put block list reverse %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/part", "", nil)
	if code != 200 || string(raw) != "BA" {
		t.Fatalf("get assembled reverse %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/part?comp=blocklist", `<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>missing</Latest></BlockList>`, nil)
	if code != 400 || h.Get("x-ms-error-code") != "InvalidBlockList" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing block id %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/log?comp=appendblock", "one", nil)
	if code >= 300 {
		t.Fatalf("append %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/log?comp=appendblock", "two", nil)
	if code >= 300 {
		t.Fatalf("append 2 %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/log", "", nil)
	if code != 200 || string(raw) != "onetwo" {
		t.Fatalf("get append %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr?restype=container&comp=metadata", "", map[string]string{"x-ms-meta-keya": "vala"})
	if code >= 300 {
		t.Fatalf("set metadata %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr?restype=container&comp=metadata", "", nil)
	if code != 200 || h.Get("x-ms-meta-keya") != "vala" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("get metadata %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr?restype=container&comp=acl", "<SignedIdentifiers><SignedIdentifier><Id>p1</Id></SignedIdentifier></SignedIdentifiers>", map[string]string{"x-ms-blob-public-access": "blob"})
	if code >= 300 {
		t.Fatalf("set acl %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr?restype=container&comp=acl", "", nil)
	if code != 200 || !strings.Contains(string(raw), "p1") || h.Get("x-ms-blob-public-access") != "blob" {
		t.Fatalf("get acl %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr?restype=container&comp=lease", "", map[string]string{"x-ms-lease-action": "acquire", "x-ms-proposed-lease-id": "ca761232ed4211cebacd00aa0057b223", "x-ms-lease-duration": "-1"})
	if code != 201 || h.Get("x-ms-lease-id") != "ca761232ed4211cebacd00aa0057b223" {
		t.Fatalf("acquire %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr?restype=container", "", nil)
	if code != 200 || h.Get("x-ms-lease-status") != "locked" || h.Get("x-ms-lease-state") != "leased" {
		t.Fatalf("leased props %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr?restype=container&comp=lease", "", map[string]string{"x-ms-lease-action": "release", "x-ms-lease-id": "ca761232ed4211cebacd00aa0057b223"})
	if code >= 300 {
		t.Fatalf("release %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/missing?restype=container&comp=metadata", "", nil)
	if code != 404 || !strings.Contains(string(raw), "ContainerNotFound") || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing metadata %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/?restype=service&comp=properties", "", nil)
	if code != 200 || !strings.Contains(string(raw), "StorageServiceProperties") {
		t.Fatalf("get service props %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/?restype=service&comp=properties", "<StorageServiceProperties><Cors><CorsRule><AllowedOrigins>example.com</AllowedOrigins><AllowedMethods>GET</AllowedMethods><MaxAgeInSeconds>1</MaxAgeInSeconds><ExposedHeaders></ExposedHeaders><AllowedHeaders></AllowedHeaders></CorsRule></Cors></StorageServiceProperties>", nil)
	if code != 202 {
		t.Fatalf("set service props %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/?restype=service&comp=properties", "", nil)
	if code != 200 || !strings.Contains(string(raw), "example.com") {
		t.Fatalf("get service props after set %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/?restype=account&comp=properties", "", nil)
	if code != 200 || h.Get("x-ms-account-kind") != "StorageV2" || h.Get("x-ms-sku-name") != "Standard_RAGRS" || h.Get("x-ms-is-hns-enabled") != "false" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("account info %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr?restype=account&comp=properties", "", nil)
	if code != 200 || h.Get("x-ms-account-kind") != "StorageV2" {
		t.Fatalf("container account info %d %#v", code, h)
	}
	code, raw, h = do(http.MethodGet, "/ctr/o?restype=account&comp=properties", "", nil)
	if code != 200 || h.Get("x-ms-account-kind") != "StorageV2" {
		t.Fatalf("blob account info %d %#v", code, h)
	}
	code, raw, h = do(http.MethodGet, "/?restype=service&comp=stats", "", nil)
	if code != 400 || h.Get("x-ms-error-code") != "InvalidQueryParameterValue" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("stats primary %d %#v %s", code, h, raw)
	}
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/?restype=service&comp=stats", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "acct-secondary.blob.core.windows.net"
	req.Header.Set("Authorization", "Bearer test")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(string(b), "<Status>live</Status>") || res.Header.Get("x-amzn-errortype") != "" {
		t.Fatalf("stats secondary %d %#v %s", res.StatusCode, res.Header, b)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/o", "hello-azure", map[string]string{"x-ms-blob-type": "BlockBlob"})
	if code != 201 {
		t.Fatalf("re-put blob %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/o?comp=metadata", "", map[string]string{"x-ms-meta-a": "b"})
	if code >= 300 {
		t.Fatalf("set blob metadata %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr/o?comp=metadata", "", nil)
	if code != 200 || h.Get("x-ms-meta-a") != "b" {
		t.Fatalf("get blob metadata %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/o?comp=properties", "", map[string]string{"x-ms-blob-content-type": "text/plain", "x-ms-blob-cache-control": "no-cache"})
	if code >= 300 {
		t.Fatalf("set blob properties %d %s", code, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/o", "", nil)
	if code != 200 || h.Get("x-ms-meta-a") != "b" || h.Get("Content-Type") != "text/plain" || h.Get("Cache-Control") != "no-cache" || h.Get("x-ms-blob-type") != "BlockBlob" || len(raw) != 0 {
		t.Fatalf("head properties %d %#v %q", code, h, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/missing", "", nil)
	if code != 404 || h.Get("x-ms-error-code") != "BlobNotFound" || h.Get("Content-Type") != "" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("head missing %d %#v %s", code, h, raw)
	}
}

func TestBootedServerAzureQueue(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"azure.queue"}
	cfg.Seed = "azq-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "acct.queue.core.windows.net"
		req.Header.Set("Authorization", "Bearer test")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}
	code, raw, _ := do(http.MethodPut, "/q1", "")
	if code >= 300 {
		t.Fatalf("create queue %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/?comp=list", "")
	if code != 200 || !strings.Contains(string(raw), "q1") {
		t.Fatalf("list queues %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/q1/messages", "hello-q")
	if code >= 300 {
		t.Fatalf("put message %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/q1/messages", "")
	if code != 200 || !strings.Contains(string(raw), "hello-q") {
		t.Fatalf("get messages %d %s", code, raw)
	}
	code, raw, h := do(http.MethodDelete, "/missing", "")
	if code != 404 || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing queue %d %#v %s", code, h, raw)
	}
}

func TestBootedServerAzureTable(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"azure.table"}
	cfg.Seed = "azt-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "acct.table.core.windows.net"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}
	code, raw, _ := do(http.MethodPost, "/Tables", `{"TableName":"t1"}`)
	if code >= 300 {
		t.Fatalf("create table %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/Tables", "")
	if code != 200 || !strings.Contains(string(raw), "t1") {
		t.Fatalf("list tables %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/t1", `{"PartitionKey":"p","RowKey":"r"}`)
	if code >= 300 {
		t.Fatalf("insert entity %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/t1()", "")
	if code != 200 || !strings.Contains(string(raw), "PartitionKey") {
		t.Fatalf("query entities %d %s", code, raw)
	}
	code, raw, h := do(http.MethodDelete, "/Tables('missing')", "")
	if code != 404 || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing table %d %#v %s", code, h, raw)
	}
}
