package spine

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

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
	code, raw, _ = do(http.MethodPut, "/ctr/log", "", map[string]string{"x-ms-blob-type": "AppendBlob"})
	if code != 201 {
		t.Fatalf("create append blob %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/log?comp=appendblock", "one", nil)
	if code != 201 || h.Get("x-ms-blob-append-offset") != "0" || h.Get("x-ms-blob-committed-block-count") != "1" {
		t.Fatalf("append %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/log?comp=appendblock", "two", nil)
	if code != 201 || h.Get("x-ms-blob-append-offset") != "3" || h.Get("x-ms-blob-committed-block-count") != "2" {
		t.Fatalf("append 2 %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/log", "", nil)
	if code != 200 || string(raw) != "onetwo" {
		t.Fatalf("get append %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/nolog?comp=appendblock", "x", nil)
	if code != 404 || h.Get("x-ms-error-code") != "BlobNotFound" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("append missing %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/part?comp=appendblock", "x", nil)
	if code != 409 || h.Get("x-ms-error-code") != "InvalidBlobType" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("append to block blob %d %#v %s", code, h, raw)
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
	etag, lastmod := h.Get("ETag"), h.Get("Last-Modified")
	if etag == "" || lastmod == "" {
		t.Fatalf("head missing entity headers %#v", h)
	}
	// Conditional headers against the real clock.
	code, raw, h = do(http.MethodHead, "/ctr/o", "", map[string]string{"If-None-Match": etag})
	if code != 304 || len(raw) != 0 || h.Get("x-ms-error-code") != "" {
		t.Fatalf("if-none-match %d %#v %q", code, h, raw)
	}
	code, raw, _ = do(http.MethodHead, "/ctr/o", "", map[string]string{"If-Match": etag})
	if code != 200 {
		t.Fatalf("if-match %d %s", code, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/o", "", map[string]string{"If-Match": `"bogus"`})
	if code != 412 || h.Get("x-ms-error-code") != "ConditionNotMet" {
		t.Fatalf("if-match bogus %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/o", "", map[string]string{"If-Modified-Since": "Mon, 01 Jan 2018 00:00:00 GMT"})
	if code != 200 {
		t.Fatalf("if-modified-since past %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/o", "", map[string]string{"If-Modified-Since": "Wed, 01 Jan 2120 00:00:00 GMT"})
	if code != 304 || len(raw) != 0 {
		t.Fatalf("if-modified-since future %d %q", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr/o", "", map[string]string{"If-Unmodified-Since": "Mon, 01 Jan 2018 00:00:00 GMT"})
	if code != 412 || h.Get("x-ms-error-code") != "ConditionNotMet" {
		t.Fatalf("if-unmodified-since past %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/o", "", map[string]string{"If-Unmodified-Since": lastmod})
	if code != 200 {
		t.Fatalf("if-unmodified-since equal %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr/o", "", map[string]string{"If-None-Match": "*"})
	if code != 400 || h.Get("x-ms-error-code") != "UnsatisfiableCondition" {
		t.Fatalf("if-none-match star %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodDelete, "/ctr/o", "", map[string]string{"If-Match": etag, "If-Modified-Since": "Mon, 01 Jan 2018 00:00:00 GMT"})
	if code != 400 || h.Get("x-ms-error-code") != "MultipleConditionHeadersNotSupported" {
		t.Fatalf("condition combination %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/ap", "", map[string]string{"x-ms-blob-type": "AppendBlob"})
	if code != 201 {
		t.Fatalf("create append blob %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/ap?comp=appendblock", "x", nil)
	if code != 201 {
		t.Fatalf("append block %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/ap?comp=appendblock", "y", map[string]string{"x-ms-blob-condition-appendpos": "0"})
	if code != 412 || h.Get("x-ms-error-code") != "AppendPositionConditionNotMet" {
		t.Fatalf("append position %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/ap?comp=appendblock", "y", map[string]string{"x-ms-blob-condition-maxsize": "1"})
	if code != 412 || h.Get("x-ms-error-code") != "MaxBlobSizeConditionNotMet" {
		t.Fatalf("append max size %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/ap?comp=appendblock", "y", map[string]string{"x-ms-blob-condition-appendpos": "1"})
	if code != 201 {
		t.Fatalf("append position ok %d %s", code, raw)
	}

	// Tags: x-ms-tags header on put, tag count on HEAD, set/get XML, filter.
	code, raw, _ = do(http.MethodPut, "/ctr/tg", "tagged", map[string]string{"x-ms-tags": "k1=v1&k2=v2"})
	if code != 201 {
		t.Fatalf("put tagged %d %s", code, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/tg", "", nil)
	if code != 200 || h.Get("x-ms-tag-count") != "2" {
		t.Fatalf("tag count %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/tg?comp=tags", `<?xml version="1.0" encoding="utf-8"?><Tags><TagSet><Tag><Key>c</Key><Value>3</Value></Tag></TagSet></Tags>`, nil)
	if code != 204 {
		t.Fatalf("set tags %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/tg?comp=tags", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Key>c</Key>") || !strings.Contains(string(raw), "<Value>3</Value>") {
		t.Fatalf("get tags %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/tg?comp=tags", `<Tags><TagSet><Tag><Key>bad~key</Key><Value>v</Value></Tag></TagSet></Tags>`, nil)
	if code != 400 || h.Get("x-ms-error-code") != "DuplicateTagNames" {
		t.Fatalf("bad tag chars %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/tg2", "x", map[string]string{"x-ms-tags": "a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9&j=10&k=11"})
	if code != 400 || h.Get("x-ms-error-code") != "TagsTooLarge" {
		t.Fatalf("too many tags %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/tg", "", map[string]string{"x-ms-if-tags": "c='3'"})
	if code != 200 {
		t.Fatalf("if-tags pass %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr/tg", "", map[string]string{"x-ms-if-tags": "c='no'"})
	if code != 412 || h.Get("x-ms-error-code") != "ConditionNotMet" {
		t.Fatalf("if-tags fail %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/ctr/tg", "", map[string]string{"x-ms-if-tags": "c=='3'"})
	if code != 400 || h.Get("x-ms-error-code") != "InvalidHeaderValue" {
		t.Fatalf("if-tags invalid %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/?comp=blobs&where=c%3D%273%27", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Name>tg</Name>") || !strings.Contains(string(raw), "<ContainerName>ctr</ContainerName>") || !strings.Contains(string(raw), "<Where>c='3'</Where>") {
		t.Fatalf("filter blobs %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/?comp=blobs", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Blobs></Blobs>") {
		t.Fatalf("filter blobs where-less %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr?restype=container&comp=blobs&where=c%3D%273%27", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Name>tg</Name>") {
		t.Fatalf("container filter blobs %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/?comp=blobs&where=c%3D%3D%273%27", "", nil)
	if code != 400 || h.Get("x-ms-error-code") != "InvalidQueryParameterValue" {
		t.Fatalf("filter invalid where %d %#v %s", code, h, raw)
	}
	// Hierarchy and include projections on List Blobs.
	code, raw, _ = do(http.MethodPut, "/ctr/dir/a", "x", nil)
	if code != 201 {
		t.Fatalf("put dir blob %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr?restype=container&comp=list&delimiter=/", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<BlobPrefix><Name>dir/</Name></BlobPrefix>") {
		t.Fatalf("hierarchy list %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/tg?comp=snapshot", "", nil)
	if code != 201 {
		t.Fatalf("snapshot tagged %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr?restype=container&comp=list&prefix=tg&include=snapshots,tags", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Snapshot>") || !strings.Contains(string(raw), "<Tag><Key>c</Key><Value>3</Value></Tag>") {
		t.Fatalf("include list %d %s", code, raw)
	}

	// SubmitBatch: multipart fan-out with per-part statuses.
	batchBody := func(parts ...string) string {
		var b strings.Builder
		for i, p := range parts {
			fmt.Fprintf(&b, "--bb%d\r\nContent-Type: application/http\r\nContent-ID: %d\r\n\r\n%s\r\n", 1, i, p)
		}
		fmt.Fprintf(&b, "--bb%d--\r\n", 1)
		return b.String()
	}
	batchCT := "multipart/mixed; boundary=bb1"
	code, raw, _ = do(http.MethodPut, "/ctr/b1", "one", nil)
	if code != 201 {
		t.Fatalf("put b1 %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/b2", "two", nil)
	if code != 201 {
		t.Fatalf("put b2 %d %s", code, raw)
	}
	body := batchBody(
		"DELETE https://acct.blob.core.windows.net/ctr/b1 HTTP/1.1\r\nx-ms-version: 2020-10-02\r\n\r\n",
		"DELETE https://acct.blob.core.windows.net/ctr/ghost HTTP/1.1\r\n\r\n",
		"DELETE /ctr/b2 HTTP/1.1\r\n\r\n",
	)
	code, raw, h = do(http.MethodPost, "/?comp=batch", body, map[string]string{"Content-Type": batchCT})
	if code != 202 || !strings.Contains(string(raw), "HTTP/1.1 202") || !strings.Contains(string(raw), "HTTP/1.1 404") || !containsFold(string(raw), "x-ms-error-code: BlobNotFound") || !strings.Contains(h.Get("Content-Type"), "multipart/mixed") {
		t.Fatalf("batch %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/b1", "", nil)
	if code != 404 {
		t.Fatalf("batch deleted b1 %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/b2", "", nil)
	if code != 404 {
		t.Fatalf("batch deleted b2 %d %s", code, raw)
	}
	// Case-insensitive boundary parameter name is accepted.
	code, raw, _ = do(http.MethodPut, "/ctr/b3", "three", nil)
	if code != 201 {
		t.Fatalf("put b3 %d %s", code, raw)
	}
	body = batchBody("DELETE /ctr/b3 HTTP/1.1\r\n\r\n")
	code, raw, _ = do(http.MethodPost, "/?comp=batch", body, map[string]string{"Content-Type": "multipart/mixed; BOUNDARY=bb1"})
	if code != 202 || !strings.Contains(string(raw), "HTTP/1.1 202") {
		t.Fatalf("batch boundary case %d %s", code, raw)
	}
	// Malformed envelopes fail inside a 202, except a missing Content-Type.
	code, raw, _ = do(http.MethodPost, "/?comp=batch", body, map[string]string{"Content-Type": "multipart/mixed"})
	if code != 202 || !containsFold(string(raw), "x-ms-error-code: InvalidHeaderValue") {
		t.Fatalf("batch no boundary %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/?comp=batch", body, map[string]string{"Content-Type": "multipart/mixed; boundary=a; boundary=b"})
	if code != 202 || !containsFold(string(raw), "x-ms-error-code: InvalidInput") {
		t.Fatalf("batch dup boundary %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/?comp=batch", body, nil)
	if code != 400 {
		t.Fatalf("batch no content-type %d %s", code, raw)
	}
	// Container-scoped batch rejects out-of-scope sub-requests per part.
	code, raw, _ = do(http.MethodPut, "/ctr2?restype=container", "", nil)
	if code != 201 {
		t.Fatalf("create ctr2 %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr2/x", "cross", nil)
	if code != 201 {
		t.Fatalf("put ctr2 blob %d %s", code, raw)
	}
	body = batchBody("DELETE /ctr2/x HTTP/1.1\r\n\r\n")
	code, raw, _ = do(http.MethodPost, "/ctr?restype=container&comp=batch", body, map[string]string{"Content-Type": batchCT})
	if code != 202 || !containsFold(string(raw), "x-ms-error-code: InvalidInput") {
		t.Fatalf("batch cross container %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr2/x", "", nil)
	if code != 200 {
		t.Fatalf("cross-container blob survived %d %s", code, raw)
	}
	body = batchBody("DELETE /ctr2/x HTTP/1.1\r\n\r\n")
	code, raw, _ = do(http.MethodPost, "/ctr2?restype=container&comp=batch", body, map[string]string{"Content-Type": batchCT})
	if code != 202 || !strings.Contains(string(raw), "HTTP/1.1 202") {
		t.Fatalf("batch in scope %d %s", code, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/missing", "", nil)
	if code != 404 || h.Get("x-ms-error-code") != "BlobNotFound" || h.Get("Content-Type") != "" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("head missing %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/p", "", map[string]string{"x-ms-blob-type": "PageBlob", "x-ms-blob-content-length": "1024"})
	if code != 201 {
		t.Fatalf("create page blob %d %s", code, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/p", "", nil)
	if code != 200 || h.Get("x-ms-blob-type") != "PageBlob" || h.Get("Content-Length") != "1024" || h.Get("x-ms-blob-sequence-number") != "0" {
		t.Fatalf("page head %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/p?comp=page", strings.Repeat("a", 512), map[string]string{"x-ms-page-write": "update", "x-ms-range": "bytes=0-511"})
	if code != 201 {
		t.Fatalf("put page %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/p?comp=page", strings.Repeat("b", 512), map[string]string{"x-ms-page-write": "update", "x-ms-range": "bytes=512-1023"})
	if code != 201 {
		t.Fatalf("put page 2 %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/p?comp=pagelist", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Start>0</Start><End>1023</End>") {
		t.Fatalf("page ranges %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/p?comp=pagelist", "", map[string]string{"x-ms-range": "bytes=0-511"})
	if code != 200 || !strings.Contains(string(raw), "<Start>0</Start><End>511</End>") || strings.Contains(string(raw), "1023") {
		t.Fatalf("clipped page ranges %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/p", "", nil)
	if code != 200 || len(raw) != 1024 || string(raw[:512]) != strings.Repeat("a", 512) || string(raw[512:]) != strings.Repeat("b", 512) {
		t.Fatalf("page download %d %d", code, len(raw))
	}
	code, raw, _ = do(http.MethodPut, "/ctr/p?comp=page", "", map[string]string{"x-ms-page-write": "clear", "x-ms-range": "bytes=0-511"})
	if code != 201 {
		t.Fatalf("clear pages %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/p?comp=pagelist", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Start>512</Start><End>1023</End>") {
		t.Fatalf("ranges after clear %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/p?comp=properties", "", map[string]string{"x-ms-blob-content-length": "512"})
	if code != 200 {
		t.Fatalf("resize %d %s", code, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/p", "", nil)
	if code != 200 || h.Get("Content-Length") != "512" {
		t.Fatalf("head after resize %d %#v", code, h)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/p?comp=properties", "", map[string]string{"x-ms-sequence-number-action": "increment"})
	if code != 200 {
		t.Fatalf("sequence increment %d %s", code, raw)
	}
	code, raw, h = do(http.MethodHead, "/ctr/p", "", nil)
	if code != 200 || h.Get("x-ms-blob-sequence-number") != "1" {
		t.Fatalf("head after increment %d %#v", code, h)
	}
	code, raw, h = do(http.MethodPut, "/ctr/p?comp=page", strings.Repeat("a", 512), map[string]string{"x-ms-page-write": "update", "x-ms-range": "bytes=1536-2047"})
	if code != 416 || h.Get("x-ms-error-code") != "RequestedRangeNotSatisfiable" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("put page beyond size %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/o?comp=page", strings.Repeat("a", 512), map[string]string{"x-ms-page-write": "update", "x-ms-range": "bytes=0-511"})
	if code != 409 || h.Get("x-ms-error-code") != "InvalidBlobType" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("put page on block blob %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/p", "", map[string]string{"x-ms-blob-type": "PageBlob", "x-ms-blob-content-length": "512"})
	if code != 409 || h.Get("x-ms-error-code") != "BlobAlreadyExists" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("recreate page blob %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/o?comp=snapshot", "", nil)
	if code != 201 || h.Get("x-ms-snapshot") == "" {
		t.Fatalf("snapshot %d %#v %s", code, h, raw)
	}
	snapID := h.Get("x-ms-snapshot")
	code, raw, _ = do(http.MethodPut, "/ctr/o", "changed", map[string]string{"x-ms-blob-type": "BlockBlob"})
	if code != 201 {
		t.Fatalf("overwrite after snapshot %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/o?snapshot="+url.QueryEscape(snapID), "", nil)
	if code != 200 || string(raw) != "hello-azure" {
		t.Fatalf("snapshot download %d %s", code, raw)
	}
	code, raw, h = do(http.MethodDelete, "/ctr/o", "", nil)
	if code != 409 || h.Get("x-ms-error-code") != "SnapshotsPresent" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete with snapshots %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodDelete, "/ctr/o?snapshot="+url.QueryEscape(snapID), "", nil)
	if code != 202 {
		t.Fatalf("delete snapshot %d %s", code, raw)
	}
	code, _, _ = do(http.MethodGet, "/ctr/o?snapshot="+url.QueryEscape(snapID), "", nil)
	if code != 404 {
		t.Fatalf("snapshot after delete %d", code)
	}
	copySrc := ts.URL + "/ctr/o"
	code, raw, h = do(http.MethodPut, "/ctr/copy", "", map[string]string{"x-ms-copy-source": copySrc})
	if code != 202 || h.Get("x-ms-copy-status") != "success" || h.Get("x-ms-copy-id") == "" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("copy %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/copy", "", nil)
	if code != 200 || string(raw) != "changed" {
		t.Fatalf("copy download %d %s", code, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/copy2", "", map[string]string{"x-ms-copy-source": copySrc, "x-ms-requires-sync": "true"})
	if code != 202 || h.Get("x-ms-copy-status") != "success" {
		t.Fatalf("sync copy %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/copy3", "", map[string]string{"x-ms-copy-source": "/devstoreaccount1/ctr/o"})
	if code != 400 || h.Get("x-ms-error-code") != "InvalidHeaderValue" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("copy invalid source %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/ctr/copy?comp=copy&copyid=nope", "", map[string]string{"x-ms-copy-action": "abort"})
	if code != 409 || h.Get("x-ms-error-code") != "NoPendingCopyOperation" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("abort copy %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/sb?comp=block&blockid=YQ==", "", map[string]string{"x-ms-copy-source": copySrc, "x-ms-source-range": "bytes=0-3"})
	if code != 201 {
		t.Fatalf("stage block from url %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/ctr/sb?comp=blocklist", `<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>YQ==</Latest></BlockList>`, nil)
	if code >= 300 {
		t.Fatalf("commit staged copy %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/ctr/sb", "", nil)
	if code != 200 || string(raw) != "chan" {
		t.Fatalf("staged from url download %d %s", code, raw)
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
		req.Host = "acct.queue.core.windows.net"
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
	envelope := func(text string) string {
		return `<?xml version="1.0" encoding="utf-8"?><QueueMessage><MessageText>` + text + `</MessageText></QueueMessage>`
	}
	code, raw, _ := do(http.MethodPut, "/q1", "", map[string]string{"x-ms-meta-app": "demo"})
	if code != 201 {
		t.Fatalf("create queue %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/?comp=list", "", nil)
	if code != 200 || !strings.Contains(string(raw), "q1") {
		t.Fatalf("list queues %d %s", code, raw)
	}
	// Metadata + approximate count.
	code, raw, h := do(http.MethodGet, "/q1?comp=metadata", "", nil)
	if code != 200 || h.Get("x-ms-meta-app") != "demo" || h.Get("x-ms-approximate-messages-count") != "0" {
		t.Fatalf("queue properties %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodPut, "/q1?comp=metadata", "", map[string]string{"x-ms-meta-app": "v2"})
	if code != 204 {
		t.Fatalf("set queue metadata %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/q1?comp=metadata", "", nil)
	if code != 200 || h.Get("x-ms-meta-app") != "v2" {
		t.Fatalf("queue metadata %d %#v %s", code, h, raw)
	}
	// ACL round-trip.
	code, raw, _ = do(http.MethodPut, "/q1?comp=acl", `<?xml version="1.0" encoding="utf-8"?><SignedIdentifiers><SignedIdentifier><Id>id1</Id></SignedIdentifier></SignedIdentifiers>`, nil)
	if code != 204 {
		t.Fatalf("set queue acl %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/q1?comp=acl", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<Id>id1</Id>") {
		t.Fatalf("get queue acl %d %s", code, raw)
	}
	// Service properties + stats.
	code, raw, _ = do(http.MethodGet, "/?restype=service&comp=properties", "", nil)
	if code != 200 || !strings.Contains(string(raw), "StorageServiceProperties") {
		t.Fatalf("queue service properties %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/?restype=service&comp=stats", "", nil)
	if code != 400 || h.Get("x-ms-error-code") != "InvalidQueryParameterValue" {
		t.Fatalf("queue stats primary %d %#v %s", code, h, raw)
	}
	// Enqueue / peek / dequeue / clear.
	code, raw, _ = do(http.MethodPost, "/q1/messages", envelope("hello-q"), nil)
	if code != 201 || !strings.Contains(string(raw), "<PopReceipt>") || !strings.Contains(string(raw), "<MessageText>hello-q</MessageText>") {
		t.Fatalf("put message %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/q1/messages", envelope("second"), nil)
	if code != 201 {
		t.Fatalf("put second %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/q1/messages", envelope("raw~bad"), nil)
	if code == 201 {
		// no-op: envelope is well-formed here; the raw-body negative is below
	}
	code, raw, h = do(http.MethodPost, "/q1/messages", "not xml at all <<<", nil)
	if code != 400 || h.Get("x-ms-error-code") != "InvalidXmlDocument" {
		t.Fatalf("malformed message body %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/q1/messages?peekonly=true", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<MessageText>hello-q</MessageText>") || strings.Contains(string(raw), "second") || strings.Contains(string(raw), "<PopReceipt>") {
		t.Fatalf("peek %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodGet, "/q1/messages", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<MessageText>hello-q</MessageText>") || !strings.Contains(string(raw), "<PopReceipt>") || strings.Contains(string(raw), "second") {
		t.Fatalf("dequeue %d %s", code, raw)
	}
	popReceipt := ""
	if i := strings.Index(string(raw), "<PopReceipt>"); i >= 0 {
		rest := string(raw)[i+len("<PopReceipt>"):]
		popReceipt = rest[:strings.Index(rest, "</PopReceipt>")]
	}
	if popReceipt == "" {
		t.Fatalf("no pop receipt in %s", raw)
	}
	msgID := ""
	if i := strings.Index(string(raw), "<MessageId>"); i >= 0 {
		rest := string(raw)[i+len("<MessageId>"):]
		msgID = rest[:strings.Index(rest, "</MessageId>")]
	}
	// The dequeued message is invisible to the next dequeue.
	code, raw, _ = do(http.MethodGet, "/q1/messages", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<MessageText>second</MessageText>") || strings.Contains(string(raw), "hello-q") {
		t.Fatalf("second dequeue %d %s", code, raw)
	}
	// Update with the wrong receipt is a 400; the right one works.
	code, raw, h = do(http.MethodPut, "/q1/messages/"+msgID+"?popreceipt=wrong&visibilitytimeout=5", envelope("changed"), nil)
	if code != 400 || h.Get("x-ms-error-code") != "PopReceiptMismatch" {
		t.Fatalf("update wrong receipt %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPut, "/q1/messages/"+msgID+"?popreceipt="+url.QueryEscape(popReceipt)+"&visibilitytimeout=5", envelope("changed"), nil)
	if code != 204 || h.Get("x-ms-popreceipt") == "" || h.Get("x-ms-time-next-visible") == "" {
		t.Fatalf("update %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodPost, "/q1/messages?visibilitytimeout=691200", envelope("x"), nil)
	if code != 400 || h.Get("x-ms-error-code") != "OutOfRangeQueryParameterValue" {
		t.Fatalf("invalid visibilitytimeout %d %#v %s", code, h, raw)
	}
	code, raw, _ = do(http.MethodDelete, "/q1/messages", "", nil)
	if code != 204 {
		t.Fatalf("clear %d %s", code, raw)
	}
	// Real clock: a short TTL expires the message, and a dequeued message
	// reappears after its visibility timeout.
	code, raw, _ = do(http.MethodPost, "/q1/messages?messagettl=1", envelope("short"), nil)
	if code != 201 {
		t.Fatalf("put short ttl %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/q1/messages", envelope("linger"), nil)
	if code != 201 {
		t.Fatalf("put linger %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/q1/messages?visibilitytimeout=1", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<MessageText>short</MessageText>") {
		t.Fatalf("dequeue short %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/q1/messages?peekonly=true", "", nil)
	if code != 200 || !strings.Contains(string(raw), "<MessageText>linger</MessageText>") || strings.Contains(string(raw), "short") {
		t.Fatalf("peek hides invisible %d %s", code, raw)
	}
	time.Sleep(1200 * time.Millisecond)
	code, raw, _ = do(http.MethodGet, "/q1/messages?peekonly=true&numofmessages=5", "", nil)
	if code != 200 || strings.Contains(string(raw), "short") || !strings.Contains(string(raw), "<MessageText>linger</MessageText>") {
		t.Fatalf("peek after ttl %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodDelete, "/q1/messages", "", nil)
	if code != 204 {
		t.Fatalf("clear %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/q1/messages?peekonly=true", "", nil)
	if code != 200 || strings.Contains(string(raw), "<QueueMessage>") {
		t.Fatalf("peek after clear %d %s", code, raw)
	}
	code, raw, h = do(http.MethodDelete, "/missing", "", nil)
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
		req.Host = "acct.table.core.windows.net"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
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
	code, raw, _ := do(http.MethodPost, "/Tables", `{"TableName":"t1"}`, nil)
	if code >= 300 {
		t.Fatalf("create table %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/Tables", "", nil)
	if code != 200 || !strings.Contains(string(raw), "t1") {
		t.Fatalf("list tables %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/t1", `{"PartitionKey":"p","RowKey":"r"}`, nil)
	if code >= 300 {
		t.Fatalf("insert entity %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/t1()", "", nil)
	if code != 200 || !strings.Contains(string(raw), "PartitionKey") {
		t.Fatalf("query entities %d %s", code, raw)
	}
	// Single-entity read, replace, merge, conditional delete.
	code, raw, _ = do(http.MethodPost, "/t1", `{"PartitionKey":"p","RowKey":"r2","Name":"ada"}`, nil)
	if code != 201 || !strings.Contains(string(raw), `"Name":"ada"`) || !strings.Contains(string(raw), "odata.etag") {
		t.Fatalf("insert with props %d %s", code, raw)
	}
	code, raw, h := do(http.MethodGet, "/t1(PartitionKey='p',RowKey='r2')", "", nil)
	if code != 200 || !strings.Contains(string(raw), `"Name":"ada"`) || h.Get("ETag") == "" {
		t.Fatalf("get entity %d %#v %s", code, h, raw)
	}
	_ = h.Get("ETag")
	code, raw, _ = do(http.MethodPut, "/t1(PartitionKey='p',RowKey='r2')", `{"PartitionKey":"p","RowKey":"r2","City":"london"}`, nil)
	if code != 204 {
		t.Fatalf("update entity %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/t1(PartitionKey='p',RowKey='r2')", "", nil)
	if code != 200 || strings.Contains(string(raw), "ada") || !strings.Contains(string(raw), "london") {
		t.Fatalf("replaced entity %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPatch, "/t1(PartitionKey='p',RowKey='r2')", `{"Name":"grace"}`, nil)
	if code != 204 {
		t.Fatalf("merge entity %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/t1(PartitionKey='p',RowKey='r2')", "", nil)
	if code != 200 || !strings.Contains(string(raw), "grace") || !strings.Contains(string(raw), "london") {
		t.Fatalf("merged entity %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodDelete, "/t1(PartitionKey='p',RowKey='r2')", "", map[string]string{"If-Match": `"stale"`})
	if code != 412 || !strings.Contains(string(raw), "UpdateConditionNotSatisfied") {
		t.Fatalf("delete wrong etag %d %s", code, raw)
	}
	code, raw, h = do(http.MethodGet, "/t1(PartitionKey='p',RowKey='r2')", "", nil)
	if code != 200 {
		t.Fatalf("get before delete %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodDelete, "/t1(PartitionKey='p',RowKey='r2')", "", map[string]string{"If-Match": h.Get("ETag")})
	if code != 204 {
		t.Fatalf("delete with etag %d %s", code, raw)
	}
	// Batch: two inserts through a changeset, then a mixed batch with a 404.
	batch := func(parts ...string) (string, string) {
		var cs strings.Builder
		for i, p := range parts {
			fmt.Fprintf(&cs, "--cs1\r\nContent-Type: application/http\r\nContent-ID: %d\r\n\r\n%s\r\n", i, p)
		}
		cs.WriteString("--cs1--\r\n")
		body := "--tb1\r\nContent-Type: multipart/mixed; boundary=cs1\r\n\r\n" + cs.String() + "--tb1--\r\n"
		return body, "multipart/mixed; boundary=tb1"
	}
	subPost := func(payload string) string {
		return fmt.Sprintf("POST https://acct.table.core.windows.net/t1 HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(payload), payload)
	}
	body, cty := batch(
		subPost(`{"PartitionKey":"p","RowKey":"b1","Name":"one"}`),
		subPost(`{"PartitionKey":"p","RowKey":"b2","Name":"two"}`),
	)
	code, raw, _ = do(http.MethodPost, "/$batch", body, map[string]string{"Content-Type": cty})
	if code != 202 || !strings.Contains(string(raw), "HTTP/1.1 201") {
		t.Fatalf("batch insert %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/t1(PartitionKey='p',RowKey='b1')", "", nil)
	if code != 200 || !strings.Contains(string(raw), "one") {
		t.Fatalf("batch inserted %d %s", code, raw)
	}
	body, cty = batch("DELETE https://acct.table.core.windows.net/t1(PartitionKey='p',RowKey='ghost') HTTP/1.1\r\nIf-Match: *\r\n\r\n")
	code, raw, _ = do(http.MethodPost, "/$batch", body, map[string]string{"Content-Type": cty})
	if code != 202 || !strings.Contains(string(raw), "HTTP/1.1 404") || !strings.Contains(string(raw), "EntityNotFound") {
		t.Fatalf("batch missing %d %s", code, raw)
	}
	code, raw, h = do(http.MethodDelete, "/Tables('missing')", "", nil)
	if code != 404 || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing table %d %#v %s", code, h, raw)
	}
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}
