package opensearch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestOpenSearchHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 14 {
		t.Fatalf("opensearch Operations() %d want 14", n)
	}
}

func TestBootedServerOpenSearchDomainAndSearch(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.es"}
	cfg.Seed = "os-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/es/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonOpenSearchService."+op)
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
	created := call("CreateDomain", `{"DomainName":"d1"}`)
	st, _ := created["DomainStatus"].(map[string]any)
	if st["DomainName"] != "d1" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeDomain", `{"DomainName":"d1"}`)
	if got["DomainStatus"] == nil {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListDomainNames", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "d1") {
		t.Fatalf("list %s", raw)
	}
	idx := call("IndexDocument", `{"DomainName":"d1","Index":"cities","Id":"1","name":"austin","state":"tx"}`)
	if idx["result"] != "created" {
		t.Fatalf("index %v", idx)
	}
	doc := call("GetDocument", `{"DomainName":"d1","Index":"cities","Id":"1"}`)
	if doc["found"] != true {
		t.Fatalf("get doc %v", doc)
	}
	search := call("Search", `{"DomainName":"d1","Index":"cities","query":{"match":{"name":"austin"}}}`)
	hits, _ := search["hits"].(map[string]any)
	if hits == nil {
		t.Fatalf("search %v", search)
	}
	total, _ := hits["total"].(map[string]any)
	if total["value"] != float64(1) && fmtInt(total["value"]) != 1 {
		t.Fatalf("hits %v", search)
	}
	call("DeleteDocument", `{"DomainName":"d1","Index":"cities","Id":"1"}`)
	goneDoc := call("GetDocument", `{"DomainName":"d1","Index":"cities","Id":"1"}`)
	if goneDoc["found"] == true {
		t.Fatalf("doc still present %v", goneDoc)
	}
	call("DeleteDomain", `{"DomainName":"d1"}`)
	gone := call("ListDomainNames", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"d1"`) {
		t.Fatalf("domain still present %s", raw)
	}
}

func TestBootedServerOpenSearchRESTDataPlane(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.es"}
	cfg.Seed = "os-rest"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/es/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q %s", res.Header.Get("x-mirror-fidelity"), raw)
		}
		return res.StatusCode, string(raw)
	}
	code, created := do(http.MethodPut, "/cities/_doc/1", `{"name":"austin"}`)
	if code >= 300 || !strings.Contains(created, "created") {
		t.Fatalf("put %d %s", code, created)
	}
	code, got := do(http.MethodGet, "/cities/_doc/1", "")
	if code >= 300 || !strings.Contains(got, "austin") {
		t.Fatalf("get %d %s", code, got)
	}
	code, search := do(http.MethodPost, "/cities/_search", `{"query":{"match":{"name":"austin"}}}`)
	if code >= 300 || !strings.Contains(search, "austin") {
		t.Fatalf("search %d %s", code, search)
	}
	code, _ = do(http.MethodDelete, "/cities/_doc/1", "")
	if code >= 300 {
		t.Fatalf("delete %d", code)
	}
	_, gone := do(http.MethodGet, "/cities/_doc/1", "")
	if strings.Contains(gone, `"found":true`) {
		t.Fatalf("still present %s", gone)
	}
}

func TestIndexDocumentRejectsMissingDomain(t *testing.T) {
	p := New(spitest.Deps(t))
	_, err := p.Invoke(context.Background(), &spi.Request{
		Identity: spi.Identity{Account: "123456789012", Region: "us-east-1"}, Operation: "IndexDocument",
		Input: map[string]any{"DomainName": "missing", "Index": "events", "Document": map[string]any{"message": "lost"}},
	})
	if fault, ok := err.(*spi.Fault); !ok || fault.Code != "ResourceNotFoundException" {
		t.Fatalf("missing domain error %#v", err)
	}
}

func fmtInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}
