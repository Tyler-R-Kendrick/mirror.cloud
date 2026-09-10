package spine

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/azure/blobs"
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
	if code != 200 || !strings.Contains(string(raw), "<Name>ctr</Name>") {
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
}
