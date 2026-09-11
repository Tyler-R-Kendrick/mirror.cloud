package azure

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/azure/blobs"
)

func TestAzureBlobBehavior(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"azure.blobs"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	call := func(method, path, body string, hdr map[string]string) (int, []byte, http.Header) {
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
	t.Run("Given a container When created Then it is listed and fetched", func(t *testing.T) {
		code, raw, _ := call(http.MethodPut, "/bdd?restype=container", "", nil)
		if code != 201 {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/?comp=list", "", nil)
		if code != 200 || !strings.Contains(string(raw), "<Name>bdd</Name>") {
			t.Fatalf("list %d %s", code, raw)
		}
		code, _, _ = call(http.MethodGet, "/bdd?restype=container", "", nil)
		if code != 200 {
			t.Fatalf("get %d", code)
		}
	})
	t.Run("Given a duplicate container When created Then 409 ContainerAlreadyExists", func(t *testing.T) {
		code, raw, _ := call(http.MethodPut, "/bdd?restype=container", "", nil)
		if code != 409 || !strings.Contains(string(raw), "ContainerAlreadyExists") {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given a blob When put Then GET returns the bytes", func(t *testing.T) {
		code, raw, _ := call(http.MethodPut, "/bdd/o", "hello-bdd", map[string]string{"x-ms-blob-type": "BlockBlob"})
		if code != 201 {
			t.Fatalf("put %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/bdd/o", "", nil)
		if code != 200 || string(raw) != "hello-bdd" {
			t.Fatalf("get %d %s", code, raw)
		}
		code, _, _ = call(http.MethodDelete, "/bdd/o", "", nil)
		if code != 202 {
			t.Fatalf("del %d", code)
		}
	})
	t.Run("Given a missing blob When fetched Then Azure XML fault without AWS headers", func(t *testing.T) {
		code, raw, hdr := call(http.MethodGet, "/bdd/nope", "", nil)
		if code != 404 || !strings.Contains(string(raw), "<Code>BlobNotFound</Code>") || hdr.Get("x-ms-error-code") != "BlobNotFound" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %d %#v %s", code, hdr, raw)
		}
	})
	t.Run("Given a missing blob or container When deleted Then 404 not 202", func(t *testing.T) {
		code, raw, hdr := call(http.MethodDelete, "/bdd/nope", "", nil)
		if code != 404 || !strings.Contains(string(raw), "<Code>BlobNotFound</Code>") || hdr.Get("x-ms-error-code") != "BlobNotFound" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing blob %d %#v %s", code, hdr, raw)
		}
		code, raw, hdr = call(http.MethodDelete, "/missing?restype=container", "", nil)
		if code != 404 || !strings.Contains(string(raw), "<Code>ContainerNotFound</Code>") || hdr.Get("x-ms-error-code") != "ContainerNotFound" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing container %d %#v %s", code, hdr, raw)
		}
	})
}
