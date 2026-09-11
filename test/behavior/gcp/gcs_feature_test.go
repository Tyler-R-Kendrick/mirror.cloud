package gcp

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcp/gcs"
)

func TestGCSJSONBehavior(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"gcp.storage"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	call := func(method, path, body, ctype string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "storage.googleapis.com"
		req.Header.Set("Authorization", "Bearer test")
		if ctype != "" {
			req.Header.Set("Content-Type", ctype)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}
	t.Run("Given a bucket When created Then it is listed and fetched", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/storage/v1/b", `{"name":"bdd"}`, "application/json")
		got := map[string]any{}
		_ = json.Unmarshal(raw, &got)
		if code != 200 || got["name"] != "bdd" {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/storage/v1/b", "", "")
		if code != 200 || !strings.Contains(string(raw), `"bdd"`) {
			t.Fatalf("list %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/storage/v1/b/bdd", "", "")
		_ = json.Unmarshal(raw, &got)
		if code != 200 || got["name"] != "bdd" {
			t.Fatalf("get %d %s", code, raw)
		}
	})
	t.Run("Given a duplicate bucket When created Then 409 is returned", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/storage/v1/b", `{"name":"bdd"}`, "application/json")
		if code != 409 {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given an object When uploaded Then GET alt=media returns the bytes", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/upload/storage/v1/b/bdd/o?uploadType=media&name=o", "hello-bdd", "text/plain")
		if code >= 300 {
			t.Fatalf("insert %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/storage/v1/b/bdd/o/o?alt=media", "", "")
		if code != 200 || string(raw) != "hello-bdd" {
			t.Fatalf("media %d %s", code, raw)
		}
		code, _, _ = call(http.MethodDelete, "/storage/v1/b/bdd/o/o", "", "")
		if code >= 300 && code != 204 {
			t.Fatalf("del %d", code)
		}
	})
	t.Run("Given a missing object When fetched Then GCS fault without AWS headers", func(t *testing.T) {
		code, raw, hdr := call(http.MethodGet, "/storage/v1/b/bdd/o/nope", "", "")
		miss := map[string]any{}
		_ = json.Unmarshal(raw, &miss)
		errObj, _ := miss["error"].(map[string]any)
		if code != 404 || errObj == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %d %#v %s", code, hdr, raw)
		}
	})
	t.Run("Given a missing object or bucket When deleted Then 404 not 204", func(t *testing.T) {
		code, raw, hdr := call(http.MethodDelete, "/storage/v1/b/bdd/o/nope", "", "")
		miss := map[string]any{}
		_ = json.Unmarshal(raw, &miss)
		errObj, _ := miss["error"].(map[string]any)
		if code != 404 || errObj == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing object %d %#v %s", code, hdr, raw)
		}
		code, raw, hdr = call(http.MethodDelete, "/storage/v1/b/missing", "", "")
		_ = json.Unmarshal(raw, &miss)
		errObj, _ = miss["error"].(map[string]any)
		if code != 404 || errObj == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing bucket %d %#v %s", code, hdr, raw)
		}
	})
}
