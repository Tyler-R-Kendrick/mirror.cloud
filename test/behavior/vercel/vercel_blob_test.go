package vercel

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

// TestVercelBlobBehavior drives the Blob data plane over its real HTTP
// surface: management calls on blob.vercel-storage.com with the store Bearer
// token, content serving on the same edge at /blob/<store>/<pathname>. The
// expectations are the vendor-authored oracle's (vercel-labs/emulate, pinned
// in specs/vercel/emulate-inventory.json), captured live on 2026-09-18.
func TestVercelBlobBehavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"vercel.blob"}
	cfg.Seed = "vercel-blob-bdd"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	const tok = "Bearer vercel_blob_rw_st1_abc"
	const host = "blob.vercel-storage.com"
	call := func(method, path, body, auth string, hdr map[string]string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = host
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
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

	t.Run("Given no token When uploading Then 403 forbidden", func(t *testing.T) {
		code, raw, _ := call(http.MethodPut, "/api/blob/?pathname=hello.txt", "hi blob", "", nil)
		if code != 403 || !strings.Contains(string(raw), `"forbidden"`) {
			t.Fatalf("unauthenticated %d %s", code, raw)
		}
	})

	t.Run("Given a store token When a blob round-trips Then upload list head serve delete", func(t *testing.T) {
		code, raw, h := call(http.MethodPut, "/api/blob/?pathname=hello.txt", "hi blob", tok, map[string]string{"Content-Type": "text/plain"})
		if code != 200 || !strings.Contains(string(raw), `"pathname":"hello.txt"`) || !strings.Contains(string(raw), `"contentType":"text/plain"`) {
			t.Fatalf("upload %d %s", code, raw)
		}
		if !strings.Contains(string(raw), `"url":"`) || !strings.Contains(string(raw), "/blob/st1/hello.txt") {
			t.Fatalf("upload url %s", raw)
		}
		_ = h

		// A duplicate without allow-overwrite is the oracle's 400.
		code, raw, _ = call(http.MethodPut, "/api/blob/?pathname=hello.txt", "again", tok, nil)
		if code != 400 || !strings.Contains(string(raw), "allowOverwrite") {
			t.Fatalf("dup %d %s", code, raw)
		}

		code, raw, _ = call(http.MethodGet, "/api/blob", "", tok, nil)
		if code != 200 || !strings.Contains(string(raw), `"hello.txt"`) || !strings.Contains(string(raw), `"hasMore":false`) {
			t.Fatalf("list %d %s", code, raw)
		}

		code, raw, _ = call(http.MethodGet, "/api/blob?url=hello.txt", "", tok, nil)
		if code != 200 || !strings.Contains(string(raw), `"cacheControl":"public, max-age=2592000"`) || !strings.Contains(string(raw), `"size":`) {
			t.Fatalf("head %d %s", code, raw)
		}

		// Content serving is public: no token, and the answer is the bytes.
		code, raw, h = call(http.MethodGet, "/blob/st1/hello.txt", "", "", nil)
		if code != 200 || string(raw) != "hi blob" {
			t.Fatalf("serve %d %s", code, raw)
		}
		if h.Get("ETag") == "" || !strings.Contains(h.Get("Cache-Control"), "max-age=2592000") {
			t.Fatalf("serve headers %v", h)
		}

		// ?download=1 adds the disposition; If-None-Match on the etag is a 304.
		etag := h.Get("ETag")
		code, raw, h = call(http.MethodGet, "/blob/st1/hello.txt?download=1", "", "", nil)
		if code != 200 || !strings.Contains(h.Get("Content-Disposition"), `filename="hello.txt"`) {
			t.Fatalf("download %d %v", code, h)
		}
		code, raw, h = call(http.MethodGet, "/blob/st1/hello.txt", "", "", map[string]string{"If-None-Match": etag})
		if code != 304 {
			t.Fatalf("if-none-match %d %s", code, raw)
		}

		// Multipart is the oracle's documented refusal, not a gap here.
		code, raw, _ = call(http.MethodPost, "/api/blob/mpu", "{}", tok, nil)
		if code != 400 || !strings.Contains(string(raw), "not supported by the emulator") {
			t.Fatalf("mpu %d %s", code, raw)
		}

		code, raw, _ = call(http.MethodPost, "/api/blob/delete", `{"urls":["hello.txt"]}`, tok, nil)
		if code != 200 || string(raw) != "null" {
			t.Fatalf("delete %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/api/blob?url=hello.txt", "", tok, nil)
		if code != 404 || !strings.Contains(string(raw), `"not_found"`) {
			t.Fatalf("head after delete %d %s", code, raw)
		}
	})

	t.Run("Given a folded list When blobs share a folder Then the folder is reported", func(t *testing.T) {
		for _, p := range []string{"dir/a.txt", "dir/deep/b.txt", "top.txt"} {
			code, raw, _ := call(http.MethodPut, "/api/blob/?pathname="+p, "x", tok, nil)
			if code != 200 {
				t.Fatalf("upload %s %d %s", p, code, raw)
			}
		}
		code, raw, _ := call(http.MethodGet, "/api/blob?mode=folded", "", tok, nil)
		if code != 200 || !strings.Contains(string(raw), `"folders":["dir/"]`) || strings.Contains(string(raw), "dir/a.txt") {
			t.Fatalf("folded %d %s", code, raw)
		}
		if !strings.Contains(string(raw), `"top.txt"`) {
			t.Fatalf("folded keeps top-level %s", raw)
		}
	})
}
