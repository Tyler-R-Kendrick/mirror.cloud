package spine

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcp/gcs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerGCSSection48(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"gcp.storage"}
	cfg.Seed = "gcs-48"
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
	js := func(b []byte) map[string]any {
		m := map[string]any{}
		_ = json.Unmarshal(b, &m)
		return m
	}

	code, b, h := do(http.MethodPost, "/storage/v1/b", `{"name":"bk"}`, nil)
	if code >= 300 || js(b)["name"] != "bk" {
		t.Fatalf("insert bucket %d %s", code, b)
	}
	if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("fidelity %q", h.Get("x-mirror-fidelity"))
	}
	if code, b, _ := do(http.MethodGet, "/storage/v1/b/bk", "", nil); code != 200 || js(b)["name"] != "bk" {
		t.Fatalf("get bucket %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/storage/v1/b", "", nil); code != 200 || !bytes.Contains(b, []byte("bk")) {
		t.Fatalf("list buckets %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPatch, "/storage/v1/b/bk", `{"location":"EU"}`, nil); code >= 300 {
		t.Fatalf("patch bucket %d %s", code, b)
	}

	code, b, _ = do(http.MethodPost, "/upload/storage/v1/b/bk/o?uploadType=media&name=form", "hello-form", map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if code >= 300 {
		t.Fatalf("form insert %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/storage/v1/b/bk/o/form?alt=media", "", nil); code != 200 || string(b) != "hello-form" {
		t.Fatalf("form media %d %s", code, b)
	}
	code, b, _ = do(http.MethodPost, "/upload/storage/v1/b/bk/o?uploadType=media&name=o", "hello-gcs", map[string]string{"Content-Type": "text/plain"})
	if code >= 300 {
		t.Fatalf("insert object %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/storage/v1/b/bk/o/o", "", nil); code != 200 || js(b)["name"] != "o" {
		t.Fatalf("get meta %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/storage/v1/b/bk/o/o?alt=media", "", nil); code != 200 || string(b) != "hello-gcs" {
		t.Fatalf("get media %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/storage/v1/b/bk/o/o?alt=media", "", map[string]string{"Range": "bytes=0-4"}); code != 206 || string(b) != "hello" {
		t.Fatalf("range %d %s", code, b)
	}
	if code, _, _ := do(http.MethodGet, "/storage/v1/b/bk/o/o?ifGenerationMatch=nope", "", nil); code != 412 {
		t.Fatalf("ifGenerationMatch %d", code)
	}

	do(http.MethodPost, "/upload/storage/v1/b/bk/o?uploadType=media&name=a/x", "1", nil)
	do(http.MethodPost, "/upload/storage/v1/b/bk/o?uploadType=media&name=a/y", "2", nil)
	do(http.MethodPost, "/upload/storage/v1/b/bk/o?uploadType=media&name=a/n/z", "3", nil)
	code, listed, _ := do(http.MethodGet, "/storage/v1/b/bk/o?prefix=a/&delimiter=/", "", nil)
	if code != 200 {
		t.Fatalf("list %d %s", code, listed)
	}
	lm := js(listed)
	if len(asSlice(lm["items"])) != 2 {
		t.Fatalf("list items %s", listed)
	}
	if !bytes.Contains(listed, []byte("a/n/")) {
		t.Fatalf("prefixes %s", listed)
	}
	code, p1, _ := do(http.MethodGet, "/storage/v1/b/bk/o?maxResults=1", "", nil)
	if code != 200 {
		t.Fatalf("page %d %s", code, p1)
	}

	if code, b, _ := do(http.MethodPost, "/storage/v1/b/bk/o/o/copyTo/b/bk/o/copied", "", nil); code >= 300 {
		t.Fatalf("copy %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPost, "/storage/v1/b/bk/o/o/rewriteTo/b/bk/o/rewritten", "", nil); code >= 300 {
		t.Fatalf("rewrite %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPost, "/storage/v1/b/bk/o/composed/compose", `{"sourceObjects":[{"name":"o"},{"name":"copied"}]}`, nil); code >= 300 {
		t.Fatalf("compose %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPatch, "/storage/v1/b/bk/o/o", `{"contentType":"text/html"}`, nil); code >= 300 {
		t.Fatalf("patch obj %d %s", code, b)
	}

	code, sess, hs := do(http.MethodPost, "/upload/storage/v1/b/bk/o?uploadType=resumable&name=r", "", nil)
	if code >= 300 {
		t.Fatalf("resumable start %d %s", code, sess)
	}
	loc := hs.Get("Location")
	if loc == "" {
		t.Fatalf("no location %s %v", sess, hs)
	}
	if u, err := http.NewRequest(http.MethodGet, loc, nil); err == nil && u.URL != nil {
		loc = ts.URL + u.URL.RequestURI()
	} else if !strings.HasPrefix(loc, "http") {
		loc = ts.URL + loc
	}
	put, _ := http.NewRequest(http.MethodPut, loc, strings.NewReader("RESUMED"))
	put.Header.Set("Content-Range", "bytes 0-6/7")
	pres, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	pb, _ := io.ReadAll(pres.Body)
	pres.Body.Close()
	if pres.StatusCode >= 300 && pres.StatusCode != 308 {
		t.Fatalf("resumable put %d loc=%s body=%s", pres.StatusCode, loc, pb)
	}
	if pres.StatusCode == 308 {
		t.Fatalf("resumable stayed 308 loc=%s body=%s hdr=%v", loc, pb, pres.Header)
	}
	if code, b, _ := do(http.MethodGet, "/storage/v1/b/bk/o/r?alt=media", "", nil); code != 200 || string(b) != "RESUMED" {
		t.Fatalf("resumable get %d %s put=%s loc=%s", code, b, pb, loc)
	}

	if code, b, _ := do(http.MethodDelete, "/storage/v1/b/bk/o/copied", "", nil); code >= 300 && code != 204 {
		t.Fatalf("delete obj %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPost, "/storage/v1/b/bk/o/batch", "", nil); code != 501 {
		t.Fatalf("batch %d %s", code, b)
	}
}

func TestGCSHTTPProvenOps(t *testing.T) {
	want := []string{
		"storage.buckets.insert", "storage.buckets.get", "storage.buckets.list",
		"storage.buckets.delete", "storage.buckets.patch",
		"storage.objects.insert", "storage.objects.get", "storage.objects.list",
		"storage.objects.delete", "storage.objects.copy", "storage.objects.rewrite",
		"storage.objects.compose", "storage.objects.patch",
	}

	assertSame(t, "gcs", gcs.New(spitest.Deps(t)).Operations(), append(want, gcs.ExtraOps()...))
}
