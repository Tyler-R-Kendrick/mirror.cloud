package gcprest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestGCPRESTRoutes(t *testing.T) {
	codec := Codec{}
	if codec.Protocol() != model.ProtoGCPRESTSON {
		t.Fatal(codec.Protocol())
	}
	svc := &model.Service{ID: "gcp.storage", Operations: []model.Operation{{Name: "fallback"}}}
	for _, test := range []struct{ method, path, want string }{
		{http.MethodPost, "/batch/storage/v1", "storage.objects.insert"},
		{http.MethodPost, "/upload/storage/v1/b/b/o", "storage.objects.insert"},
		{http.MethodPut, "/upload/storage/v1/b/b/o", "storage.objects.insert"},
		{http.MethodPost, "/storage/v1/b/a/o/x/copyTo/b/b/o/y", "storage.objects.copy"},
		{http.MethodPost, "/storage/v1/b/a/o/x/rewriteTo/b/b/o/y", "storage.objects.rewrite"},
		{http.MethodPost, "/storage/v1/b/a/o/x/compose", "storage.objects.compose"},
		{http.MethodPatch, "/storage/v1/b/a/o/x", "storage.objects.patch"},
		{http.MethodPatch, "/storage/v1/b/a", "storage.buckets.patch"},
		{http.MethodGet, "/storage/v1/b/a/o/x", "storage.objects.get"},
		{http.MethodGet, "/storage/v1/b/a/o", "storage.objects.list"},
		{http.MethodPost, "/storage/v1/b", "storage.buckets.insert"},
		{http.MethodGet, "/storage/v1/b/a", "storage.buckets.get"},
		{http.MethodGet, "/storage/v1/b", "storage.buckets.list"},
		{http.MethodDelete, "/storage/v1/b/a/o/x", "storage.objects.delete"},
		{http.MethodDelete, "/storage/v1/b/a", "storage.buckets.delete"},
		{http.MethodOptions, "/unknown", "fallback"},
	} {
		request := httptest.NewRequest(test.method, test.path, nil)
		op, err := codec.Route(svc, request)
		if err != nil || op.Name != test.want {
			t.Errorf("%s %s: %#v %v, want %s", test.method, test.path, op, err, test.want)
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/?Action=custom.operation", nil)
	op, err := codec.Route(svc, request)
	if err != nil || op.Name != "custom.operation" {
		t.Fatalf("Action route %#v %v", op, err)
	}
	if _, err := codec.Route(&model.Service{ID: "gcp.empty"}, httptest.NewRequest(http.MethodOptions, "/unknown", nil)); err == nil {
		t.Fatal("routed empty unknown service")
	}
}

func TestGCPRESTDecodeEncodeAndFault(t *testing.T) {
	codec := Codec{}
	svc := &model.Service{ID: "gcp.storage"}
	op := &model.Operation{Name: "storage.objects.insert"}
	request := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/b/o?name=a&name=b", strings.NewReader(`{"contentType":"text/plain"}`))
	decoded, err := codec.Decode(svc, op, request)
	if err != nil || decoded.Input["contentType"] != "text/plain" || decoded.Input["name"] != "a" || decoded.Input["_path"] != "/upload/storage/v1/b/b/o" {
		t.Fatalf("decode %#v %v", decoded, err)
	}
	body, _ := io.ReadAll(decoded.Body)
	if string(body) != `{"contentType":"text/plain"}` {
		t.Fatalf("upload body %q", body)
	}

	w := httptest.NewRecorder()
	if err := codec.Encode(svc, op, w, &spi.Response{Status: http.StatusCreated, Headers: http.Header{"X-Goog-Test": {"one", "two"}}, Output: map[string]any{"name": "a"}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusCreated || len(w.Header().Values("X-Goog-Test")) != 2 || !strings.Contains(w.Body.String(), `"name":"a"`) {
		t.Fatalf("JSON response %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, op, w, &spi.Response{Stream: io.NopCloser(strings.NewReader("object"))}); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || w.Body.String() != "object" || w.Header().Get("Content-Type") != "" {
		t.Fatalf("stream response %d %#v %q", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, op, w, &spi.Response{}); err != nil || w.Body.Len() != 0 {
		t.Fatalf("empty response %v %q", err, w.Body.String())
	}

	w = httptest.NewRecorder()
	if err := codec.EncodeFault(svc, op, w, spi.NotImplemented(svc.ID, op.Name, "emulate"), "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusNotImplemented || w.Header().Get("x-mirror-not-implemented") != "gcp.storage.storage.objects.insert" || !strings.Contains(w.Body.String(), `"reason":"MirrorNotImplemented"`) {
		t.Fatalf("fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
}
