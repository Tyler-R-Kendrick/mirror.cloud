package spine

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
)

func TestMockLabeledAndStrictRefuses(t *testing.T) {
	call := func(strict bool) *http.Response {
		t.Helper()
		cfg := config.Default()
		cfg.Services = nil
		cfg.Strict = strict
		rt, err := runtime.Boot(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(rt.Handler())
		t.Cleanup(ts.Close)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{}`))
		req.Header.Set("X-Amz-Target", "TrentService.CreateKey")
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	res := call(false)
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("mock create %d %s", res.StatusCode, b)
	}
	if res.Header.Get("x-mirror-fidelity") != "mock" {
		t.Fatalf("want mock, got %q body %s", res.Header.Get("x-mirror-fidelity"), b)
	}

	res = call(true)
	b, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 501 {
		t.Fatalf("strict status %d %s", res.StatusCode, b)
	}
	if res.Header.Get("x-mirror-not-implemented") == "" && !strings.Contains(string(b), "MirrorNotImplemented") {
		t.Fatalf("strict not 501 envelope: %s %v", b, res.Header)
	}
}
