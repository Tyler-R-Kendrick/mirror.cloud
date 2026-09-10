package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/hostinger/api"
)

func TestBootedServerHostingerAPI(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"hostinger.dns"}
	cfg.Seed = "hs-1"
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
		req.Host = "api.hostinger.com"
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
	code, raw, _ := do(http.MethodPost, "/api/domains/v1/portfolio", `{"domain":"boot.test"}`)
	dom := map[string]any{}
	_ = json.Unmarshal(raw, &dom)
	if code != 200 || dom["domain"] != "boot.test" {
		t.Fatalf("create %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPut, "/api/dns/v1/zones/boot.test", `{"overwrite":true,"zone":[{"name":"@","type":"A","ttl":300,"records":[{"content":"1.2.3.4"}]}]}`)
	acc := map[string]any{}
	_ = json.Unmarshal(raw, &acc)
	if code != 200 || acc["message"] != "Request accepted" {
		t.Fatalf("update %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/api/dns/v1/zones/boot.test", "")
	var recs []any
	if err := json.Unmarshal(raw, &recs); err != nil || code != 200 || len(recs) != 1 {
		t.Fatalf("get records %d %s", code, raw)
	}
	code, _, _ = do(http.MethodDelete, "/api/dns/v1/zones/boot.test", "")
	if code != 200 {
		t.Fatalf("delete %d", code)
	}
	code, raw, _ = do(http.MethodGet, "/api/dns/v1/zones/boot.test", "")
	_ = json.Unmarshal(raw, &recs)
	if code != 200 || len(recs) != 0 {
		t.Fatalf("after delete %d %s", code, raw)
	}
	code, raw, hdr := do(http.MethodGet, "/api/domains/v1/portfolio/missing.test", "")
	miss := map[string]any{}
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["message"] == nil || hdr.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing %d %#v %s", code, hdr, raw)
	}
}
