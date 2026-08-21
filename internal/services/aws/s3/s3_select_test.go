package s3

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

func TestBootedServerS3SelectSQL(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.Seed = "s3-sel-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body, ctype string) (int, []byte, string) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", auth)
		if ctype != "" {
			req.Header.Set("Content-Type", ctype)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, raw, res.Header.Get("x-mirror-fidelity")
	}
	if code, b, _ := do(http.MethodPut, "/selb", "", ""); code >= 300 {
		t.Fatalf("bucket %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/selb/rows", "alice,1\nbob,2\n", ""); code >= 300 {
		t.Fatalf("put %d %s", code, b)
	}
	code, b, fid := do(http.MethodPost, "/selb/rows?select&select-type=2", "Expression=SELECT _2 WHERE s._1 = 'alice'", "application/x-www-form-urlencoded")
	if code >= 300 || fid != "emulate" {
		t.Fatalf("select %d %s fid %s", code, b, fid)
	}
	if !strings.Contains(string(b), "1") || strings.Contains(string(b), "alice") || strings.Contains(string(b), "bob") {
		t.Fatalf("want projected column 1 without alice/bob, got %q", b)
	}
	code, b, fid = do(http.MethodPost, "/selb/rows?select&select-type=2", "Expression=SELECT * WHERE s._1 = 'alice'", "application/x-www-form-urlencoded")
	if code >= 300 || fid != "emulate" {
		t.Fatalf("select * %d %s fid %s", code, b, fid)
	}
	if !strings.Contains(string(b), "alice") || strings.Contains(string(b), "bob") {
		t.Fatalf("select * filter %s", b)
	}
}
