package tagging

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

func TestBootedServerTaggingRoundTrip(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.tagging"}
	cfg.Seed = "tag-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/tagging/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126."+op)
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
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	call("TagResources", `{"ResourceARNList":["arn:aws:s3:::b"],"Tags":{"env":"dev"}}`)
	got := call("GetResources", `{}`)
	if got["ResourceTagMappingList"] == nil {
		t.Fatalf("get %v", got)
	}
	keys := call("GetTagKeys", `{}`)
	if keys["TagKeys"] == nil {
		t.Fatalf("keys %v", keys)
	}
}
