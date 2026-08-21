package cloudfront

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestCloudFrontHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("cloudfront Operations() %d want 9", n)
	}
}

func TestBootedServerCloudFrontDistribution(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudfront"}
	cfg.Seed = "cf-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudfront/aws4_request, SignedHeaders=host, Signature=00"
	call := func(vals url.Values) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/?"+vals.Encode(), nil)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", vals.Get("Action"), res.StatusCode, b)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		return string(b)
	}
	created := call(url.Values{"Action": {"CreateDistribution"}, "CallerReference": {"c1"}, "DomainName": {"b.s3.amazonaws.com"}})
	if !strings.Contains(created, "<Id>") || !strings.Contains(created, "Deployed") {
		t.Fatalf("create %s", created)
	}
	id := between(created, "<Id>", "</Id>")
	if id == "" {
		t.Fatalf("id %s", created)
	}
	got := call(url.Values{"Action": {"GetDistribution"}, "Id": {id}})
	if !strings.Contains(got, id) {
		t.Fatalf("get %s", got)
	}
	listed := call(url.Values{"Action": {"ListDistributions"}})
	if !strings.Contains(listed, id) {
		t.Fatalf("list %s", listed)
	}
	inv := call(url.Values{"Action": {"CreateInvalidation"}, "Id": {id}})
	if !strings.Contains(inv, "Completed") && !strings.Contains(inv, "<Id>") {
		t.Fatalf("invalidate %s", inv)
	}
	call(url.Values{"Action": {"DeleteDistribution"}, "Id": {id}})
	gone := call(url.Values{"Action": {"ListDistributions"}})
	if strings.Contains(gone, "<Id>"+id+"</Id>") {
		t.Fatalf("still present %s", gone)
	}
}
