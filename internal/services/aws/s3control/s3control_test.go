package s3control

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestS3ControlHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 7 {
		t.Fatalf("s3control Operations() %d want 7", n)
	}
}

func TestBootedServerS3ControlCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3control"}
	cfg.Seed = "s3c-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3-control/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSS3ControlService."+op)
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
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreateAccessPoint", `{"Name":"ap1","Bucket":"b1"}`)
	if created["AccessPointArn"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetAccessPoint", `{"Name":"ap1"}`)
	if got["Name"] != "ap1" {
		t.Fatalf("get %v", got)
	}
	call("PutPublicAccessBlock", `{"PublicAccessBlockConfiguration":{"BlockPublicAcls":true}}`)
	pab := call("GetPublicAccessBlock", `{}`)
	if pab["PublicAccessBlockConfiguration"] == nil {
		t.Fatalf("pab %v", pab)
	}
	call("DeleteAccessPoint", `{"Name":"ap1"}`)
	listed := call("ListAccessPoints", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"Name":"ap1"`) {
		t.Fatalf("still present %s", raw)
	}
}
