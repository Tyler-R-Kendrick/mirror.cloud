package mediaconnect

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

func TestMediaConnectHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("mediaconnect Operations() %d want 6", n)
	}
}

func TestBootedServerMediaConnectCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.mediaconnect"}
	cfg.Seed = "mc-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/mediaconnect/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "MediaConnect."+op)
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
	created := call("CreateFlow", `{"Name":"flow1"}`)
	fl, _ := created["Flow"].(map[string]any)
	arn, _ := fl["FlowArn"].(string)
	if arn == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeFlow", `{"FlowArn":"`+arn+`"}`)
	if got["Flow"] == nil {
		t.Fatalf("get %v", got)
	}
	call("DeleteFlow", `{"FlowArn":"`+arn+`"}`)
	listed := call("ListFlows", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), arn) {
		t.Fatalf("still present %s", raw)
	}
}
