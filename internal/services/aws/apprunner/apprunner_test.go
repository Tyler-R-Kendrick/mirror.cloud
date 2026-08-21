package apprunner

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

func TestAppRunnerHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("apprunner Operations() %d want 6", n)
	}
}

func TestBootedServerAppRunnerCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.apprunner"}
	cfg.Seed = "ar-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/apprunner/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AppRunner."+op)
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
	created := call("CreateService", `{"ServiceName":"svc1","SourceConfiguration":{"ImageRepository":{"ImageIdentifier":"public.ecr.aws/aws-containers/hello-app-runner:latest","ImageRepositoryType":"ECR_PUBLIC"}}}`)
	if created["Service"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeService", `{"ServiceArn":"arn:aws:apprunner:us-east-1:000000000000:service/svc1"}`)
	if got["Service"] == nil {
		t.Fatalf("get %v", got)
	}
	call("DeleteService", `{"ServiceArn":"arn:aws:apprunner:us-east-1:000000000000:service/svc1"}`)
	listed := call("ListServices", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"svc1"`) {
		t.Fatalf("still present %s", raw)
	}
}
