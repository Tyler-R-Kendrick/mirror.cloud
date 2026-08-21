package kinesisanalytics

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

func TestKinesisAnalyticsHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("kinesisanalytics Operations() %d want 9", n)
	}
}

func TestBootedServerKinesisAnalyticsCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.kinesisanalytics"}
	cfg.Seed = "ka-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/kinesisanalytics/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "KinesisAnalytics_20150814."+op)
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
	created := call("CreateApplication", `{"ApplicationName":"app1"}`)
	if created["ApplicationSummary"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeApplication", `{"ApplicationName":"app1"}`)
	det, _ := got["ApplicationDetail"].(map[string]any)
	if det["ApplicationName"] != "app1" {
		t.Fatalf("describe %v", got)
	}
	call("StartApplication", `{"ApplicationName":"app1"}`)
	run := call("DescribeApplication", `{"ApplicationName":"app1"}`)
	det, _ = run["ApplicationDetail"].(map[string]any)
	if det["ApplicationStatus"] != "RUNNING" {
		t.Fatalf("start %v", run)
	}
	call("DeleteApplication", `{"ApplicationName":"app1"}`)
	listed := call("ListApplications", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"ApplicationName":"app1"`) {
		t.Fatalf("still present %s", raw)
	}
}
