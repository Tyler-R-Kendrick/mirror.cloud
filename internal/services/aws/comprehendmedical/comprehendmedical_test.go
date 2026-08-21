package comprehendmedical

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

func TestComprehendMedicalHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("comprehendmedical Operations() %d want 6", n)
	}
}

func TestBootedServerComprehendMedicalCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.comprehendmedical"}
	cfg.Seed = "cm-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/comprehendmedical/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "ComprehendMedical_20181030."+op)
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
	det := call("DetectEntitiesV2", `{"Text":"patient has a cough"}`)
	if det["Entities"] == nil {
		t.Fatalf("detect %v", det)
	}
	started := call("StartEntitiesDetectionV2Job", `{"JobName":"j1","InputDataConfig":{"S3Bucket":"b"},"OutputDataConfig":{"S3Bucket":"b"},"DataAccessRoleArn":"arn:aws:iam::000000000000:role/r"}`)
	id, _ := started["JobId"].(string)
	if id == "" {
		t.Fatalf("start %v", started)
	}
	got := call("DescribeEntitiesDetectionV2Job", `{"JobId":"`+id+`"}`)
	props, _ := got["ComprehendMedicalAsyncJobProperties"].(map[string]any)
	if props["JobStatus"] != "COMPLETED" {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListEntitiesDetectionV2Jobs", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("list %s", raw)
	}
}
