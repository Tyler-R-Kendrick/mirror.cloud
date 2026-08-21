package configservice

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

func TestConfigHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 16 {
		t.Fatalf("config Operations() %d want 16", n)
	}
}

func TestBootedServerConfigRecorderRuleEval(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.config"}
	cfg.Seed = "cfg-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/config/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "StarlingDoveService."+op)
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
	call("PutConfigurationRecorder", `{"ConfigurationRecorder":{"name":"default","roleARN":"arn:aws:iam::000000000000:role/config"}}`)
	listed := call("DescribeConfigurationRecorders", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "default") {
		t.Fatalf("recorders %s", raw)
	}
	call("PutConfigRule", `{"ConfigRule":{"ConfigRuleName":"r1","Source":{"Owner":"AWS","SourceIdentifier":"S3_BUCKET_VERSIONING_ENABLED"}}}`)
	rules := call("DescribeConfigRules", `{}`)
	raw, _ = json.Marshal(rules)
	if !strings.Contains(string(raw), "r1") {
		t.Fatalf("rules %s", raw)
	}
	call("PutEvaluations", `{"Evaluations":[{"ComplianceResourceId":"bucket-1","ComplianceResourceType":"AWS::S3::Bucket","ComplianceType":"NON_COMPLIANT"}]}`)
	hist := call("GetResourceConfigHistory", `{"resourceId":"bucket-1"}`)
	raw, _ = json.Marshal(hist)
	if !strings.Contains(string(raw), "bucket-1") {
		t.Fatalf("history %s", raw)
	}
	call("DeleteConfigRule", `{"ConfigRuleName":"r1"}`)
	gone := call("DescribeConfigRules", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"ConfigRuleName":"r1"`) {
		t.Fatalf("rule still present %s", raw)
	}
	call("DeleteConfigurationRecorder", `{"ConfigurationRecorderName":"default"}`)
	empty := call("DescribeConfigurationRecorders", `{}`)
	raw, _ = json.Marshal(empty)
	if strings.Contains(string(raw), `"name":"default"`) {
		t.Fatalf("recorder still present %s", raw)
	}
}
