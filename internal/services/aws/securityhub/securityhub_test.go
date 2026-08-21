package securityhub

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

func TestSecurityHubHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("securityhub Operations() %d want 9", n)
	}
}

func TestBootedServerSecurityHubCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.securityhub"}
	cfg.Seed = "sh-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/securityhub/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "SecurityHub."+op)
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
	en := call("EnableSecurityHub", `{}`)
	if en["HubArn"] == nil {
		t.Fatalf("enable %v", en)
	}
	got := call("DescribeHub", `{}`)
	if got["HubArn"] == nil {
		t.Fatalf("describe %v", got)
	}
	call("BatchImportFindings", `{"Findings":[{"Id":"f1","SchemaVersion":"2018-10-08","ProductArn":"arn:aws:securityhub:us-east-1::product/aws/securityhub","GeneratorId":"g","AwsAccountId":"000000000000","CreatedAt":"2020-01-01T00:00:00Z","UpdatedAt":"2020-01-01T00:00:00Z","Title":"t","Description":"d","Resources":[{"Type":"AwsAccount","Id":"AWS::::Account:000000000000"}]}]}`)
	findings := call("GetFindings", `{}`)
	raw, _ := json.Marshal(findings)
	if !strings.Contains(string(raw), "f1") {
		t.Fatalf("findings %s", raw)
	}
	ins := call("CreateInsight", `{"Name":"i1","Filters":{},"GroupByAttribute":"ResourceType"}`)
	arn, _ := ins["InsightArn"].(string)
	listed := call("GetInsights", `{}`)
	raw, _ = json.Marshal(listed)
	if !strings.Contains(string(raw), arn) {
		t.Fatalf("insights %s", raw)
	}
	call("DeleteInsight", `{"InsightArn":"`+arn+`"}`)
	call("DisableSecurityHub", `{}`)
}
