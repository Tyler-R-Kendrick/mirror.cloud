package guardduty

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

func TestGuardDutyHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 20 {
		t.Fatalf("guardduty Operations() %d want 20", n)
	}
}

func TestBootedServerGuardDutyDetectorFindings(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.guardduty"}
	cfg.Seed = "gd-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/guardduty/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSGuardDuty."+op)
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
	created := call("CreateDetector", `{"Enable":true}`)
	id, _ := created["DetectorId"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("GetDetector", `{"DetectorId":"`+id+`"}`)
	if got["Status"] != "ENABLED" {
		t.Fatalf("get %v", got)
	}
	listed := call("ListDetectors", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("list %s", raw)
	}
	call("CreateSampleFindings", `{"DetectorId":"`+id+`"}`)
	finds := call("ListFindings", `{"DetectorId":"`+id+`"}`)
	ids, _ := finds["FindingIds"].([]any)
	if len(ids) == 0 {
		t.Fatalf("findings %v", finds)
	}
	gotF := call("GetFindings", `{"DetectorId":"`+id+`","FindingIds":["`+ids[0].(string)+`"]}`)
	if gotF["Findings"] == nil {
		t.Fatalf("get findings %v", gotF)
	}
	call("DeleteDetector", `{"DetectorId":"`+id+`"}`)
	gone := call("ListDetectors", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
