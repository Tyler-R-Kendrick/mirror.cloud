package textract

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

func TestTextractHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("textract Operations() %d want 6", n)
	}
}

func TestBootedServerTextractCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.textract"}
	cfg.Seed = "tx-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/textract/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Textract."+op)
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
	det := call("DetectDocumentText", `{"Document":{"Bytes":"QQ=="}}`)
	if det["Blocks"] == nil {
		t.Fatalf("detect %v", det)
	}
	started := call("StartDocumentTextDetection", `{"DocumentLocation":{"S3Object":{"Bucket":"b","Name":"a.png"}}}`)
	id, _ := started["JobId"].(string)
	if id == "" {
		t.Fatalf("start %v", started)
	}
	got := call("GetDocumentTextDetection", `{"JobId":"`+id+`"}`)
	if got["JobStatus"] != "SUCCEEDED" {
		t.Fatalf("get %v", got)
	}
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), "HELLO") {
		t.Fatalf("blocks %s", raw)
	}
}
