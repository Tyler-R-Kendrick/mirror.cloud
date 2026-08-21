package comprehend

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

func TestComprehendHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("comprehend Operations() %d want 12", n)
	}
}

func TestBootedServerComprehendCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.comprehend"}
	cfg.Seed = "cp-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/comprehend/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Comprehend_20171127."+op)
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
	sent := call("DetectSentiment", `{"Text":"great","LanguageCode":"en"}`)
	if sent["Sentiment"] != "POSITIVE" {
		t.Fatalf("sentiment %v", sent)
	}
	created := call("CreateEndpoint", `{"EndpointName":"e1","ModelArn":"arn:aws:comprehend:us-east-1:000000000000:document-classifier/m"}`)
	arn, _ := created["EndpointArn"].(string)
	if arn == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeEndpoint", `{"EndpointArn":"`+arn+`"}`)
	if got["EndpointProperties"] == nil {
		t.Fatalf("describe %v", got)
	}
	call("DeleteEndpoint", `{"EndpointArn":"`+arn+`"}`)
	listed := call("ListEndpoints", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"e1"`) {
		t.Fatalf("still present %s", raw)
	}
}
