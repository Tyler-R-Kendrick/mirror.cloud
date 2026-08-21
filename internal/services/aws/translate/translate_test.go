package translate

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

func TestTranslateHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("translate Operations() %d want 9", n)
	}
}

func TestBootedServerTranslateCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.translate"}
	cfg.Seed = "tl-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/translate/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSShineFrontendService_20170701."+op)
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
	tr := call("TranslateText", `{"Text":"hello","SourceLanguageCode":"en","TargetLanguageCode":"es"}`)
	if tr["TranslatedText"] != "hello" {
		t.Fatalf("translate %v", tr)
	}
	created := call("CreateTerminology", `{"Name":"t1","SourceLanguageCode":"en"}`)
	if created["TerminologyProperties"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetTerminology", `{"Name":"t1"}`)
	if got["TerminologyProperties"] == nil {
		t.Fatalf("get %v", got)
	}
	call("DeleteTerminology", `{"Name":"t1"}`)
	listed := call("ListTerminologies", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"Name":"t1"`) {
		t.Fatalf("still present %s", raw)
	}
}
