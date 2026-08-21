package transcribe

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

func TestTranscribeHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 8 {
		t.Fatalf("transcribe Operations() %d want 8", n)
	}
}

func TestBootedServerTranscribeCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.transcribe"}
	cfg.Seed = "tr-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/transcribe/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Transcribe."+op)
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
	created := call("StartTranscriptionJob", `{"TranscriptionJobName":"j1","LanguageCode":"en-US","Media":{"MediaFileUri":"s3://b/a.wav"}}`)
	job, _ := created["TranscriptionJob"].(map[string]any)
	if job["TranscriptionJobStatus"] != "COMPLETED" {
		t.Fatalf("start %v", created)
	}
	got := call("GetTranscriptionJob", `{"TranscriptionJobName":"j1"}`)
	if got["TranscriptionJob"] == nil {
		t.Fatalf("get %v", got)
	}
	call("CreateVocabulary", `{"VocabularyName":"v1","LanguageCode":"en-US"}`)
	call("DeleteTranscriptionJob", `{"TranscriptionJobName":"j1"}`)
	listed := call("ListTranscriptionJobs", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"TranscriptionJobName":"j1"`) {
		t.Fatalf("still present %s", raw)
	}
}
