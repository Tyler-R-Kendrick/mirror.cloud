package polly

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

func TestPollyHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 5 {
		t.Fatalf("polly Operations() %d want 5", n)
	}
}

func TestBootedServerPollyCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.polly"}
	cfg.Seed = "po-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/polly/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Polly."+op)
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
	syn := call("SynthesizeSpeech", `{"Text":"hello","VoiceId":"Joanna","OutputFormat":"mp3"}`)
	if syn["ContentType"] != "audio/mpeg" {
		t.Fatalf("synth %v", syn)
	}
	voices := call("DescribeVoices", `{}`)
	if voices["Voices"] == nil {
		t.Fatalf("voices %v", voices)
	}
	created := call("StartSpeechSynthesisTask", `{"Text":"hello","VoiceId":"Joanna","OutputFormat":"mp3","OutputS3BucketName":"b"}`)
	task, _ := created["SynthesisTask"].(map[string]any)
	id, _ := task["TaskId"].(string)
	if id == "" {
		t.Fatalf("start %v", created)
	}
	got := call("GetSpeechSynthesisTask", `{"TaskId":"`+id+`"}`)
	if got["SynthesisTask"] == nil {
		t.Fatalf("get %v", got)
	}
}
