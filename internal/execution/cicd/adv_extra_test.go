package cicd_test

import (
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/lifecycle"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
)

func TestADV_INTENT_CRASH(t *testing.T) {
	s := lifecycle.NewMemoryStore()
	res, err := s.Admit(lifecycle.AdmitRequest{
		Scope:         cicd.Scope{Environment: "local", Account: "a", Project: "p"},
		SubmissionKey: "intent-crash",
		Fingerprint:   "fp1",
		Mode:          "process",
		Profile:       "default",
		ConfigRef:     "cfg",
		Action:        "execute",
		Backend:       "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s.GetIntent(res.Intent.ID)
	if !ok || got.ID != res.Intent.ID {
		t.Fatalf("intent lost: ok=%v %#v", ok, got)
	}
	if _, ok := s.GetAttempt(res.Attempt.ID); !ok {
		t.Fatal("attempt lost")
	}
}

func TestADV_EGRESS(t *testing.T) {
	in := []string{
		"PATH=/bin", "HOME=/tmp", "SAFE=1",
		"AWS_ACCESS_KEY_ID=AKIA", "AWS_SECRET_ACCESS_KEY=x",
		"CF_API_TOKEN=tok", "HTTP_PROXY=http://evil", "NODE_OPTIONS=--require=x",
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://evil",
	}
	out := process.ScrubEnv(in)
	joined := strings.Join(out, "\n")
	for _, bad := range []string{"AWS_ACCESS_KEY_ID", "CF_API_TOKEN", "HTTP_PROXY", "NODE_OPTIONS", "OTEL_"} {
		if strings.Contains(joined, bad) {
			t.Fatalf("scrub leaked %s in %q", bad, joined)
		}
	}
	if !strings.Contains(joined, "SAFE=1") || !strings.Contains(joined, "PATH=") {
		t.Fatalf("scrub dropped safe keys: %q", joined)
	}
}
