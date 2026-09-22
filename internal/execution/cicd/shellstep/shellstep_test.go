package shellstep_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/shellstep"
)

func TestRunEcho(t *testing.T) {
	var buf bytes.Buffer
	code, err := shellstep.Run(context.Background(), t.TempDir(), "echo hi", nil, &buf)
	if err != nil || code != 0 {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if !strings.Contains(buf.String(), "hi") {
		t.Fatalf("out=%q", buf.String())
	}
}

func TestMergeEnvOverrides(t *testing.T) {
	got := shellstep.MergeEnv([]string{"A=1", "B=2"}, map[string]string{"B": "9"})
	joined := strings.Join(got, ",")
	if !strings.Contains(joined, "B=9") || strings.Contains(joined, "B=2") {
		t.Fatalf("%v", got)
	}
}
