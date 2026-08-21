package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

func TestExpandServices(t *testing.T) {
	got := runtime.ExpandServices([]string{"s3,sqs"}, "", false)
	if len(got) != 2 || got[0] != "aws.s3" || got[1] != "aws.sqs" {
		t.Fatalf("got %v", got)
	}
	if runtime.ExpandServices(nil, "", true) != nil {
		t.Fatal("all should be nil (enable every catalog service)")
	}
	got = runtime.ExpandServices(nil, "aws-core", false)
	if len(got) != 151 {
		t.Fatalf("aws-core: %v", got)
	}
}

func TestEnvOutput(t *testing.T) {
	t.Setenv("MIRROR_BIND", "127.0.0.1:4566")
	var buf bytes.Buffer
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	if err := cmdEnv(nil); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	os.Stdout = old
	_, _ = buf.ReadFrom(r)
	out := buf.String()
	for _, want := range []string{
		"AWS_ENDPOINT_URL=http://127.0.0.1:4566",
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"STORAGE_EMULATOR_HOST=",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
}

func TestSplitFlagsAllowsBindAfterService(t *testing.T) {
	flags, pos := splitFlags([]string{"s3", "--bind", "127.0.0.1:9", "--seed", "x"}, map[string]bool{"all": true, "strict": true})
	if len(pos) != 1 || pos[0] != "s3" {
		t.Fatalf("pos %v", pos)
	}
	joined := strings.Join(flags, " ")
	if !strings.Contains(joined, "--bind 127.0.0.1:9") || !strings.Contains(joined, "--seed x") {
		t.Fatalf("flags %v", flags)
	}
}

func TestUsageNonEmpty(t *testing.T) {
	var b bytes.Buffer
	usage(&b)
	if !strings.Contains(b.String(), "mirror up") {
		t.Fatal(b.String())
	}
}
