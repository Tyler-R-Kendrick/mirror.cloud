package process_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
)

func TestScrubEnv(t *testing.T) {
	in := []string{
		"PATH=/bin",
		"AWS_SECRET=x",
		"AWS_REGION=us-east-1",
		"CF_API_TOKEN=t",
		"CF_API_EMAIL=e",
		"HTTP_PROXY=http://evil",
		"HTTPS_PROXY=https://evil",
		"ALL_PROXY=socks5://evil",
		"CLOUDFLARE_API_TOKEN=tok",
		"NODE_OPTIONS=--require evil",
		"OTEL_EXPORTER=otlp",
		"OTEL_TRACES_EXPORTER=otlp",
		"HOME=/tmp",
	}
	out := process.ScrubEnv(in)
	joined := strings.Join(out, "\n")
	for _, bad := range []string{"AWS_", "CF_API", "CLOUDFLARE_API", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NODE_OPTIONS", "OTEL"} {
		for _, e := range out {
			key, _, _ := strings.Cut(e, "=")
			if strings.HasPrefix(key, bad) || key == bad {
				t.Fatalf("scrub missed %q in %v", e, out)
			}
		}
		_ = bad
	}
	if !strings.Contains(joined, "PATH=/bin") || !strings.Contains(joined, "HOME=/tmp") {
		t.Fatalf("kept keys lost: %v", out)
	}
}

func TestRunCombinedOutputExit(t *testing.T) {
	ctx := context.Background()
	var buf strings.Builder
	code, err := process.Run(ctx, process.Config{
		Path: "/bin/sh",
		Args: []string{"-c", "echo hi; echo err >&2; exit 3"},
	}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != 3 {
		t.Fatalf("exit %d", code)
	}
	out := buf.String()
	if !strings.Contains(out, "hi") || !strings.Contains(out, "err") {
		t.Fatalf("output %q", out)
	}
}

func TestStartCloseIdempotent_True(t *testing.T) {
	ctx := context.Background()
	p, err := process.Start(ctx, process.Config{Path: "/bin/true"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestReadyFileAndCancelCleanup(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	script := filepath.Join(dir, "helper.sh")
	// Local helper: touch ready, then sleep until killed. No network.
	body := "#!/bin/sh\ntouch \"$1\"\nexec sleep 60\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := process.Start(ctx, process.Config{
		Path:         script,
		Args:         []string{ready},
		ReadyFile:    ready,
		ReadyTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ready); err != nil {
		t.Fatalf("ready file: %v", err)
	}

	cancel() // parent cancel must tear down child
	// Close still idempotent after cancel
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
