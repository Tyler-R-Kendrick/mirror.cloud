//go:build celld

package execution

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func celldSpec(t *testing.T) CelldStartSpec {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	spec := CelldStartSpec{
		RuntimeDir:    filepath.Join(root, "tools", "celld-runtime"),
		ProjectDir:    filepath.Join(root, "tools", "celld-runtime", "fixture"),
		ReadyDeadline: 60 * time.Second,
	}
	if err := CelldAvailable(spec); err != nil {
		t.Fatalf("celld environment unavailable: %v", err)
	}
	return spec
}

func TestCelldLiveIdentityAndBindings(t *testing.T) {
	spec := celldSpec(t)
	ctx := context.Background()
	s, err := CelldStart(ctx, spec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if !strings.HasPrefix(s.Identity(), "celld/0.5.1") {
		t.Fatalf("identity %q", s.Identity())
	}

	// KV
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, s.BaseURL()+"/kv?k=a", strings.NewReader("celld-kv"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = s.Fetch(ctx, http.MethodGet, "/kv?k=a", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "celld-kv" {
		t.Fatalf("kv %q", body)
	}

	// D1
	resp, err = s.Fetch(ctx, http.MethodGet, "/d1", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello" {
		t.Fatalf("d1 %q", body)
	}

	// R2
	req, _ = http.NewRequestWithContext(ctx, http.MethodPut, s.BaseURL()+"/r2?k=obj", strings.NewReader("r2-bytes"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = s.Fetch(ctx, http.MethodGet, "/r2?k=obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "r2-bytes" {
		t.Fatalf("r2 %q", body)
	}

	// Queues (producer send)
	req, _ = http.NewRequestWithContext(ctx, http.MethodPost, s.BaseURL()+"/queue", strings.NewReader("msg-1"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "queued" {
		t.Fatalf("queue %q", body)
	}

	// Durable Object
	req, _ = http.NewRequestWithContext(ctx, http.MethodPost, s.BaseURL()+"/do", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "1" {
		t.Fatalf("do first %q", body)
	}
	req, _ = http.NewRequestWithContext(ctx, http.MethodPost, s.BaseURL()+"/do", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "2" {
		t.Fatalf("do second %q", body)
	}
}

func TestCelldAvailableRejectsBadDigest(t *testing.T) {
	spec := celldSpec(t)
	spec.RuntimeDir = t.TempDir()
	if err := CelldAvailable(spec); err == nil {
		t.Fatal("want missing PIN error")
	}
}
