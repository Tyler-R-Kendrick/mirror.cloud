package miniflare_test

import (
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/miniflare"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func runtimeRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// internal/execution/miniflare → repo root → tools/cloudflare-runtime
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "tools", "cloudflare-runtime"))
}

func requireLive(t *testing.T) (node, helper string) {
	t.Helper()
	if !liveRequested() {
		t.Skip("set MIRROR_MINIFLARE=1 or go test -tags=miniflare")
	}
	root := runtimeRoot(t)
	modules := filepath.Join(root, "node_modules", "miniflare")
	if _, err := os.Stat(modules); err != nil {
		t.Skipf("npm ci not run in %s: %v", root, err)
	}
	helper = filepath.Join(root, "helper.mjs")
	node = os.Getenv("MIRROR_NODE")
	if node == "" {
		node = "/home/codex/.local/bin/node"
	}
	if _, err := os.Stat(node); err != nil {
		if p, lookErr := exec.LookPath("node"); lookErr == nil {
			node = p
		} else {
			t.Skipf("node not found: %v", err)
		}
	}
	return node, helper
}

func TestLiveKVPutGet(t *testing.T) {
	node, helper := requireLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s := &miniflare.Session{Node: node, Helper: helper}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Close(ctx)

	id, caps, err := s.Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id.Name != "miniflare" || len(caps) != 2 {
		t.Fatalf("describe: id=%+v caps=%d", id, len(caps))
	}

	ref := spi.ResourceRef{Kind: "kv_namespace", ID: "ns1", Account: "acct"}
	payload := []byte("hello\x00miniflare\xff")
	_, err = s.Call(ctx, spi.CapabilityCall{
		Resource: ref,
		Action:   "kv.put",
		Args: map[string]any{
			"key":   "k1",
			"value": string(payload),
		},
	})
	if err != nil {
		t.Fatalf("kv.put: %v", err)
	}
	res, err := s.Call(ctx, spi.CapabilityCall{
		Resource: ref,
		Action:   "kv.get",
		Args:     map[string]any{"key": "k1"},
	})
	if err != nil {
		t.Fatalf("kv.get: %v", err)
	}
	gotB64, _ := res.Output["value_b64"].(string)
	got, err := base64.StdEncoding.DecodeString(gotB64)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("bytes mismatch: got %q want %q", got, payload)
	}
}

func TestLiveWorkerFetchThenKVGet(t *testing.T) {
	node, helper := requireLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s := &miniflare.Session{Node: node, Helper: helper}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Close(ctx)

	ref := spi.ResourceRef{Kind: "kv_namespace", ID: "ns1"}
	payload := []byte("via-worker")
	b64 := base64.StdEncoding.EncodeToString(payload)
	_, err := s.Call(ctx, spi.CapabilityCall{
		Resource: ref,
		Action:   "worker.fetch",
		Args: map[string]any{
			"key":       "w1",
			"method":    "PUT",
			"value_b64": b64,
		},
	})
	if err != nil {
		t.Fatalf("worker.fetch PUT: %v", err)
	}
	res, err := s.Call(ctx, spi.CapabilityCall{
		Resource: ref,
		Action:   "kv.get",
		Args:     map[string]any{"key": "w1"},
	})
	if err != nil {
		t.Fatalf("kv.get after worker: %v", err)
	}
	gotB64, _ := res.Output["value_b64"].(string)
	got, err := base64.StdEncoding.DecodeString(gotB64)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("coherence fail: got %q want %q", got, payload)
	}
}
