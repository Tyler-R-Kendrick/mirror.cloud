package vercel_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/vercel"
)

// VC-BOA-STATIC: Build Output API v3 static/ + optional config.json → PublishDir HTTP.
func TestVC_BOA_STATIC(t *testing.T) {
	nonce := "boa-nonce-" + t.Name()
	out := t.TempDir()
	static := filepath.Join(out, "static")
	if err := os.MkdirAll(static, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(static, "index.html"), []byte(nonce), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{
		"version": 3,
		"routes": []any{
			map[string]any{"src": "/", "dest": "/index.html"},
		},
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "config.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	site, err := vercel.PublishBOA(out)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = site.Close() })
	if err := site.VerifyNonce("index.html", nonce); err != nil {
		t.Fatal(err)
	}

	bad := t.TempDir()
	if err := os.MkdirAll(filepath.Join(bad, "static"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, "static", "ok.html"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	badCfg, _ := json.Marshal(map[string]any{
		"version": 3,
		"routes": []any{
			map[string]any{"src": "/gone", "dest": "/missing.html"},
		},
	})
	if err := os.WriteFile(filepath.Join(bad, "config.json"), badCfg, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := vercel.PublishBOA(bad); err == nil {
		t.Fatal("want reject missing dest referenced in config")
	}
}

// VC-BOA-FUNCTION: static PublishBOA + Node .func via InvokeFunction; bad config rejects.
func TestVC_BOA_FUNCTION(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not in PATH")
	}
	nonce := "boa-fn-" + t.Name()
	out := t.TempDir()
	static := filepath.Join(out, "static")
	if err := os.MkdirAll(static, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(static, "index.html"), []byte(nonce+"-static"), 0o644); err != nil {
		t.Fatal(err)
	}
	funcDir := filepath.Join(out, "functions", "api", "hello.func")
	if err := os.MkdirAll(funcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	vc, _ := json.Marshal(map[string]any{"runtime": "nodejs20.x", "handler": "index.js"})
	if err := os.WriteFile(filepath.Join(funcDir, ".vc-config.json"), vc, 0o644); err != nil {
		t.Fatal(err)
	}
	handler := "module.exports = (req, res) => { res.end(" + strconv.Quote(nonce) + "); };\n"
	if err := os.WriteFile(filepath.Join(funcDir, "index.js"), []byte(handler), 0o644); err != nil {
		t.Fatal(err)
	}

	site, err := vercel.PublishBOA(out)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = site.Close() })
	if err := site.VerifyNonce("index.html", nonce+"-static"); err != nil {
		t.Fatal(err)
	}
	// The deadline is the caller's. Here it only stops a hang: how long Node
	// takes to start on a busy machine is not what this test is about.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	body, err := vercel.InvokeFunction(ctx, out, "api/hello", "/api/hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != nonce {
		t.Fatalf("function body %q want %q", body, nonce)
	}

	badDir := filepath.Join(out, "functions", "bad.func")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	badVC, _ := json.Marshal(map[string]any{"runtime": "edge", "handler": "index.js"})
	if err := os.WriteFile(filepath.Join(badDir, ".vc-config.json"), badVC, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "index.js"), []byte("module.exports = () => {};"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := vercel.InvokeFunction(ctx, out, "bad", "/"); err == nil {
		t.Fatal("want reject non-nodejs runtime")
	}
}

// A function that outlives its caller's deadline is stopped at it, and the
// error says so: the context's own, not the "signal: killed" of the process
// that was stopped. The handler waits on a timer, so nothing here depends on
// how fast the machine is beyond Node starting within the hang guard.
func TestInvokeFunctionStopsAtItsCallersDeadline(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not in PATH")
	}
	out := t.TempDir()
	funcDir := filepath.Join(out, "functions", "slow.func")
	if err := os.MkdirAll(funcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	vc, _ := json.Marshal(map[string]any{"runtime": "nodejs20.x", "handler": "index.js"})
	if err := os.WriteFile(filepath.Join(funcDir, ".vc-config.json"), vc, 0o644); err != nil {
		t.Fatal(err)
	}
	// Says it has started, then never answers.
	started := filepath.Join(out, "started")
	handler := "const fs = require('fs');\nmodule.exports = (req, res) => { fs.writeFileSync(" + strconv.Quote(started) + ", ''); return new Promise(() => setInterval(() => {}, 3600000)); };\n"
	if err := os.WriteFile(filepath.Join(funcDir, "index.js"), []byte(handler), 0o644); err != nil {
		t.Fatal(err)
	}

	// Cancelled once the handler is running: the wait is for the handler, not
	// for a length of time.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			if _, err := os.Stat(started); err == nil {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	guard := time.AfterFunc(5*time.Minute, cancel)
	defer guard.Stop()
	_, err := vercel.InvokeFunction(ctx, out, "slow", "/")
	if _, statErr := os.Stat(started); statErr != nil {
		t.Fatalf("the handler never started: %v (invoke: %v)", statErr, err)
	}
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "signal: killed") || !strings.Contains(err.Error(), "invoke slow") {
		t.Fatalf("invoke stopped by its caller: %v", err)
	}

	// A deadline already past: the same error, the deadline's.
	past, cancelPast := context.WithTimeout(context.Background(), -time.Second)
	defer cancelPast()
	if _, err := vercel.InvokeFunction(past, out, "slow", "/"); !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("invoke past its deadline: %v", err)
	}
	// And one that passes on the way: whether it catches Node starting or the
	// handler waiting, the answer is the deadline's.
	short, cancelShort := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelShort()
	if _, err := vercel.InvokeFunction(short, out, "slow", "/"); !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("invoke that outlives its deadline: %v", err)
	}
}
