package vercel_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

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
	body, err := vercel.InvokeFunction(out, "api/hello", "/api/hello")
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
	if _, err := vercel.InvokeFunction(out, "bad", "/"); err == nil {
		t.Fatal("want reject non-nodejs runtime")
	}
}
