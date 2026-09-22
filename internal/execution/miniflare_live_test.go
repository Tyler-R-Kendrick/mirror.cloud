//go:build miniflare

// Live Miniflare integration: real node, real workerd, real helper process.
//
// Gated behind `-tags miniflare` so the default `go test ./...` path stays
// usable with no optional runtimes installed. The gate is NOT a skip: built
// with the tag, a missing environment FAILS, because the make target that
// builds with the tag (`test-cloudflare-miniflare`) is the required suite and
// must not report success when the backend is absent.
package execution

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func helperSpec(t *testing.T) MiniflareStartSpec {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	spec := MiniflareStartSpec{
		NodeBin:       "node",
		HelperDir:     filepath.Join(wd, "..", "..", "tools", "cloudflare-runtime"),
		ReadyDeadline: 45 * time.Second,
	}
	if err := MiniflareAvailable(spec); err != nil {
		// Required-suite semantics: with the tag built in, an unprovisioned
		// environment is a failure, not a pass and not a skip.
		t.Fatalf("miniflare environment unavailable: %v", err)
	}
	return spec
}

// TestMiniflareLiveSession drives the full session lifecycle against a real
// workerd: start, describe, apply, both-direction KV coherence, binary
// exactness, failure classes, quiesce, close, and no-orphan reaping.
func TestMiniflareLiveSession(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()

	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	childPid := s.cmd.Process.Pid
	t.Cleanup(func() { _ = s.Close() })

	// Identity: the helper reports the pinned miniflare version it runs.
	if !strings.HasPrefix(s.Identity(), "miniflare/") {
		t.Fatalf("identity %q", s.Identity())
	}
	if v := strings.TrimPrefix(s.Identity(), "miniflare/"); v == "unknown" || v == "" {
		t.Fatalf("helper did not report its pinned version: %q", v)
	}

	// CF-ADMIN: the control channel is authenticated independently of any
	// public dummy credential. A wrong bearer on the same listener fails
	// (and we never copy the session struct -- it holds a mutex).
	{
		port := strings.TrimPrefix(s.baseURL, "http://127.0.0.1:")
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
		if err != nil {
			t.Fatalf("control port not listening: %v", err)
		}
		conn.Close()
		req, _ := http.NewRequest(http.MethodGet, s.baseURL+"/v1/describe", nil)
		req.Header.Set("Authorization", "Bearer not-the-session-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("wrong-token probe: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("wrong token answered %d, want 401", resp.StatusCode)
		}
	}

	// A call before apply is unavailable, not absent and not a success.
	var f *Failure
	_, err = s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "k"})
	if !errors.As(err, &f) || f.Class != ClassUnavailable {
		t.Fatalf("pre-apply call: %v", err)
	}

	// Apply a real Worker bound to one KV namespace. The Worker reads and
	// writes env.DATA; the control path addresses the SAME namespace object.
	worker := `export default { async fetch(req, env) {
		const u = new URL(req.url);
		if (req.method === 'PUT' && u.pathname === '/raw') {
			await env.DATA.put(u.searchParams.get('k'), await req.arrayBuffer());
			return new Response('raw-stored');
		}
		if (req.method === 'PUT') {
			await env.DATA.put(u.searchParams.get('k'), await req.text());
			return new Response('stored');
		}
		if (u.pathname === '/list') {
			const p = await env.DATA.list({prefix: u.searchParams.get('prefix') || ''});
			return new Response(JSON.stringify(p.keys.map(k => k.name)));
		}
		const v = await env.DATA.get(u.searchParams.get('k'));
		return new Response(v === null ? 'miss' : 'v=' + v);
	}};`
	applied, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name":              "app",
			"script":            worker,
			"compatibilityDate": "2025-01-01",
			"kvNamespaces":      []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if gen, _ := applied["generation"].(float64); gen != 1 {
		t.Fatalf("first apply generation %v, want 1", applied["generation"])
	}

	// A graph asking for remote bindings is refused before Miniflare sees
	// it: offline is admission, not a hope. The failed apply must not take
	// down the serving graph either (failed construction keeps last good).
	_, err = s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "x", "script": "export default {}", "compatibilityDate": "2025-01-01",
			"remoteBindings": true,
		}},
	})
	if !errors.As(err, &f) || f.Class != ClassValidation {
		t.Fatalf("remote-binding graph: %v", err)
	}
	after, err := s.Describe(ctx)
	if err != nil {
		t.Fatalf("describe after refused apply: %v", err)
	}
	if g, _ := after["generation"].(float64); g != 1 {
		t.Fatalf("generation after refused apply %v, want 1", after["generation"])
	}

	// COHERENCE direction 1: control write -> Worker read.
	b64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	if _, err := s.Call(ctx, "kv.put", map[string]any{
		"namespace": "DATA", "key": "from-control", "value": b64([]byte("hello-backend")),
	}); err != nil {
		t.Fatalf("control put: %v", err)
	}
	resp, err := s.Call(ctx, "worker.dispatch", map[string]any{"path": "/?k=from-control"})
	if err != nil {
		t.Fatalf("dispatch read: %v", err)
	}
	if got := decodeDispatch(t, resp); got != "v=hello-backend" {
		t.Fatalf("worker read %q, want v=hello-backend", got)
	}

	// COHERENCE direction 2: Worker write -> control read.
	resp, err = s.Call(ctx, "worker.dispatch", map[string]any{
		"method": "PUT", "path": "/?k=from-worker", "body": b64([]byte("worker-says")),
	})
	if err != nil || resp["status"].(float64) != 200 {
		t.Fatalf("dispatch write: %v %v", err, resp)
	}
	got, err := s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "from-worker"})
	if err != nil {
		t.Fatalf("control get: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(got["value"].(string))
	if err != nil || string(raw) != "worker-says" {
		t.Fatalf("coherence read %q err %v", raw, err)
	}

	// BINARY exactness through every hop: invalid UTF-8 control put, Worker
	// raw echo, control read -- bytes identical, no transcoding anywhere.
	bin := []byte{0x00, 0xff, 0xfe, 0x0a, 0x80}
	if _, err := s.Call(ctx, "kv.put", map[string]any{
		"namespace": "DATA", "key": "bin", "value": b64(bin),
	}); err != nil {
		t.Fatalf("binary put: %v", err)
	}
	resp, err = s.Call(ctx, "worker.dispatch", map[string]any{
		"method": "PUT", "path": "/raw?k=bin", "body": b64(bin),
	})
	if err != nil || resp["status"].(float64) != 200 {
		t.Fatalf("worker raw write: %v %v", err, resp)
	}
	got, err = s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "bin"})
	if err != nil {
		t.Fatalf("binary get: %v", err)
	}
	back, err := base64.StdEncoding.DecodeString(got["value"].(string))
	if err != nil || string(back) != string(bin) {
		t.Fatalf("binary round-trip %v, want %v", back, bin)
	}

	// LIST with prefix through the control path.
	listed, err := s.Call(ctx, "kv.list", map[string]any{"namespace": "DATA", "prefix": "from-"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if keys, _ := listed["keys"].([]any); len(keys) != 2 {
		t.Fatalf("prefix listing %v, want 2 keys", keys)
	}

	// Failure classes on the live backend: absent namespace, unsupported
	// action (no arbitrary eval exists), missing key as found=false.
	_, err = s.Call(ctx, "kv.get", map[string]any{"namespace": "nope", "key": "k"})
	if !errors.As(err, &f) || f.Class != ClassAbsent {
		t.Fatalf("unregistered namespace: %v", err)
	}
	_, err = s.Call(ctx, "eval", map[string]any{"code": "process.exit(1)"})
	if !errors.As(err, &f) || f.Class != ClassUnsupported {
		t.Fatalf("arbitrary eval attempt: %v", err)
	}
	miss, err := s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "never-set"})
	if err != nil || miss["found"] != false {
		t.Fatalf("missing key: %v %v", err, miss)
	}

	// Re-apply (reconfiguration): generation advances and prior data
	// survives, because namespace identity is the registered name and no
	// caller-side handle is ever cached across a swap.
	re, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app2", "script": worker,
			"compatibilityDate": "2025-01-01", "kvNamespaces": []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	})
	if err != nil || re["generation"].(float64) != 2 {
		t.Fatalf("re-apply: %v %v", err, re)
	}
	got, err = s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "from-worker"})
	if err != nil || got["found"] != true {
		t.Fatalf("data lost across re-apply: %v %v", err, got)
	}

	// Quiesce: stable-state refusal, distinguishable from absence.
	if _, err := s.Quiesce(ctx); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	_, err = s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "x"})
	if !errors.As(err, &f) || f.Class != ClassUnavailable {
		t.Fatalf("post-quiesce call: %v", err)
	}

	// Close: the helper exits and the process group is reaped -- no orphan
	// node, no orphan workerd. Double close is safe. The wait uses real
	// monotonic timer channels (operational deadline), never time.Now --
	// the determinism lint keeps Now() outside /clock.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reaped := make(chan bool, 1)
	go func() {
		for {
			if err := syscall.Kill(childPid, 0); err != nil {
				reaped <- true
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	select {
	case <-reaped:
	case <-time.After(5 * time.Second):
		t.Fatalf("helper pid %d survived Close", childPid)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestMiniflareStartFailsCleanly proves a bad environment fails BEFORE any
// spawn and leaks no helper process.
func TestMiniflareStartFailsCleanly(t *testing.T) {
	spec := helperSpec(t)
	bad := spec
	bad.HelperDir = t.TempDir()
	if err := MiniflareAvailable(bad); err == nil {
		t.Fatal("availability accepted an empty helper dir")
	}
	if _, err := MiniflareStart(context.Background(), bad); err == nil {
		t.Fatal("start accepted an empty helper dir")
	}
	worse := spec
	worse.NodeBin = "definitely-not-a-real-binary-mirror"
	if err := MiniflareAvailable(worse); err == nil {
		t.Fatal("availability accepted a missing node")
	}
	out := leakyHelpers()
	if len(out) != 0 {
		t.Fatalf("failed Start leaked helper processes: %v", out)
	}
}

// leakyHelpers returns pids whose argv is structurally a helper spawn:
// argv[0] resolves to node and argv[1] is session.mjs. Reading /proc argv
// instead of running pgrep -f keeps any shell whose command line merely
// CONTAINS the pattern (the test runner itself, this repository's tooling)
// from satisfying the check -- a substring match must not decide whether a
// leak happened, one way or the other.
func leakyHelpers() []string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil // non-Linux: no /proc to scan; spawn checks above still bound Start
	}
	var found []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		args := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		if len(args) >= 2 && strings.HasSuffix(args[0], "node") && args[1] == "session.mjs" {
			found = append(found, e.Name())
		}
	}
	return found
}

func decodeDispatch(t *testing.T, resp map[string]any) string {
	t.Helper()
	body, err := base64.StdEncoding.DecodeString(resp["body"].(string))
	if err != nil {
		t.Fatalf("dispatch body b64: %v", err)
	}
	return string(body)
}
