//go:build miniflare

package execution

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMiniflareSnapshotRestore(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	worker := `export default { fetch() { return new Response("ok"); } };`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
			"kvNamespaces": []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	}); err != nil {
		t.Fatal(err)
	}
	b64 := base64.StdEncoding.EncodeToString([]byte("snap-value"))
	if _, err := s.Call(ctx, "kv.put", map[string]any{
		"namespace": "DATA", "key": "k", "value": b64,
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := s.SnapshotKV(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Call(ctx, "kv.delete", map[string]any{"namespace": "DATA", "key": "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RestoreKV(ctx, snap); err != nil {
		t.Fatal(err)
	}
	got, err := s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "k"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.StdEncoding.DecodeString(got["value"].(string))
	if string(raw) != "snap-value" {
		t.Fatalf("restore %q", raw)
	}
}

func TestMiniflareOfflineNetNS(t *testing.T) {
	if err := exec.Command("unshare", "-n", "true").Run(); err != nil {
		t.Skipf("unshare -n not permitted: %v", err)
	}
	spec := helperSpec(t)
	spec.OfflineNetNS = true
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatalf("start in netns: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	worker := `export default { fetch() { return new Response("ns-ok"); } };`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
			"kvNamespaces": []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := s.Call(ctx, "worker.dispatch", map[string]any{"path": "/"})
	if err != nil {
		t.Fatal(err)
	}
	if decodeDispatch(t, resp) != "ns-ok" {
		t.Fatalf("%v", resp)
	}
	if s.cmd == nil || s.cmd.Process == nil {
		t.Fatal("no child")
	}
	// Child netns must not reach the public Internet. Prefer nsenter; fall back
	// to offline.probe inside the already-isolated helper.
	out, err := exec.Command("nsenter", "-t", strconv.Itoa(s.cmd.Process.Pid), "-n",
		"bash", "-c", "timeout 1 bash -c 'echo >/dev/tcp/1.1.1.1/443' 2>&1 || echo blocked").CombinedOutput()
	if err != nil || (!strings.Contains(string(out), "blocked") &&
		!strings.Contains(string(out), "Network") &&
		!strings.Contains(string(out), "Connection") &&
		!strings.Contains(string(out), "No route")) {
		probe, perr := s.Call(ctx, "offline.probe", map[string]any{})
		if perr != nil {
			t.Fatalf("netns outbound check inconclusive (%v / %q); probe: %v", err, out, perr)
		}
		if blocked, _ := probe["blocked"].(bool); !blocked {
			t.Fatalf("child reached outbound; nsenter=%q probe=%#v", out, probe)
		}
	}
}

func TestMiniflareOfflineTripwire(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	worker := `export default { fetch() { return new Response("ok"); } };`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
			"kvNamespaces": []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	}); err != nil {
		t.Fatal(err)
	}
	out, err := s.Call(ctx, "offline.probe", map[string]any{})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if blocked, _ := out["blocked"].(bool); !blocked {
		t.Fatalf("want blocked outbound, got %#v", out)
	}
}

func TestMiniflareCrashMidInvoke(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	// No Cleanup Close — Kill owns the process.
	worker := `export default { fetch() { return new Response("ok"); } };`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
			"kvNamespaces": []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	}); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), "hang", map[string]any{"ms": 30000})
		errCh <- err
	}()
	time.Sleep(300 * time.Millisecond)
	s.Kill()
	select {
	case err := <-errCh:
		var f *Failure
		if !errors.As(err, &f) || f.Class != ClassUnavailable {
			t.Fatalf("mid-kill want ClassUnavailable, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("hang call did not return after Kill")
	}
}

func TestMiniflareWorkerdTTL(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	worker := `export default { fetch() { return new Response("ok"); } };`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
			"kvNamespaces": []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	}); err != nil {
		t.Fatal(err)
	}
	b64 := base64.StdEncoding.EncodeToString([]byte("ttl-bytes"))
	// workerd enforces Cloudflare's 60s minimum TTL. Wait past that under the
	// real backend clock — CF-CLOCK external half.
	if _, err := s.Call(ctx, "kv.put", map[string]any{
		"namespace": "DATA", "key": "temp", "value": b64, "expirationTtl": 60,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "temp"})
	if err != nil {
		t.Fatal(err)
	}
	if found, _ := got["found"].(bool); !found {
		t.Fatal("key missing immediately after put")
	}
	// Fixed iteration budget (~90s): no time.Now — determinism lint forbids it
	// outside /clock, and wall sleep is the point of the external-clock half.
	for i := 0; i < 45; i++ {
		got, err = s.Call(ctx, "kv.get", map[string]any{"namespace": "DATA", "key": "temp"})
		if err != nil {
			t.Fatal(err)
		}
		if found, _ := got["found"].(bool); !found {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("workerd did not expire key with expirationTtl=60 within ~90s")
}

func TestMiniflareD1R2Queue(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	worker := `export default { async fetch(req, env) {
		if (new URL(req.url).pathname === '/q') {
			await env.Q.send('via-worker');
			return new Response('queued');
		}
		return new Response('ok');
	}};`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
			"d1Databases": []string{"DB"}, "r2Buckets": []string{"BUCKET"},
			"queueProducers": []string{"Q"},
		}},
		"d1Databases": []string{"DB"}, "r2Buckets": []string{"BUCKET"},
		"queueProducers": []string{"Q"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Call(ctx, "d1.exec", map[string]any{
		"database": "DB", "sql": "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT);",
	}); err != nil {
		t.Fatalf("d1.exec: %v", err)
	}
	if _, err := s.Call(ctx, "d1.query", map[string]any{
		"database": "DB", "sql": "INSERT INTO t (v) VALUES (?)", "binds": []any{"hello"},
	}); err != nil {
		t.Fatalf("d1.insert: %v", err)
	}
	q, err := s.Call(ctx, "d1.query", map[string]any{
		"database": "DB", "sql": "SELECT v FROM t LIMIT 1",
	})
	if err != nil {
		t.Fatalf("d1.select: %v", err)
	}
	rows, _ := q["results"].([]any)
	if len(rows) != 1 {
		t.Fatalf("d1 rows %#v", q)
	}
	b64 := base64.StdEncoding.EncodeToString([]byte("r2-bytes"))
	if _, err := s.Call(ctx, "r2.put", map[string]any{
		"bucket": "BUCKET", "key": "obj", "value": b64,
	}); err != nil {
		t.Fatalf("r2.put: %v", err)
	}
	got, err := s.Call(ctx, "r2.get", map[string]any{"bucket": "BUCKET", "key": "obj"})
	if err != nil {
		t.Fatalf("r2.get: %v", err)
	}
	raw, _ := base64.StdEncoding.DecodeString(got["value"].(string))
	if string(raw) != "r2-bytes" {
		t.Fatalf("r2 %q", raw)
	}
	if _, err := s.Call(ctx, "queue.send", map[string]any{
		"queue": "Q", "body": "via-control",
	}); err != nil {
		t.Fatalf("queue.send: %v", err)
	}
	if _, err := s.Call(ctx, "worker.dispatch", map[string]any{"path": "/q"}); err != nil {
		t.Fatalf("queue worker: %v", err)
	}
}

func TestMiniflareWorkflows(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	worker := `
import { WorkflowEntrypoint } from "cloudflare:workers";
export class EchoWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    return await step.do("echo", async () => ({ ok: true, n: event.payload?.n ?? 0 }));
  }
}
export default {
  async fetch(req, env) {
    const inst = await env.ECHO.create({ params: { n: 7 } });
    return Response.json({ id: inst.id, status: typeof inst.status });
  }
};
`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
		}},
		"workflows": map[string]any{
			"ECHO": map[string]any{"name": "echo", "className": "EchoWorkflow"},
		},
	}); err != nil {
		t.Fatalf("apply workflows: %v", err)
	}
	resp, err := s.Call(ctx, "worker.dispatch", map[string]any{"path": "/"})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	body := decodeDispatch(t, resp)
	if !strings.Contains(body, `"id"`) {
		t.Fatalf("workflow create answer %q", body)
	}
}

func TestMiniflareReentrant(t *testing.T) {
	spec := helperSpec(t)
	ctx := context.Background()
	s, err := MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var mu sync.Mutex
	var mirrorHits int
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		mirrorHits++
		mu.Unlock()
		// Re-enter the same Miniflare session from "mirror" while Worker runs.
		b64 := base64.StdEncoding.EncodeToString([]byte("reentered"))
		if _, err := s.Call(r.Context(), "kv.put", map[string]any{
			"namespace": "DATA", "key": "re", "value": b64,
		}); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("mirrored"))
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	mirrorURL := "http://" + ln.Addr().String() + "/cb"

	worker := `export default { async fetch(req, env) {
		const r = await fetch("` + mirrorURL + `");
		const text = await r.text();
		const v = await env.DATA.get("re");
		return new Response(text + "|" + (v || "miss"));
	}};`
	if _, err := s.Apply(ctx, map[string]any{
		"workers": []any{map[string]any{
			"name": "app", "script": worker, "compatibilityDate": "2025-01-01",
			"kvNamespaces": []string{"DATA"},
		}},
		"kvNamespaces": []string{"DATA"},
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := s.Call(ctx, "worker.dispatch", map[string]any{"path": "/"})
	if err != nil {
		t.Fatalf("reentrant dispatch: %v", err)
	}
	if got := decodeDispatch(t, resp); got != "mirrored|reentered" {
		t.Fatalf("reentrant got %q", got)
	}
	mu.Lock()
	hits := mirrorHits
	mu.Unlock()
	if hits != 1 {
		t.Fatalf("mirror hits %d", hits)
	}
}
