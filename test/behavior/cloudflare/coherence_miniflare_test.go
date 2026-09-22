//go:build miniflare

package cloudflare_test

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestCFKVCoherenceViaExecute(t *testing.T) {
	root := repoRoot(t)
	spec := execution.MiniflareStartSpec{
		NodeBin:       "node",
		HelperDir:     filepath.Join(root, "tools", "cloudflare-runtime"),
		ReadyDeadline: 0,
	}
	if err := execution.MiniflareAvailable(spec); err != nil {
		t.Fatalf("miniflare environment unavailable: %v", err)
	}
	ctx := context.Background()
	sess, err := execution.MiniflareStart(ctx, spec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })

	deps := spitest.Deps(t)
	reg, err := execution.NewRegistry(&execution.MiniflareBackend{Session: sess})
	if err != nil {
		t.Fatal(err)
	}
	deps.Executor = &execution.EnsuringMiniflare{
		Session: sess,
		Inner:   execution.RegistryExecutor{Reg: reg},
	}

	pack, err := bundled.New("cloudflare.api", deps)
	if err != nil {
		t.Fatal(err)
	}
	ident := spi.Identity{Account: "cohere-acct", Region: "us-east-1"}
	invoke := func(op string, in map[string]any) (map[string]any, error) {
		t.Helper()
		resp, err := pack.Invoke(ctx, &spi.Request{
			ServiceID: "cloudflare.api", Operation: op, Input: in, Identity: ident,
		})
		if err != nil {
			return nil, err
		}
		return resp.Output, nil
	}

	out, err := invoke("WorkersKvNamespaceCreateANamespace",
		map[string]any{"account_id": "cohere-acct", "title": "cohere"})
	if err != nil {
		t.Fatal(err)
	}
	ns := out["result"].(map[string]any)["id"].(string)


	if _, err := invoke("WorkersKvNamespaceWriteKeyValuePairWithMetadata", map[string]any{
		"account_id": "cohere-acct", "namespace_id": ns,
		"key_name": "from-rest", "body": "rest-bytes",
	}); err != nil {
		t.Fatalf("REST write: %v", err)
	}
	// EnsuringMiniflare Apply runs on first KV execute — Worker binding uses ns query.
	resp, err := sess.Call(ctx, "worker.dispatch", map[string]any{"path": "/?ns=" + ns + "&k=from-rest"})
	if err != nil {
		t.Fatalf("worker read: %v", err)
	}
	bodyB64, _ := resp["body"].(string)
	raw, err := base64.StdEncoding.DecodeString(bodyB64)
	if err != nil {
		t.Fatalf("body decode: %v %#v", err, resp)
	}
	if string(raw) != "v=rest-bytes" {
		t.Fatalf("worker read %q", raw)
	}

	if _, err := sess.Call(ctx, "worker.dispatch", map[string]any{
		"method": "PUT", "path": "/?ns=" + ns + "&k=from-worker",
		"body": base64.StdEncoding.EncodeToString([]byte("worker-bytes")),
	}); err != nil {
		t.Fatalf("worker write: %v", err)
	}
	gotOut, err := invoke("WorkersKvNamespaceReadKeyValuePair", map[string]any{
		"account_id": "cohere-acct", "namespace_id": ns, "key_name": "from-worker",
	})
	if err != nil {
		t.Fatalf("REST read: %v", err)
	}
	if got, _ := gotOut["_raw"].(string); got != "worker-bytes" {
		t.Fatalf("REST read %#v", gotOut)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
