package cloudflare_test

import (
	"context"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestCloudflarePublicSurfaceWorkersD1R2QueuesWorkflows(t *testing.T) {
	deps := spitest.Deps(t)
	pack, err := bundled.New("cloudflare.api", deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "surf-acct", Region: "us-east-1"}
	invoke := func(op string, in map[string]any) map[string]any {
		t.Helper()
		resp, err := pack.Invoke(ctx, &spi.Request{
			ServiceID: "cloudflare.api", Operation: op, Input: in, Identity: id,
		})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp.Output
	}

	up := invoke("WorkerScriptUploadWorkerModule", map[string]any{
		"account_id": "surf-acct", "script_name": "hello",
		"body": "export default { fetch(){ return new Response('hi'); } };",
	})
	if r, _ := up["result"].(map[string]any); r == nil || r["id"] != "hello" {
		t.Fatalf("upload %#v", up)
	}
	listed := invoke("WorkerScriptListWorkers", map[string]any{"account_id": "surf-acct"})
	if items, _ := listed["result"].([]any); len(items) != 1 {
		t.Fatalf("list workers %#v", listed)
	}
	raw := invoke("WorkerScriptDownloadWorker", map[string]any{
		"account_id": "surf-acct", "script_name": "hello",
	})
	// _raw may land as top-level string or under a codec convention
	body := ""
	if s, ok := raw["_raw"].(string); ok {
		body = s
	} else if s, ok := raw["body"].(string); ok {
		body = s
	} else {
		for _, v := range raw {
			if s, ok := v.(string); ok && strings.Contains(s, "fetch") {
				body = s
			}
		}
	}
	if !strings.Contains(body, "fetch") {
		t.Fatalf("download %#v", raw)
	}

	db := invoke("D1CreateDatabase", map[string]any{
		"account_id": "surf-acct", "name": "app-db",
	})
	dbID, _ := db["result"].(map[string]any)["uuid"].(string)
	if dbID == "" {
		t.Fatalf("d1 create %#v", db)
	}
	invoke("D1QueryDatabase", map[string]any{
		"account_id": "surf-acct", "database_id": dbID,
		"body": []any{map[string]any{"sql": "SELECT 1"}},
	})

	invoke("R2CreateBucket", map[string]any{"account_id": "surf-acct", "name": "bucket1"})
	invoke("R2PutObject", map[string]any{
		"account_id": "surf-acct", "bucket_name": "bucket1", "object_key": "k", "body": "bytes",
	})
	obj := invoke("R2GetObject", map[string]any{
		"account_id": "surf-acct", "bucket_name": "bucket1", "object_key": "k",
	})
	_ = obj

	q := invoke("QueuesCreate", map[string]any{
		"account_id": "surf-acct", "queue_name": "q1",
	})
	qid, _ := q["result"].(map[string]any)["queue_id"].(string)
	invoke("QueuesPushMessage", map[string]any{
		"account_id": "surf-acct", "queue_id": qid,
	})

	invoke("WorCreateOrModifyWorkflow", map[string]any{
		"account_id": "surf-acct", "workflow_name": "wf1",
		"class_name": "Echo", "script_name": "hello",
	})
	wfs := invoke("WorListWorkflows", map[string]any{"account_id": "surf-acct"})
	if items, _ := wfs["result"].([]any); len(items) != 1 {
		t.Fatalf("workflows %#v", wfs)
	}

	invoke("DurableObjectsNamespaceListNamespaces", map[string]any{"account_id": "surf-acct"})
	invoke("WorkerScriptDeleteWorker", map[string]any{
		"account_id": "surf-acct", "script_name": "hello",
	})
}
