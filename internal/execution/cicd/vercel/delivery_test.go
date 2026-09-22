package vercel_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// VC-DELIVERY: CreateDeployment with prebuilt static files → READY → promote
// isolates production pointer; bad deploy + RequestRollback restores prior id;
// bytes verified via mirror-owned StaticSite (B-IR does not retain bodies).
func TestVC_DELIVERY(t *testing.T) {
	deps := spitest.Deps(t)
	clk, ok := deps.Clock.(*clock.Controllable)
	if !ok {
		t.Fatal("want controllable clock")
	}
	pack, err := bundled.New("vercel.api", deps)
	if err != nil {
		t.Fatal(err)
	}
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	call := func(op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := pack.Invoke(ctx, &spi.Request{
			ServiceID: "vercel.api", Identity: id, Operation: op, Input: in,
		})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}

	previewNonce := "nonce-preview-" + t.Name()
	prodNonce := "nonce-prod-" + t.Name()

	prj := call("CreateProject", map[string]any{"name": "vc-delivery"})
	pid, _ := prj["id"].(string)
	if pid == "" {
		t.Fatalf("CreateProject %#v", prj)
	}

	previewBody := []byte(previewNonce)
	preview := call("CreateDeployment", map[string]any{
		"name":    "vc-delivery",
		"project": "vc-delivery",
		"files": []any{
			map[string]any{
				"file": "index.html",
				"sha":  sha256Hex(previewBody),
				"size": len(previewBody),
			},
		},
	})
	previewID, _ := preview["id"].(string)
	if previewID == "" || preview["readyState"] != "QUEUED" {
		t.Fatalf("preview deploy %#v", preview)
	}
	if err := clk.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	got := call("GetDeployment", map[string]any{"idOrUrl": previewID})
	if got["readyState"] != "READY" {
		t.Fatalf("preview want READY, got %#v", got)
	}

	prodBody := []byte(prodNonce)
	prod := call("CreateDeployment", map[string]any{
		"name":    "vc-delivery",
		"project": "vc-delivery",
		"files": []any{
			map[string]any{
				"file": "index.html",
				"sha":  sha256Hex(prodBody),
				"size": len(prodBody),
			},
		},
	})
	prodID, _ := prod["id"].(string)
	if prodID == "" {
		t.Fatalf("prod deploy %#v", prod)
	}
	if err := clk.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	got = call("GetDeployment", map[string]any{"idOrUrl": prodID})
	if got["readyState"] != "READY" {
		t.Fatalf("prod want READY, got %#v", got)
	}

	call("RequestPromote", map[string]any{
		"projectId":    pid,
		"deploymentId": prodID,
	})
	if got := promotedDeploymentID(t, call, pid); got != prodID {
		t.Fatalf("after promote want production=%s got %s", prodID, got)
	}

	// Bad/new deployment promoted, then rollback to prior production id.
	badNonce := "nonce-bad-" + t.Name()
	badBody := []byte(badNonce)
	bad := call("CreateDeployment", map[string]any{
		"name":    "vc-delivery",
		"project": "vc-delivery",
		"files": []any{
			map[string]any{
				"file": "index.html",
				"sha":  sha256Hex(badBody),
				"size": len(badBody),
			},
		},
	})
	badID, _ := bad["id"].(string)
	if badID == "" {
		t.Fatalf("bad deploy %#v", bad)
	}
	if err := clk.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if call("GetDeployment", map[string]any{"idOrUrl": badID})["readyState"] != "READY" {
		t.Fatalf("bad want READY")
	}
	call("RequestPromote", map[string]any{"projectId": pid, "deploymentId": badID})
	if got := promotedDeploymentID(t, call, pid); got != badID {
		t.Fatalf("after bad promote want %s got %s", badID, got)
	}
	call("RequestRollback", map[string]any{"projectId": pid, "deploymentId": prodID})
	call("UpdateProjectsByProjectIdRollbackByDeploymentIdUpdateDescription", map[string]any{
		"projectId": pid, "deploymentId": prodID, "description": "rollback-to-prior-prod",
	})
	if got := promotedDeploymentID(t, call, pid); got != prodID {
		t.Fatalf("after rollback want production=%s got %s (bad=%s)", prodID, got, badID)
	}

	// Bytes: GetDeploymentFileContents retains no body — publish same prebuilt output locally.
	previewSite, err := delivery.Publish(map[string][]byte{"index.html": previewBody})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = previewSite.Close() })
	prodSite, err := delivery.Publish(map[string][]byte{"index.html": prodBody})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prodSite.Close() })
	badSite, err := delivery.Publish(map[string][]byte{"index.html": badBody})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = badSite.Close() })

	if err := previewSite.VerifyNonce("index.html", previewNonce); err != nil {
		t.Fatal(err)
	}
	if err := prodSite.VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal(err)
	}
	if err := badSite.VerifyNonce("index.html", badNonce); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(previewSite.Root, "index.html", prodNonce); err == nil {
		t.Fatal("preview root leaked production nonce")
	}
	if err := delivery.VerifyNonceFile(prodSite.Root, "index.html", previewNonce); err == nil {
		t.Fatal("production root leaked preview nonce")
	}
	if err := delivery.VerifyNonceFile(prodSite.Root, "index.html", badNonce); err == nil {
		t.Fatal("production root leaked bad nonce")
	}
}

func promotedDeploymentID(t *testing.T, call func(string, map[string]any) map[string]any, projectID string) string {
	t.Helper()
	aliases := call("ListPromoteAliases", map[string]any{"projectId": projectID})
	var promoted string
	n := 0
	for _, row := range asSlice(aliases["aliases"]) {
		m, _ := row.(map[string]any)
		if m["status"] != "PROMOTED" {
			continue
		}
		n++
		als, _ := m["alias"].(string)
		if als == "" {
			als, _ = m["id"].(string)
		}
		got := call("GetAlias", map[string]any{"idOrAlias": als})
		promoted, _ = got["deploymentId"].(string)
	}
	if n != 1 || promoted == "" {
		t.Fatalf("want one PROMOTED with deploymentId, got n=%d id=%q aliases=%#v", n, promoted, aliases)
	}
	return promoted
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}
