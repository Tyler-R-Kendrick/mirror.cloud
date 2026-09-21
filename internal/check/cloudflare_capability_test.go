package check

import (
	"path/filepath"
	"testing"
)

var cloudflareNativePassIDs = []string{
	"CF-BOOT-NATIVE",
	"CF-ALIASES",
	"CF-TITLE",
	"CF-KV-BINARY",
	"CF-KV-TIME",
	"CF-KV-PAGE",
	"CF-DELETE",
	"CF-KV-MULTIPART",
}

var cloudflarePendingIDs = []string{
	"CF-KV-COHERENCE",
	"CF-WORKER-UPLOAD",
	"CF-DO-RESTART",
	"CF-CELLD-IDENTITY",
	"CF-OFFLINE",
	"CF-SNAPSHOT",
}

// TestCloudflareCapabilityManifest loads the Cloudflare capability ledger and
// asserts every case status is one of the allowed set; native cases already
// covered by tests stay pass with test name references; miniflare/celld cases
// stay not-run/unavailable (never pass).
func TestCloudflareCapabilityManifest(t *testing.T) {
	root := findMod(t)
	doc := loadJSON(t, filepath.Join(root, "docs", "cloudflare-capability.manifest.json"))
	rawCases, ok := doc["cases"].([]any)
	if !ok || len(rawCases) == 0 {
		t.Fatal("cases missing or empty")
	}

	allowed := map[string]bool{"pass": true, "fail": true, "unavailable": true, "not-run": true}
	byID := map[string]map[string]any{}
	for i, raw := range rawCases {
		c, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("cases[%d]: not an object", i)
		}
		id, _ := c["id"].(string)
		status, _ := c["status"].(string)
		if id == "" {
			t.Fatalf("cases[%d]: missing id", i)
		}
		if !allowed[status] {
			t.Fatalf("%s: status %q not in {pass,fail,unavailable,not-run}", id, status)
		}
		if _, dup := byID[id]; dup {
			t.Fatalf("duplicate case id %s", id)
		}
		byID[id] = c
	}

	for _, id := range cloudflareNativePassIDs {
		c, ok := byID[id]
		if !ok {
			t.Fatalf("missing native case %s", id)
		}
		if c["status"] != "pass" {
			t.Fatalf("%s: status=%v want pass", id, c["status"])
		}
		if c["backend"] != "native" {
			t.Fatalf("%s: backend=%v want native", id, c["backend"])
		}
		if c["evidence"] != "test/behavior/cloudflare" {
			t.Fatalf("%s: evidence=%v want test/behavior/cloudflare", id, c["evidence"])
		}
		tests, _ := c["tests"].([]any)
		if len(tests) == 0 {
			t.Fatalf("%s: pass case needs non-empty tests references", id)
		}
		for j, tr := range tests {
			s, _ := tr.(string)
			if s == "" {
				t.Fatalf("%s: tests[%d] empty", id, j)
			}
		}
	}

	const pendingReason = "miniflare/celld integration pending"
	for _, id := range cloudflarePendingIDs {
		c, ok := byID[id]
		if !ok {
			t.Fatalf("missing pending case %s", id)
		}
		status, _ := c["status"].(string)
		if status != "not-run" && status != "unavailable" {
			t.Fatalf("%s: status=%q want not-run or unavailable (do not claim miniflare pass)", id, status)
		}
		if c["reason"] != pendingReason {
			t.Fatalf("%s: reason=%v want %q", id, c["reason"], pendingReason)
		}
	}
}
