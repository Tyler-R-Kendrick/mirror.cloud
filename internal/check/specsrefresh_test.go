package check

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestSpecsRefreshScopesByName exercises the predicate that decides which
// upstreams a sync refetches.
//
// It is tested here, from Go, because that is what CI runs. The predicate is
// eleven lines of shell in a script nothing else covers, and the cost of it
// being wrong is not a failed build: a refresh that is too broad SUCCEEDS, and
// lands a pull request carrying upstream drift nobody asked for. That happened
// once already -- refreshing one service to pick up a schema change re-pinned
// AWS and rewrote twenty-nine unrelated models, and every check agreed, because
// the models were regenerated consistently from the documents that had moved.
//
// The predicate is lifted out of the script rather than reimplemented, so this
// tests the shell that actually runs rather than a Go paraphrase of it.
func TestSpecsRefreshScopesByName(t *testing.T) {
	script := filepath.Join("..", "..", "scripts", "specs-sync.sh")
	body, err := os.ReadFile(script)
	if err != nil {
		t.Fatalf("read %s: %v", script, err)
	}
	fn := extractShellFunc(string(body), "refreshes")
	if fn == "" {
		t.Fatal("scripts/specs-sync.sh no longer defines refreshes(); this test lifts it out by name")
	}

	for _, tc := range []struct {
		refresh string
		id      string
		want    bool
	}{
		// Unset reproduces the lock. This is the default and the whole reason
		// the lock is trustworthy as a build input.
		{"", "aws", false},
		{"", "railway.graphql", false},

		// Both spellings of "everything", which is what a deliberate vendor
		// sweep wants.
		{"1", "aws", true},
		{"1", "railway.graphql", true},
		{"all", "aws", true},
		{"all", "railway.graphql", true},

		// One name refreshes exactly that one.
		{"railway.graphql", "railway.graphql", true},
		{"railway.graphql", "aws", false},
		{"railway.graphql", "gcp.storage", false},

		// `aws` is the pin -- one commit covering every aws.* service -- and it
		// does not reach the URL-served documents.
		{"aws", "aws", true},
		{"aws", "railway.graphql", false},

		// A list refreshes each member and nothing else.
		{"aws,gcp.storage", "aws", true},
		{"aws,gcp.storage", "gcp.storage", true},
		{"aws,gcp.storage", "vercel.api", false},

		// A prefix is NOT a match. Substring matching is the defect this
		// repository has now fixed twice in routing, and a refresh that
		// matched prefixes would quietly widen itself -- `vercel` taking
		// vercel.kv along with vercel.api is the same mistake wearing
		// different clothes.
		{"vercel", "vercel.api", false},
		{"vercel.api", "vercel.kv", false},
		{"railway", "railway.graphql", false},
	} {
		t.Run(tc.refresh+"/"+tc.id, func(t *testing.T) {
			got := runsRefresh(t, fn, tc.refresh, tc.id)
			if got != tc.want {
				t.Errorf("SPECS_REFRESH=%q refreshes %s = %v, want %v", tc.refresh, tc.id, got, tc.want)
			}
		})
	}
}

// extractShellFunc returns a shell function's text, from `name() {` to the
// closing brace in the first column.
func extractShellFunc(body, name string) string {
	start := strings.Index(body, name+"() {")
	if start < 0 {
		return ""
	}
	end := strings.Index(body[start:], "\n}\n")
	if end < 0 {
		return ""
	}
	return body[start : start+end+3]
}

// runsRefresh asks the lifted predicate about one id under one setting.
func runsRefresh(t *testing.T, fn, refresh, id string) bool {
	t.Helper()
	script := fn + "\nREFRESH=\"$1\"\nif refreshes \"$2\"; then echo yes; else echo no; fi\n"
	out, err := exec.Command("bash", "-c", script, "bash", refresh, id).Output()
	if err != nil {
		t.Fatalf("running the predicate: %v", err)
	}
	switch answer := strings.TrimSpace(string(out)); answer {
	case "yes":
		return true
	case "no":
		return false
	default:
		t.Fatalf("predicate answered %q, which is neither yes nor no", answer)
		return false
	}
}
