package vercel

import (
	"net/http"
	"testing"
)

// TestVercelLegacyVersionsAreServed covers the addresses real clients call that
// the vendored document does not list. The document names each endpoint once,
// at its newest version; Vercel keeps answering the versions it superseded, and
// the clients written against them are not wrong. Before the aliases existed,
// POST /v9/projects was claimed by no model, fell through the demux to the S3
// path-style default, and answered NoSuchBucket for a bucket named "v9".
func TestVercelLegacyVersionsAreServed(t *testing.T) {
	v := newVX(t, "vercel-versions", "vercel.api", "aws.s3")

	t.Run("Given the three versions of project create When each is called Then one operation answers all of them", func(t *testing.T) {
		ids := map[string]string{}
		for _, version := range []string{"v9", "v10", "v11"} {
			r := v.do(http.MethodPost, "/"+version+"/projects", map[string]any{"name": "app-" + version})
			if r.status != 200 || str(r.body, "name") != "app-"+version || str(r.body, "id") == "" {
				t.Fatalf("POST /%s/projects = %d %s", version, r.status, r.raw)
			}
			ids[version] = str(r.body, "id")
		}
		// The same store behind all three: the project made through the oldest
		// address is the one the newest address's duplicate check sees.
		dup := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "app-v9"})
		if dup.status != http.StatusConflict {
			t.Fatalf("duplicate through another version = %d %s, want 409", dup.status, dup.raw)
		}
		got := v.do(http.MethodGet, "/v9/projects/app-v9", nil)
		if got.status != 200 || str(got.body, "id") != ids["v9"] {
			t.Fatalf("get %d %s", got.status, got.raw)
		}
	})

	t.Run("Given the CLI's team lookup When /v1/teams is called Then the teams listing answers", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v1/teams", nil)
		if r.status != 200 || r.body["teams"] == nil {
			t.Fatalf("GET /v1/teams = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given a version nobody declared When it is called Then it is not served as Vercel", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v8/projects", map[string]any{"name": "app-v8"})
		if r.status == 200 {
			t.Fatalf("an undeclared version created a project: %s", r.raw)
		}
	})
}
