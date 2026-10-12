package vercel

import (
	"net/http"
	"testing"
)

// TestVercelThePersonalAccountIsAlsoAddressedByTheUsersOwnId holds what the
// CLI relies on after it links a directory. It writes the user's id as the org
// of the linked project (`orgId` in .vercel/project.json), and `vercel ls` sends
// that back as ?teamId= even for a personal account. Treating the id as the name
// of an empty team listed none of what the caller had just deployed.
func TestVercelThePersonalAccountIsAlsoAddressedByTheUsersOwnId(t *testing.T) {
	v := newVX(t, "vercel-personal")
	me := v.do(http.MethodGet, "/v2/user", nil)
	user, _ := me.body["user"].(map[string]any)
	uid := str(user, "id")
	if me.status != 200 || uid == "" {
		t.Fatalf("GET /v2/user = %d %s", me.status, me.raw)
	}
	if r := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "mine"}); r.status != 200 {
		t.Fatalf("create = %d %s", r.status, r.raw)
	}

	for _, scope := range []string{"", "?teamId=" + uid} {
		if r := v.do(http.MethodGet, "/v9/projects/mine"+scope, nil); r.status != 200 {
			t.Errorf("GET /v9/projects/mine%s = %d %s, want the caller's own project", scope, r.status, r.raw)
		}
		list := v.do(http.MethodGet, "/v10/projects"+scope, nil)
		if projects, _ := list.body["projects"].([]any); len(projects) != 1 {
			t.Errorf("GET /v10/projects%s lists %d projects, want 1: %s", scope, len(projects), list.raw)
		}
	}

	// Only the caller's own id is the personal account. Another id is still
	// another account, which has nothing of the caller's.
	if r := v.do(http.MethodGet, "/v9/projects/mine?teamId=usr_somebody_else", nil); r.status != 404 {
		t.Errorf("another account's id = %d %s, want 404", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v9/projects/mine?teamId=team_other", nil); r.status != 404 {
		t.Errorf("another team's id = %d %s, want 404", r.status, r.raw)
	}
}
