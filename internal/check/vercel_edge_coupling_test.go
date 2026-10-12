package check

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
)

// The Vercel edge turns ?slug= into a team by reading the slug index CreateTeam
// writes, out of the store, by name. That is a coupling nothing declares, in the
// shape internal/check/coupling_test.go describes for CloudControl, and it fails
// the same quiet way: rename the collection in the bundle, or stop writing the
// member, and every ?slug= request answers 403 while every test of the bundle and
// of the edge, each in its own terms, passes.
//
// These tests state the coupling so the rename that would break it fails here.
func TestVercelEdgeReadsTheSlugIndexTheBundleWrites(t *testing.T) {
	root := findMod(t)
	src, err := os.ReadFile(filepath.Join(root, "internal", "edge", "vercel.go"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`vercelTeamSlugs\s*=\s*"([^"]+)"`).FindSubmatch(src)
	if m == nil {
		t.Fatal("internal/edge/vercel.go no longer names vercelTeamSlugs; if the edge stopped resolving ?slug= through the store, delete this test")
	}
	collection := string(m[1])

	svc := loadGenerated(t)["vercel.api"]
	if svc == nil {
		t.Fatal("no generated model for vercel.api")
	}
	bundle, err := behaviors.Load("vercel.api", svc)
	if err != nil {
		t.Fatal(err)
	}

	res, ok := bundle.Resources["team_slug"]
	if !ok {
		t.Fatal("the bundle has no team_slug resource for the edge to read")
	}
	if res.Collection != collection {
		t.Errorf("the edge reads collection %q and the bundle's team_slug lives in %q", collection, res.Collection)
	}
	if !res.Global {
		t.Error("team_slug is not global: the edge reads the shared scope, so the index has to be written there")
	}
	wrote := false
	for _, eff := range bundle.Operations["CreateTeam"].Effects {
		if eff.Put != nil && eff.Put.Resource == "team_slug" {
			_, wrote = eff.Put.Record["teamId"]
		}
	}
	if !wrote {
		t.Error("CreateTeam does not write a teamId member into team_slug, which is what the edge resolves a slug to")
	}
}
