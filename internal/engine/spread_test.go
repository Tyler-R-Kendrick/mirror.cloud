package engine_test

import (
	"strings"
	"testing"
	"testing/fstest"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A write that spreads the request stores the members the caller sent and only
// those. Both halves of that matter and they fail differently, so each has its
// own test: storing too little loses data the pack kept, and storing too much
// -- a null for every member the request omitted -- puts members into a later
// read that were never sent.

// TestSpreadStoresWhatTheRequestCarried is the first half. CodeDeploy's
// CreateDeployment keeps the whole request, so a description and a revision
// the caller sent come back from GetDeployment unchanged, nested values
// included.
func TestSpreadStoresWhatTheRequestCarried(t *testing.T) {
	p := served(t, "aws.codedeploy")
	rev := map[string]any{
		"revisionType": "S3",
		"s3Location":   map[string]any{"bucket": "b", "key": "k", "bundleType": "zip"},
	}
	id := invoke(t, p, "CreateDeployment", map[string]any{
		"applicationName":               "app",
		"deploymentGroupName":           "grp",
		"description":                   "kept",
		"ignoreApplicationStopFailures": true,
		"revision":                      rev,
	})["deploymentId"]

	got, _ := invoke(t, p, "GetDeployment", map[string]any{
		"deploymentId": id,
	})["deploymentInfo"].(map[string]any)
	if got == nil {
		t.Fatal("GetDeployment answered no deploymentInfo")
	}
	for k, want := range map[string]any{
		"description":         "kept",
		"deploymentGroupName": "grp",
		"applicationName":     "app",
	} {
		if got[k] != want {
			t.Errorf("deploymentInfo[%q] = %v, want %v", k, got[k], want)
		}
	}
	if got["ignoreApplicationStopFailures"] != true {
		t.Errorf("a non-string member did not survive the spread: %v",
			got["ignoreApplicationStopFailures"])
	}
	nested, _ := got["revision"].(map[string]any)
	if nested == nil {
		t.Fatalf("revision = %v, want the structure the request sent", got["revision"])
	}
	loc, _ := nested["s3Location"].(map[string]any)
	if loc == nil || loc["bucket"] != "b" {
		t.Errorf("s3Location = %v, want the nested structure intact", nested["s3Location"])
	}

	// The members the effect declares are there alongside the copied ones.
	// Which of the two wins a collision is TestDeclaredMembersWinOverTheSpread;
	// no member of these three bundles collides, so this is not that test.
	if got["status"] != "Succeeded" {
		t.Errorf("status = %v, want Succeeded", got["status"])
	}
	if got["deploymentId"] != id {
		t.Errorf("deploymentId = %v, want %v", got["deploymentId"], id)
	}
}

// TestSpreadStoresNothingTheRequestOmitted is the second half, and it is the
// reason the spread exists rather than an enumeration of the input shape. A
// bundle spelling out `'x' in input ? input.x : null` for each of the twelve
// members CreateDeploymentInput declares would store ten nulls for a request
// that sent two, and GetDeployment would answer them -- so a caller could not
// tell a member the request omitted from one it sent as null.
func TestSpreadStoresNothingTheRequestOmitted(t *testing.T) {
	p := served(t, "aws.codedeploy")
	id := invoke(t, p, "CreateDeployment", map[string]any{
		"applicationName": "bare",
	})["deploymentId"]

	got, _ := invoke(t, p, "GetDeployment", map[string]any{
		"deploymentId": id,
	})["deploymentInfo"].(map[string]any)
	if got == nil {
		t.Fatal("GetDeployment answered no deploymentInfo")
	}
	want := map[string]bool{"applicationName": true, "deploymentId": true, "status": true}
	for k := range got {
		if !want[k] {
			t.Errorf("deploymentInfo carries %q, which the request did not send", k)
		}
	}
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("deploymentInfo is missing %q", k)
		}
	}
}

// TestSpreadRefusesAnythingButInput keeps the one property that makes the copy
// safe. The request has been checked against the generated input shape by the
// time an effect runs, so a spread of it cannot store a member no SDK could
// send. Spreading anything else -- a read binding, say -- would copy a record
// nothing validated, so the loader refuses it rather than the engine ignoring
// it at request time.
func TestSpreadRefusesAnythingButInput(t *testing.T) {
	svc, err := generated.Model("aws.codedeploy")
	if err != nil {
		t.Fatal(err)
	}
	doc := `schema: bir/1
service: aws.codedeploy
resources:
  deployment:
    collection: cddep
    id:
      input_members: [deploymentId]
operations:
  CreateDeployment:
    effects:
      - create: { resource: deployment, spread: deployment }
    output:
      deploymentId: rec.deploymentId
`
	fsys := fstest.MapFS{"svc/service.yaml": &fstest.MapFile{Data: []byte(doc)}}
	if _, err := bir.Load(fsys, "svc", svc); err == nil {
		t.Fatal("a write spreading a read binding loaded without complaint")
	} else if !strings.Contains(err.Error(), "spread") {
		t.Fatalf("the complaint does not name the field: %v", err)
	}
}

// A bundle that stopped spreading would not fail to load and would not fail to
// answer -- it would answer a smaller record. So the field itself is pinned:
// removing it from a bundle has to be a visible change rather than a quiet
// loss of every member the request carried.
func TestBundlesThatSpreadStillDo(t *testing.T) {
	for _, want := range []struct{ service, operation string }{
		{"aws.codedeploy", "CreateDeployment"},
		{"aws.amplify", "CreateApp"},
		{"aws.amplify", "UpdateApp"},
	} {
		svc, err := generated.Model(want.service)
		if err != nil {
			t.Fatal(err)
		}
		ir, err := behaviors.Load(want.service, svc)
		if err != nil {
			t.Fatal(err)
		}
		spreads := false
		for _, eff := range ir.Operations[want.operation].Effects {
			for _, w := range []*bir.WriteEffect{eff.Create, eff.Put, eff.Patch} {
				if w != nil && w.Spread == "input" {
					spreads = true
				}
			}
		}
		if !spreads {
			t.Errorf("%s %s no longer spreads the request; every member the "+
				"caller sends is now dropped", want.service, want.operation)
		}
	}
}

// TestDeclaredMembersWinOverTheSpread pins the precedence rule, which no
// shipped bundle exercises: none of them declares a member the request can
// also carry. It is still the rule the schema states, and the reason the
// spread is usable at all -- "keep what the caller sent, but this member is
// mine" is what every pack doing this does, and a bundle relying on it would
// otherwise find out at request time that the caller had overwritten it.
func TestDeclaredMembersWinOverTheSpread(t *testing.T) {
	svc, err := generated.Model("aws.codedeploy")
	if err != nil {
		t.Fatal(err)
	}
	doc := `schema: bir/1
service: aws.codedeploy
provenance: authored
resources:
  deployment:
    collection: cddep
    id:
      input_members: [deploymentId]
    record:
      deploymentId: id
operations:
  CreateDeployment:
    effects:
      - generate: { bind: did, kind: hex, bytes: 8 }
      - create:
          resource: deployment
          key: "fx.did"
          spread: input
          record:
            description: "'declared'"
    output:
      deploymentId: rec.deploymentId
  GetDeployment:
    reads:
      deployment: { resource: deployment }
    output:
      deploymentInfo: deployment
`
	fsys := fstest.MapFS{"svc/service.yaml": &fstest.MapFile{Data: []byte(doc)}}
	ir, err := bir.Load(fsys, "svc", svc)
	if err != nil {
		t.Fatal(err)
	}
	p, err := engine.New(spitest.Deps(t), ir, svc)
	if err != nil {
		t.Fatal(err)
	}
	id := invoke(t, p, "CreateDeployment", map[string]any{
		"applicationName": "app",
		"description":     "sent by the caller",
	})["deploymentId"]

	got, _ := invoke(t, p, "GetDeployment", map[string]any{
		"deploymentId": id,
	})["deploymentInfo"].(map[string]any)
	if got["description"] != "declared" {
		t.Errorf("description = %v, want declared: the request overwrote a member "+
			"the effect declares", got["description"])
	}
	if got["applicationName"] != "app" {
		t.Errorf("applicationName = %v: the collision cost the members that did "+
			"not collide", got["applicationName"])
	}
}
