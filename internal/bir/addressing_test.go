package bir_test

import (
	"strings"
	"testing"
	"testing/fstest"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
)

// An operation that resolves a resource's key from the request must declare at
// least one of the members that resource is addressed by. Two extracted packs
// did not, and in both the wrong lookup produced an empty key rather than an
// error: every such call in the account wrote to one shared row, every call
// succeeded, and only a later describe showed nothing had moved.
//
// A hand-written pack had no declaration of what it was allowed to read, so
// nothing could have caught it. A bundle does, so this compares the two.

func load(t *testing.T, service, doc string) error {
	t.Helper()
	svc, err := generated.Model(service)
	if err != nil {
		t.Fatal(err)
	}
	fsys := fstest.MapFS{"svc/service.yaml": &fstest.MapFile{Data: []byte(doc)}}
	_, err = bir.Load(fsys, "svc", svc)
	return err
}

// TestAddressingByAnUndeclaredMemberIsRefused is the check itself. DeleteEndpoint
// declares EndpointArn and nothing else that names an endpoint, so a bundle
// keying on EndpointIdentifier would write to the empty key.
func TestAddressingByAnUndeclaredMemberIsRefused(t *testing.T) {
	err := load(t, "aws.dms", `schema: bir/1
service: aws.dms
provenance: authored
resources:
  dmsendpoint:
    collection: dmsep
    id:
      input_members: [EndpointIdentifier]
    record:
      EndpointIdentifier: id
operations:
  DeleteEndpoint:
    effects:
      - delete: { resource: dmsendpoint, missing: ignore }
`)
	if err == nil {
		t.Fatal("a bundle addressing a resource by a member the operation does not declare loaded")
	}
	for _, want := range []string{"dmsendpoint", "EndpointIdentifier", "addressing"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the complaint does not mention %q: %v", want, err)
		}
	}
}

// TestAddressingCheckReadsDeriveExpressions keeps the two spellings equal.
// `workspaces` expresses the same lookup as a derive rather than as
// input_members, and a check that looked only at input_members would pass it --
// which is how the identical defect would go on being merged in the spelling
// that reads as more deliberate.
func TestAddressingCheckReadsDeriveExpressions(t *testing.T) {
	err := load(t, "aws.workspaces", `schema: bir/1
service: aws.workspaces
provenance: authored
resources:
  workspace:
    collection: ws
    id:
      derive: >
        'WorkspaceId' in input ? string(input.WorkspaceId) : ''
    record:
      WorkspaceId: id
operations:
  StopWorkspaces:
    effects:
      - put:
          resource: workspace
          record:
            State: "'STOPPED'"
`)
	if err == nil {
		t.Fatal("a derive naming a member the operation does not declare loaded")
	}
	if !strings.Contains(err.Error(), "WorkspaceId") {
		t.Errorf("the complaint does not name the member the derive reads: %v", err)
	}
}

// TestAnExplicitKeyIsNotSecondGuessed keeps the check narrow. An effect with
// its own `key:` has said how it resolves the key, and that expression is
// compiled and scoped like any other -- there is nothing here to infer.
func TestAnExplicitKeyIsNotSecondGuessed(t *testing.T) {
	err := load(t, "aws.dms", `schema: bir/1
service: aws.dms
provenance: authored
resources:
  dmsendpoint:
    collection: dmsep
    id:
      input_members: [EndpointIdentifier]
    record:
      EndpointIdentifier: id
operations:
  DeleteEndpoint:
    effects:
      - delete:
          resource: dmsendpoint
          missing: ignore
          key: "string(input.EndpointArn)"
`)
	if err != nil {
		t.Fatalf("an effect with an explicit key was refused: %v", err)
	}
}

// TestAnExemptionNeedsAReason is what keeps the escape hatch from becoming the
// way through. The exemption exists to record a transcribed defect; one with
// nothing to say is indistinguishable from one added to make the check quiet.
func TestAnExemptionNeedsAReason(t *testing.T) {
	doc := `schema: bir/1
service: aws.dms
provenance: authored
resources:
  dmsendpoint:
    collection: dmsep
    id:
      input_members: [EndpointIdentifier]
    record:
      EndpointIdentifier: id
operations:
  DeleteEndpoint:
    addressing:
      dmsendpoint: %s
    effects:
      - delete: { resource: dmsendpoint, missing: ignore }
`
	if err := load(t, "aws.dms", strings.Replace(doc, "%s", `""`, 1)); err == nil {
		t.Error("an exemption with an empty reason loaded")
	} else if !strings.Contains(err.Error(), "no reason") {
		t.Errorf("the complaint does not say the reason is missing: %v", err)
	}

	reason := `"DeleteEndpointMessage declares only EndpointArn; transcribed, not corrected."`
	if err := load(t, "aws.dms", strings.Replace(doc, "%s", reason, 1)); err != nil {
		t.Errorf("an exemption with a reason was refused: %v", err)
	}
}

// TestExemptionsNameAResourceThatExists catches the exemption that stops
// exempting anything -- a resource rename would otherwise leave a stale entry
// silently covering nothing while the check it was written for fires again.
func TestExemptionsNameAResourceThatExists(t *testing.T) {
	err := load(t, "aws.dms", `schema: bir/1
service: aws.dms
provenance: authored
resources:
  dmsendpoint:
    collection: dmsep
    id:
      input_members: [EndpointArn]
    record:
      EndpointArn: id
operations:
  DeleteEndpoint:
    addressing:
      endpointNamedSomethingElse: "stale after a rename"
    effects:
      - delete: { resource: dmsendpoint, missing: ignore }
`)
	if err == nil || !strings.Contains(err.Error(), "unknown resource") {
		t.Fatalf("an exemption naming no resource loaded: %v", err)
	}
}

// TestTheTranscribedDefectsStayExempt pins the two bundles that carry this
// defect on purpose. If either stopped being exempt the recording would still
// pass -- the behavior is unchanged either way -- so what this defends is the
// statement that it is deliberate.
func TestTheTranscribedDefectsStayExempt(t *testing.T) {
	for _, want := range []struct{ service, operation, resource string }{
		{"aws.dms", "StartReplicationTask", "task"},
		{"aws.dms", "StopReplicationTask", "task"},
		{"aws.dms", "DeleteReplicationTask", "task"},
		{"aws.dms", "DeleteEndpoint", "dmsendpoint"},
		{"aws.dms", "DeleteReplicationInstance", "instance"},
		{"aws.workspaces", "StartWorkspaces", "workspace"},
		{"aws.workspaces", "StopWorkspaces", "workspace"},
		{"aws.workspaces", "RebootWorkspaces", "workspace"},
		{"aws.workspaces", "TerminateWorkspaces", "workspace"},
	} {
		svc, err := generated.Model(want.service)
		if err != nil {
			t.Fatal(err)
		}
		ir, err := behaviors.Load(want.service, svc)
		if err != nil {
			t.Fatal(err)
		}
		why := ir.Operations[want.operation].Addressing[want.resource]
		if strings.TrimSpace(why) == "" {
			t.Errorf("%s %s no longer records why it addresses %q by a member the "+
				"operation does not declare", want.service, want.operation, want.resource)
		}
	}
}
