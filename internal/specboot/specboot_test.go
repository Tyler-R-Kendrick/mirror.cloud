package specboot

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// TestBundleServesEveryGeneratedService checks the shape of the served model:
// every generated service is in it, under its own ID or the wire name
// servedAs gives it and never both, GCS among them, and the three services no
// specification describes are there with exactly the operations their packs
// serve.
func TestBundleServesEveryGeneratedService(t *testing.T) {
	b := Bundle()
	wire := map[string]string{}
	for served, spec := range servedAs {
		wire[spec] = served
	}
	for _, id := range generated.ServiceIDs() {
		self := b.ServiceByID(id) != nil
		alias, renamed := wire[id]
		switch {
		case !renamed && !self:
			t.Errorf("%s is generated and not served", id)
		case renamed && b.ServiceByID(alias) == nil:
			t.Errorf("%s is generated and its wire name %s is not served", id, alias)
		case renamed && self != servedAlsoAsSpec[id]:
			t.Errorf("%s served under its own ID = %v beside %s; servedAlsoAsSpec says %v", id, self, alias, servedAlsoAsSpec[id])
		}
	}
	gcs := b.ServiceByID("gcp.storage")
	if gcs == nil || gcs.Protocol != model.ProtoGCPRESTSON {
		t.Fatalf("gcp.storage = %v", gcs)
	}
	for _, u := range unmodeled() {
		got := b.ServiceByID(u.ID)
		if got == nil || len(got.Operations) != len(u.Operations) {
			t.Errorf("%s: served %v, want %d operations", u.ID, got, len(u.Operations))
		}
	}
	for i := 1; i < len(b.Services); i++ {
		if b.Services[i-1].ID >= b.Services[i].ID {
			t.Fatalf("services are not sorted by ID at %s", b.Services[i].ID)
		}
	}
}

// TestUnmodeledOperationsSurvive: a pack may serve an operation the vendored
// specification does not carry, and replacing the operation list rather than
// unioning it would take that operation away -- silently, since a missing
// operation answers as a 501 rather than a failure.
func TestUnmodeledOperationsSurvive(t *testing.T) {
	b := Bundle()
	for id, names := range unmodeledOps {
		svc := b.ServiceByID(id)
		if svc == nil {
			t.Fatalf("%s missing", id)
		}
		for _, n := range names {
			if svc.OperationByName(n) == nil {
				t.Errorf("%s.%s missing -- unmodeled operations must survive the union", id, n)
			}
		}
	}
}
