package capability_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/capability"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// fakeBackend is a tiny in-memory CapabilitySession. Two instances with
// different names exercise the same Dispatcher; no Cloudflare branch.
type fakeBackend struct {
	name    string
	version string
	caps    []spi.CapabilityDescriptor
	store   map[string]string // key = account/kind/id → value
}

func (f *fakeBackend) Describe(context.Context) (spi.BackendIdentity, []spi.CapabilityDescriptor, error) {
	out := make([]spi.CapabilityDescriptor, len(f.caps))
	copy(out, f.caps)
	return spi.BackendIdentity{Name: f.name, Version: f.version}, out, nil
}

func (f *fakeBackend) Call(_ context.Context, req spi.CapabilityCall) (*spi.CapabilityResult, error) {
	key := req.Resource.Account + "/" + req.Resource.Kind + "/" + req.Resource.ID
	switch req.Action {
	case "kv.put":
		v, _ := req.Args["value"].(string)
		if f.store == nil {
			f.store = map[string]string{}
		}
		f.store[key] = v
		return &spi.CapabilityResult{Output: map[string]any{"ok": true, "backend": f.name}}, nil
	case "kv.get":
		v, ok := f.store[key]
		if !ok {
			return nil, &spi.CapabilityError{Kind: spi.CapErrAbsent, Capability: "kv.get", Message: "missing"}
		}
		return &spi.CapabilityResult{Output: map[string]any{"value": v, "backend": f.name}}, nil
	default:
		return nil, &spi.CapabilityError{Kind: spi.CapErrUnsupported, Capability: req.Action}
	}
}

func kvCaps(req string) []spi.CapabilityDescriptor {
	return []spi.CapabilityDescriptor{
		{
			Name:               "kv.put",
			InputSchema:        "tag:kv.put.in",
			OutputSchema:       "tag:kv.put.out",
			ResourceKind:       "kv_namespace",
			BodyMode:           spi.BodyNone,
			FailureClass:       "kv",
			EffectClass:        spi.EffectWrite,
			BackendRequirement: req,
		},
		{
			Name:               "kv.get",
			InputSchema:        "tag:kv.get.in",
			OutputSchema:       "tag:kv.get.out",
			ResourceKind:       "kv_namespace",
			BodyMode:           spi.BodyNone,
			FailureClass:       "kv",
			EffectClass:        spi.EffectRead,
			BackendRequirement: req,
		},
	}
}

func TestDispatcher_TwoFakeBackendsSameContract(t *testing.T) {
	ctx := context.Background()
	backends := []*fakeBackend{
		{name: "fake-alpha", version: "1", caps: kvCaps("fake-alpha>=1")},
		{name: "fake-beta", version: "2", caps: kvCaps("fake-beta>=2")},
	}
	for _, b := range backends {
		d := &capability.Dispatcher{Session: b}
		id, caps, err := d.Describe(ctx)
		if err != nil {
			t.Fatalf("%s describe: %v", b.name, err)
		}
		if id.Name != b.name || id.Version != b.version {
			t.Fatalf("identity: got %+v want %s/%s", id, b.name, b.version)
		}
		if len(caps) != 2 {
			t.Fatalf("%s caps: got %d", b.name, len(caps))
		}
		ref := spi.ResourceRef{Account: "acct", Kind: "kv_namespace", ID: "ns1"}
		_, err = d.Call(ctx, spi.CapabilityCall{
			Resource: ref,
			Action:   "kv.put",
			Args:     map[string]any{"value": "hello-" + b.name},
		})
		if err != nil {
			t.Fatalf("%s put: %v", b.name, err)
		}
		res, err := d.Call(ctx, spi.CapabilityCall{Resource: ref, Action: "kv.get"})
		if err != nil {
			t.Fatalf("%s get: %v", b.name, err)
		}
		if res.Output["value"] != "hello-"+b.name {
			t.Fatalf("%s value: %#v", b.name, res.Output)
		}
		if res.Output["backend"] != b.name {
			t.Fatalf("%s backend tag: %#v", b.name, res.Output["backend"])
		}
	}
}

func TestDispatcher_UnsupportedAction(t *testing.T) {
	d := &capability.Dispatcher{Session: &fakeBackend{
		name: "fake-alpha", version: "1", caps: kvCaps("fake-alpha>=1"),
	}}
	_, err := d.Call(context.Background(), spi.CapabilityCall{
		Resource: spi.ResourceRef{Kind: "kv_namespace", ID: "x"},
		Action:   "kv.delete",
	})
	ce, ok := err.(*spi.CapabilityError)
	if !ok || ce.Kind != spi.CapErrUnsupported {
		t.Fatalf("want unsupported, got %T %v", err, err)
	}
}

func TestDispatcher_ResourceKindMismatch(t *testing.T) {
	d := &capability.Dispatcher{Session: &fakeBackend{
		name: "fake-beta", version: "2", caps: kvCaps("fake-beta>=2"),
	}}
	_, err := d.Call(context.Background(), spi.CapabilityCall{
		Resource: spi.ResourceRef{Kind: "r2_bucket", ID: "b"},
		Action:   "kv.get",
	})
	ce, ok := err.(*spi.CapabilityError)
	if !ok || ce.Kind != spi.CapErrValidation {
		t.Fatalf("want validation, got %T %v", err, err)
	}
}

func TestDispatcher_SchemaFingerprintMismatch(t *testing.T) {
	// Helper claims kv.put but with a different input schema than the peer.
	alpha := &fakeBackend{name: "fake-alpha", version: "1", caps: kvCaps("fake-alpha>=1")}
	betaCaps := kvCaps("fake-beta>=2")
	betaCaps[0].InputSchema = "tag:kv.put.in.v2" // deliberate diverge
	beta := &fakeBackend{name: "fake-beta", version: "2", caps: betaCaps}

	da := &capability.Dispatcher{Session: alpha}
	db := &capability.Dispatcher{Session: beta}
	_, aCaps, err := da.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, bCaps, err := db.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if aCaps[0].InputSchema == bCaps[0].InputSchema {
		t.Fatal("expected schema fingerprint diverge for mismatch check")
	}
}

func TestCapabilityErrorKinds(t *testing.T) {
	kinds := []spi.CapabilityErrorKind{
		spi.CapErrValidation,
		spi.CapErrUnsupported,
		spi.CapErrUnavailable,
		spi.CapErrAbsent,
		spi.CapErrConflict,
		spi.CapErrOutcomeUnknown,
	}
	for _, k := range kinds {
		e := &spi.CapabilityError{Kind: k, Message: "x"}
		if e.Error() == "" {
			t.Fatalf("empty error for %s", k)
		}
	}
}
