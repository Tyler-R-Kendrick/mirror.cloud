package discovery

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestIngestStorage(t *testing.T) {
	src := `{
	  "name": "storage",
	  "version": "v1",
	  "discoveryVersion": "v1",
	  "resources": {
	    "buckets": {
	      "methods": {
	        "insert": {"id": "storage.buckets.insert", "httpMethod": "POST", "path": "b"}
	      }
	    }
	  }
	}`
	svcs, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "storage.json"}, []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if len(svcs) != 1 || svcs[0].ID != "gcp.storage" {
		t.Fatalf("%+v", svcs)
	}
	if len(svcs[0].Operations) != 1 {
		t.Fatalf("ops %+v", svcs[0].Operations)
	}
}

// The receiver used to stop at a method's name, verb and path. Everything
// below is what it stopped short of, and each case is something a Behavior IR
// bundle cannot be written without: an operation needs an output shape before
// any output member of it can be checked, and a member needs a shape that
// exists before anything can be said about its type.

const storageDoc = `{
  "name": "storage",
  "version": "v1",
  "discoveryVersion": "v1",
  "parameters": {
    "alt": {"type": "string", "location": "query", "enum": ["json", "media"]},
    "prettyPrint": {"type": "boolean", "location": "query"}
  },
  "resources": {
    "buckets": {
      "methods": {
        "insert": {
          "id": "storage.buckets.insert", "httpMethod": "POST", "path": "b",
          "parameters": {
            "project": {"type": "string", "location": "query", "required": true},
            "projection": {"type": "string", "location": "query", "enum": ["full", "noAcl"]}
          },
          "request": {"$ref": "Bucket"},
          "response": {"$ref": "Bucket"}
        },
        "get": {
          "id": "storage.buckets.get", "httpMethod": "GET", "path": "b/{bucket}",
          "parameters": {"bucket": {"type": "string", "location": "path", "required": true}},
          "response": {"$ref": "Bucket"}
        },
        "delete": {
          "id": "storage.buckets.delete", "httpMethod": "DELETE", "path": "b/{bucket}",
          "parameters": {"bucket": {"type": "string", "location": "path", "required": true}}
        }
      }
    }
  },
  "schemas": {
    "Bucket": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "metageneration": {"type": "string", "format": "int64"},
        "acl": {"type": "array", "items": {"$ref": "BucketAccessControl"}},
        "billing": {"type": "object", "properties": {"requesterPays": {"type": "boolean"}}},
        "labels": {"type": "object", "additionalProperties": {"type": "string"}},
        "retentionPolicy": {
          "type": "object",
          "properties": {"retentionPeriod": {"type": "string", "format": "int64", "minimum": "0", "maximum": "3155760000"}}
        }
      }
    },
    "BucketAccessControl": {"type": "object", "properties": {"role": {"type": "string"}}}
  }
}`

func ingest(t *testing.T) *model.Service {
	t.Helper()
	svcs, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "storage.json"}, []byte(storageDoc))
	if err != nil {
		t.Fatal(err)
	}
	if len(svcs) != 1 {
		t.Fatalf("%d services", len(svcs))
	}
	return &svcs[0]
}

func op(t *testing.T, svc *model.Service, name string) model.Operation {
	t.Helper()
	for _, o := range svc.Operations {
		if o.Name == name {
			return o
		}
	}
	t.Fatalf("no operation %s", name)
	return model.Operation{}
}

// TestEveryOperationHasShapes is the property the whole change exists for. An
// operation with no output shape can serve only from hand-written Go: the
// Behavior IR loader validates each output member against that shape and
// refuses a bundle that names one when there is none.
func TestEveryOperationHasShapes(t *testing.T) {
	svc := ingest(t)
	for _, o := range svc.Operations {
		if o.Input == "" {
			t.Errorf("%s has no input shape", o.Name)
		}
		if o.Output == "" {
			t.Errorf("%s has no output shape", o.Name)
		}
		if _, ok := svc.Shapes[o.Input]; o.Input != "" && !ok {
			t.Errorf("%s names input shape %q, which is not in the model", o.Name, o.Input)
		}
		if _, ok := svc.Shapes[o.Output]; o.Output != "" && !ok {
			t.Errorf("%s names output shape %q, which is not in the model", o.Name, o.Output)
		}
	}
}

// TestNoShapeRefersToOneThatIsNotThere is the other half, and the defect that
// made the model look populated while carrying nothing usable: a member's
// shape was set to the member's own *name*, so `Bucket.acl` pointed at a shape
// called `acl`, and every member of every schema dangled.
func TestNoShapeRefersToOneThatIsNotThere(t *testing.T) {
	svc := ingest(t)
	check := func(from, ref string) {
		if ref == "" {
			return
		}
		if _, ok := svc.Shapes[ref]; !ok {
			t.Errorf("%s refers to %q, which is not in the model", from, ref)
		}
	}
	for id, sh := range svc.Shapes {
		for name, m := range sh.Members {
			check(id+"."+name, m.Shape)
		}
		check(id+" (member)", sh.Member)
		if sh.Kind == model.KindMap {
			check(id+" (key)", sh.Key)
		}
	}
}

// TestARequestCarriesParametersAndBody pins the flattening. The codec presents
// the global parameters, the method's own parameters and the body's members to
// a service as one map, so the request shape describes one structure -- and a
// bundle addressing a record by `bucket` or by `name` needs both halves to be
// declared or the loader will not accept it.
func TestARequestCarriesParametersAndBody(t *testing.T) {
	svc := ingest(t)
	in := svc.Shapes[op(t, svc, "storage.buckets.insert").Input]
	for _, want := range []string{
		"alt",        // document-level parameter
		"project",    // method parameter
		"projection", // method parameter
		"name",       // body member
		"acl",        // body member
	} {
		if _, ok := in.Members[want]; !ok {
			t.Errorf("the insert request does not declare %q", want)
		}
	}
	if !in.Members["project"].Required {
		t.Error("project is required and the request shape does not say so")
	}
	if in.Members["name"].Required {
		t.Error("a body member was marked required; Discovery states required on parameters only")
	}
	if got := in.Members["bucket"].Shape; got != "" {
		t.Errorf("the insert request declares a bucket parameter it does not have: %q", got)
	}
}

// TestParameterPlacementIsCarried keeps the path and query split, which is the
// only thing that says which members address the record and which modify the
// request.
func TestParameterPlacementIsCarried(t *testing.T) {
	svc := ingest(t)
	in := svc.Shapes[op(t, svc, "storage.buckets.get").Input]
	if got := in.Members["bucket"].Binding.Location; got != "label" {
		t.Errorf("bucket is a path parameter and carries location %q", got)
	}
	if got := in.Members["alt"].Binding.Location; got != "query" {
		t.Errorf("alt is a query parameter and carries location %q", got)
	}
}

// TestAMethodWithNoResponseGetsAnEmptyOne. A delete answers with nothing, and
// no shape at all is not the same as an empty one: a bundle that names an
// output member on it should be told the shape has no such member, which an
// operation with no output shape cannot say.
func TestAMethodWithNoResponseGetsAnEmptyOne(t *testing.T) {
	svc := ingest(t)
	out := op(t, svc, "storage.buckets.delete").Output
	shape, ok := svc.Shapes[out]
	if !ok {
		t.Fatalf("the delete names output shape %q, which is not in the model", out)
	}
	if len(shape.Members) != 0 {
		t.Errorf("the delete answers with %d members", len(shape.Members))
	}
	if shape.Kind != model.KindStructure {
		t.Errorf("the delete's output shape is a %s", shape.Kind)
	}
}

// TestNestedSchemasAreNamedForWhereTheySit. Discovery nests objects, arrays
// and maps inline without naming them, and a member refers to a shape by ID,
// so each nesting level needs one.
func TestNestedSchemasAreNamedForWhereTheySit(t *testing.T) {
	svc := ingest(t)
	bucket := svc.Shapes["Bucket"]

	if got := bucket.Members["acl"].Shape; got != "Bucket.acl" {
		t.Errorf("acl is a list and its shape is %q", got)
	}
	if list := svc.Shapes["Bucket.acl"]; list.Kind != model.KindList || list.Member != "BucketAccessControl" {
		t.Errorf("Bucket.acl is %s of %q, want a list of BucketAccessControl", list.Kind, list.Member)
	}
	if inline := svc.Shapes["Bucket.billing"]; inline.Kind != model.KindStructure {
		t.Errorf("Bucket.billing is %s, want a structure", inline.Kind)
	} else if _, ok := inline.Members["requesterPays"]; !ok {
		t.Error("Bucket.billing lost its members")
	}
	if labels := svc.Shapes["Bucket.labels"]; labels.Kind != model.KindMap {
		t.Errorf("Bucket.labels is %s, want a map", labels.Kind)
	}
}

// TestScalarsShareAShapeUnlessTheySaySomething. A scalar stated inline at a
// thousand sites should not become a thousand shapes; one that enumerates its
// values or states a bound is carrying something the shared shape cannot.
func TestScalarsShareAShapeUnlessTheySaySomething(t *testing.T) {
	svc := ingest(t)
	bucket := svc.Shapes["Bucket"]

	if got := bucket.Members["name"].Shape; got != "google.discovery#string" {
		t.Errorf("a plain string member has its own shape %q", got)
	}
	if got := bucket.Members["metageneration"].Shape; got != "google.discovery#long.int64" {
		t.Errorf("an int64-formatted string is %q, want a long", got)
	}
	if k := svc.Shapes["google.discovery#long.int64"].Kind; k != model.KindLong {
		t.Errorf("google.discovery#long.int64 is a %s", k)
	}

	in := svc.Shapes[op(t, svc, "storage.buckets.insert").Input]
	enum := svc.Shapes[in.Members["projection"].Shape]
	if enum.Kind != model.KindEnum {
		t.Fatalf("projection is a %s, want an enum", enum.Kind)
	}
	if len(enum.EnumValues) != 2 {
		t.Errorf("projection permits %v", enum.EnumValues)
	}

	// A bound is enforced on every request, and was being dropped.
	period := svc.Shapes[svc.Shapes["Bucket.retentionPolicy"].Members["retentionPeriod"].Shape]
	if period.Constraints.MaxValue == nil || *period.Constraints.MaxValue != 3155760000 {
		t.Errorf("retentionPeriod's maximum is %v", period.Constraints.MaxValue)
	}
}

// TestIngestIsDeterministic. `make generate` output is checked in CI to follow
// byte-for-byte from the pinned lock, and this walks maps.
func TestIngestIsDeterministic(t *testing.T) {
	first, second := ingest(t), ingest(t)
	if len(first.Operations) != len(second.Operations) {
		t.Fatalf("%d operations then %d", len(first.Operations), len(second.Operations))
	}
	for i := range first.Operations {
		if first.Operations[i].Name != second.Operations[i].Name {
			t.Fatalf("operation %d is %s then %s", i,
				first.Operations[i].Name, second.Operations[i].Name)
		}
	}
}
