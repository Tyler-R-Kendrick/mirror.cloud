package specboot

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// TestBundleServesTheCatalogSet checks the shape of the served model: every
// service the catalog names is in it, GCS among them, and adoption drops
// nothing. The name is no longer "falls back or ingests" because there is no
// fallback: the bundle is the catalog's service list with the generated
// models' wire facts, and does not depend on whether `specs/` is present.
func TestBundleServesTheCatalogSet(t *testing.T) {
	b := Bundle()
	if b == nil || len(b.Services) == 0 {
		t.Fatal("empty bundle")
	}
	if b.ServiceByID("aws.s3") == nil {
		t.Fatal("missing aws.s3")
	}
	gcs := b.ServiceByID("gcp.storage")
	if gcs == nil {
		t.Fatal("gcp.storage missing from Bundle — Detect head likely truncated before discoveryVersion")
	}
	if gcs.Protocol != model.ProtoGCPRESTSON {
		t.Fatalf("gcp.storage protocol %q", gcs.Protocol)
	}
	cat := catalog.Bundle()
	if len(b.Services) < len(cat.Services) {
		t.Fatalf("served bundle smaller than catalog (%d < %d)", len(b.Services), len(cat.Services))
	}
}

func TestBundleMergesCatalogStreamOps(t *testing.T) {
	b := Bundle()
	ddb := b.ServiceByID("aws.dynamodb")
	if ddb == nil {
		t.Fatal("missing aws.dynamodb")
	}
	if ddb.OperationByName("ListStreams") == nil {
		t.Fatal("ListStreams missing from dynamodb — catalog-only operations must survive adoption")
	}
}
