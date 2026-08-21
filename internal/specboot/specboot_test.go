package specboot

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestBundleFallsBackOrIngests(t *testing.T) {
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
		t.Fatalf("ingested bundle smaller than catalog (%d < %d)", len(b.Services), len(cat.Services))
	}
}

func TestBundleMergesCatalogStreamOps(t *testing.T) {
	b := Bundle()
	ddb := b.ServiceByID("aws.dynamodb")
	if ddb == nil {
		t.Fatal("missing aws.dynamodb")
	}
	if ddb.OperationByName("ListStreams") == nil {
		t.Fatal("ListStreams missing from ingested dynamodb — catalog ops must merge")
	}
}
