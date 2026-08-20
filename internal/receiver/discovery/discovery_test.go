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
