package check_test

import (
	"testing"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudcontrol"
)

// CloudControl does not call the services it reports on. It reads their store
// collections directly, by name -- `backingCollection` maps a CloudFormation
// type name to the collection the owning service happens to keep its records
// in, and `s3Resource` reads three of S3's.
//
// That is a coupling nothing declares. It survived API Gateway v2's extraction
// only because the bundle kept the pack's collection name, and nothing
// anywhere said it had to: renaming `ag2` in the bundle would leave
// GetResource answering ResourceNotFoundException for every API in the
// account, with every test in both services still passing.
//
// These tests do not remove the coupling. They state it, so that the rename
// that would break it fails here instead of in production, and so that the
// next service extracted has a place to check.
//
// They live here rather than beside CloudControl because a service pack may
// not import the generated models -- a gate in this package says so, and it is
// right: a pack that reads the model is a pack deciding its own wire shapes.
// A cross-service invariant is this package's business.

// TestBackingCollectionsExistInTheBundlesThatOwnThem covers the half that can
// be checked mechanically: a bundle declares its collections, so a name
// CloudControl reads can be looked for among them.
func TestBackingCollectionsExistInTheBundlesThatOwnThem(t *testing.T) {
	for _, want := range []struct{ typeName, service string }{
		{"AWS::ApiGatewayV2::Api", "aws.apigatewayv2"},
	} {
		collection := cloudcontrol.BackingCollection(want.typeName)
		if collection == "" {
			t.Errorf("%s: CloudControl no longer reads a collection for this type",
				want.typeName)
			continue
		}
		svc, err := generated.Model(want.service)
		if err != nil {
			t.Fatal(err)
		}
		ir, err := behaviors.Load(want.service, svc)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, res := range ir.Resources {
			if res.Collection == collection {
				found = true
				break
			}
		}
		if !found {
			have := make([]string, 0, len(ir.Resources))
			for _, res := range ir.Resources {
				have = append(have, res.Collection)
			}
			t.Errorf("CloudControl reads %q for %s, and no resource in the %s "+
				"bundle keeps records there (it has: %v).\n\tCloudControl reads "+
				"that service's store directly rather than calling it, so a "+
				"renamed collection makes GetResource answer "+
				"ResourceNotFoundException for every one of them, silently.",
				collection, want.typeName, want.service, have)
		}
	}
}

// TestBackingCollectionsArePinned covers the other half. The rest of the
// services CloudControl reads are still hand-written packs, which declare
// nothing, so there is no collection list to check against -- only the
// literal names. Pinning them means an extraction that changes one has to
// change this too, which is the moment to notice.
func TestBackingCollectionsArePinned(t *testing.T) {
	for typeName, want := range map[string]string{
		"AWS::ApiGatewayV2::Api": "ag2",
		"AWS::RDS::DBInstance":   "dbinst",
		"AWS::RDS::DBCluster":    "dbcluster",
	} {
		if got := cloudcontrol.BackingCollection(typeName); got != want {
			t.Errorf("cloudcontrol.BackingCollection(%q) = %q, want %q\n\tIf the owning "+
				"service moved its records, this map has to move with them.",
				typeName, got, want)
		}
	}
	// An unknown type reads nothing, rather than falling back to some
	// collection that happens to exist.
	if got := cloudcontrol.BackingCollection("AWS::Nonesuch::Thing"); got != "" {
		t.Errorf("an unmapped type reads %q", got)
	}
}
