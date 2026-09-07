package bundled

import (
	"sort"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
)

// TestEveryServedServiceCouldBeABundle states that extraction has no hidden
// ceiling: any service the runtime serves can be built as a bundle, so the
// only thing standing between a pack and its YAML is writing the YAML.
//
// It did not hold. `internal/bundled` resolved a model with
// generated.Model(id), and four services are served under an ID the
// specification does not use -- CloudWatch signs as `monitoring`, ELBv2 as
// `elasticloadbalancing`, and the catalog shortened ECR and the tagging API.
// Those four had full models, 410, 470, 463 and 89 shapes, reachable under the
// specification's name and not under the served one. Writing a bundle for any
// of them would have failed at the factory with "no model for aws.monitoring",
// which reads like a missing spec rather than a lookup that skipped a mapping.
//
// Nothing caught it because nothing tried. The gap opens only when someone
// extracts one of those four, and it closes the moment a bundle is built from
// the model the runtime actually serves.
func TestEveryServedServiceCouldBeABundle(t *testing.T) {
	served := specboot.Bundle()
	var unbuildable []string
	for i := range served.Services {
		svc := &served.Services[i]
		// A service with no shapes has no specification behind it at all --
		// AWS publishes no model for three of them -- and a bundle could not
		// be validated against it. That is a missing spec, not a lookup fault.
		if len(svc.Shapes) == 0 {
			continue
		}
		if _, err := servedModel(svc.ID); err != nil {
			unbuildable = append(unbuildable, svc.ID)
		}
	}
	sort.Strings(unbuildable)
	if len(unbuildable) > 0 {
		t.Errorf("%d served service(s) cannot be resolved to a model, so no bundle "+
			"could ever be written for them: %v", len(unbuildable), unbuildable)
	}
}

// TestTheServedNamesResolve pins the four by name. The general test above
// would pass again if the mapping were dropped and those services vanished
// from the bundle entirely, so this states that they are present and
// resolvable, with the shapes a bundle would be validated against.
func TestTheServedNamesResolve(t *testing.T) {
	for _, id := range []string{
		"aws.monitoring", "aws.elasticloadbalancing", "aws.api.ecr", "aws.tagging",
	} {
		svc, err := servedModel(id)
		if err != nil {
			t.Errorf("%s does not resolve to a model: %v", id, err)
			continue
		}
		if len(svc.Shapes) == 0 {
			t.Errorf("%s resolves to a model with no shapes; a bundle could not "+
				"be validated against it", id)
		}
		if svc.ID != id {
			t.Errorf("%s resolved to a model whose ID is %q; the bundle would be "+
				"served under one name and built under another", id, svc.ID)
		}
	}
}
