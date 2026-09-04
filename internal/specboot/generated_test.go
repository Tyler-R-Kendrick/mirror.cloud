package specboot

import (
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// TestEveryServedServiceIsDescribedByASpecification is the ratchet on the
// remaining hand-authored description. Three services are described only by
// the catalog because AWS publishes no model for them in api-models-aws; every
// other service the runtime serves takes its protocol, its target prefix and
// its shapes from a vendored specification.
//
// The list may shrink and must not grow. A service added to the catalog
// without a spec behind it is the drift this whole pipeline exists to stop.
func TestEveryServedServiceIsDescribedByASpecification(t *testing.T) {
	_, uncovered := GeneratedCoverage(Bundle())
	want := []string{"aws.elastictranscoder", "aws.lookoutmetrics", "aws.qldb"}
	if len(uncovered) > len(want) {
		t.Fatalf("services with no generated model grew to %d: %v", len(uncovered), uncovered)
	}
	have := map[string]bool{}
	for _, id := range uncovered {
		have[id] = true
	}
	for _, id := range uncovered {
		found := false
		for _, w := range want {
			if id == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s has no generated model and is not one of the three AWS "+
				"publishes no model for", id)
		}
	}
	for _, id := range want {
		if !have[id] {
			t.Logf("%s now has a generated model; it can come off the list", id)
		}
	}
}

// TestTheAdoptedModelIsTheGeneratedOne states the invariant the adoption
// exists for: a service is served with the same protocol and target prefix it
// is validated against. Fifty-three of the ninety-seven behavior bundles
// disagreed before this ran, `aws.guardduty` among them -- validated as
// restJson1 with ninety operations, served as awsJson1_1 with twenty.
func TestTheAdoptedModelIsTheGeneratedOne(t *testing.T) {
	b := Bundle()
	for i := range b.Services {
		svc := &b.Services[i]
		gen, err := generated.Model(generatedID(svc.ID))
		if err != nil {
			continue
		}
		if svc.Protocol != gen.Protocol {
			t.Errorf("%s is served as %s and validated as %s", svc.ID, svc.Protocol, gen.Protocol)
		}
		if svc.TargetPrefix != gen.TargetPrefix {
			t.Errorf("%s is served with target prefix %q and validated with %q",
				svc.ID, svc.TargetPrefix, gen.TargetPrefix)
		}
		if svc.EndpointPrefix != gen.EndpointPrefix {
			t.Errorf("%s is served at %q and validated at %q",
				svc.ID, svc.EndpointPrefix, gen.EndpointPrefix)
		}
		if len(svc.Shapes) != len(gen.Shapes) {
			t.Errorf("%s is served with %d shapes and validated against %d",
				svc.ID, len(svc.Shapes), len(gen.Shapes))
		}
	}
}

// TestNoServiceLosesAnOperation keeps the union honest. A pack may serve an
// operation the vendored specification does not carry, and replacing the
// operation list rather than unioning it would take that operation away --
// silently, since a missing operation answers as a 501 rather than a failure.
func TestNoServiceLosesAnOperation(t *testing.T) {
	adopted := Bundle()
	for _, before := range catalog.Bundle().Services {
		after := adopted.ServiceByID(before.ID)
		if after == nil {
			t.Errorf("%s is in the catalog and not in the served bundle", before.ID)
			continue
		}
		have := map[string]bool{}
		for _, op := range after.Operations {
			have[op.Name] = true
		}
		var lost []string
		for _, op := range before.Operations {
			if !have[op.Name] {
				lost = append(lost, op.Name)
			}
		}
		sort.Strings(lost)
		if len(lost) > 0 {
			t.Errorf("%s lost %d operation(s) to adoption: %v", before.ID, len(lost), lost)
		}
	}
}

// TestServedAsIsLoadBearing is the third property an exemption needs, applied
// to the four services the runtime serves under an ID the specification does
// not use. Each entry must name a service on both sides, and must be doing
// work: an entry whose served ID already has a generated model of its own is
// defending nothing.
func TestServedAsIsLoadBearing(t *testing.T) {
	b := Bundle()
	for served, spec := range ServedAs() {
		if b.ServiceByID(served) == nil {
			t.Errorf("%q is served as %q and the bundle has no such service", spec, served)
		}
		if _, err := generated.Model(spec); err != nil {
			t.Errorf("%q maps to %q, which has no generated model: %v", served, spec, err)
		}
		if _, err := generated.Model(served); err == nil {
			t.Errorf("%q has a generated model of its own; the mapping to %q "+
				"excuses nothing and should be dropped", served, spec)
		}
	}
}

// TestTheSigningNameIsCarried checks that the receiver put the SigV4 signing
// name where the demux can find it. It is the name a client writes into the
// credential scope, it differs from the endpoint prefix for seventy-seven
// upstream models, and without it Lex Model Building -- signed as `lex`,
// reached at `models.lex` -- is addressable by neither.
func TestTheSigningNameIsCarried(t *testing.T) {
	b := Bundle()
	carried := 0
	for i := range b.Services {
		svc := &b.Services[i]
		for _, alias := range svc.Aliases {
			if alias == "" {
				t.Errorf("%s carries an empty alias", svc.ID)
			}
			if strings.EqualFold(alias, svc.EndpointPrefix) {
				t.Errorf("%s carries %q as an alias of its own endpoint prefix", svc.ID, alias)
			}
			carried++
		}
	}
	if carried == 0 {
		t.Fatal("no service carries an alias; the receiver is not recording the signing name")
	}
	if svc := b.ServiceByID("aws.lex-models"); svc == nil {
		t.Fatal("aws.lex-models is not in the bundle")
	} else if !hasAlias(svc, "lex") {
		t.Errorf("aws.lex-models is reached at %q and signs as `lex`, which it does "+
			"not carry: %v", svc.EndpointPrefix, svc.Aliases)
	}
	t.Logf("%d signing names differ from the endpoint prefix", carried)
}

func hasAlias(svc *model.Service, name string) bool {
	for _, alias := range svc.Aliases {
		if strings.EqualFold(alias, name) {
			return true
		}
	}
	return false
}
