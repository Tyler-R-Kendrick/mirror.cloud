package specboot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// TestEveryServedServiceIsDescribedByASpecification is the ratchet on the
// remaining hand-authored description. A few services are described only by
// the catalog because AWS publishes no model for them in api-models-aws; every
// other service the runtime serves takes its protocol, its target prefix and
// its shapes from a vendored specification.
//
// The list may shrink and must not grow. A service added to the catalog
// without a spec behind it is the drift this whole pipeline exists to stop.
//
// Which services those are is READ from specs/aws-dirs.json rather than written
// here. The file already carries them, with a reason each, and
// resolve_specs.py and TestGeneratedCoversDeclaredSet both read it; a fourth
// copy of the same three names was one more place for the fact to drift, and
// the fact had already drifted -- known-red.json described these as models the
// receiver could not ingest, which sent a reader looking for a receiver bug
// that does not exist. There is nothing to ingest.
func TestEveryServedServiceIsDescribedByASpecification(t *testing.T) {
	_, uncovered := GeneratedCoverage(Bundle())
	want := servicesWithNoUpstreamModel(t)
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
			t.Errorf("%s has no generated model and is not one of the %d services "+
				"specs/aws-dirs.json records as having none upstream (%v). Run "+
				"`make specs-sync && make generate`, or record it there with a reason.",
				id, len(want), want)
		}
	}
	for _, id := range want {
		if !have[id] {
			t.Logf("%s now has a generated model; it can come out of the "+
				"\"unavailable\" map of specs/aws-dirs.json", id)
		}
	}
}

// servicesWithNoUpstreamModel reads the services specs/aws-dirs.json records
// as having no model published upstream at all.
//
// Sorted so a failure message names them in a stable order, and fatal on a
// missing or unreadable file: an empty list would silently turn the test above
// into one that demands a generated model for every served service, which is
// a different assertion than the one it is making.
func servicesWithNoUpstreamModel(t *testing.T) []string {
	t.Helper()
	// The test binary runs in the package directory.
	body, err := os.ReadFile(filepath.Join("..", "..", "specs", "aws-dirs.json"))
	if err != nil {
		t.Fatalf("read specs/aws-dirs.json: %v", err)
	}
	var dirs struct {
		Unavailable map[string]string `json:"unavailable"`
	}
	if err := json.Unmarshal(body, &dirs); err != nil {
		t.Fatalf("parse specs/aws-dirs.json: %v", err)
	}
	if len(dirs.Unavailable) == 0 {
		t.Fatal("specs/aws-dirs.json declares no \"unavailable\" services; if every " +
			"service now has an upstream model, this test and its known-red.json " +
			"entry should say so rather than reading an empty list")
	}
	// An entry only excuses a service this repository set out to generate.
	//
	// Reading the list rather than writing it down is what makes this test
	// honest about WHICH services have no upstream model -- and it is also a
	// way to make the test green that writing it down did not offer: adding
	// azure.blobs here would excuse the pack that is the entire reason this
	// test is red, and nothing else would have noticed. `unavailable` means
	// "declared in specs/mirror.set, and upstream publishes nothing for it",
	// so a service that was never declared cannot be excused by it. azure is
	// not in mirror.set at all, which is exactly why it has no model.
	allowed, undeclared := excusedServices(dirs.Unavailable, declaredServices(t))
	for _, id := range undeclared {
		t.Errorf("specs/aws-dirs.json excuses %s as having no upstream model, but "+
			"specs/mirror.set does not declare it. The \"unavailable\" map is for "+
			"services this repository set out to generate and upstream does not "+
			"publish; a service that was never declared is not one of those, and "+
			"excusing it here would quietly widen what this test allows.", id)
	}
	return allowed
}

// excusedServices splits the "unavailable" map into the entries a declared
// service earns and the entries no declared service backs.
//
// Separated from the file reading so the rule can be tested against inputs the
// tree does not currently contain. It is the guard, and today every real entry
// satisfies it, so nothing in the tree would notice if it stopped working --
// which is the shape of hole this repository keeps finding after the fact.
func excusedServices(unavailable map[string]string, declared map[string]bool) (allowed, undeclared []string) {
	for id := range unavailable {
		if declared[id] {
			allowed = append(allowed, id)
			continue
		}
		undeclared = append(undeclared, id)
	}
	sort.Strings(allowed)
	sort.Strings(undeclared)
	return allowed, undeclared
}

// declaredServices reads the service ids specs/mirror.set declares.
func declaredServices(t *testing.T) map[string]bool {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "specs", "mirror.set"))
	if err != nil {
		t.Fatalf("read specs/mirror.set: %v", err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out[strings.Fields(line)[0]] = true
	}
	return out
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

// TestOnlyADeclaredServiceCanBeExcused covers the rule that keeps the
// "unavailable" map from widening what the coverage test allows.
//
// The real map satisfies it today, so this is the only thing that would see it
// break. The case that matters is the third: azure.blobs is served, has no
// generated model, and is the single reason
// TestEveryServedServiceIsDescribedByASpecification is red -- so an entry for
// it in specs/aws-dirs.json would turn that test green while changing nothing.
// It is not in specs/mirror.set, and that is what refuses it.
func TestOnlyADeclaredServiceCanBeExcused(t *testing.T) {
	declared := map[string]bool{"aws.qldb": true, "aws.s3": true}
	for _, tc := range []struct {
		name             string
		unavailable      map[string]string
		allowed, refused []string
	}{
		{
			name:        "a declared service with no upstream model is excused",
			unavailable: map[string]string{"aws.qldb": "no model published"},
			allowed:     []string{"aws.qldb"},
		},
		{
			name:        "a service nobody declared is not",
			unavailable: map[string]string{"azure.blobs": "still a pack"},
			refused:     []string{"azure.blobs"},
		},
		{
			name: "the two are separated, not collapsed",
			unavailable: map[string]string{
				"aws.qldb": "no model published", "azure.blobs": "still a pack",
			},
			allowed: []string{"aws.qldb"}, refused: []string{"azure.blobs"},
		},
		{
			name:        "an empty map excuses nothing and refuses nothing",
			unavailable: map[string]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowed, refused := excusedServices(tc.unavailable, declared)
			if strings.Join(allowed, ",") != strings.Join(tc.allowed, ",") {
				t.Errorf("allowed = %v, want %v", allowed, tc.allowed)
			}
			if strings.Join(refused, ",") != strings.Join(tc.refused, ",") {
				t.Errorf("refused = %v, want %v", refused, tc.refused)
			}
		})
	}
}
