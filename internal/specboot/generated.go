package specboot

import (
	"sort"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// servedAs names the four services the runtime serves under an ID the
// specification does not use. Three are historical wire names the SDKs still
// send -- CloudWatch signs as `monitoring`, ELBv2 as `elasticloadbalancing` --
// and one is a shortening the catalog chose. The generated models are keyed by
// the specification's ID, so the two have to be joined somewhere; here, once,
// rather than at every reader.
var servedAs = map[string]string{
	"aws.monitoring":           "aws.cloudwatch",
	"aws.elasticloadbalancing": "aws.elbv2",
	"aws.api.ecr":              "aws.ecr",
	"aws.tagging":              "aws.resourcegroupstaggingapi",
}

// adoptGenerated replaces each service's wire description with the one
// generated from the vendored specification, keeping the ID the runtime serves
// it under.
//
// Until this ran, a bundle was *validated* against `internal/generated` and
// *served* through the hand-authored catalog, and nothing compared the two.
// They disagreed for fifty-three of the ninety-seven behavior bundles on
// protocol, on X-Amz-Target prefix, or on both: `aws.guardduty` was validated
// as restJson1 with ninety operations and served as awsJson1_1 with twenty, so
// the request an SDK makes reached nothing while the request no SDK makes
// worked. Twenty-eight of the prefix disagreements are transcription slips --
// `AmazonDMS20160101` where the specification says `AmazonDMSv20160101`.
//
// docs/BEHAVIOR_IR.md states the invariant this restores: "B-IR never
// redefines wire shapes ... the loader fails otherwise". It held for the shapes
// a bundle projects and not for the protocol those shapes travel in, because
// the loader and the edge read different descriptions of the same service.
//
// Operations are unioned rather than replaced. A pack may serve an operation
// the vendored specification does not carry -- a newer API, or one the catalog
// was written against -- and dropping it would take the service away rather
// than correct it. The generated operation wins where both have one, since it
// is the one with a URI, a target and shape IDs.
func adoptGenerated(b *model.Bundle) {
	for i := range b.Services {
		svc := &b.Services[i]
		gen, err := generated.Model(generatedID(svc.ID))
		if err != nil {
			continue
		}
		svc.Namespace = gen.Namespace
		svc.Protocol = gen.Protocol
		svc.EndpointPrefix = gen.EndpointPrefix
		svc.TargetPrefix = gen.TargetPrefix
		svc.QueryVersion = gen.QueryVersion
		svc.XMLNamespace = gen.XMLNamespace
		svc.Aliases = gen.Aliases
		svc.Shapes = gen.Shapes
		svc.Source = gen.Source
		svc.Operations = unionOperations(gen.Operations, svc.Operations)
	}
}

// generatedID maps a served service ID onto the ID the generated models are
// keyed by.
func generatedID(served string) string {
	if id, ok := servedAs[served]; ok {
		return id
	}
	return served
}

// unionOperations takes every operation the specification describes, then
// every one the catalog had that the specification does not.
func unionOperations(spec, catalog []model.Operation) []model.Operation {
	out := append([]model.Operation(nil), spec...)
	have := map[string]bool{}
	for _, op := range spec {
		have[op.Name] = true
	}
	var extra []model.Operation
	for _, op := range catalog {
		if !have[op.Name] {
			extra = append(extra, op)
		}
	}
	sort.Slice(extra, func(i, j int) bool { return extra[i].Name < extra[j].Name })
	return append(out, extra...)
}

// GeneratedCoverage reports which services in a bundle have a generated model
// behind them and which are still described only by the hand-authored catalog.
// The second list is what remains to be ingested, and a test keeps it from
// growing.
func GeneratedCoverage(b *model.Bundle) (covered, uncovered []string) {
	for i := range b.Services {
		id := b.Services[i].ID
		if _, err := generated.Model(generatedID(id)); err == nil {
			covered = append(covered, id)
			continue
		}
		uncovered = append(uncovered, id)
	}
	sort.Strings(covered)
	sort.Strings(uncovered)
	return covered, uncovered
}

// ServedAs exposes the served-to-specification ID mapping so a test can state
// that every entry still names services that exist on both sides.
func ServedAs() map[string]string {
	out := make(map[string]string, len(servedAs))
	for k, v := range servedAs {
		out[k] = v
	}
	return out
}
