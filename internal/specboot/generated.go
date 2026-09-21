package specboot

import (
	"sort"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// servedAs names the services the runtime serves under an ID the
// specification does not use. Three are historical wire names the SDKs still
// send -- CloudWatch signs as `monitoring`, ELBv2 as `elasticloadbalancing` --
// one is a shortening the catalog chose, and one is a specification vendored
// under its endpoint prefix: the OpenSearch 2021-01-01 model generated as
// aws.es while the service is served as aws.opensearch. The generated models
// are keyed by the specification's ID, so the two have to be joined somewhere;
// here, once, rather than at every reader.
var servedAs = map[string]string{
	"aws.monitoring":           "aws.cloudwatch",
	"aws.elasticloadbalancing": "aws.elbv2",
	"aws.api.ecr":              "aws.ecr",
	"aws.tagging":              "aws.resourcegroupstaggingapi",
	"aws.opensearch":           "aws.es",
}

// servedAlsoAsSpec names the specification IDs in servedAs that stay served
// under their own ID as well. OpenSearch is the one: `aws.es` carries the
// 2021 control plane with a behavior bundle of its own, beside the
// `aws.opensearch` pack. The other four are served under the wire name alone.
var servedAlsoAsSpec = map[string]bool{"aws.es": true}

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
