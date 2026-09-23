// Package specboot builds the process model: every service the vendored
// specifications describe, served under the ID an SDK addresses it by, plus
// the few services and operations no specification carries.
package specboot

import (
	"sort"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

var (
	once sync.Once
	got  *model.Bundle
)

// Bundle returns the model the runtime serves.
//
// It used to be a hand-authored catalog of a hundred and sixty-six services
// with the generated models adopted over it, and before that it walked
// `specs/` and ingested whatever it found. Both made the served model depend
// on something other than the repository: local state in the first case, and
// in the second two thousand transcribed lines whose every wire fact was
// overwritten by the generated model the moment it existed. What the catalog
// still contributed after adoption was the list of services -- which
// `internal/generated` already is -- and what `unmodeled` holds below.
//
// `internal/generated` is the ingestion run once, committed, and checked by CI
// to follow byte-for-byte from the pinned lock, so the served model is a
// property of the repository and nothing else.
func Bundle() *model.Bundle {
	once.Do(func() { got = derive() })
	return got
}

func derive() *model.Bundle {
	// served ID -> specification ID. A service is served under its own ID
	// unless servedAs gives it a wire name, in which case the wire name
	// replaces it: an SDK reaches CloudWatch only as `monitoring`, and serving
	// `aws.cloudwatch` beside `aws.monitoring` would put two services on one
	// endpoint, which is the confusion Bundle's history is about.
	served := map[string]string{}
	for _, id := range generated.ServiceIDs() {
		served[id] = id
	}
	for wire, spec := range servedAs {
		served[wire] = spec
		if !servedAlsoAsSpec[spec] {
			delete(served, spec)
		}
	}
	ids := make([]string, 0, len(served))
	for id := range served {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	b := &model.Bundle{}
	for _, id := range ids {
		gen, err := generated.Model(served[id])
		if err != nil {
			panic("specboot: " + served[id] + " is in generated.ServiceIDs() and has no model: " + err.Error())
		}
		svc := *gen
		svc.ID = id
		svc.Operations = unionOperations(gen.Operations, authored(unmodeledOps[id]...))
		b.Services = append(b.Services, svc)
	}
	sort.Slice(b.Services, func(i, j int) bool { return b.Services[i].ID < b.Services[j].ID })
	return b
}

// unmodeledOps are operations a pack serves that its service's specification
// does not describe: the data planes a control-plane model leaves out, and
// the S3 pack's bucket-level names for object lock. The generated operation
// wins wherever the specification has one (unionOperations), so an entry here
// is inert the day the vendor publishes it.
var unmodeledOps = map[string][]string{
	"aws.dynamodb":   {"ListStreams", "DescribeStream", "GetShardIterator", "GetRecords"},
	"aws.s3":         {"GetBucketObjectLockConfiguration", "PutBucketObjectLockConfiguration", "PostObject"},
	"aws.apigateway": {"ExecuteApi"},
}

// authored describes operations the way the catalog did: a POST to "/" with
// the operation's own name as target and query action, read-only by name.
func authored(names ...string) []model.Operation {
	out := make([]model.Operation, 0, len(names))
	for _, n := range names {
		ro := false
		for _, p := range []string{"Get", "Describe", "List", "Head"} {
			if len(n) >= len(p) && n[:len(p)] == p {
				ro = true
			}
		}
		out = append(out, model.Operation{
			Name:        n,
			HTTP:        model.HTTPBinding{Method: "POST", URI: "/", Code: 200},
			Target:      n,
			QueryAction: n,
			Confidence:  model.ConfDeclared,
			Readonly:    ro,
		})
	}
	return out
}
