package check

import (
	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
)

// ServedAndSpecRouting reports, for every behavior bundle, how the runtime
// would route a request to it and how the vendored specification says it
// should be routed. Each value is the protocol and the target prefix -- the
// two fields that decide whether an SDK's request reaches the service at all.
//
// The two are supposed to be the same thing. They are read from different
// places and, for about half the bundles, they disagree: a bundle is validated
// against the generated model and served through the booted catalog, and
// nothing compared the two. `aws.guardduty` is validated as restJson1 with
// ninety operations and served as awsJson1_1 with twenty, so an SDK sending
// `GET /detector` reaches nothing while `POST /` with an X-Amz-Target -- which
// no SDK sends for that service -- works.
//
// The target prefix disagrees on twenty-eight bundles in its own right, and
// those read as typos rather than protocol confusion: the catalog says
// AmazonDMS20160101 where the spec says AmazonDMSv20160101, and AWSBackup
// where the spec says CryoControllerUserManager. An SDK sends the spec's
// value, the edge matches on the catalog's, and the request routes to no
// service at all.
//
// docs/BEHAVIOR_IR.md states the intended invariant: "B-IR never redefines
// wire shapes ... the loader fails otherwise". It holds for the shapes a
// bundle projects. It does not hold for the protocol those shapes travel in,
// because the loader and the edge read different descriptions of the service.
func ServedAndSpecRouting() (served, spec map[string]string) {
	served = map[string]string{}
	spec = map[string]string{}
	bundle := specboot.Bundle()
	catalog := map[string]string{}
	for i := range bundle.Services {
		s := &bundle.Services[i]
		catalog[s.ID] = string(s.Protocol) + " " + s.TargetPrefix
	}
	for _, id := range behaviors.ServiceIDs() {
		got, ok := catalog[id]
		if !ok {
			continue
		}
		m, err := generated.Model(id)
		if err != nil {
			continue
		}
		served[id] = got
		spec[id] = string(m.Protocol) + " " + m.TargetPrefix
	}
	return served, spec
}
