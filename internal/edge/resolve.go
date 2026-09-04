package edge

import (
	"net/http"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// resolveByModel finds the service a request is addressed to using the two
// things every AWS SDK sends and the model already describes: the endpoint
// host, and the service name inside the SigV4 credential scope.
//
// The demux used to answer this with a chain of about a hundred and thirty
// hand-written `looksLike` branches, and all of them sat inside `if action !=
// ""` -- a condition only a query-protocol request satisfies. Addressed the
// way an SDK addresses them, a hundred and forty-eight of the hundred and
// fifty-two services in the bundle resolved to `aws.s3`, because the chain was
// skipped and the path-style S3 fallback at the bottom took everything.
//
// The chain was also substring matching: `looksLike(r, "es")` is true of any
// host containing those two letters. Matching a whole label against the
// model's own `EndpointPrefix` is both narrower and derived rather than
// transcribed, so a service added to a specification is addressable without
// anyone writing a branch for it.
func (s *Server) resolveByModel(r *http.Request) *model.Service {
	if svc := s.serviceByLabel(hostLabel(r.Host)); svc != nil {
		return svc
	}
	return s.serviceByLabel(credentialScopeService(r.Header.Get("Authorization")))
}

// clientSpellings are labels a client may use that no specification records.
// They are not aliases in the model's sense -- the SigV4 signing name for
// Amazon OpenSearch Service really is `es`, and for Directory Service really
// is `ds` -- but tools and hand-written clients address them by the service's
// marketing name, and the demux accepted both before it was derived from the
// model.
//
// The table is deliberately tiny, and TestEveryClientSpellingIsLoadBearing
// fails if an entry stops being needed: an entry that would resolve without
// the table is a branch defending nothing, which is how the hundred and thirty
// it replaced accumulated.
var clientSpellings = map[string]string{
	"opensearch":       "aws.es",
	"directoryservice": "aws.ds",
}

// hostLabel is the leading label of an endpoint host: the `guardduty` of
// `guardduty.us-east-1.amazonaws.com`. A bare host with no dots -- `localhost`,
// an IP -- has no service in it, and returning it costs nothing because it
// will not match any endpoint prefix.
func hostLabel(host string) string {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	label, _, _ := strings.Cut(host, ".")
	return strings.ToLower(label)
}

// credentialScopeService reads the service name out of a SigV4 Authorization
// header: the fourth field of `Credential=AK/20200101/us-east-1/guardduty/
// aws4_request`. It is the one place a request states which service it is for
// that does not depend on how the client was configured to reach it.
func credentialScopeService(authorization string) string {
	i := strings.Index(authorization, "Credential=")
	if i < 0 {
		return ""
	}
	cred := authorization[i+len("Credential="):]
	if j := strings.IndexAny(cred, ", "); j >= 0 {
		cred = cred[:j]
	}
	parts := strings.Split(cred, "/")
	if len(parts) < 4 {
		return ""
	}
	return strings.ToLower(parts[3])
}

// serviceByLabel maps one label to a service. A service's own short name wins
// over an endpoint prefix, because that is how the ambiguous prefixes are told
// apart: DocumentDB and Neptune are forks of the RDS API and their
// specifications name `rds` as the endpoint prefix for all three, but a client
// reaches DocumentDB at `docdb.<region>.amazonaws.com`.
//
// A service named by the label therefore wins outright: `rds` reaches
// `aws.rds` even though three services declare that prefix. Where none of them
// is named by the label -- `email`, which both SES versions declare -- the
// answer is the alphabetically first service ID, so it does not depend on the
// order the bundle lists services in. Where that is the wrong service, the fix
// is a specification that distinguishes them, not a branch here.
func (s *Server) serviceByLabel(label string) *model.Service {
	if label == "" {
		return nil
	}
	var byPrefix []*model.Service
	if id, ok := clientSpellings[label]; ok {
		if svc := s.bundle.ServiceByID(id); svc != nil {
			return svc
		}
	}
	for i := range s.bundle.Services {
		svc := &s.bundle.Services[i]
		if shortName(svc.ID) == label {
			return svc
		}
		if svc.EndpointPrefix != "" && strings.EqualFold(svc.EndpointPrefix, label) {
			byPrefix = append(byPrefix, svc)
		}
	}
	if len(byPrefix) == 0 {
		return nil
	}
	sort.Slice(byPrefix, func(i, j int) bool { return byPrefix[i].ID < byPrefix[j].ID })
	return byPrefix[0]
}

// shortName drops the provider from a service ID: `aws.guardduty` is reached
// at a host labelled `guardduty`.
func shortName(id string) string {
	if _, rest, ok := strings.Cut(id, "."); ok {
		return strings.ToLower(rest)
	}
	return strings.ToLower(id)
}
