package edge

import (
	"net/http"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/httpuri"
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
	if svc := s.serviceByLabel(r, hostLabel(r.Host)); svc != nil {
		return svc
	}
	return s.serviceByLabel(r, credentialScopeService(r.Header.Get("Authorization")))
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

// serviceByLabel maps one label to a service, using the request to settle the
// cases where several services answer to the same label.
//
// Five labels are shared once the models come from the specifications, and all
// five are the same shape: a service and its successor sit on one endpoint.
// API Gateway's HTTP APIs really are reached at `apigateway.<region>`, both SES
// versions at `email`, and DocumentDB and Neptune at `rds`, because they are
// forks of the RDS API. In every one of those pairs the older service's own
// name *is* the shared prefix, so preferring the namesake makes the successor
// unreachable -- which is how `aws.apigatewayv2` answered as
// `aws.apigateway.GetRestApis`.
//
// What tells them apart is the request. AWS distinguishes them by path
// (`/v2/apis` against `/restapis`) or by target, and the model describes both,
// so the service that *claims* the request wins. Only when none of them claims
// it, or several do, does this fall back to the namesake and then to the
// alphabetically first ID -- deterministic rather than dependent on the order
// the bundle lists services in.
func (s *Server) serviceByLabel(r *http.Request, label string) *model.Service {
	if label == "" {
		return nil
	}
	if id, ok := clientSpellings[label]; ok {
		if svc := s.bundle.ServiceByID(id); svc != nil {
			return svc
		}
	}
	var candidates []*model.Service
	for i := range s.bundle.Services {
		svc := &s.bundle.Services[i]
		if shortName(svc.ID) == label || answersTo(svc, label) {
			candidates = append(candidates, svc)
		}
	}
	switch len(candidates) {
	case 0:
		return nil
	case 1:
		return candidates[0]
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	var claimed []*model.Service
	for _, svc := range candidates {
		if r != nil && claims(svc, r) {
			claimed = append(claimed, svc)
		}
	}
	if len(claimed) == 1 {
		return claimed[0]
	}
	for _, svc := range candidates {
		if shortName(svc.ID) == label {
			return svc
		}
	}
	return candidates[0]
}

// claims reports whether a service's model describes the request: an
// X-Amz-Target under its prefix, an Action it declares, or a path one of its
// operations is bound to. It is only consulted to separate services sharing an
// endpoint, so a service that claims nothing costs nothing.
func claims(svc *model.Service, r *http.Request) bool {
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		if svc.TargetPrefix != "" && strings.HasPrefix(target, svc.TargetPrefix+".") {
			return true
		}
		name := target
		if i := strings.LastIndex(target, "."); i >= 0 {
			name = target[i+1:]
		}
		return svc.OperationByName(name) != nil
	}
	action := r.URL.Query().Get("Action")
	if action == "" && r.Form != nil {
		action = r.Form.Get("Action")
	}
	if action != "" {
		return svc.OperationByName(action) != nil
	}
	_, _, ok := httpuri.Match(svc, r)
	return ok
}

// answersTo reports whether a service is addressed by this label under one of
// the names its model carries: the endpoint prefix, or one of the aliases the
// receiver records.
//
// The alias that matters today is the SigV4 signing name, which is what a
// client writes into the credential scope and which differs from the endpoint
// prefix for seventy-seven upstream models. Lex Model Building signs as `lex`
// and is reached at `models.lex`; ECR signs as `ecr` and is reached at
// `api.ecr`. Matching only the prefix leaves those services unaddressable by
// the header every SDK sends.
func answersTo(svc *model.Service, label string) bool {
	if svc.EndpointPrefix != "" && strings.EqualFold(svc.EndpointPrefix, label) {
		return true
	}
	for _, alias := range svc.Aliases {
		if strings.EqualFold(alias, label) {
			return true
		}
	}
	return false
}

// shortName drops the provider from a service ID: `aws.guardduty` is reached
// at a host labelled `guardduty`.
func shortName(id string) string {
	if _, rest, ok := strings.Cut(id, "."); ok {
		return strings.ToLower(rest)
	}
	return strings.ToLower(id)
}
