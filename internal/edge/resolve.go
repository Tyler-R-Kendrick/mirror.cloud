package edge

import (
	"net/http"
	"net/url"
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
	// A dotted endpoint prefix is tried first, because the leading label of
	// such a host is not a service name at all. ECR is reached at
	// `api.ecr.<region>.amazonaws.com` and IoT Wireless at
	// `api.iotwireless.<region>...`, so the leading label of both is `api` --
	// and any service whose short name happens to be `api` then answers for
	// them. One did: naming a service `vercel.api` made its short name the
	// generic word, and both AWS services became unreachable at their own
	// endpoints. Matching the whole prefix the model records is what tells
	// `api.ecr` apart from `api`.
	if svc := s.serviceByHostPrefix(r.Host); svc != nil {
		return svc
	}
	if svc := s.serviceByLabel(r, hostLabel(r.Host)); svc != nil {
		return svc
	}
	return s.serviceByLabel(r, credentialScopeService(r.Header.Get("Authorization")))
}

// serviceByHostPrefix matches the leading labels of a host against an endpoint
// prefix that is itself dotted. The longest such prefix wins, so a service
// reached at `api.ecr` is not shadowed by one reached at `api`.
func (s *Server) serviceByHostPrefix(host string) *model.Service {
	host = endpointHost(host)
	if host == "" {
		return nil
	}
	var best *model.Service
	for i := range s.bundle.Services {
		svc := &s.bundle.Services[i]
		prefix := strings.ToLower(svc.EndpointPrefix)
		if prefix == "" || !strings.Contains(prefix, ".") {
			continue
		}
		if host != prefix && !strings.HasPrefix(host, prefix+".") {
			continue
		}
		if best == nil || len(prefix) > len(best.EndpointPrefix) {
			best = svc
		}
	}
	return best
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

// endpointHost strips the port and the endpoint variant a client may have been
// configured to use, leaving the host the model's endpoint prefixes describe.
//
// AWS marks a FIPS endpoint by suffixing the service label: DynamoDB's is
// `dynamodb-fips.<region>.amazonaws.com` and ECR's is
// `api.ecr-fips.<region>...`. Nothing matched those, so every FIPS endpoint fell
// past the model to the path-style S3 default at the bottom of the demux, and a
// client asking DynamoDB a question got S3's answer -- silently, because a wrong
// service still replies.
//
// The dualstack form needs nothing: it inserts labels after the service rather
// than changing it, and a prefix match already reads only the leading ones.
func endpointHost(host string) string {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	host = strings.ToLower(host)
	// The marker counts only where a label ends with it, so a service whose own
	// name contains the letters is left alone.
	return strings.ReplaceAll(host, "-fips.", ".")
}

// hostLabel is the leading label of an endpoint host: the `guardduty` of
// `guardduty.us-east-1.amazonaws.com`. A dotted prefix like `api.ecr` is not
// this function's job -- serviceByHostPrefix matches those against the whole
// prefix the model records, which also carries the dualstack form that a
// region-stripping heuristic here could not. A bare host with no dots --
// `localhost`, an IP -- has no service in it, and returning it costs nothing
// because it will not match any endpoint prefix.
func hostLabel(host string) string {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	label, _, _ := strings.Cut(endpointHost(host), ".")
	return label
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

// How a request that names no AWS service finds the one it is for.
//
// C34 records what this replaced: eight hand-written predicates, each asking
// whether a host or a path CONTAINED a vendor's name. Every one of them took
// an AWS service the first time that name was also a common word. Vercel took
// aws.api.ecr and aws.iotwireless because the short name of `vercel.api` is
// the generic word `api`. Fly took aws.pinpoint because `/v1/apps` is also
// Pinpoint's GetApps. The collision always lived in the NEW service, so no test
// of the service that was taken could fail, and nobody found them by reading.
//
// The facts needed to answer without guessing are ones the vendor wrote down.
// An OpenAPI document declares its production host in `servers` and its
// operations' paths in `paths`, and both are now in the model. So a request is
// placed by matching the WHOLE host against a declared one, and otherwise by
// matching the path against the operation patterns the same way every AWS REST
// service is routed.
//
// Measured across every operation path the nine non-AWS models declare -- 838
// of them -- path matching alone names exactly one service for every single
// path: no ambiguity, no miss, nothing wrong. That is the whole finding. The
// four collisions that remain when AWS services are included are the
// Fly-against-Pinpoint one C34 names, and they do not arise here because these
// resolvers are only consulted for a request that named no AWS service in any
// of the three ways an AWS client names one -- an X-Amz-Target, a SigV4
// credential scope, or an AWS endpoint host. A request that declined to say it
// was for AWS is not answered by an AWS model.

// serviceByDeclaredHost matches a request's host against the hosts a
// specification declares.
//
// The match is the whole host or a subdomain of it, never a substring: the
// difference between `strings.Contains(host, "vercel")` and this is exactly
// the difference between claiming `api.iotwireless.us-east-1.amazonaws.com`
// and not. The subdomain form is load-bearing rather than defensive -- the
// Vercel KV document says in so many words that deployments address a per-store
// subdomain of the host it declares.
func (s *Server) serviceByDeclaredHost(host string) *model.Service {
	host = endpointHost(host)
	if host == "" {
		return nil
	}
	var best *model.Service
	var bestLen int
	for i := range s.bundle.Services {
		svc := &s.bundle.Services[i]
		if awsProvider(svc.ID) {
			continue
		}
		for _, declared := range svc.Hosts {
			declared = strings.ToLower(declared)
			if declared == "" {
				continue
			}
			if host != declared && !strings.HasSuffix(host, "."+declared) {
				continue
			}
			// The longest declared host wins, so a service reached at a
			// subdomain of another's host is not shadowed by the shorter one.
			if best == nil || len(declared) > bestLen || (len(declared) == bestLen && svc.ID < best.ID) {
				best, bestLen = svc, len(declared)
			}
		}
	}
	return best
}

// serviceByPath matches a request against the operation patterns of every
// non-AWS model, and answers only when exactly one service matches.
//
// Declining a tie is the point. Two services claiming one path is a fact about
// the models that a router cannot resolve from the request, and answering it
// with either one is the guess this exists to remove; falling through leaves
// the request to the rest of the demux, which fails visibly rather than
// replying as the wrong service. No provider path ties today, and a test
// measures that so a specification that introduces one is seen.
func (s *Server) serviceByPath(r *http.Request) *model.Service {
	// Parsed once rather than per service. This walks every operation of every
	// non-AWS model, so anything done per candidate is done several hundred
	// times for one request.
	query := r.URL.Query()
	parts := httpuri.SplitPath(r.URL.EscapedPath())
	var found *model.Service
	for i := range s.bundle.Services {
		svc := &s.bundle.Services[i]
		// Azure is out alongside AWS, for the mirror-image reason: AWS says
		// which service it means in the credential scope, and Azure says it in
		// the host and the query (`restype=container`, `comp=block`) -- never
		// in a path its operations own. Its one distinctive path shape,
		// `/{container}/{blob}`, is every two-segment path in the bundle, so
		// its models would claim half of them and tie with the rest. Its
		// requests are placed by the declared hosts alone; a host-less request
		// carrying `restype=container` is left for the rest of the demux rather
		// than claimed by a query sniff.
		if awsProvider(svc.ID) || azureProvider(svc.ID) {
			continue
		}
		if !matchesSomePath(svc, r.Method, parts, query) {
			continue
		}
		if found != nil {
			return nil // ambiguous: two models claim this path
		}
		found = svc
	}
	return found
}

// matchesSomePath reports whether any of a service's operations is bound to a
// path this request addresses.
//
// A pattern of `/` is not a path a service claims -- it is what a model carries
// for an operation that is addressed some other way, and this tree has two such
// services. Matching it would hand every unplaced root request to whichever of
// them came first.
func matchesSomePath(svc *model.Service, method string, parts []string, query url.Values) bool {
	for i := range svc.Operations {
		op := &svc.Operations[i]
		if op.HTTP.URI == "" || op.HTTP.URI == "/" || !strings.EqualFold(op.HTTP.Method, method) {
			continue
		}
		if httpuri.Compile(op.HTTP.URI).Claims(parts, query) {
			return true
		}
	}
	return false
}

// awsProvider reports whether a service id names an AWS service.
//
// The demux already distinguishes AWS from everything else -- `awsAddressed`
// asks the same question of a request -- because the three things an AWS client
// sends to say which service it means are AWS-specific and nothing else sends
// them. Reading the provider off the id is the cheapest honest way to ask it of
// a service: the id is `<provider>.<service>` by construction, and every
// receiver builds it from where the document sits.
//
// Not `awsService`, which is what this was called for about ten minutes. A
// function in this package whose name is a provider followed by `Service` or
// `Request` is what MeasureDemuxGuesses counts, and it counted this one -- so
// a helper written to REMOVE the guesses would have read as a ninth. C32
// records the same shape from the other side: a generic word in a name is a
// collision waiting for the metric that reads names.
func awsProvider(id string) bool {
	provider, _, ok := strings.Cut(id, ".")
	return ok && provider == "aws"
}

// azureProvider reads the provider off the id the same way awsProvider does.
// It exists for path resolution, where Azure's models must not take part, and
// it is deliberately not named after the provider-plus-Request/Service shape
// MeasureDemuxGuesses counts.
func azureProvider(id string) bool {
	provider, _, ok := strings.Cut(id, ".")
	return ok && provider == "azure"
}
