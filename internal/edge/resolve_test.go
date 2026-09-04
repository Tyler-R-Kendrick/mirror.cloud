package edge

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
)

// sdkRequest is the request an SDK makes: the service's regional endpoint as
// the Host, and the service's own name inside the SigV4 credential scope. No
// Action, no X-Amz-Target -- those are protocol details, and a REST service
// sends neither.
func sdkRequest(svc *model.Service) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Host = svc.EndpointPrefix + ".us-east-1.amazonaws.com"
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=t/20200101/us-east-1/"+
		svc.EndpointPrefix+"/aws4_request, SignedHeaders=host, Signature=0")
	return r
}

// TestEveryServiceIsReachableTheWayAnSDKAddressesIt is the gate this package
// did not have. Addressed this way, a hundred and forty-eight of the hundred
// and fifty-two services in the bundle used to resolve to `aws.s3`: the chain
// of per-service branches sat inside `if action != ""`, which only a
// query-protocol request satisfies, so every REST-shaped request fell through
// to the path-style S3 default at the bottom.
//
// Nothing said so, because every booted-server test called its service the way
// the demux happened to accept rather than the way a client does.
func TestEveryServiceIsReachableTheWayAnSDKAddressesIt(t *testing.T) {
	bundle := catalog.Bundle()
	server := &Server{bundle: bundle}
	var wrong, missing []string
	for i := range bundle.Services {
		svc := &bundle.Services[i]
		if svc.EndpointPrefix == "" {
			continue
		}
		got := server.demux(sdkRequest(svc))
		switch {
		case got == nil:
			missing = append(missing, svc.ID)
		case got.ID != svc.ID:
			// A prefix several services share cannot be told apart from the
			// credential scope alone, and the host carries the same prefix.
			// Those are named by TestSharedEndpointPrefixesAreNamed.
			if len(sharing(bundle, svc.EndpointPrefix)) > 1 {
				continue
			}
			wrong = append(wrong, svc.ID+" -> "+got.ID)
		}
	}
	sort.Strings(wrong)
	sort.Strings(missing)
	for _, w := range wrong {
		t.Errorf("resolved to the wrong service: %s", w)
	}
	for _, m := range missing {
		t.Errorf("not reachable at its own endpoint: %s", m)
	}
}

// TestSharedEndpointPrefixesAreNamed pins the services a request cannot
// distinguish from the credential scope alone, because they declare the same
// endpoint prefix. All five are a service and its successor sharing one
// endpoint, which is what AWS actually does.
//
// It reads the bundle the runtime boots rather than the catalog. An earlier
// version of this test read the catalog, which declared no shared prefix at
// all, and so passed while `aws.apigatewayv2` was answering as
// `aws.apigateway`. That is the same mistake as the one that hid the original
// routing disagreement: a test written against a description of the system
// other than the one the system uses.
func TestSharedEndpointPrefixesAreNamed(t *testing.T) {
	bundle := specboot.Bundle()
	want := map[string][]string{
		"apigateway":       {"aws.apigateway", "aws.apigatewayv2"},
		"email":            {"aws.ses", "aws.sesv2"},
		"es":               {"aws.elasticsearch", "aws.es"},
		"kinesisanalytics": {"aws.kinesisanalytics", "aws.kinesisanalyticsv2"},
		"rds":              {"aws.docdb", "aws.neptune", "aws.rds"},
	}
	got := map[string][]string{}
	seen := map[string]bool{}
	for i := range bundle.Services {
		prefix := bundle.Services[i].EndpointPrefix
		if prefix == "" || seen[prefix] {
			continue
		}
		seen[prefix] = true
		if ids := sharing(bundle, prefix); len(ids) > 1 {
			got[prefix] = ids
		}
	}
	if len(got) != len(want) {
		t.Fatalf("shared endpoint prefixes changed: %v, want %v", got, want)
	}
	for prefix, ids := range want {
		if len(got[prefix]) != len(ids) {
			t.Errorf("%s is shared by %v, want %v", prefix, got[prefix], ids)
			continue
		}
		for i := range ids {
			if got[prefix][i] != ids[i] {
				t.Errorf("%s is shared by %v, want %v", prefix, got[prefix], ids)
				break
			}
		}
	}
	// Every one of them must be reachable at the endpoint it shares, by a
	// request only its own model claims. That is the route that matters: it is
	// how AWS separates them, and it is the one that was broken --
	// `aws.apigatewayv2` answered as `aws.apigateway.GetRestApis`.
	//
	// Three services cannot be reached that way at all. DocumentDB and Neptune
	// are forks of the RDS API with the same action names on the same endpoint
	// prefix, so no request distinguishes them; a client reaches them at
	// `docdb.<region>.amazonaws.com`, which the specification does not record.
	// They are named here rather than excused silently, and each must still be
	// reachable at its own host. RDS itself is not on that list: it is the
	// namesake of the prefix the three share, so `rds` still reaches it -- and
	// this test fails if an entry stops being needed, which is how that was
	// caught.
	indistinguishable := map[string]string{
		"aws.docdb":   "a fork of the RDS API: same actions, same endpoint prefix",
		"aws.neptune": "a fork of the RDS API: same actions, same endpoint prefix",
	}
	server := &Server{bundle: bundle}
	for _, ids := range want {
		for _, id := range ids {
			svc := bundle.ServiceByID(id)
			if svc == nil {
				t.Fatalf("%s is not in the bundle", id)
			}
			r, ok := claimingRequest(svc)
			if !ok {
				t.Errorf("%s declares no operation a request can name it by", id)
				continue
			}
			r.Host = svc.EndpointPrefix + ".us-east-1.amazonaws.com"
			got := server.demux(r)
			if got != nil && got.ID == id {
				if why, listed := indistinguishable[id]; listed {
					t.Errorf("%s is reachable at the endpoint it shares after all; "+
						"drop it from the list (%q)", id, why)
				}
				continue
			}
			if _, listed := indistinguishable[id]; !listed {
				t.Errorf("%s is unreachable at the endpoint it shares: a request "+
					"only it claims resolved to %s", id, serviceID(got))
				continue
			}
			byHost := httptest.NewRequest(http.MethodPost, "/", nil)
			byHost.Host = shortName(id) + ".us-east-1.amazonaws.com"
			if got := server.demux(byHost); got == nil || got.ID != id {
				t.Errorf("%s is reachable by nothing: no request names it and its "+
					"own host resolved to %s", id, serviceID(got))
			}
		}
	}
	for id := range indistinguishable {
		if bundle.ServiceByID(id) == nil {
			t.Errorf("%s is listed as indistinguishable and is not in the bundle", id)
		}
	}
}

// serviceID names a service for a failure message without printing the whole
// model, which for RDS is several hundred kilobytes.
func serviceID(svc *model.Service) string {
	if svc == nil {
		return "nothing"
	}
	return svc.ID
}

// claimingRequest builds a request only one of the services sharing an
// endpoint can claim: a target under its own prefix, its own Action, or a path
// only its model binds.
func claimingRequest(svc *model.Service) (*http.Request, bool) {
	for i := range svc.Operations {
		op := &svc.Operations[i]
		switch svc.Protocol {
		case model.ProtoAWSJSON10, model.ProtoAWSJSON11:
			if svc.TargetPrefix == "" {
				continue
			}
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("X-Amz-Target", svc.TargetPrefix+"."+op.Name)
			return r, true
		case model.ProtoAWSQuery, model.ProtoEC2Query:
			action := op.QueryAction
			if action == "" {
				action = op.Name
			}
			return httptest.NewRequest(http.MethodPost, "/?Action="+action, nil), true
		default:
			uri := op.HTTP.URI
			if uri == "" || strings.ContainsAny(uri, "{}?") {
				continue
			}
			method := op.HTTP.Method
			if method == "" {
				method = http.MethodPost
			}
			return httptest.NewRequest(method, uri, nil), true
		}
	}
	return nil, false
}

// TestTheHostWinsOverTheCredentialScope states the mechanism that tells apart
// services sharing an endpoint prefix. The credential scope is the same for
// DocumentDB, Neptune and RDS -- all three sign as `rds` -- and only the host
// says which one a client meant.
//
// The two agree for every service in today's bundle, so nothing else here
// would notice the host being ignored.
func TestTheHostWinsOverTheCredentialScope(t *testing.T) {
	bundle := catalog.Bundle()
	server := &Server{bundle: bundle}
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Host = "guardduty.us-east-1.amazonaws.com"
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=t/20200101/us-east-1/sqs/aws4_request")
	if got := server.demux(r); got == nil || got.ID != "aws.guardduty" {
		t.Fatalf("host and scope disagree: resolved %#v, want aws.guardduty", got)
	}
	// With no service in the host, the scope still answers.
	r = httptest.NewRequest(http.MethodPost, "/", nil)
	r.Host = "localhost:4566"
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=t/20200101/us-east-1/sqs/aws4_request")
	if got := server.demux(r); got == nil || got.ID != "aws.sqs" {
		t.Fatalf("scope alone: resolved %#v, want aws.sqs", got)
	}
}

// TestASharedPrefixResolvesTheSameWayEveryTime covers the tie-break with a
// bundle built for it, since the booted bundle has no shared prefix yet. What
// matters is not which service wins but that the same one always does: a
// tie-break that reads the bundle's order makes the answer depend on the order
// specifications happen to be ingested in.
func TestASharedPrefixResolvesTheSameWayEveryTime(t *testing.T) {
	forward := &Server{bundle: &model.Bundle{Services: []model.Service{
		{ID: "aws.docdb", EndpointPrefix: "rds"},
		{ID: "aws.neptune", EndpointPrefix: "rds"},
		{ID: "aws.rds", EndpointPrefix: "rds"},
	}}}
	reversed := &Server{bundle: &model.Bundle{Services: []model.Service{
		{ID: "aws.rds", EndpointPrefix: "rds"},
		{ID: "aws.neptune", EndpointPrefix: "rds"},
		{ID: "aws.docdb", EndpointPrefix: "rds"},
	}}}
	a, b := forward.serviceByLabel(nil, "rds"), reversed.serviceByLabel(nil, "rds")
	if a == nil || b == nil {
		t.Fatalf("a shared prefix resolved to nothing: %#v %#v", a, b)
	}
	if a.ID != b.ID {
		t.Fatalf("the answer depends on bundle order: %s and %s", a.ID, b.ID)
	}
	// One of the three is named `rds`, and its own name wins outright.
	if a.ID != "aws.rds" {
		t.Fatalf("`rds` resolved to %s, want the service of that name", a.ID)
	}
	// With none of them named by the label the sort decides, and it must
	// decide the same way whichever order the bundle lists them in.
	noNamesake := func(services ...model.Service) *Server {
		return &Server{bundle: &model.Bundle{Services: services}}
	}
	one := noNamesake(
		model.Service{ID: "aws.sesv2", EndpointPrefix: "email"},
		model.Service{ID: "aws.ses", EndpointPrefix: "email"},
	)
	other := noNamesake(
		model.Service{ID: "aws.ses", EndpointPrefix: "email"},
		model.Service{ID: "aws.sesv2", EndpointPrefix: "email"},
	)
	x, y := one.serviceByLabel(nil, "email"), other.serviceByLabel(nil, "email")
	if x == nil || y == nil || x.ID != y.ID {
		t.Fatalf("the answer depends on bundle order: %#v and %#v", x, y)
	}
	if x.ID != "aws.ses" {
		t.Fatalf("`email` resolved to %s, want the alphabetically first", x.ID)
	}
	// Each is still reachable by its own name, which is what makes the
	// tie-break tolerable rather than a silent misroute.
	for _, id := range []string{"aws.docdb", "aws.neptune", "aws.rds"} {
		if got := forward.serviceByLabel(nil, shortName(id)); got == nil || got.ID != id {
			t.Errorf("%s is not reachable by its own name: %#v", id, got)
		}
	}
}

// TestEveryServiceIsReachableByItsOwnName covers the label that is neither the
// endpoint prefix nor a signing name: the service's own ID. `aws.access-analyzer`
// is reached at `accessanalyzer` and named `access-analyzer`, and a client
// pointed at the emulator by service name uses the latter.
//
// Services whose own name is also another service's is the shared case, and
// TestSharedEndpointPrefixesAreNamed covers those.
func TestEveryServiceIsReachableByItsOwnName(t *testing.T) {
	bundle := specboot.Bundle()
	server := &Server{bundle: bundle}
	checked := 0
	for i := range bundle.Services {
		svc := &bundle.Services[i]
		name := shortName(svc.ID)
		if len(servicesAnsweringTo(bundle, name)) > 1 {
			continue
		}
		if got := server.serviceByLabel(nil, name); got == nil || got.ID != svc.ID {
			t.Errorf("%s is not reachable by its own name %q: resolved %s",
				svc.ID, name, serviceID(got))
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no service was checked")
	}
	t.Logf("%d services reachable by their own name", checked)
}

// TestAServiceIsReachableBySigningName covers the name a client actually
// writes into the credential scope. For fifteen of the services served here
// that is not the endpoint prefix: Lex Model Building signs as `lex` and is
// reached at `models.lex`, ECR signs as `ecr` and is reached at `api.ecr`.
//
// Matching only the endpoint prefix left those unreachable by the header every
// SDK sends, which is how `aws.lex-models` answered "unknown service" to its
// own booted test.
func TestAServiceIsReachableBySigningName(t *testing.T) {
	bundle := specboot.Bundle()
	server := &Server{bundle: bundle}
	checked := 0
	for i := range bundle.Services {
		svc := &bundle.Services[i]
		for _, alias := range svc.Aliases {
			// An alias several services answer to is settled by the request,
			// which TestSharedEndpointPrefixesAreNamed covers; here the point
			// is that the name reaches the service at all.
			if len(servicesAnsweringTo(bundle, alias)) > 1 {
				continue
			}
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=t/20200101/us-east-1/"+alias+"/aws4_request")
			if got := server.demux(r); got == nil || got.ID != svc.ID {
				t.Errorf("%s signs as %q and a request scoped to it resolved to %s",
					svc.ID, alias, serviceID(got))
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no service carries a signing name of its own; nothing was checked")
	}
	t.Logf("%d signing names checked", checked)
}

// servicesAnsweringTo lists every service a label reaches by prefix or alias,
// ignoring which of them a request would pick.
func servicesAnsweringTo(bundle *model.Bundle, label string) []string {
	var ids []string
	for i := range bundle.Services {
		svc := &bundle.Services[i]
		if shortName(svc.ID) == label || answersTo(svc, label) {
			ids = append(ids, svc.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// TestEveryClientSpellingIsLoadBearing is the third property an exemption
// needs. Both entries name a service that exists and say why; this is the one
// that reports an entry when it stops applying, so the table cannot grow
// branches that defend nothing.
func TestEveryClientSpellingIsLoadBearing(t *testing.T) {
	bundle := catalog.Bundle()
	server := &Server{bundle: bundle}
	bare := &Server{bundle: bundle}
	for label, id := range clientSpellings {
		if bundle.ServiceByID(id) == nil {
			t.Errorf("%q spells %s, which the bundle does not have", label, id)
			continue
		}
		if got := server.serviceByLabel(nil, label); got == nil || got.ID != id {
			t.Errorf("%q did not resolve to %s: %#v", label, id, got)
		}
		// Without the table the label must resolve to something else, or to
		// nothing. If it already resolves correctly the entry is dead weight.
		delete(clientSpellings, label)
		got := bare.serviceByLabel(nil, label)
		clientSpellings[label] = id
		if got != nil && got.ID == id {
			t.Errorf("%q resolves to %s from the model alone; the entry excuses "+
				"nothing and should be dropped", label, id)
		}
	}
}

// TestCredentialScopeService reads the service out of a SigV4 header, which is
// the one place a request names its service independently of how the client
// was pointed at it.
func TestCredentialScopeService(t *testing.T) {
	for _, tc := range []struct{ header, want string }{
		{"AWS4-HMAC-SHA256 Credential=AK/20200101/us-east-1/guardduty/aws4_request, SignedHeaders=host, Signature=0", "guardduty"},
		{"AWS4-HMAC-SHA256 Credential=AK/20200101/us-east-1/GuardDuty/aws4_request", "guardduty"},
		{"AWS4-HMAC-SHA256 Credential=AK/20200101/us-east-1/s3/aws4_request", "s3"},
		{"AWS4-HMAC-SHA256 Credential=AK/20200101/us-east-1", ""},
		{"Bearer token", ""},
		{"", ""},
	} {
		if got := credentialScopeService(tc.header); got != tc.want {
			t.Errorf("credentialScopeService(%q) = %q, want %q", tc.header, got, tc.want)
		}
	}
}

// TestHostLabel takes the leading label of an endpoint host, and nothing from
// a host that has no service in it.
func TestHostLabel(t *testing.T) {
	for _, tc := range []struct{ host, want string }{
		{"guardduty.us-east-1.amazonaws.com", "guardduty"},
		{"GuardDuty.us-east-1.amazonaws.com:443", "guardduty"},
		{"localhost:4566", "localhost"},
		{"127.0.0.1:4566", "127"},
		{"", ""},
	} {
		if got := hostLabel(tc.host); got != tc.want {
			t.Errorf("hostLabel(%q) = %q, want %q", tc.host, got, tc.want)
		}
	}
}

// sharing lists every service in the bundle declaring one endpoint prefix.
func sharing(bundle *model.Bundle, prefix string) []string {
	var ids []string
	for i := range bundle.Services {
		if bundle.Services[i].EndpointPrefix == prefix {
			ids = append(ids, bundle.Services[i].ID)
		}
	}
	sort.Strings(ids)
	return ids
}
