package edge

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
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
// endpoint prefix. The booted bundle declares none today.
//
// It will. DocumentDB and Neptune are forks of the RDS API and the vendored
// specifications name `rds` as the endpoint prefix for all three; both SES
// versions name `email`. Booting from the generated models therefore makes
// this test fail, which is the point: the ambiguity has to be dealt with in
// the open rather than absorbed by the gate above, which excuses exactly the
// prefixes this test names.
//
// A client reaches those services at a host carrying the service's own name,
// which is why the host label is consulted before the credential scope, and
// the second half of this test states that.
func TestSharedEndpointPrefixesAreNamed(t *testing.T) {
	bundle := catalog.Bundle()
	want := map[string][]string{}
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
	// Each of them is still reachable by its own name, which is the whole
	// reason the host label is preferred.
	server := &Server{bundle: bundle}
	for _, ids := range want {
		for _, id := range ids {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Host = shortName(id) + ".us-east-1.amazonaws.com"
			if got := server.demux(r); got == nil || got.ID != id {
				t.Errorf("%s is not reachable at its own host: %#v", id, got)
			}
		}
	}
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
	a, b := forward.serviceByLabel("rds"), reversed.serviceByLabel("rds")
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
	x, y := one.serviceByLabel("email"), other.serviceByLabel("email")
	if x == nil || y == nil || x.ID != y.ID {
		t.Fatalf("the answer depends on bundle order: %#v and %#v", x, y)
	}
	if x.ID != "aws.ses" {
		t.Fatalf("`email` resolved to %s, want the alphabetically first", x.ID)
	}
	// Each is still reachable by its own name, which is what makes the
	// tie-break tolerable rather than a silent misroute.
	for _, id := range []string{"aws.docdb", "aws.neptune", "aws.rds"} {
		if got := forward.serviceByLabel(shortName(id)); got == nil || got.ID != id {
			t.Errorf("%s is not reachable by its own name: %#v", id, got)
		}
	}
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
		if got := server.serviceByLabel(label); got == nil || got.ID != id {
			t.Errorf("%q did not resolve to %s: %#v", label, id, got)
		}
		// Without the table the label must resolve to something else, or to
		// nothing. If it already resolves correctly the entry is dead weight.
		delete(clientSpellings, label)
		got := bare.serviceByLabel(label)
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
