package edge

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
)

// TestEveryDeclaredProviderPathResolvesUniquely is the measurement that
// unblocked C34, kept as a gate.
//
// It asks the whole question at once: take every operation path every non-AWS
// model declares, address it the way a client with no AWS credentials would --
// a host that names nothing -- and check the demux answers with the service
// whose model declared it. Eight hundred and thirty-eight paths, and every one
// of them resolves to exactly one service.
//
// That number is why the eight substring predicates could go. The critique had
// recorded the opposite conclusion: that path resolution could not work because
// S3 claims every path. S3 does claim `/{Bucket}/{Key+}`, but a request that
// named no AWS service is never answered by an AWS model, and with AWS excluded
// there is no contention left to resolve -- not one ambiguity, not one miss.
//
// Failing here means a specification has introduced a path two provider models
// both claim. The demux declines such a tie rather than guessing, so the
// symptom in production is a request reaching nothing; the fix is to look at
// the two documents, not to add a predicate.
func TestEveryDeclaredProviderPathResolvesUniquely(t *testing.T) {
	b := specboot.Bundle()
	server := &Server{bundle: b}
	tested := 0
	for i := range b.Services {
		svc := &b.Services[i]
		if awsProvider(svc.ID) {
			continue
		}
		for j := range svc.Operations {
			op := &svc.Operations[j]
			// `/` is what a model carries for an operation addressed some
			// other way; matchesSomePath declines it and so does this.
			if op.HTTP.URI == "" || op.HTTP.URI == "/" || op.HTTP.Method == "" {
				continue
			}
			tested++
			path := instantiate(op.HTTP.URI)
			req := httptest.NewRequest(op.HTTP.Method, "http://mirror.invalid"+path, nil)
			got := server.demux(req)
			if got == nil {
				t.Errorf("%s %s %s: reached no service, but %s declares it",
					svc.ID, op.HTTP.Method, path, svc.ID)
				continue
			}
			if got.ID != svc.ID {
				t.Errorf("%s %s %s: reached %s", svc.ID, op.HTTP.Method, path, got.ID)
			}
		}
	}
	// A floor, so that a model that stopped loading turns this green-by-vacuum
	// into a failure. The measured count was 838.
	if tested < 800 {
		t.Errorf("only %d provider paths were tested; the models declare about 838, "+
			"so something is not loading", tested)
	}
}

// instantiate turns a URI pattern into a path a client would send, by giving
// each label one segment and dropping the query the pattern may require.
func instantiate(uri string) string {
	p, _, _ := strings.Cut(uri, "?")
	var out []string
	for _, seg := range strings.Split(strings.Trim(p, "/"), "/") {
		switch {
		case seg == "":
		case strings.HasPrefix(seg, "{"):
			out = append(out, "x")
		default:
			out = append(out, seg)
		}
	}
	return "/" + strings.Join(out, "/")
}

// TestDeclaredHostsPlaceTheirService checks the other half: a request to the
// host a specification declares reaches that specification's service.
//
// It matters most where the path cannot answer. Vercel KV's whole surface is
// `POST /`, which belongs to no one, so the declared host `kv.vercel-storage.com`
// is the only thing that distinguishes it -- and the document says deployments
// address a per-store subdomain of it, which is why the subdomain form is
// tested rather than assumed.
func TestDeclaredHostsPlaceTheirService(t *testing.T) {
	b := specboot.Bundle()
	server := &Server{bundle: b}
	declared := 0
	for i := range b.Services {
		svc := &b.Services[i]
		if awsProvider(svc.ID) {
			continue
		}
		for _, host := range svc.Hosts {
			declared++
			for _, h := range []string{host, "store-abc." + host, strings.ToUpper(host), host + ":8443"} {
				req := httptest.NewRequest(http.MethodPost, "http://"+strings.ToLower(host)+"/", nil)
				req.Host = h
				got := server.demux(req)
				if got == nil || got.ID != svc.ID {
					t.Errorf("%s declares host %q; a request to %q reached %v",
						svc.ID, host, h, serviceID(got))
				}
			}
		}
	}
	if declared < 7 {
		t.Errorf("only %d declared hosts were found in the served model; the "+
			"seven OpenAPI documents each declare one, so the receiver or "+
			"adoptGenerated has stopped carrying them", declared)
	}
}

// TestADeclaredHostIsMatchedWholeNotAsASubstring is the anti-substring
// property, asserted rather than assumed.
//
// Every predicate this PR deleted asked `strings.Contains`, and the whole
// claim of the replacement is that it does not. `Contains` would be satisfied
// by a host that merely carries a declared one inside it, so
// `api.vercel.com.example.invalid` would be answered as Vercel -- a host under
// somebody else's domain entirely. Exact-or-subdomain is the difference, and
// without this test a rewrite back to `Contains` passes everything: the
// mutation harness found exactly that hole here.
func TestADeclaredHostIsMatchedWholeNotAsASubstring(t *testing.T) {
	b := specboot.Bundle()
	server := &Server{bundle: b}
	checked := 0
	for i := range b.Services {
		svc := &b.Services[i]
		for _, host := range svc.Hosts {
			for _, impostor := range []string{
				host + ".example.invalid", // the declared host is a PREFIX
				"not-" + host,             // and here a suffix of a longer label
				host + "x",
			} {
				checked++
				req := httptest.NewRequest(http.MethodGet, "http://"+impostor+"/v1/whatever", nil)
				req.Host = impostor
				if got := server.serviceByDeclaredHost(impostor); got != nil {
					t.Errorf("%q is not %q nor a subdomain of it, but it resolved to %s",
						impostor, host, got.ID)
				}
				_ = req
			}
		}
	}
	if checked == 0 {
		t.Error("no declared hosts to test against; the model has stopped carrying them")
	}
}

// TestAWSServicesAreNotAnsweredByProviderModels pins the guard the whole
// approach rests on.
//
// `/v1/apps` is Fly's app list and AWS Pinpoint's GetApps, and it is the one
// path where two models genuinely collide. Which one answers is decided before
// any path is looked at: a request carrying a SigV4 credential scope naming
// Pinpoint has said which service it is for, so the provider resolvers are not
// consulted at all. This is the collision C34 names, and it is the reason the
// resolvers are scoped rather than general.
func TestAWSServicesAreNotAnsweredByProviderModels(t *testing.T) {
	server := &Server{bundle: specboot.Bundle()}

	signed := httptest.NewRequest(http.MethodGet, "http://localhost/v1/apps", nil)
	signed.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/pinpoint/aws4_request, Signature=x")
	if got := server.demux(signed); got == nil || got.ID != "aws.pinpoint" {
		t.Errorf("a request signed for pinpoint reached %v, want aws.pinpoint", serviceID(got))
	}

	unsigned := httptest.NewRequest(http.MethodGet, "http://localhost/v1/apps", nil)
	if got := server.demux(unsigned); got == nil || got.ID != "fly.machines" {
		t.Errorf("an unsigned request to /v1/apps reached %v, want fly.machines", serviceID(got))
	}
}
