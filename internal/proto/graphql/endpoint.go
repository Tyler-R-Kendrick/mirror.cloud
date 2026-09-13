package graphql

import (
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// endpoint is the path this service's operations are bound to.
//
// Every operation of a GraphQL service shares one path -- that is what makes
// the document, rather than the URL, the thing that selects an operation -- so
// the first one that declares a URI answers for all of them. The receiver sets
// it from where the schema was fetched, which is the only place the path is
// recorded: an introspection result does not carry the endpoint it was served
// from.
func endpoint(svc *model.Service) string {
	if svc == nil {
		return ""
	}
	for i := range svc.Operations {
		if uri := svc.Operations[i].HTTP.URI; uri != "" {
			return trimSlash(uri)
		}
	}
	return ""
}

func trimSlash(p string) string {
	if p != "/" {
		p = strings.TrimSuffix(p, "/")
	}
	return p
}

func path(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	return r.URL.Path
}

// servedHere reports whether a request arrived at this service's endpoint.
//
// It is an exact path match, not a prefix or a substring. A substring test is
// what the code this replaces did, and it is the same mistake one layer up from
// the routing bug it sat beside: `/graphql/v2-staging` and
// `/tenant/graphql/v2` both contain the endpoint and neither is it.
//
// A service whose model declares no path at all is served wherever it is
// reached. That is not a gap being papered over -- it is what a schema with no
// provenance URL can honestly say -- and the layer above has already decided
// the request belongs to this service.
func servedHere(svc *model.Service, r *http.Request) bool {
	want := endpoint(svc)
	if want == "" {
		return true
	}
	return trimSlash(path(r)) == want
}
