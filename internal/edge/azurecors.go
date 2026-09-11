package edge

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Azure Storage CORS and credential parsing, at the edge because both are
// cross-cutting: preflight short-circuits before routing, CORS response
// headers apply to every answer including faults, and credential presence is
// checked before dispatch. SharedKey is parsed, never HMAC-verified (the
// standing plan decision); SAS is parsed and checked for expiry, not-before,
// and coarse method-class permissions, never HMAC-verified.

func isAzureStorage(id string) bool {
	return id == "azure.blobs" || id == "azure.queue" || id == "azure.table"
}

// azureCorsRule is one parsed rule from the stored StorageServiceProperties.
type azureCorsRule struct {
	origins  []string
	methods  []string
	headers  []string
	exposed  []string
	maxAge   int
	wildcard bool
}

// azureCorsRulesFor reads the service properties the account stored via Set
// Service Properties and parses its CORS rules. No stored properties, or no
// <Cors> content, means CORS is not enabled.
func (s *Server) azureCorsRulesFor(ctx context.Context, id spi.Identity, svcID string) []azureCorsRule {
	coll := ""
	switch svcID {
	case "azure.blobs":
		coll = "azacct"
	case "azure.queue":
		coll = "azqacct"
	}
	if coll == "" {
		return nil
	}
	raw, found, err := s.deps.Store.Scope(id.Account, id.Region).Collection(coll).Get(ctx, "default")
	if err != nil || !found {
		return nil
	}
	var rec map[string]any
	if json.Unmarshal(raw, &rec) != nil {
		return nil
	}
	props, _ := rec["properties"].(string)
	var doc struct {
		Rules []struct {
			AllowedOrigins  string `xml:"AllowedOrigins"`
			AllowedMethods  string `xml:"AllowedMethods"`
			AllowedHeaders  string `xml:"AllowedHeaders"`
			ExposedHeaders  string `xml:"ExposedHeaders"`
			MaxAgeInSeconds int    `xml:"MaxAgeInSeconds"`
		} `xml:"Cors>CorsRule"`
	}
	if props == "" || xml.Unmarshal([]byte(props), &doc) != nil {
		return nil
	}
	split := func(s string) []string {
		var out []string
		for _, p := range strings.Split(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	var rules []azureCorsRule
	for _, r := range doc.Rules {
		rule := azureCorsRule{
			origins: split(r.AllowedOrigins),
			methods: split(r.AllowedMethods),
			headers: split(r.AllowedHeaders),
			exposed: split(r.ExposedHeaders),
			maxAge:  r.MaxAgeInSeconds,
		}
		for _, o := range rule.origins {
			if o == "*" {
				rule.wildcard = true
			}
		}
		rules = append(rules, rule)
	}
	return rules
}

// azureCorsMatch finds the first rule covering the origin, method, and
// (for preflight) request headers.
func azureCorsMatch(rules []azureCorsRule, origin, method, reqHeaders string) *azureCorsRule {
	originOK := func(rule *azureCorsRule) bool {
		for _, o := range rule.origins {
			if o == "*" || o == origin {
				return true
			}
			if strings.Contains(o, "*") {
				prefix, suffix, _ := strings.Cut(o, "*")
				if strings.HasPrefix(origin, prefix) && strings.HasSuffix(origin, suffix) {
					return true
				}
			}
		}
		return false
	}
	for i := range rules {
		rule := &rules[i]
		if !originOK(rule) {
			continue
		}
		methodOK := false
		for _, m := range rule.methods {
			if strings.EqualFold(m, method) {
				methodOK = true
			}
		}
		if !methodOK {
			continue
		}
		headersOK := true
		if reqHeaders != "" {
			allowed := map[string]bool{}
			for _, h := range rule.headers {
				allowed[strings.ToLower(h)] = true
			}
			if len(allowed) > 0 {
				for _, h := range strings.Split(reqHeaders, ",") {
					if !allowed[strings.ToLower(strings.TrimSpace(h))] {
						headersOK = false
					}
				}
			}
		}
		if headersOK {
			return rule
		}
	}
	return nil
}

// azureCorsPreflight answers an OPTIONS preflight Azurite-shaped: a matching
// rule is a 200 with the allow headers; anything else is 403
// CorsPreflightFailure.
func (s *Server) azureCorsPreflight(w http.ResponseWriter, r *http.Request, id spi.Identity, svc *model.Service, rid string) {
	fail := func() {
		s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: "Options"}, &spi.Fault{
			Code:       "CorsPreflightFailure",
			Message:    "CORS not enabled or no matching rule found for this request.",
			HTTPStatus: http.StatusForbidden,
			Fault:      "client",
		}, rid)
	}
	origin := r.Header.Get("Origin")
	method := r.Header.Get("Access-Control-Request-Method")
	if origin == "" || method == "" {
		fail()
		return
	}
	rule := azureCorsMatch(s.azureCorsRulesFor(r.Context(), id, svc.ID), origin, method, r.Header.Get("Access-Control-Request-Headers"))
	if rule == nil {
		fail()
		return
	}
	if rule.wildcard {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
	}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(rule.methods, ", "))
	if len(rule.headers) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(rule.headers, ", "))
	}
	if rule.maxAge > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(rule.maxAge))
	}
	w.WriteHeader(http.StatusOK)
}

// azureCorsResponseWriter injects CORS response headers into every answer,
// faults included, exactly once.
type azureCorsResponseWriter struct {
	http.ResponseWriter
	headers http.Header
	wrote   bool
}

func (w *azureCorsResponseWriter) WriteHeader(status int) {
	if !w.wrote {
		w.wrote = true
		for k, vs := range w.headers {
			for _, v := range vs {
				w.ResponseWriter.Header().Add(k, v)
			}
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *azureCorsResponseWriter) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.ResponseWriter.Write(b)
}

// azureCorsResponseHeaders computes the headers an actual (non-preflight)
// request with an Origin gets from the matching rule, or nil when no rule
// matches (Azurite adds nothing then).
func (s *Server) azureCorsResponseHeaders(r *http.Request, id spi.Identity, svcID string) http.Header {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil
	}
	rule := azureCorsMatch(s.azureCorsRulesFor(r.Context(), id, svcID), origin, r.Method, "")
	if rule == nil {
		return nil
	}
	h := http.Header{}
	if rule.wildcard {
		h.Set("Access-Control-Allow-Origin", "*")
	} else {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Vary", "Origin")
	}
	if len(rule.exposed) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(rule.exposed, ", "))
	}
	return h
}

// azureAuthFault parses the request's credential. It never verifies an HMAC;
// it enforces presence, the SharedKey account, SAS expiry/not-before, and
// coarse method-class SAS permissions.
func azureAuthFault(r *http.Request, now time.Time) *spi.Fault {
	failed := func(detail string) *spi.Fault {
		return &spi.Fault{
			Code:       "AuthenticationFailed",
			Message:    "Server failed to authenticate the request. Make sure the value of the Authorization header is formed correctly including the signature. " + detail,
			HTTPStatus: http.StatusForbidden,
			Fault:      "client",
		}
	}
	q := r.URL.Query()
	if q.Get("sig") != "" {
		if sp := q.Get("sp"); sp != "" {
			if need, ok := azureSASPermissionClass(r); ok {
				have := false
				for _, letter := range need {
					if strings.Contains(sp, letter) {
						have = true
					}
				}
				if !have {
					return &spi.Fault{
						Code:       "AuthorizationPermissionMismatch",
						Message:    "This request is not authorized to perform this operation using this permission.",
						HTTPStatus: http.StatusForbidden,
						Fault:      "client",
					}
				}
			}
		}
		if se := q.Get("se"); se != "" {
			if t, err := time.Parse(time.RFC3339, se); err == nil && now.After(t) {
				return failed("Signature not valid in the specified time frame.")
			}
		}
		if st := q.Get("st"); st != "" {
			if t, err := time.Parse(time.RFC3339, st); err == nil && now.Before(t) {
				return failed("Signature not valid in the specified time frame.")
			}
		}
		return nil
	}
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return failed("")
	}
	if strings.HasPrefix(auth, "SharedKey ") || strings.HasPrefix(auth, "SharedKeyLite ") {
		cred := strings.TrimPrefix(strings.TrimPrefix(auth, "SharedKeyLite "), "SharedKey ")
		acct, _, _ := strings.Cut(cred, ":")
		host := r.Host
		if i := strings.IndexByte(host, '.'); i > 0 {
			if want := host[:i]; acct != "" && acct != want {
				return failed("The account name in the credential does not match the request.")
			}
		}
	}
	return nil
}

// azureSASPermissionClass maps the request's method to the SAS permission
// letters that satisfy it, per service family: blob racwdl, queue raup,
// table raud. One matching letter is enough.
func azureSASPermissionClass(r *http.Request) ([]string, bool) {
	host := r.Host
	switch {
	case strings.Contains(host, ".table."):
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			return []string{"r"}, true
		case http.MethodPost:
			return []string{"a"}, true
		case http.MethodPut, http.MethodPatch:
			return []string{"u"}, true
		case http.MethodDelete:
			return []string{"d"}, true
		}
	case strings.Contains(host, ".queue."):
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			return []string{"r", "p"}, true
		case http.MethodPost:
			return []string{"a"}, true
		case http.MethodPut:
			return []string{"u"}, true
		case http.MethodDelete:
			return []string{"d", "p"}, true
		}
	default:
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			return []string{"r", "l"}, true
		case http.MethodDelete:
			return []string{"d"}, true
		case http.MethodPut, http.MethodPost, http.MethodPatch:
			return []string{"w", "c", "a"}, true
		}
	}
	return nil, false
}
