// Package openapi ingests an OpenAPI 3.x document into the canonical model.
//
// It exists because of what its absence forced. A behavior bundle is validated
// against a generated model, models come from receivers, and mirror had
// receivers for Smithy and for Google Discovery -- neither of which reads
// OpenAPI. Every other provider mirror targets publishes OpenAPI and nothing
// else: Cloudflare, Hostinger, Vercel, DigitalOcean, Hetzner. So a new provider
// could arrive only as hand-written Go, which is the one thing `ratchet.json`
// exists to refuse, and three arrived that way anyway. The ratchet could report
// that and nothing could fix it, because the path it points at did not exist.
//
// What it produces is the same shape the other receivers produce: one
// model.Service with operations bound to their HTTP method and URI, an input
// shape carrying each parameter under the placement the document gives it, an
// output shape from the success response, and a shape graph with no dangling
// reference. The REST/JSON codec already serves that, so no new protocol is
// needed -- only the model that was missing.
package openapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"unicode"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Receiver implements receiver.Receiver for OpenAPI 3.x.
type Receiver struct{}

func (Receiver) Name() string { return "openapi" }

// Detect looks for the `openapi` version field, which is the one member the
// specification requires of every document and which no other format mirror
// ingests carries. Smithy documents declare `smithy`, Discovery declares
// `discoveryVersion`, so the three cannot be confused.
func (Receiver) Detect(_ string, head []byte) bool {
	var probe struct {
		OpenAPI string `json:"openapi"`
	}
	if json.Unmarshal(head, &probe) != nil {
		// A document larger than the head window truncates mid-value, so fall
		// back to looking for the key itself rather than refusing the file.
		return strings.Contains(string(head), `"openapi"`)
	}
	return strings.HasPrefix(probe.OpenAPI, "3.")
}

const componentSchemas = "#/components/schemas/"

type document struct {
	OpenAPI string `json:"openapi"`
	Info    struct {
		Title string `json:"title"`
	} `json:"info"`
	Servers []struct {
		URL string `json:"url"`
	} `json:"servers"`
	Paths      map[string]json.RawMessage `json:"paths"`
	Components struct {
		Schemas    map[string]schema    `json:"schemas"`
		Parameters map[string]parameter `json:"parameters"`
	} `json:"components"`
}

type operation struct {
	OperationID string      `json:"operationId"`
	Summary     string      `json:"summary"`
	Parameters  []parameter `json:"parameters"`
	RequestBody *struct {
		Required bool                 `json:"required"`
		Content  map[string]mediaType `json:"content"`
	} `json:"requestBody"`
	Responses map[string]struct {
		Content map[string]mediaType `json:"content"`
	} `json:"responses"`
}

type mediaType struct {
	Schema *schema `json:"schema"`
}

type parameter struct {
	Ref         string  `json:"$ref"`
	Name        string  `json:"name"`
	In          string  `json:"in"`
	Required    bool    `json:"required"`
	Description string  `json:"description"`
	Schema      *schema `json:"schema"`
}

type schema struct {
	Ref                  string            `json:"$ref"`
	Type                 json.RawMessage   `json:"type"`
	Format               string            `json:"format"`
	Properties           map[string]schema `json:"properties"`
	Required             []string          `json:"required"`
	Items                *schema           `json:"items"`
	AdditionalProperties json.RawMessage   `json:"additionalProperties"`
	Enum                 []any             `json:"enum"`
	AllOf                []schema          `json:"allOf"`
	OneOf                []schema          `json:"oneOf"`
	AnyOf                []schema          `json:"anyOf"`
	Description          string            `json:"description"`
	MinLength            *int64            `json:"minLength"`
	MaxLength            *int64            `json:"maxLength"`
	Minimum              *float64          `json:"minimum"`
	Maximum              *float64          `json:"maximum"`
	Pattern              string            `json:"pattern"`
	UniqueItems          bool              `json:"uniqueItems"`
}

// typeName reads the `type` keyword, which is a string in 3.0 and may be a list
// in 3.1 (`["string","null"]`). The first non-null entry is the type; the null
// is nullability, which the model expresses by a member simply being absent.
func (s schema) typeName() string {
	if len(s.Type) == 0 {
		return ""
	}
	var one string
	if json.Unmarshal(s.Type, &one) == nil {
		return one
	}
	var many []string
	if json.Unmarshal(s.Type, &many) == nil {
		for _, t := range many {
			if t != "null" {
				return t
			}
		}
	}
	return ""
}

// httpMethods are the path-item keys that are operations. Everything else a
// path item may carry -- `parameters`, `summary`, `servers`, an extension --
// is not one, and treating an unknown key as an operation would invent
// operations no client can call.
var httpMethods = []string{"get", "put", "post", "delete", "patch", "head", "options", "trace"}

func (Receiver) Ingest(ctx context.Context, src model.SourceRef, data []byte) ([]model.Service, error) {
	_ = ctx
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if doc.OpenAPI == "" {
		return nil, fmt.Errorf("openapi: %s: no `openapi` version field", src.Path)
	}
	id := serviceID(src.Path)
	base := basePath(doc)
	sh := &shaper{shapes: map[string]model.Shape{}}
	// The named schemas first, so a `$ref` from an operation or from another
	// schema resolves to a shape that is already there.
	for _, name := range sortedKeys(doc.Components.Schemas) {
		sh.define(name, doc.Components.Schemas[name])
	}

	svc := model.Service{
		ID:             id,
		Namespace:      doc.Info.Title,
		Protocol:       model.ProtoRESTJSON1,
		EndpointPrefix: provider(id),
		Source:         src,
	}
	for _, uri := range sortedKeys(doc.Paths) {
		item, shared, err := pathItem(doc.Paths[uri])
		if err != nil {
			return nil, fmt.Errorf("openapi: %s: path %s: %w", src.Path, uri, err)
		}
		for _, method := range httpMethods {
			raw, ok := item[method]
			if !ok {
				continue
			}
			var op operation
			if err := json.Unmarshal(raw, &op); err != nil {
				return nil, fmt.Errorf("openapi: %s: %s %s: %w", src.Path, method, uri, err)
			}
			name := operationName(op, method, uri)
			params := resolveParams(append(append([]parameter{}, shared...), op.Parameters...), doc.Components.Parameters, sh)
			svc.Operations = append(svc.Operations, model.Operation{
				Name: name,
				HTTP: model.HTTPBinding{
					Method: strings.ToUpper(method),
					URI:    base + templated(uri),
					Code:   successCode(op),
				},
				Input:      sh.request(name, params, op),
				Output:     sh.response(name, op),
				Readonly:   method == "get" || method == "head",
				Idempotent: method == "put" || method == "delete",
				Confidence: model.ConfDeclared,
				Source:     src,
			})
		}
	}
	// Paths are walked in name order and methods in a fixed order, so the same
	// document ingests to the same list every time. A map range would not.
	sort.SliceStable(svc.Operations, func(i, j int) bool {
		return svc.Operations[i].Name < svc.Operations[j].Name
	})
	sh.mergeComposites()
	svc.Shapes = sh.shapes
	if err := resolves(&svc, sh.missing); err != nil {
		return nil, err
	}
	return []model.Service{svc}, nil
}

// pathItem splits a path item into its operations and the parameters it shares
// with all of them. A shared parameter is as binding as one written on the
// operation, and dropping it would lose the path label on every document that
// factors labels out -- which is most of them.
func pathItem(raw json.RawMessage) (map[string]json.RawMessage, []parameter, error) {
	var item map[string]json.RawMessage
	if err := json.Unmarshal(raw, &item); err != nil {
		return nil, nil, err
	}
	var shared []parameter
	if p, ok := item["parameters"]; ok {
		if err := json.Unmarshal(p, &shared); err != nil {
			return nil, nil, fmt.Errorf("parameters: %w", err)
		}
	}
	return item, shared, nil
}

// resolveParams dereferences `$ref` parameters against the document's component
// parameters. A reference that names nothing is recorded rather than dropped:
// a parameter that vanishes is a path label the router will not bind, and the
// only symptom would be an operation addressing the zero value.
func resolveParams(in []parameter, components map[string]parameter, sh *shaper) []parameter {
	const componentParams = "#/components/parameters/"
	out := make([]parameter, 0, len(in))
	for _, p := range in {
		if p.Ref == "" {
			out = append(out, p)
			continue
		}
		name := strings.TrimPrefix(p.Ref, componentParams)
		resolved, ok := components[name]
		if !ok {
			sh.missing = append(sh.missing, "parameter "+p.Ref)
			continue
		}
		out = append(out, resolved)
	}
	return out
}

// successCode is the first 2xx response the operation declares, which is the
// status the codec answers with. A document that declares none is answered 200,
// the same default every other receiver uses.
func successCode(op operation) int {
	best := 0
	for code := range op.Responses {
		var n int
		if _, err := fmt.Sscanf(code, "%d", &n); err != nil {
			continue
		}
		if n < 200 || n > 299 {
			continue
		}
		if best == 0 || n < best {
			best = n
		}
	}
	if best == 0 {
		return 200
	}
	return best
}

// operationName is the document's own operationId where it has one, and a name
// built from the method and path where it does not. The built name has to be
// stable and unique, because it is what a client names the operation and what a
// bundle keys its behavior on.
func operationName(op operation, method, uri string) string {
	if op.OperationID != "" {
		return exported(op.OperationID)
	}
	var b strings.Builder
	b.WriteString(strings.Title(method)) //nolint:staticcheck // ASCII method names
	for _, seg := range strings.Split(strings.Trim(uri, "/"), "/") {
		if seg == "" {
			continue
		}
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			b.WriteString("By")
			seg = seg[1 : len(seg)-1]
		}
		b.WriteString(exported(seg))
	}
	return b.String()
}

// exported turns a document's own spelling into one identifier-shaped token:
// `list-dns-zones` and `list_dns_zones` and `listDnsZones` all become
// ListDnsZones, so two documents spelling the same operation differently do not
// produce two different names.
func exported(s string) string {
	var b strings.Builder
	upper := true
	for _, r := range s {
		switch {
		case r == '-' || r == '_' || r == '.' || r == ' ' || r == '/':
			upper = true
		case upper:
			b.WriteRune(unicode.ToUpper(r))
			upper = false
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// templated normalizes a path into the pattern `httpuri` parses. OpenAPI and
// Smithy agree on `{name}`, so this only guarantees the leading slash.
func templated(uri string) string {
	if !strings.HasPrefix(uri, "/") {
		return "/" + uri
	}
	return uri
}

// basePath is the path component of the document's first server URL, which is
// part of every address a client uses and is not repeated in the path items.
//
// Ignoring it produced a service bound to `/servers` when every hcloud client
// calls `/v1/servers`: OpenAPI factors the version out of the paths and into
// the server URL, where Smithy and Discovery both carry it in the operation.
// The two disagreed silently -- the model is well-formed either way, and the
// only symptom is a router that answers nothing.
//
// A server URL may be templated (`https://{region}.example.com/{version}`).
// A variable in the path cannot be resolved from the document alone, and
// guessing one would bind every operation to an address no client uses, so
// such a base is dropped rather than substituted: the paths stay as written,
// which is the behavior every document had before this existed.
func basePath(doc document) string {
	if len(doc.Servers) == 0 {
		return ""
	}
	raw := doc.Servers[0].URL
	if strings.ContainsAny(raw, "{}") {
		return ""
	}
	// A server URL may be absolute or relative to where the document is
	// served; url.Parse answers with the path either way.
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	p := strings.TrimSuffix(u.Path, "/")
	if p == "" || p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// serviceID derives `<provider>.<service>` from where the document sits, which
// is the same identity the lock and the rest of the system use. specs/
// hostinger/api.json is hostinger.api.
func serviceID(rel string) string {
	rel = strings.TrimSuffix(strings.ToLower(path.Clean(rel)), ".json")
	parts := strings.Split(rel, "/")
	for i, p := range parts {
		parts[i] = strings.TrimSuffix(p, ".openapi")
	}
	switch len(parts) {
	case 0:
		return "openapi.unknown"
	case 1:
		return parts[0]
	default:
		return parts[0] + "." + parts[len(parts)-1]
	}
}

// provider is the first segment of a service ID, and the endpoint label the
// demux resolves on. It is deliberately not the service's own short name: a
// document sitting at `vercel/api.json` is `vercel.api`, whose short name is
// the generic word `api`, and a service answering to `api` claims every AWS
// endpoint whose prefix begins `api.` -- which is exactly how ECR and IoT
// Wireless became unreachable once such a service existed.
func provider(id string) string {
	if p, _, ok := strings.Cut(id, "."); ok {
		return p
	}
	return id
}
