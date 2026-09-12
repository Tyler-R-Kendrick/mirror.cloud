// Package restjson implements restJson1.
package restjson

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/awsjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/httpuri"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec is restJson1, reusing JSON encode/decode with HTTP routing from the model.
type Codec struct{}

func (Codec) Protocol() model.Protocol { return model.ProtoRESTJSON1 }

func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	if svc.ID == "aws.lambda" {
		return lambdaOp(svc, r), nil
	}
	if svc.ID == "aws.apigateway" {
		return apigatewayOp(svc, r), nil
	}
	if svc.ID == "aws.eks" {
		return eksOp(svc, r), nil
	}
	if svc.ID == "aws.es" {
		return opensearchOp(svc, r), nil
	}
	if svc.ID == "railway.graphql" {
		return railwayOp(svc, r), nil
	}
	// An X-Amz-Target names an operation outright, and an explicit statement
	// beats one inferred from a path. No SDK sends it for a restJson1 service,
	// but this project's own recordings and pack tests do, and some services
	// carry a pack-defined operation bound to "/" -- AppSync's GraphQL
	// endpoint -- which a POST to the root would otherwise always match.
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		name := target
		if i := strings.LastIndex(target, "."); i >= 0 {
			name = target[i+1:]
		}
		if op := svc.OperationByName(name); op != nil {
			return op, nil
		}
	}
	if op, _, ok := httpuri.Match(svc, r); ok {
		return op, nil
	}
	// Nothing in the model claims this path. A restJson1 service is not
	// addressed by X-Amz-Target by any SDK, but the project's own recordings
	// and pack tests call these services RPC-style, and an unknown target is
	// still a better answer than an arbitrary operation that happens to share
	// the request's HTTP method.
	return awsjson.New10().Route(svc, r)
}

func lambdaOp(svc *model.Service, r *http.Request) *model.Operation {
	if a := r.URL.Query().Get("Action"); a != "" {
		if op := svc.OperationByName(a); op != nil {
			return op
		}
		return &model.Operation{Name: a, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
	}
	path, m := r.URL.Path, r.Method
	name := "CreateFunction"
	switch {
	case strings.Contains(path, "/invocations"):
		name = "Invoke"
	case strings.Contains(path, "/event-source-mappings"):
		switch m {
		case http.MethodPost:
			name = "CreateEventSourceMapping"
		case http.MethodPut:
			name = "UpdateEventSourceMapping"
		case http.MethodDelete:
			name = "DeleteEventSourceMapping"
		case http.MethodGet:
			if strings.HasSuffix(path, "/event-source-mappings") || strings.HasSuffix(path, "/event-source-mappings/") {
				name = "ListEventSourceMappings"
			} else {
				name = "GetEventSourceMapping"
			}
		}
	case strings.Contains(path, "/tags"):
		switch m {
		case http.MethodPost:
			name = "TagResource"
		case http.MethodDelete:
			name = "UntagResource"
		default:
			name = "ListTags"
		}
	case strings.Contains(path, "/code") && m == http.MethodPut:
		name = "UpdateFunctionCode"
	case strings.Contains(path, "/configuration") && m == http.MethodPut:
		name = "UpdateFunctionConfiguration"
	case strings.Contains(path, "/configuration"):
		name = "GetFunctionConfiguration"
	case strings.Contains(path, "/versions") && m == http.MethodPost:
		name = "PublishVersion"
	case strings.Contains(path, "/versions"):
		name = "ListVersionsByFunction"
	case strings.Contains(path, "/aliases"):
		switch m {
		case http.MethodPost:
			name = "CreateAlias"
		case http.MethodPut:
			name = "UpdateAlias"
		case http.MethodDelete:
			name = "DeleteAlias"
		case http.MethodGet:
			if strings.HasSuffix(path, "/aliases") || strings.HasSuffix(path, "/aliases/") {
				name = "ListAliases"
			} else {
				name = "GetAlias"
			}
		}
	case strings.Contains(path, "/policy"):
		switch m {
		case http.MethodPost:
			name = "AddPermission"
		case http.MethodDelete:
			name = "RemovePermission"
		default:
			name = "GetPolicy"
		}
	case strings.Contains(path, "/concurrency"):
		switch m {
		case http.MethodPut:
			name = "PutFunctionConcurrency"
		case http.MethodDelete:
			name = "DeleteFunctionConcurrency"
		default:
			name = "GetFunctionConcurrency"
		}
	case m == http.MethodGet && strings.Contains(path, "/functions/") && !strings.HasSuffix(path, "/functions"):
		name = "GetFunction"
	case m == http.MethodGet:
		name = "ListFunctions"
	case m == http.MethodDelete:
		name = "DeleteFunction"
	case m == http.MethodPost && strings.Contains(path, "/functions"):
		name = "CreateFunction"
	}
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func apigatewayOp(svc *model.Service, r *http.Request) *model.Operation {
	if a := r.URL.Query().Get("Action"); a != "" {
		if op := svc.OperationByName(a); op != nil {
			return op
		}
		return &model.Operation{Name: a, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
	}
	path := r.URL.Path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	name := "GetRestApis"
	m := r.Method
	switch {
	case strings.Contains(path, "_user_request_"):
		name = "ExecuteApi"
	case len(parts) == 1 && parts[0] == "restapis" && m == http.MethodPost:
		name = "CreateRestApi"
	case len(parts) == 1 && parts[0] == "restapis" && m == http.MethodGet:
		name = "GetRestApis"
	case len(parts) == 2 && parts[0] == "restapis" && m == http.MethodDelete:
		name = "DeleteRestApi"
	case len(parts) == 2 && parts[0] == "restapis" && m == http.MethodGet:
		name = "GetRestApi"
	case len(parts) == 3 && parts[2] == "resources" && m == http.MethodGet:
		name = "GetResources"
	case len(parts) == 4 && parts[2] == "resources" && m == http.MethodPost:
		name = "CreateResource"
	case strings.Contains(path, "/integration/responses/"):
		switch m {
		case http.MethodPut:
			name = "PutIntegrationResponse"
		case http.MethodDelete:
			name = "DeleteIntegrationResponse"
		default:
			name = "GetIntegrationResponse"
		}
	case strings.Contains(path, "/responses/"):
		switch m {
		case http.MethodPut:
			name = "PutMethodResponse"
		case http.MethodDelete:
			name = "DeleteMethodResponse"
		default:
			name = "GetMethodResponse"
		}
	case strings.Contains(path, "/integration"):
		switch m {
		case http.MethodPut:
			name = "PutIntegration"
		case http.MethodDelete:
			name = "DeleteIntegration"
		default:
			name = "GetIntegration"
		}
	case strings.Contains(path, "/methods/"):
		switch m {
		case http.MethodPut:
			name = "PutMethod"
		case http.MethodDelete:
			name = "DeleteMethod"
		default:
			name = "GetMethod"
		}
	case len(parts) >= 4 && parts[2] == "resources" && m == http.MethodGet:
		name = "GetResource"
	case len(parts) >= 4 && parts[2] == "resources" && m == http.MethodDelete:
		name = "DeleteResource"
	case len(parts) == 3 && parts[2] == "deployments" && m == http.MethodPost:
		name = "CreateDeployment"
	case len(parts) >= 4 && parts[2] == "deployments" && m == http.MethodGet:
		name = "GetDeployment"
	case len(parts) >= 4 && parts[2] == "deployments" && m == http.MethodDelete:
		name = "DeleteDeployment"
	case len(parts) == 3 && parts[2] == "deployments" && m == http.MethodGet:
		name = "GetDeployments"
	case len(parts) >= 3 && parts[2] == "stages" && m == http.MethodPost:
		name = "CreateStage"
	case len(parts) >= 4 && parts[2] == "stages" && m == http.MethodPatch:
		name = "UpdateStage"
	case len(parts) >= 4 && parts[2] == "stages" && m == http.MethodDelete:
		name = "DeleteStage"
	case len(parts) >= 4 && parts[2] == "stages" && m == http.MethodGet:
		name = "GetStage"
	case len(parts) == 3 && parts[2] == "authorizers" && m == http.MethodPost:
		name = "CreateAuthorizer"
	case len(parts) == 3 && parts[2] == "authorizers" && m == http.MethodGet:
		name = "GetAuthorizers"
	case len(parts) >= 4 && parts[2] == "authorizers" && m == http.MethodDelete:
		name = "DeleteAuthorizer"
	case len(parts) >= 4 && parts[2] == "authorizers":
		name = "GetAuthorizer"
	case len(parts) >= 1 && parts[0] == "apikeys" && m == http.MethodPost && len(parts) == 1:
		name = "CreateApiKey"
	case len(parts) == 1 && parts[0] == "apikeys" && m == http.MethodGet:
		name = "GetApiKeys"
	case len(parts) == 2 && parts[0] == "apikeys" && m == http.MethodDelete:
		name = "DeleteApiKey"
	case len(parts) == 2 && parts[0] == "apikeys":
		name = "GetApiKey"
	case len(parts) == 1 && parts[0] == "usageplans" && m == http.MethodPost:
		name = "CreateUsagePlan"
	case len(parts) == 1 && parts[0] == "usageplans" && m == http.MethodGet:
		name = "GetUsagePlans"
	case len(parts) == 2 && parts[0] == "usageplans" && m == http.MethodDelete:
		name = "DeleteUsagePlan"
	case len(parts) == 2 && parts[0] == "usageplans":
		name = "GetUsagePlan"
	case len(parts) == 2 && parts[0] == "restapis" && m == http.MethodPatch:
		name = "UpdateRestApi"
	}
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func eksOp(svc *model.Service, r *http.Request) *model.Operation {
	if a := r.URL.Query().Get("Action"); a != "" {
		if op := svc.OperationByName(a); op != nil {
			return op
		}
		return &model.Operation{Name: a, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
	}
	path, m := r.URL.Path, r.Method
	name := "ListClusters"
	switch {
	case strings.Contains(path, "/node-groups") && m == http.MethodPost:
		name = "CreateNodegroup"
	case strings.Contains(path, "/node-groups") && strings.Count(path, "/") >= 4 && m == http.MethodGet:
		name = "DescribeNodegroup"
	case strings.Contains(path, "/node-groups") && m == http.MethodGet:
		name = "ListNodegroups"
	case strings.Contains(path, "/node-groups") && m == http.MethodDelete:
		name = "DeleteNodegroup"
	case path == "/clusters" && m == http.MethodPost:
		name = "CreateCluster"
	case path == "/clusters" && m == http.MethodGet:
		name = "ListClusters"
	case strings.HasPrefix(path, "/clusters/") && m == http.MethodDelete:
		name = "DeleteCluster"
	case strings.HasPrefix(path, "/clusters/") && m == http.MethodGet:
		name = "DescribeCluster"
	}
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func opensearchOp(svc *model.Service, r *http.Request) *model.Operation {
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		name := target
		if i := strings.LastIndex(target, "."); i >= 0 {
			name = target[i+1:]
		}
		if op := svc.OperationByName(name); op != nil {
			return op
		}
		return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
	}
	path, m := r.URL.Path, r.Method
	name := "ListDomainNames"
	switch {
	case strings.Contains(path, "/_search"):
		name = "Search"
	case strings.Contains(path, "/_doc") && m == http.MethodPut:
		name = "IndexDocument"
	case strings.Contains(path, "/_doc") && m == http.MethodGet:
		name = "GetDocument"
	case strings.Contains(path, "/_doc") && m == http.MethodDelete:
		name = "DeleteDocument"
	case strings.Contains(path, "/opensearch/domain") && m == http.MethodPost:
		name = "CreateDomain"
	case strings.Contains(path, "/opensearch/domain") && m == http.MethodGet:
		name = "DescribeDomain"
	case strings.Contains(path, "/opensearch/domain") && m == http.MethodDelete:
		name = "DeleteDomain"
	}
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func railwayOp(svc *model.Service, r *http.Request) *model.Operation {
	name := railwayRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func railwayRoute(r *http.Request) string {
	if r.URL != nil && !strings.Contains(r.URL.Path, "/graphql/v2") {
		return "Unknown"
	}
	if r.Body == nil {
		return "Unknown"
	}
	b, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(b))
	in := map[string]any{}
	_ = json.Unmarshal(b, &in)
	q, _ := in["query"].(string)
	return gqlRootField(q)
}

// gqlRootField answers which field a GraphQL document selects, which for a
// service whose every request is one POST to one path IS the operation.
//
// It scans rather than substring-matches because substring matching is wrong on
// ordinary documents, not just adversarial ones. Railway's schema declares both
// `project` and `projectId` on Service, so `{ service(id:"s") { id projectId } }`
// contains "project" and a longest-name-first switch answered a service lookup
// with the project handler. The same switch misroutes on an operation name
// (`query GetService($projectId: String!)`), on a comment, and on a string
// literal -- three ways to be wrong that a scanner simply does not have.
//
// This is deliberately a router, not a GraphQL implementation: it answers which
// field, and nothing about the selection set, because nothing in this tree
// projects one yet.
// seekFragment finds `fragment NAME on Type {` anywhere in a document and
// leaves *pos just inside that brace, so a root selection which is a spread can
// be followed to the field it actually selects. It is a separate scan rather
// than a reuse of gqlRootField's closures because it must not disturb their
// position until it succeeds.
func seekFragment(q, want string, pos *int, n int) bool {
	isName := func(c byte, first bool) bool {
		return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (!first && c >= '0' && c <= '9')
	}
	for i := 0; i+8 <= n; i++ {
		if q[i] != 'f' || q[i:i+8] != "fragment" {
			continue
		}
		if i > 0 && isName(q[i-1], false) { // part of a longer word
			continue
		}
		j := i + 8
		if j < n && isName(q[j], false) {
			continue
		}
		// the fragment's name
		for j < n && (q[j] == ' ' || q[j] == '\t' || q[j] == '\n' || q[j] == '\r' || q[j] == ',') {
			j++
		}
		start := j
		for j < n && isName(q[j], j == start) {
			j++
		}
		if q[start:j] != want {
			continue
		}
		// its body opens at the next brace outside a string, comment or group
		depth := 0
		for ; j < n; j++ {
			switch q[j] {
			case '"':
				for j++; j < n && q[j] != '"'; j++ {
					if q[j] == '\\' {
						j++
					}
				}
			case '#':
				for ; j < n && q[j] != '\n'; j++ {
				}
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			case '{':
				if depth == 0 {
					*pos = j + 1
					return true
				}
			}
		}
		return false
	}
	return false
}

func gqlRootField(q string) string {
	i, n := 0, len(q)

	// A comment runs to the end of its line. Every scan below has to know this,
	// not just the one that skips ignored tokens: a comment is the one place a
	// brace can appear that does not open anything, and a scan that reads it
	// raw lets a comment's TEXT decide which operation ran -- the exact defect
	// this function replaced a substring switch to avoid.
	skipComment := func() {
		for i < n && q[i] != '\n' {
			i++
		}
	}
	// GraphQL's ignored tokens: whitespace, commas, and # comments.
	skip := func() {
		for i < n {
			switch c := q[i]; {
			case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',':
				i++
			case c == '#':
				skipComment()
			default:
				return
			}
		}
	}
	// A string literal, skipped wherever one may appear, so that a brace or a
	// paren inside one cannot close a group early.
	skipString := func() {
		i++
		for i < n && q[i] != '"' {
			if q[i] == '\\' {
				i++
			}
			i++
		}
	}
	name := func() string {
		start := i
		for i < n {
			c := q[i]
			if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9' && i > start) {
				i++
				continue
			}
			break
		}
		return q[start:i]
	}
	// The selection set opens at the first brace that is not inside a string or
	// a variable-definition group -- which is all that an operation name,
	// variable definitions and directives can put in the way.
	toSelectionSet := func() bool {
		depth := 0
		for i < n {
			switch q[i] {
			case '"':
				skipString()
			case '#':
				skipComment()
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			case '{':
				if depth == 0 {
					i++
					return true
				}
			}
			i++
		}
		return false
	}
	// From just past an opening brace to just past its match.
	skipBlock := func() bool {
		depth := 1
		for i < n {
			switch q[i] {
			case '"':
				skipString()
			case '#':
				skipComment()
			case '{':
				depth++
			case '}':
				if depth--; depth == 0 {
					i++
					return true
				}
			}
			i++
		}
		return false
	}
	// The first field of a selection set, seeing past an alias, an inline
	// fragment and a named spread. Depth-bounded: a document may define
	// fragments that refer to each other in a cycle, and this runs on input
	// nobody vouched for.
	var selection func(depth int) string
	selection = func(depth int) string {
		if depth > 8 {
			return "Unknown"
		}
		skip()
		if i+2 < n && q[i] == '.' && q[i+1] == '.' && q[i+2] == '.' {
			i += 3
			skip()
			switch word := name(); word {
			case "on": // inline fragment with a type condition
				skip()
				name()
				if !toSelectionSet() {
					return "Unknown"
				}
				return selection(depth + 1)
			case "": // inline fragment with no type condition
				if !toSelectionSet() {
					return "Unknown"
				}
				return selection(depth + 1)
			default: // a named spread: the field lives in its definition
				if !seekFragment(q, word, &i, n) {
					return "Unknown"
				}
				return selection(depth + 1)
			}
		}
		first := name()
		if first == "" {
			return "Unknown"
		}
		skip()
		if i < n && q[i] == ':' {
			i++
			skip()
			if aliased := name(); aliased != "" {
				return aliased
			}
			return "Unknown"
		}
		return first
	}
	field := func() string { return selection(0) }

	for {
		skip()
		if i >= n {
			return "Unknown"
		}
		if q[i] == '{' { // query shorthand, with no operation type at all
			i++
			return field()
		}
		switch name() {
		case "query", "mutation", "subscription":
			if !toSelectionSet() {
				return "Unknown"
			}
			return field()
		case "fragment":
			// A fragment definition may precede the operation it serves.
			if !toSelectionSet() || !skipBlock() {
				return "Unknown"
			}
		default:
			return "Unknown"
		}
	}
}
func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	body, _ := io.ReadAll(r.Body)
	in := map[string]any{}
	// A member bound to the payload whose shape is a string or a blob IS the
	// request body: Cloudflare's KV write declares `body` that way, Lambda
	// declares `Payload`, Glacier declares `body`. Parsing such a body as JSON
	// and splattering its keys across the input is how a value that happens to
	// be an object arrives as several inputs and never as itself.
	//
	// A payload member that is a structure says the opposite -- CloudFront's
	// DistributionConfig means "the body is this structure, serialized" -- so
	// model.PayloadMember answers only for the opaque case and everything else
	// decodes as before. This was a branch keyed on one service id and one
	// operation name, which is C41's shape: what in it mentioned the provider?
	if name, ok := svc.PayloadMember(op); ok {
		in[name] = string(body)
		body = nil
	}
	// The third answer to "what is this payload": a bare JSON array, which has
	// no object to splat into the input. Unmarshalling one into a map fails, so
	// without this the operation is handed nothing and the error is dropped --
	// which is what `_redis` was for. That name was the provider's for a rule
	// the model states: Cloudflare's bulk write and both bulk deletes are
	// shaped this way too, and they decode to an empty input today.
	if name, ok := svc.ListPayloadMember(op); ok {
		var list []any
		if err := json.Unmarshal(body, &list); err == nil {
			in[name] = list
		}
		body = nil
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}
	for k, vs := range r.URL.Query() {
		if _, ok := in[k]; !ok {
			in[k] = vs[0]
		}
	}
	// A REST operation carries part of its input in the path: DeleteDetector
	// is `DELETE /detector/{DetectorId}` and nothing else names the detector.
	// Without this the input arrives empty and the operation addresses the
	// zero value. The pattern is re-matched here rather than carried over from
	// Route so that decoding a request stays independent of how it was routed.
	if op.HTTP.URI != "" {
		if bound, ok := httpuri.Parse(op.HTTP.URI).Match(r.URL.EscapedPath(), r.URL.Query()); ok {
			for k, v := range bound {
				in[k] = v
			}
		}
	}
	return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
}

func (Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	status := resp.Status
	if status == 0 {
		status = op.HTTP.Code
		if status == 0 {
			status = 200
		}
	}
	for k, vs := range resp.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	// `_raw` is the engine's name for an operation whose body is the value
	// itself -- bir.TopLevelRaw -- and it is answered before any provider
	// envelope, because an opaque body has nowhere to put one. Cloudflare's KV
	// read is the first such operation; its document answers the stored bytes
	// as application/octet-stream, not the {success, errors, result} envelope
	// every other Cloudflare operation carries.
	if raw, ok := rawBody(resp); ok {
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/octet-stream")
		}
		w.WriteHeader(status)
		_, err := io.WriteString(w, raw)
		return err
	}
	if svc.ID == "railway.graphql" {
		return encodeRailway(w, status, resp)
	}
	// A status that forbids a body gets none. An engine-served operation
	// always projects an output map -- empty when its response shape declares
	// no members -- so without this a 204 reaches net/http with `{}` behind
	// it, which the server drops while logging that the response code does not
	// allow a body. DeleteSshKey is the first such operation; the rule is the
	// protocol's, not Hetzner's, so it lives here rather than in a branch.
	if status == http.StatusNoContent || status == http.StatusNotModified || status < 200 {
		w.WriteHeader(status)
		return nil
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	if resp.Stream != nil {
		_, err := io.Copy(w, resp.Stream)
		return err
	}
	if resp.Output == nil {
		return nil
	}
	// `_list` is the engine's name for an operation whose whole body is an
	// array -- bir.TopLevelList -- and it is a convention of the engine and
	// this codec, not of any one provider. Hostinger had a branch here that
	// was the generic encoder plus these two lines; Fly's machine listing is
	// the second such operation, and a third branch would have made it a
	// pattern. The member cannot collide with a real one: the loader checks
	// every output member against the model, and no shape declares `_list`.
	if lst, ok := resp.Output[bir.TopLevelList]; ok {
		return json.NewEncoder(w).Encode(lst)
	}
	return json.NewEncoder(w).Encode(resp.Output)
}

func encodeRailway(w http.ResponseWriter, status int, resp *spi.Response) error {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	if resp.Output == nil {
		return json.NewEncoder(w).Encode(map[string]any{"data": nil})
	}
	wrap, _ := resp.Output["_wrap"].(string)
	if lst, ok := resp.Output["_list"]; ok {
		items, _ := lst.([]any)
		if items == nil {
			items = []any{}
		}
		var edges []any
		for _, item := range items {
			edges = append(edges, map[string]any{"node": item})
		}
		if edges == nil {
			edges = []any{}
		}
		if wrap == "" {
			wrap = "projects"
		}
		return json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{wrap: map[string]any{"edges": edges}}})
	}
	if wrap != "" {
		if rec, ok := resp.Output[wrap]; ok {
			return json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{wrap: rec}})
		}
	}
	return json.NewEncoder(w).Encode(map[string]any{"data": resp.Output})
}
func (Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	status := f.HTTPStatus
	if status == 0 {
		status = 400
	}
	w.Header().Set("Content-Type", "application/json")
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
		status = 501
	}
	if svc.ID == "vercel.api" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": f.Code, "message": f.Message}})
	}
	// Vercel KV is a second product on a second host, and its errors are not
	// shaped like the REST API's. The deleted pack served both through one
	// registration and therefore through the envelope above, wrapping an
	// Upstash error in Vercel's {error: {code, message}}. The KV document
	// declares `error` as a plain string, which is what Upstash answers, so
	// this follows the document rather than the pack -- a fault envelope is
	// not compared by the equivalence recording, which gates the code, status
	// and class, so the change is stated as a quirk instead of hidden by one.
	if svc.ID == "vercel.kv" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": f.Message})
	}
	if svc.ID == "cloudflare.api" {
		var code any = f.Code
		if n, err := strconv.Atoi(f.Code); err == nil {
			code = n
		}
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{
			"success":  false,
			"errors":   []any{map[string]any{"code": code, "message": f.Message}},
			"messages": []any{},
			"result":   nil,
		})
	}
	if svc.ID == "hostinger.api" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"message": f.Message, "correlation_id": "mirror"})
	}
	if svc.ID == "digitalocean.v2" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"id": f.Code, "message": f.Message})
	}
	if svc.ID == "hetzner.v1" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": f.Code, "message": f.Message}})
	}
	if svc.ID == "railway.graphql" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"errors": []any{map[string]any{"message": f.Message, "extensions": map[string]any{"code": f.Code}}}})
	}
	if svc.ID == "fly.machines" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": f.Message})
	}
	w.Header().Set("x-amzn-errortype", f.Code)
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(map[string]any{"message": f.Message, "__type": f.Code})
}

// rawBody reports the opaque body an operation projected, if it projected one.
func rawBody(resp *spi.Response) (string, bool) {
	if resp == nil || resp.Output == nil {
		return "", false
	}
	raw, ok := resp.Output[bir.TopLevelRaw].(string)
	return raw, ok
}
