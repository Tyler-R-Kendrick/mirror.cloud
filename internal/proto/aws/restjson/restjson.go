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
	if svc.ID == "azure.table" {
		return azureTableOp(svc, r), nil
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
func gqlRootField(q string) string {
	i, n := 0, len(q)

	// GraphQL's ignored tokens: whitespace, commas, and # comments.
	skip := func() {
		for i < n {
			switch c := q[i]; {
			case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',':
				i++
			case c == '#':
				for i < n && q[i] != '\n' {
					i++
				}
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
	// The first field of a selection set, seeing past an alias.
	field := func() string {
		skip()
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

// azureTableETagList splits an If-Match header into dequoted tokens, like
// Azurite's etag adapter.
func azureTableETagList(v string) []any {
	parts := strings.Split(v, ",")
	out := make([]any, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(strings.TrimSpace(p), `"`)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func azureTableOp(svc *model.Service, r *http.Request) *model.Operation {
	name := azureTableRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func azureTableRoute(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	m := r.Method
	if path == "$batch" && m == http.MethodPost {
		return "SubmitBatch"
	}
	if path == "Tables" || path == "Tables()" {
		if m == http.MethodPost {
			return "CreateTable"
		}
		return "ListTables"
	}
	if strings.HasPrefix(path, "Tables('") && strings.HasSuffix(path, "')") && m == http.MethodDelete {
		return "DeleteTable"
	}
	if strings.Contains(path, "PartitionKey=") {
		switch m {
		case http.MethodGet:
			return "GetEntity"
		case http.MethodPut:
			return "UpdateEntity"
		case http.MethodPatch:
			return "MergeEntity"
		case http.MethodDelete:
			return "DeleteEntity"
		}
		return "Unknown"
	}
	if strings.HasSuffix(path, "()") && m == http.MethodGet {
		return "QueryEntities"
	}
	if m == http.MethodPost {
		return "InsertEntity"
	}
	if m == http.MethodGet {
		return "QueryEntities"
	}
	return "Unknown"
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
	if svc.ID == "azure.table" {
		// The entity body spreads into the record at write time, so it must
		// not also pollute the control members: it moves under __entity.
		var ent map[string]any
		if len(body) > 0 && json.Unmarshal(body, &ent) == nil {
			in["__entity"] = ent
			for k := range ent {
				delete(in, k)
			}
		}
		if tn, ok := ent["TableName"]; ok {
			in["table"] = tn
		}
		if v := r.Header.Get("If-Match"); v != "" {
			in["if_match_list"] = azureTableETagList(v)
		}
		if v := r.Header.Get("Prefer"); v != "" {
			in["prefer"] = v
		}
		path := strings.Trim(r.URL.Path, "/")
		if strings.HasPrefix(path, "Tables('") && strings.HasSuffix(path, "')") {
			in["table"] = strings.TrimSuffix(strings.TrimPrefix(path, "Tables('"), "')")
		} else if path != "Tables" && path != "Tables()" && path != "" && !strings.HasPrefix(path, "Tables") && path != "$batch" {
			tbl := path
			if i := strings.IndexByte(tbl, '('); i >= 0 {
				tbl = tbl[:i]
			}
			if in["table"] == nil {
				in["table"] = tbl
			}
			if i := strings.Index(path, "PartitionKey='"); i >= 0 {
				rest := path[i+len("PartitionKey='"):]
				if j := strings.IndexByte(rest, '\''); j >= 0 {
					in["PartitionKey"] = rest[:j]
				}
			}
			if i := strings.Index(path, "RowKey='"); i >= 0 {
				rest := path[i+len("RowKey='"):]
				if j := strings.IndexByte(rest, '\''); j >= 0 {
					in["RowKey"] = rest[:j]
				}
			}
		}
		if in["PartitionKey"] == nil && ent != nil {
			in["PartitionKey"] = ent["PartitionKey"]
		}
		if in["RowKey"] == nil && ent != nil {
			in["RowKey"] = ent["RowKey"]
		}
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
	if svc.ID == "azure.table" {
		return encodeAzureTable(w, status, op, resp)
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

// encodeAzureTable answers the OData JSON shapes: single-entity bodies are
// the record (etag renamed to odata.etag and echoed as the ETag header),
// collections are {"value": [...]}, and update/merge/delete are header-only.
func encodeAzureTable(w http.ResponseWriter, status int, op *model.Operation, resp *spi.Response) error {
	entity := func() map[string]any {
		m, _ := resp.Output["entity"].(map[string]any)
		return m
	}
	switch op.Name {
	case "UpdateEntity", "MergeEntity":
		w.Header().Set("ETag", azureTableEntityETag(entity()))
		w.WriteHeader(status)
		return nil
	case "DeleteEntity":
		w.WriteHeader(status)
		return nil
	case "InsertEntity":
		m := entity()
		w.Header().Set("ETag", azureTableEntityETag(m))
		if prefer, _ := resp.Output["prefer"].(string); strings.Contains(prefer, "return-no-content") {
			w.Header().Set("Preference-Applied", "return-no-content")
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(azureTableEntityJSON(m))
	case "GetEntity":
		m := entity()
		w.Header().Set("ETag", azureTableEntityETag(m))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(azureTableEntityJSON(m))
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	if resp.Output == nil {
		return nil
	}
	if lst, ok := resp.Output["_list"]; ok {
		items, _ := lst.([]any)
		out := make([]any, 0, len(items))
		for _, item := range items {
			m, _ := item.(map[string]any)
			out = append(out, azureTableEntityJSON(m))
		}
		return json.NewEncoder(w).Encode(map[string]any{"value": out})
	}
	return json.NewEncoder(w).Encode(resp.Output)
}

// azureTableEntityETag reads the record's etag member.
func azureTableEntityETag(m map[string]any) string {
	s, _ := m["etag"].(string)
	return s
}

// azureTableEntityJSON copies an entity record for the wire: the etag member
// becomes odata.etag.
func azureTableEntityJSON(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if k == "etag" {
			out["odata.etag"] = v
			continue
		}
		out[k] = v
	}
	return out
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
	if svc.ID == "azure.table" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"odata.error": map[string]any{"code": f.Code, "message": map[string]any{"lang": "en-US", "value": f.Message}}})
	}
	if svc.ID == "vercel.api" {
		// Vercel's fault envelope outlives the pack, as Hetzner's and
		// DigitalOcean's did: it is the shape the vendor's own API answers
		// faults in.
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
