// Package restjson implements restJson1.
package restjson

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	// A form body is as much a restJson payload as a JSON one: Vercel's OAuth
	// callback and token exchange are declared form operations, and
	// JSON-unmarshalling `code=...&state=...` into a map silently decodes to
	// nothing. The demux parses forms on the way past (the AWS query protocol
	// lives on r.Form), which drains the body, so the already parsed answer is
	// read first and the raw body is the fallback. Outside the body-length
	// guard, because the drained case is the common one.
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		vals := r.PostForm
		if len(vals) == 0 && len(body) > 0 {
			if parsed, err := url.ParseQuery(string(body)); err == nil {
				vals = parsed
			}
		}
		for k, vs := range vals {
			assignForm(in, k, vs)
		}
	} else if len(body) > 0 {
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
	if (svc.ID == "aws.opensearch" || svc.ID == "aws.es") && op != nil {
		// The document plane is addressed by path segments, not members: a
		// PUT to /cities/_doc/1 names the index and the id the way the
		// pack's fill() read them, because no SDK spelling exists for them.
		// Members the request already carries win over the path.
		switch op.Name {
		case "IndexDocument", "GetDocument", "DeleteDocument", "Search":
			fillOpenSearchPath(in, r.URL.Path)
		}
	}
	for k, vs := range r.URL.Query() {
		if _, ok := in[k]; !ok {
			// A member the model declares as a list collects every repeated
			// value -- Stripe's expand[]=customer&expand[]=charge is one
			// member arriving several times, and keeping only the first
			// would silently drop every expansion but one.
			if isListMember(svc, op, k) {
				all := make([]any, 0, len(vs))
				for _, v := range vs {
					all = append(all, v)
				}
				in[k] = all
			} else {
				in[k] = vs[0]
			}
		}
	}
	// Header-bound members arrive by their wire name: UploadFile's digest is
	// x-vercel-digest and nothing else names it, so an operation whose input
	// the document puts in a header would otherwise address the zero value.
	// Content-Length is the exception Go keeps out of Header, so it comes from
	// the request field instead.
	if op.Input != "" {
		if shape, ok := svc.Shapes[op.Input]; ok {
			for name, m := range shape.Members {
				if m.Binding.Location != "header" {
					continue
				}
				hdr := m.Binding.Name
				if hdr == "" {
					hdr = name
				}
				v := r.Header.Get(hdr)
				if v == "" && hdr == "Content-Length" && r.ContentLength >= 0 {
					v = strconv.FormatInt(r.ContentLength, 10)
				}
				if v != "" {
					in[name] = v
				}
			}
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
	if svc.ID == "vercel.blob" && op.Name == "ServeBlob" {
		return encodeVercelBlobContent(w, status, resp)
	}
	// Vercel's local OAuth flow has two wire shapes the JSON envelope cannot
	// carry: the authorize page is HTML a browser renders, and the callback's
	// answer is the redirect, not a body.
	if svc.ID == "vercel.api" && op.Name == "OauthAuthorize" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_, err := io.WriteString(w, fmt.Sprint(resp.Output["page"]))
		return err
	}
	if svc.ID == "vercel.api" && op.Name == "OauthAuthorizeCallback" {
		w.Header().Set("Location", fmt.Sprint(resp.Output["location"]))
		w.WriteHeader(http.StatusFound)
		return nil
	}
	// Stripe's hosted checkout has the same two wire shapes as Vercel's OAuth
	// flow, beside them because it is the same pattern: the page is HTML a
	// browser renders (its status rides in the envelope, since a missing
	// session is the 404 page), and the complete POST's answer is the redirect
	// when there is one, the receipt page when there is not.
	if svc.ID == "stripe.api" && op.Name == "GetCheckoutPage" {
		switch n := resp.Output["status"].(type) {
		case int64:
			status = int(n)
		case float64:
			status = int(n)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_, err := io.WriteString(w, fmt.Sprint(resp.Output["page"]))
		return err
	}
	if svc.ID == "stripe.api" && op.Name == "CompleteCheckoutSession" {
		if loc, _ := resp.Output["location"].(string); loc != "" {
			w.Header().Set("Location", loc)
			w.WriteHeader(http.StatusFound)
			return nil
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_, err := io.WriteString(w, fmt.Sprint(resp.Output["page"]))
		return err
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
	// Vercel's REST API and Blob data plane share the {error:{code,message}}
	// envelope -- emulate's blobErr is the same shape the REST API answers --
	// while KV below is the product whose errors differ. The token endpoint's
	// faults are RFC 6749's instead: {error, error_description} flat, because
	// that is what every OAuth client parses.
	if svc.ID == "vercel.api" && op != nil && op.Name == "OauthToken" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": f.Code, "error_description": f.Message})
	}
	if svc.ID == "vercel.api" || svc.ID == "vercel.blob" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": f.Code, "message": f.Message}})
	}
	// Stripe's envelope is {error: {type, message, code?, param?}}. The type
	// is the oracle's constant invalid_request_error -- no route in the
	// pinned package answers card_error or api_error. The code is omitted
	// where the bundle declares none (its missing-param errors carry no code
	// member on the wire), and param rides in the fault's Fields.
	if svc.ID == "stripe.api" {
		e := map[string]any{"type": "invalid_request_error", "message": f.Message}
		code := f.Code
		if c, ok := f.Fields["code"].(string); ok {
			code = c
		}
		if code != "" {
			e["code"] = code
		}
		if p, ok := f.Fields["param"]; ok && p != nil && fmt.Sprint(p) != "" {
			e["param"] = p
		}
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": e})
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
	if svc.ID == "fly.machines" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"error": f.Message})
	}
	w.Header().Set("x-amzn-errortype", f.Code)
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(map[string]any{"message": f.Message, "__type": f.Code})
}

// encodeVercelBlobContent unfolds ServeBlob's envelope: the wire answer is the
// stored bytes with ETag/Cache-Control (and, on a full answer, Content-Type and
// an optional Content-Disposition) as headers, or a header-only 304 when
// If-None-Match matched. The bundle projects the record's members; the codec
// places them, the same division of labour as writeAzureBlobHeaders.
func encodeVercelBlobContent(w http.ResponseWriter, status int, resp *spi.Response) error {
	out := resp.Output
	if nm, _ := out["not_modified"].(bool); nm {
		status = http.StatusNotModified
	}
	if s, _ := out["etag"].(string); s != "" {
		w.Header().Set("ETag", s)
	}
	if s, _ := out["cache_control"].(string); s != "" {
		w.Header().Set("Cache-Control", s)
	}
	if status == http.StatusNotModified {
		w.WriteHeader(status)
		return nil
	}
	if s, _ := out["content_type"].(string); s != "" {
		w.Header().Set("Content-Type", s)
	}
	if s, _ := out["content_disposition"].(string); s != "" {
		w.Header().Set("Content-Disposition", s)
	}
	w.WriteHeader(status)
	body, _ := out["body"].(string)
	_, err := io.WriteString(w, body)
	return err
}

// rawBody reports the opaque body an operation projected, if it projected one.
func rawBody(resp *spi.Response) (string, bool) {
	if resp == nil || resp.Output == nil {
		return "", false
	}
	raw, ok := resp.Output[bir.TopLevelRaw].(string)
	return raw, ok
}

// isListMember reports whether the operation's input shape declares this
// member with a list shape.
// fillOpenSearchPath reads document coordinates off an Elasticsearch-style
// path the way the pack's fill did: the segment before _doc or _search is
// the index, the one after _doc is the id, and a domain/opensearch/_aws
// prefix names the domain. The pack guarded only the domain/opensearch
// prefix form; the index, id and _aws forms overwrite what the request
// carried, and so does this.
func fillOpenSearchPath(in map[string]any, path string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		if (part == "domain" || part == "opensearch") && i+1 < len(parts) && parts[i+1] != "domain" && parts[i+1] != "list" {
			if _, ok := in["DomainName"]; !ok {
				in["DomainName"] = parts[i+1]
			}
		}
		if part == "_doc" || part == "_search" {
			if i > 0 {
				in["Index"] = parts[i-1]
			}
			if part == "_doc" && i+1 < len(parts) {
				in["Id"] = parts[i+1]
			}
		}
		if part == "_aws" && i+2 < len(parts) && parts[i+1] == "opensearch" {
			in["DomainName"] = parts[i+2]
		}
	}
}

func isListMember(svc *model.Service, op *model.Operation, name string) bool {
	if op.Input == "" {
		return false
	}
	shape, ok := svc.Shapes[op.Input]
	if !ok {
		return false
	}
	m, ok := shape.Members[name]
	if !ok {
		return false
	}
	return svc.Shapes[m.Shape].Kind == model.KindList
}

// assignForm places one form field into the input, nesting Rack-style bracket
// keys: `line_items[0][price]=x` is line_items -> [0] -> price, `metadata[k]=v`
// is metadata -> k, and `ids[]=a&ids[]=b` appends. A flat key is the common
// case and assigns directly. Stripe's SDK form-encodes every nested structure
// this way, and the parse mirrors the vendor oracle's parseStripeBody -- minus
// its numeric coercion, which here is the bundle's job (a member's declared
// shape says whether it is a number; the form itself does not).
func assignForm(in map[string]any, key string, vs []string) {
	if !strings.Contains(key, "[") {
		in[key] = vs[len(vs)-1]
		return
	}
	parts := strings.Split(strings.ReplaceAll(key, "]", ""), "[")
	in[parts[0]] = formAssign(in[parts[0]], parts[1:], vs)
}

// formAssign walks parts into the container cur -- nil, a map, or a list --
// and answers the updated container. A numeric or empty part indexes a list
// (empty appends), anything else a map member.
func formAssign(cur any, parts []string, vs []string) any {
	if len(parts) == 0 {
		return vs[len(vs)-1]
	}
	head := parts[0]
	if head == "" {
		list, _ := cur.([]any)
		for _, v := range vs {
			list = append(list, v)
		}
		return list
	}
	if isDigits(head) {
		list, _ := cur.([]any)
		n, _ := strconv.Atoi(head)
		for len(list) <= n {
			list = append(list, nil)
		}
		list[n] = formAssign(list[n], parts[1:], vs)
		return list
	}
	m, ok := cur.(map[string]any)
	if !ok || m == nil {
		m = map[string]any{}
	}
	m[head] = formAssign(m[head], parts[1:], vs)
	return m
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
