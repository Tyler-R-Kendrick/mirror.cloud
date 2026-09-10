// Package restjson implements restJson1.
package restjson

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

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
	if svc.ID == "vercel.api" {
		return vercelOp(svc, r), nil
	}
	if svc.ID == "cloudflare.kv" {
		return cloudflareOp(svc, r), nil
	}
	if svc.ID == "hostinger.dns" {
		return hostingerOp(svc, r), nil
	}
	if svc.ID == "digitalocean.v2" {
		return digitaloceanOp(svc, r), nil
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

func digitaloceanOp(svc *model.Service, r *http.Request) *model.Operation {
	name := digitaloceanRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func digitaloceanRoute(r *http.Request) string {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	m := r.Method
	if len(parts) >= 2 && parts[0] == "v2" && parts[1] == "droplets" {
		if len(parts) == 2 && m == http.MethodPost {
			return "CreateDroplet"
		}
		if len(parts) == 2 && m == http.MethodGet {
			return "ListDroplets"
		}
		if len(parts) >= 3 && m == http.MethodGet {
			return "GetDroplet"
		}
		if len(parts) >= 3 && m == http.MethodDelete {
			return "DeleteDroplet"
		}
	}
	if len(parts) >= 2 && parts[0] == "v2" && parts[1] == "domains" {
		if len(parts) == 2 && m == http.MethodPost {
			return "CreateDomain"
		}
		if len(parts) == 2 && m == http.MethodGet {
			return "ListDomains"
		}
		if len(parts) >= 3 && m == http.MethodGet {
			return "GetDomain"
		}
		if len(parts) >= 3 && m == http.MethodDelete {
			return "DeleteDomain"
		}
	}
	return "Unknown"
}

func hostingerOp(svc *model.Service, r *http.Request) *model.Operation {
	name := hostingerRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func hostingerRoute(r *http.Request) string {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	m := r.Method
	if len(parts) >= 4 && parts[0] == "api" && parts[1] == "domains" && parts[2] == "v1" && parts[3] == "portfolio" {
		if len(parts) == 4 && m == http.MethodPost {
			return "CreateDomain"
		}
		if len(parts) == 4 && m == http.MethodGet {
			return "ListDomains"
		}
		if len(parts) >= 5 && m == http.MethodGet {
			return "GetDomain"
		}
	}
	if len(parts) >= 5 && parts[0] == "api" && parts[1] == "dns" && parts[2] == "v1" && parts[3] == "zones" {
		switch m {
		case http.MethodGet:
			return "GetDNSRecords"
		case http.MethodPut:
			return "UpdateDNSRecords"
		case http.MethodDelete:
			return "DeleteDNSRecords"
		}
	}
	return "Unknown"
}

func cloudflareOp(svc *model.Service, r *http.Request) *model.Operation {
	name := cloudflareRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func cloudflareRoute(r *http.Request) string {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "client" && parts[1] == "v4" {
		parts = parts[2:]
	}
	m := r.Method
	if len(parts) >= 5 && parts[0] == "accounts" && parts[2] == "storage" && parts[3] == "kv" && parts[4] == "namespaces" {
		if len(parts) == 5 && m == http.MethodPost {
			return "CreateNamespace"
		}
		if len(parts) == 5 && m == http.MethodGet {
			return "ListNamespaces"
		}
		if len(parts) == 6 && m == http.MethodGet {
			return "GetNamespace"
		}
		if len(parts) >= 8 && parts[6] == "values" {
			switch m {
			case http.MethodPut:
				return "PutValue"
			case http.MethodGet:
				return "GetValue"
			case http.MethodDelete:
				return "DeleteValue"
			}
		}
	}
	return "Unknown"
}

func vercelOp(svc *model.Service, r *http.Request) *model.Operation {
	name := vercelRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func vercelRoute(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	if path == "" {
		return "KvCommand"
	}
	parts := strings.Split(path, "/")
	if len(parts) > 0 && len(parts[0]) >= 2 && parts[0][0] == 'v' && parts[0][1] >= '0' && parts[0][1] <= '9' {
		parts = parts[1:]
	}
	if len(parts) == 0 || parts[0] == "" {
		return "KvCommand"
	}
	m := r.Method
	join := strings.Join(parts, "/")
	switch {
	case len(parts) == 0:
		return "KvCommand"
	case parts[0] == "user":
		return "GetUser"
	case join == "projects" && m == http.MethodPost:
		return "CreateProject"
	case join == "projects" && m == http.MethodGet:
		return "ListProjects"
	case len(parts) == 2 && parts[0] == "projects" && m == http.MethodGet:
		return "GetProject"
	case len(parts) == 2 && parts[0] == "projects" && m == http.MethodDelete:
		return "DeleteProject"
	case len(parts) >= 3 && parts[0] == "projects" && parts[2] == "env" && m == http.MethodGet:
		return "ListProjectEnv"
	case len(parts) >= 3 && parts[0] == "projects" && parts[2] == "env" && m == http.MethodPost:
		return "CreateProjectEnv"
	case len(parts) >= 4 && parts[0] == "projects" && parts[2] == "env" && m == http.MethodDelete:
		return "DeleteProjectEnv"
	case len(parts) >= 3 && parts[0] == "projects" && parts[2] == "domains" && m == http.MethodGet:
		return "ListProjectDomains"
	case len(parts) >= 3 && parts[0] == "projects" && parts[2] == "domains" && m == http.MethodPost:
		return "AddProjectDomain"
	case join == "deployments" && m == http.MethodPost:
		return "CreateDeployment"
	case join == "deployments" && m == http.MethodGet:
		return "ListDeployments"
	case len(parts) == 2 && parts[0] == "deployments" && m == http.MethodGet:
		return "GetDeployment"
	case len(parts) == 2 && parts[0] == "deployments" && m == http.MethodDelete:
		return "DeleteDeployment"
	}
	return "Unknown"
}

func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	body, _ := io.ReadAll(r.Body)
	in := map[string]any{}
	if svc.ID == "cloudflare.kv" && op.Name == "PutValue" {
		in["value"] = string(body)
		return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
	}
	if len(body) > 0 && body[0] == '[' {
		var cmd []any
		_ = json.Unmarshal(body, &cmd)
		in["_redis"] = cmd
	} else if len(body) > 0 {
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
	if svc.ID == "cloudflare.kv" {
		return encodeCloudflare(w, status, resp)
	}
	if svc.ID == "hostinger.dns" {
		return encodeHostinger(w, status, resp)
	}
	if svc.ID == "digitalocean.v2" {
		return encodeDigitalOcean(w, status, resp)
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
	return json.NewEncoder(w).Encode(resp.Output)
}

func encodeCloudflare(w http.ResponseWriter, status int, resp *spi.Response) error {
	if resp.Output != nil {
		if raw, ok := resp.Output["_raw"].(string); ok {
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", "text/plain")
			}
			w.WriteHeader(status)
			_, err := io.WriteString(w, raw)
			return err
		}
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	var result any
	if resp.Output != nil {
		if lst, ok := resp.Output["_list"]; ok {
			result = lst
		} else if _, ok := resp.Output["_null"]; ok {
			result = nil
		} else {
			result = resp.Output
		}
	}
	return json.NewEncoder(w).Encode(map[string]any{"success": true, "errors": []any{}, "messages": []any{}, "result": result})
}

func encodeDigitalOcean(w http.ResponseWriter, status int, resp *spi.Response) error {
	if status == 204 {
		w.WriteHeader(status)
		return nil
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	if resp.Output == nil {
		return nil
	}
	wrap, _ := resp.Output["_wrap"].(string)
	if lst, ok := resp.Output["_list"]; ok {
		items, _ := lst.([]any)
		if items == nil {
			items = []any{}
		}
		if wrap == "" {
			wrap = "droplets"
		}
		return json.NewEncoder(w).Encode(map[string]any{wrap: items, "meta": map[string]any{"total": len(items)}})
	}
	if wrap != "" {
		if rec, ok := resp.Output[wrap]; ok {
			return json.NewEncoder(w).Encode(map[string]any{wrap: rec})
		}
	}
	return json.NewEncoder(w).Encode(resp.Output)
}

func encodeHostinger(w http.ResponseWriter, status int, resp *spi.Response) error {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	if resp.Output == nil {
		return nil
	}
	if lst, ok := resp.Output["_list"]; ok {
		return json.NewEncoder(w).Encode(lst)
	}
	return json.NewEncoder(w).Encode(resp.Output)
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
	if svc.ID == "cloudflare.kv" {
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
	if svc.ID == "hostinger.dns" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"message": f.Message, "correlation_id": "mirror"})
	}
	if svc.ID == "digitalocean.v2" {
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"id": f.Code, "message": f.Message})
	}
	w.Header().Set("x-amzn-errortype", f.Code)
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(map[string]any{"message": f.Message, "__type": f.Code})
}
