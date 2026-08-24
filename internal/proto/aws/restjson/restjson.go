// Package restjson implements restJson1.
package restjson

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/awsjson"
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
	for i := range svc.Operations {
		op := &svc.Operations[i]
		if op.HTTP.Method == r.Method {
			return op, nil
		}
	}
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

func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	body, _ := io.ReadAll(r.Body)
	in := map[string]any{}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}
	for k, vs := range r.URL.Query() {
		if _, ok := in[k]; !ok {
			in[k] = vs[0]
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

func (Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	status := f.HTTPStatus
	if status == 0 {
		status = 400
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-errortype", f.Code)
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
		status = 501
	}
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(map[string]any{"message": f.Message, "__type": f.Code})
}
