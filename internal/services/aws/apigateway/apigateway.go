// Package apigateway emulates REST APIs with Lambda AWS_PROXY integration.
package apigateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.apigateway", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements API Gateway REST-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.apigateway" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateRestApi", "GetRestApi", "GetRestApis", "DeleteRestApi", "UpdateRestApi",
		"CreateResource", "GetResource", "GetResources", "DeleteResource",
		"PutMethod", "GetMethod", "DeleteMethod",
		"PutIntegration", "GetIntegration", "DeleteIntegration",
		"PutMethodResponse", "GetMethodResponse", "DeleteMethodResponse",
		"PutIntegrationResponse", "GetIntegrationResponse", "DeleteIntegrationResponse",
		"CreateDeployment", "GetDeployment", "GetDeployments", "DeleteDeployment",
		"CreateStage", "GetStage", "UpdateStage", "DeleteStage",
		"CreateAuthorizer", "GetAuthorizer", "GetAuthorizers", "DeleteAuthorizer",
		"CreateApiKey", "GetApiKey", "GetApiKeys", "DeleteApiKey",
		"CreateUsagePlan", "GetUsagePlan", "GetUsagePlans", "DeleteUsagePlan",
		"ExecuteApi"}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP != nil {
		req.Operation = route(req)
		p.fillPath(req)
	}
	switch req.Operation {
	case "CreateRestApi":
		name := str(req.Input["name"])
		if name == "" {
			name = str(req.Input["Name"])
		}
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "name": name, "rootResourceId": "root"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw").Put(ctx, id, b)
		rb, _ := json.Marshal(map[string]any{"id": "root", "path": "/", "pathPart": "", "parentId": ""})
		_ = p.col(req, "apigw-res").Put(ctx, id+"/root", rb)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetRestApi":
		id := str(req.Input["restApiId"])
		b, ok, _ := p.col(req, "apigw").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "GetRestApis":
		kvs, _, _ := p.col(req, "apigw").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"item": items}}, nil
	case "DeleteRestApi":
		_ = p.col(req, "apigw").Delete(ctx, str(req.Input["restApiId"]))
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case "UpdateRestApi":
		id := str(req.Input["restApiId"])
		b, ok, _ := p.col(req, "apigw").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if n := first(req.Input, "name", "Name"); n != "" {
			rec["name"] = n
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "apigw").Put(ctx, id, nb)
		return &spi.Response{Output: rec}, nil
	case "CreateResource":
		api := str(req.Input["restApiId"])
		parent := str(req.Input["parentId"])
		part := str(req.Input["pathPart"])
		id := p.deps.Rand.Hex(8)
		parentPath := "/"
		if pb, ok, _ := p.col(req, "apigw-res").Get(ctx, api+"/"+parent); ok {
			var pm map[string]any
			_ = json.Unmarshal(pb, &pm)
			parentPath = str(pm["path"])
		}
		path := parentPath
		if path == "/" {
			path = "/" + part
		} else {
			path = path + "/" + part
		}
		rec := map[string]any{"id": id, "parentId": parent, "pathPart": part, "path": path}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-res").Put(ctx, api+"/"+id, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetResources":
		api := str(req.Input["restApiId"])
		kvs, _, _ := p.col(req, "apigw-res").List(ctx, api+"/", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"item": items}}, nil
	case "GetResource":
		api, rid := str(req.Input["restApiId"]), str(req.Input["resourceId"])
		b, ok, _ := p.col(req, "apigw-res").Get(ctx, api+"/"+rid)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteResource":
		_ = p.col(req, "apigw-res").Delete(ctx, str(req.Input["restApiId"])+"/"+str(req.Input["resourceId"]))
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case "PutMethod":
		api, rid, meth := str(req.Input["restApiId"]), str(req.Input["resourceId"]), strings.ToUpper(str(req.Input["httpMethod"]))
		if meth == "" {
			meth = strings.ToUpper(str(req.Input["httpMethod"]))
		}
		rec := map[string]any{"httpMethod": meth, "authorizationType": first(req.Input, "authorizationType", "AuthorizationType")}
		if rec["authorizationType"] == "" {
			rec["authorizationType"] = "NONE"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-meth").Put(ctx, api+"/"+rid+"/"+meth, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetMethod":
		return p.getKeyed(ctx, req, "apigw-meth", str(req.Input["restApiId"])+"/"+str(req.Input["resourceId"])+"/"+strings.ToUpper(first(req.Input, "httpMethod")))
	case "DeleteMethod":
		_ = p.col(req, "apigw-meth").Delete(ctx, str(req.Input["restApiId"])+"/"+str(req.Input["resourceId"])+"/"+strings.ToUpper(first(req.Input, "httpMethod")))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PutIntegration":
		api, rid, meth := str(req.Input["restApiId"]), str(req.Input["resourceId"]), strings.ToUpper(first(req.Input, "httpMethod"))
		rec := map[string]any{
			"type":       first(req.Input, "type", "Type"),
			"uri":        first(req.Input, "uri", "Uri"),
			"httpMethod": first(req.Input, "integrationHttpMethod", "httpMethod"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-integ").Put(ctx, api+"/"+rid+"/"+meth, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetIntegration":
		return p.getKeyed(ctx, req, "apigw-integ", str(req.Input["restApiId"])+"/"+str(req.Input["resourceId"])+"/"+strings.ToUpper(first(req.Input, "httpMethod")))
	case "DeleteIntegration":
		_ = p.col(req, "apigw-integ").Delete(ctx, str(req.Input["restApiId"])+"/"+str(req.Input["resourceId"])+"/"+strings.ToUpper(first(req.Input, "httpMethod")))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PutMethodResponse":
		key := str(req.Input["restApiId"]) + "/" + str(req.Input["resourceId"]) + "/" + strings.ToUpper(first(req.Input, "httpMethod")) + "/" + first(req.Input, "statusCode")
		rec := map[string]any{"statusCode": first(req.Input, "statusCode")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-methresp").Put(ctx, key, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetMethodResponse":
		key := str(req.Input["restApiId"]) + "/" + str(req.Input["resourceId"]) + "/" + strings.ToUpper(first(req.Input, "httpMethod")) + "/" + first(req.Input, "statusCode")
		return p.getKeyed(ctx, req, "apigw-methresp", key)
	case "DeleteMethodResponse":
		key := str(req.Input["restApiId"]) + "/" + str(req.Input["resourceId"]) + "/" + strings.ToUpper(first(req.Input, "httpMethod")) + "/" + first(req.Input, "statusCode")
		_ = p.col(req, "apigw-methresp").Delete(ctx, key)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PutIntegrationResponse":
		key := str(req.Input["restApiId"]) + "/" + str(req.Input["resourceId"]) + "/" + strings.ToUpper(first(req.Input, "httpMethod")) + "/" + first(req.Input, "statusCode")
		rec := map[string]any{"statusCode": first(req.Input, "statusCode"), "selectionPattern": first(req.Input, "selectionPattern")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-integresp").Put(ctx, key, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetIntegrationResponse":
		key := str(req.Input["restApiId"]) + "/" + str(req.Input["resourceId"]) + "/" + strings.ToUpper(first(req.Input, "httpMethod")) + "/" + first(req.Input, "statusCode")
		return p.getKeyed(ctx, req, "apigw-integresp", key)
	case "DeleteIntegrationResponse":
		key := str(req.Input["restApiId"]) + "/" + str(req.Input["resourceId"]) + "/" + strings.ToUpper(first(req.Input, "httpMethod")) + "/" + first(req.Input, "statusCode")
		_ = p.col(req, "apigw-integresp").Delete(ctx, key)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "CreateDeployment":
		api := str(req.Input["restApiId"])
		id := p.deps.Rand.Hex(8)
		stage := first(req.Input, "stageName", "StageName")
		rec := map[string]any{"id": id, "stageName": stage}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-dep").Put(ctx, api+"/"+id, b)
		if stage != "" {
			_ = p.col(req, "apigw-stage").Put(ctx, api+"/"+stage, []byte(id))
		}
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetDeployments":
		api := str(req.Input["restApiId"])
		kvs, _, _ := p.col(req, "apigw-dep").List(ctx, api+"/", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"item": items}}, nil
	case "GetDeployment":
		api, id := str(req.Input["restApiId"]), first(req.Input, "deploymentId")
		return p.getKeyed(ctx, req, "apigw-dep", api+"/"+id)
	case "DeleteDeployment":
		_ = p.col(req, "apigw-dep").Delete(ctx, str(req.Input["restApiId"])+"/"+first(req.Input, "deploymentId"))
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case "CreateStage":
		api := str(req.Input["restApiId"])
		stage := first(req.Input, "stageName")
		dep := first(req.Input, "deploymentId")
		_ = p.col(req, "apigw-stage").Put(ctx, api+"/"+stage, []byte(dep))
		return &spi.Response{Status: 201, Output: map[string]any{"stageName": stage, "deploymentId": dep}}, nil
	case "GetStage":
		api, stage := str(req.Input["restApiId"]), first(req.Input, "stageName")
		b, ok, _ := p.col(req, "apigw-stage").Get(ctx, api+"/"+stage)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"stageName": stage, "deploymentId": string(b)}}, nil
	case "UpdateStage":
		api, stage := str(req.Input["restApiId"]), first(req.Input, "stageName")
		b, _, _ := p.col(req, "apigw-stage").Get(ctx, api+"/"+stage)
		dep := string(b)
		if d := first(req.Input, "deploymentId"); d != "" {
			dep = d
			_ = p.col(req, "apigw-stage").Put(ctx, api+"/"+stage, []byte(dep))
		}
		return &spi.Response{Output: map[string]any{"stageName": stage, "deploymentId": dep}}, nil
	case "DeleteStage":
		_ = p.col(req, "apigw-stage").Delete(ctx, str(req.Input["restApiId"])+"/"+first(req.Input, "stageName"))
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case "CreateAuthorizer":
		api := str(req.Input["restApiId"])
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "name": first(req.Input, "name", "Name"), "type": first(req.Input, "type", "Type")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-auth").Put(ctx, api+"/"+id, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetAuthorizer":
		return p.getKeyed(ctx, req, "apigw-auth", str(req.Input["restApiId"])+"/"+first(req.Input, "authorizerId"))
	case "GetAuthorizers":
		return p.listPref(ctx, req, "apigw-auth", str(req.Input["restApiId"])+"/")
	case "DeleteAuthorizer":
		_ = p.col(req, "apigw-auth").Delete(ctx, str(req.Input["restApiId"])+"/"+first(req.Input, "authorizerId"))
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case "CreateApiKey":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "name": first(req.Input, "name", "Name"), "value": p.deps.Rand.Hex(20), "enabled": true}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-key").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetApiKey":
		return p.getKeyed(ctx, req, "apigw-key", first(req.Input, "apiKey"))
	case "GetApiKeys":
		return p.listPref(ctx, req, "apigw-key", "")
	case "DeleteApiKey":
		_ = p.col(req, "apigw-key").Delete(ctx, first(req.Input, "apiKey"))
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case "CreateUsagePlan":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "name": first(req.Input, "name", "Name")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw-up").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: rec}, nil
	case "GetUsagePlan":
		return p.getKeyed(ctx, req, "apigw-up", first(req.Input, "usagePlanId"))
	case "GetUsagePlans":
		return p.listPref(ctx, req, "apigw-up", "")
	case "DeleteUsagePlan":
		_ = p.col(req, "apigw-up").Delete(ctx, first(req.Input, "usagePlanId"))
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case "ExecuteApi":
		return p.execute(ctx, req)
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) getKeyed(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, key)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) listPref(ctx context.Context, req *spi.Request, col, pfx string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, pfx, "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{"item": items}}, nil
}

func (p *Pack) execute(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	api, _, path := executeParts(req)
	meth := http.MethodGet
	if req.HTTP != nil {
		meth = req.HTTP.Method
	}
	rid := p.resourceForPath(ctx, req, api, path)
	ib, ok, _ := p.col(req, "apigw-integ").Get(ctx, api+"/"+rid+"/"+meth)
	if !ok {
		ib, ok, _ = p.col(req, "apigw-integ").Get(ctx, api+"/"+rid+"/ANY")
	}
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", Message: "no integration", HTTPStatus: 404, Fault: "client"}
	}
	var integ map[string]any
	_ = json.Unmarshal(ib, &integ)
	fn := lambdaName(str(integ["uri"]))
	if fn == "" {
		return nil, &spi.Fault{Code: "NotFoundException", Message: "integration uri is not Lambda", HTTPStatus: 404, Fault: "client"}
	}
	event := proxyEvent(req, path, meth)
	in := map[string]any{"FunctionName": fn}
	for k, v := range event {
		in[k] = v
	}
	resp, err := lambda.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Invoke", Input: in})
	if err != nil {
		return nil, err
	}
	return flattenLambda(resp)
}

func flattenLambda(resp *spi.Response) (*spi.Response, error) {
	raw, _ := json.Marshal(resp.Output["Payload"])
	if s, ok := resp.Output["Payload"].(json.RawMessage); ok {
		raw = s
	}
	var proxy map[string]any
	if json.Unmarshal(raw, &proxy) == nil {
		if _, ok := proxy["statusCode"]; ok {
			sc := 200
			switch t := proxy["statusCode"].(type) {
			case float64:
				sc = int(t)
			case int:
				sc = t
			}
			body := []byte(str(proxy["body"]))
			h := http.Header{}
			if hm, ok := proxy["headers"].(map[string]any); ok {
				for k, v := range hm {
					h.Set(k, str(v))
				}
			}
			if h.Get("Content-Type") == "" {
				h.Set("Content-Type", "application/json")
			}
			return &spi.Response{Status: sc, Headers: h, Stream: io.NopCloser(bytes.NewReader(body))}, nil
		}
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	return &spi.Response{Status: 200, Headers: h, Stream: io.NopCloser(bytes.NewReader(raw))}, nil
}

func proxyEvent(req *spi.Request, path, meth string) map[string]any {
	skip := map[string]bool{"restApiId": true, "resourceId": true, "parentId": true, "httpMethod": true, "stageName": true, "pathPart": true, "FunctionName": true}
	bodyMap := map[string]any{}
	for k, v := range req.Input {
		if skip[k] {
			continue
		}
		bodyMap[k] = v
	}
	body, _ := json.Marshal(bodyMap)
	headers, query := map[string]any{}, map[string]any{}
	if req.HTTP != nil {
		for key, values := range req.HTTP.Header {
			headers[key] = strings.Join(values, ",")
		}
		for key, values := range req.HTTP.URL.Query() {
			query[key] = strings.Join(values, ",")
		}
	}
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	return map[string]any{
		"httpMethod": meth, "path": path, "body": string(body),
		"headers": headers, "queryStringParameters": query, "isBase64Encoded": false,
		"requestContext": map[string]any{"httpMethod": meth, "path": path},
	}
}

func (p *Pack) resourceForPath(ctx context.Context, req *spi.Request, api, path string) string {
	if path == "" || path == "/" {
		return "root"
	}
	kvs, _, _ := p.col(req, "apigw-res").List(ctx, api+"/", "", 0)
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		if str(rec["path"]) == path {
			return str(rec["id"])
		}
	}
	return "root"
}

func (p *Pack) fillPath(req *spi.Request) {
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "restapis" {
		req.Input["restApiId"] = parts[1]
	}
	if len(parts) >= 4 && parts[2] == "resources" {
		req.Input["resourceId"] = parts[3]
		req.Input["parentId"] = parts[3]
	}
	if i := indexOf(parts, "methods"); i >= 0 && i+1 < len(parts) {
		req.Input["httpMethod"] = parts[i+1]
	}
	if i := indexOf(parts, "stages"); i >= 0 && i+1 < len(parts) {
		req.Input["stageName"] = parts[i+1]
	}
	if i := indexOf(parts, "authorizers"); i >= 0 && i+1 < len(parts) {
		req.Input["authorizerId"] = parts[i+1]
	}
	if i := indexOf(parts, "deployments"); i >= 0 && i+1 < len(parts) {
		req.Input["deploymentId"] = parts[i+1]
	}
	if len(parts) == 2 && parts[0] == "apikeys" {
		req.Input["apiKey"] = parts[1]
	}
	if len(parts) == 2 && parts[0] == "usageplans" {
		req.Input["usagePlanId"] = parts[1]
	}
	if i := indexOf(parts, "methodresponses"); i >= 0 && i+1 < len(parts) {
		req.Input["statusCode"] = parts[i+1]
	}
	if i := indexOf(parts, "integrationresponses"); i >= 0 && i+1 < len(parts) {
		req.Input["statusCode"] = parts[i+1]
	}
}

func route(req *spi.Request) string {
	if a := req.HTTP.URL.Query().Get("Action"); a != "" {
		return a
	}
	path := req.HTTP.URL.Path
	m := req.HTTP.Method
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case strings.Contains(path, "_user_request_"):
		return "ExecuteApi"
	case len(parts) == 1 && parts[0] == "restapis" && m == http.MethodPost:
		return "CreateRestApi"
	case len(parts) == 1 && parts[0] == "restapis" && m == http.MethodGet:
		return "GetRestApis"
	case len(parts) == 2 && parts[0] == "restapis" && m == http.MethodGet:
		return "GetRestApi"
	case len(parts) == 2 && parts[0] == "restapis" && m == http.MethodDelete:
		return "DeleteRestApi"
	case len(parts) == 3 && parts[2] == "resources" && m == http.MethodGet:
		return "GetResources"
	case len(parts) == 4 && parts[2] == "resources" && m == http.MethodPost:
		return "CreateResource"
	case strings.Contains(path, "/integrationresponses"):
		switch m {
		case http.MethodPut:
			return "PutIntegrationResponse"
		case http.MethodDelete:
			return "DeleteIntegrationResponse"
		default:
			return "GetIntegrationResponse"
		}
	case strings.Contains(path, "/methodresponses"):
		switch m {
		case http.MethodPut:
			return "PutMethodResponse"
		case http.MethodDelete:
			return "DeleteMethodResponse"
		default:
			return "GetMethodResponse"
		}
	case strings.Contains(path, "/integration"):
		switch m {
		case http.MethodPut:
			return "PutIntegration"
		case http.MethodDelete:
			return "DeleteIntegration"
		default:
			return "GetIntegration"
		}
	case strings.Contains(path, "/methods/"):
		switch m {
		case http.MethodPut:
			return "PutMethod"
		case http.MethodDelete:
			return "DeleteMethod"
		default:
			return "GetMethod"
		}
	case len(parts) >= 4 && parts[2] == "resources" && m == http.MethodGet:
		return "GetResource"
	case len(parts) >= 4 && parts[2] == "resources" && m == http.MethodDelete:
		return "DeleteResource"
	case len(parts) == 3 && parts[2] == "deployments" && m == http.MethodPost:
		return "CreateDeployment"
	case len(parts) >= 4 && parts[2] == "deployments" && m == http.MethodGet:
		return "GetDeployment"
	case len(parts) >= 4 && parts[2] == "deployments" && m == http.MethodDelete:
		return "DeleteDeployment"
	case len(parts) == 3 && parts[2] == "deployments" && m == http.MethodGet:
		return "GetDeployments"
	case len(parts) >= 3 && parts[2] == "stages" && m == http.MethodPost:
		return "CreateStage"
	case len(parts) >= 4 && parts[2] == "stages" && m == http.MethodPatch:
		return "UpdateStage"
	case len(parts) >= 4 && parts[2] == "stages" && m == http.MethodDelete:
		return "DeleteStage"
	case len(parts) >= 4 && parts[2] == "stages" && m == http.MethodGet:
		return "GetStage"
	case len(parts) == 3 && parts[2] == "authorizers" && m == http.MethodPost:
		return "CreateAuthorizer"
	case len(parts) == 3 && parts[2] == "authorizers" && m == http.MethodGet:
		return "GetAuthorizers"
	case len(parts) >= 4 && parts[2] == "authorizers" && m == http.MethodDelete:
		return "DeleteAuthorizer"
	case len(parts) >= 4 && parts[2] == "authorizers":
		return "GetAuthorizer"
	case len(parts) == 1 && parts[0] == "apikeys" && m == http.MethodPost:
		return "CreateApiKey"
	case len(parts) == 1 && parts[0] == "apikeys" && m == http.MethodGet:
		return "GetApiKeys"
	case len(parts) == 2 && parts[0] == "apikeys" && m == http.MethodDelete:
		return "DeleteApiKey"
	case len(parts) == 2 && parts[0] == "apikeys":
		return "GetApiKey"
	case len(parts) == 1 && parts[0] == "usageplans" && m == http.MethodPost:
		return "CreateUsagePlan"
	case len(parts) == 1 && parts[0] == "usageplans" && m == http.MethodGet:
		return "GetUsagePlans"
	case len(parts) == 2 && parts[0] == "usageplans" && m == http.MethodDelete:
		return "DeleteUsagePlan"
	case len(parts) == 2 && parts[0] == "usageplans":
		return "GetUsagePlan"
	case len(parts) == 2 && parts[0] == "restapis" && m == http.MethodPatch:
		return "UpdateRestApi"
	default:
		return req.Operation
	}
}

func executeParts(req *spi.Request) (api, stage, path string) {
	// /restapis/{id}/{stage}/_user_request_/{path}
	s := req.HTTP.URL.Path
	const marker = "/_user_request_"
	i := strings.Index(s, marker)
	head := s
	if i >= 0 {
		head = s[:i]
		path = s[i+len(marker):]
		if path == "" {
			path = "/"
		}
	}
	parts := strings.Split(strings.Trim(head, "/"), "/")
	if len(parts) >= 2 {
		api = parts[1]
	}
	if len(parts) >= 3 {
		stage = parts[2]
	}
	return api, stage, path
}

func lambdaName(uri string) string {
	// arn:aws:apigateway:region:lambda:path/2015-03-31/functions/arn:aws:lambda:region:acct:function:NAME/invocations
	if i := strings.Index(uri, ":function:"); i >= 0 {
		rest := uri[i+len(":function:"):]
		rest = strings.TrimSuffix(rest, "/invocations")
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			rest = rest[:j]
		}
		return rest
	}
	return ""
}

func indexOf(ss []string, w string) int {
	for i, s := range ss {
		if s == w {
			return i
		}
	}
	return -1
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
