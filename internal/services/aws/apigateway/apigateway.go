// Package apigateway is API Gateway's ExecuteApi: the one operation
// behavior/aws/apigateway lists as native, because it invokes a Lambda
// function and answers with the function's own stream. The control plane is
// the bundle's; this reads the resources and integrations it stores.
package apigateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() { bundled.RegisterNative("aws.apigateway", "ExecuteApi", Execute) }

func col(deps spi.Deps, req *spi.Request, n string) spi.Collection {
	return deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

// Execute runs a request against a deployed API's Lambda AWS_PROXY integration.
func Execute(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
	api, _, path := executeParts(req)
	meth := http.MethodGet
	if req.HTTP != nil {
		meth = req.HTTP.Method
	}
	rid := resourceForPath(ctx, deps, req, api, path)
	integrations := col(deps, req, "apigwi:"+api)
	ib, ok, _ := integrations.Get(ctx, rid+"/"+meth)
	if !ok {
		ib, ok, _ = integrations.Get(ctx, rid+"/ANY")
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
	resp, err := lambda.New(deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Invoke", Input: in})
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

func resourceForPath(ctx context.Context, deps spi.Deps, req *spi.Request, api, path string) string {
	if path == "" || path == "/" {
		return "root"
	}
	kvs, _, _ := col(deps, req, "apigwres:"+api).List(ctx, "", "", 0)
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		if str(rec["path"]) == path {
			return str(rec["id"])
		}
	}
	return "root"
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

func str(v any) string {
	s, _ := v.(string)
	return s
}
