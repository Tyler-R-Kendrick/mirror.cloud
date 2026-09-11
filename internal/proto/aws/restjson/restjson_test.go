package restjson

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	generatedpp "github.com/tyler-r-kendrick/mirror.cloud/internal/generated/aws/pinpoint"
	generatedcf "github.com/tyler-r-kendrick/mirror.cloud/internal/generated/cloudflare/api"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestRESTJSONServiceRoutes(t *testing.T) {
	codec := Codec{}
	if codec.Protocol() != model.ProtoRESTJSON1 {
		t.Fatal(codec.Protocol())
	}
	for _, test := range []struct{ service, method, path, target, want string }{
		{"aws.lambda", http.MethodPost, "/2015-03-31/functions/f/invocations", "", "Invoke"},
		{"aws.lambda", http.MethodPost, "/2015-03-31/event-source-mappings", "", "CreateEventSourceMapping"},
		{"aws.lambda", http.MethodPut, "/2015-03-31/event-source-mappings/id", "", "UpdateEventSourceMapping"},
		{"aws.lambda", http.MethodDelete, "/2015-03-31/event-source-mappings/id", "", "DeleteEventSourceMapping"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/event-source-mappings", "", "ListEventSourceMappings"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/event-source-mappings/id", "", "GetEventSourceMapping"},
		{"aws.lambda", http.MethodPost, "/2015-03-31/tags/arn", "", "TagResource"},
		{"aws.lambda", http.MethodDelete, "/2015-03-31/tags/arn", "", "UntagResource"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/tags/arn", "", "ListTags"},
		{"aws.lambda", http.MethodPut, "/2015-03-31/functions/f/code", "", "UpdateFunctionCode"},
		{"aws.lambda", http.MethodPut, "/2015-03-31/functions/f/configuration", "", "UpdateFunctionConfiguration"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/functions/f/configuration", "", "GetFunctionConfiguration"},
		{"aws.lambda", http.MethodPost, "/2015-03-31/functions/f/versions", "", "PublishVersion"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/functions/f/versions", "", "ListVersionsByFunction"},
		{"aws.lambda", http.MethodPost, "/2015-03-31/functions/f/aliases", "", "CreateAlias"},
		{"aws.lambda", http.MethodPut, "/2015-03-31/functions/f/aliases/a", "", "UpdateAlias"},
		{"aws.lambda", http.MethodDelete, "/2015-03-31/functions/f/aliases/a", "", "DeleteAlias"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/functions/f/aliases", "", "ListAliases"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/functions/f/aliases/a", "", "GetAlias"},
		{"azure.table", http.MethodPost, "/Tables", "", "CreateTable"},
		{"azure.table", http.MethodGet, "/Tables", "", "ListTables"},
		{"azure.table", http.MethodDelete, "/Tables('t')", "", "DeleteTable"},
		{"azure.table", http.MethodPost, "/t", "", "InsertEntity"},
		{"azure.table", http.MethodGet, "/t()", "", "QueryEntities"},
		{"azure.table", http.MethodGet, "/t(PartitionKey='p',RowKey='r')", "", "GetEntity"},
		{"azure.table", http.MethodPut, "/t(PartitionKey='p',RowKey='r')", "", "UpdateEntity"},
		{"azure.table", http.MethodPatch, "/t(PartitionKey='p',RowKey='r')", "", "MergeEntity"},
		{"azure.table", http.MethodDelete, "/t(PartitionKey='p',RowKey='r')", "", "DeleteEntity"},
		{"azure.table", http.MethodPost, "/$batch", "", "SubmitBatch"},
		{"aws.lambda", http.MethodPost, "/2015-03-31/functions/f/policy", "", "AddPermission"},
		{"aws.lambda", http.MethodDelete, "/2015-03-31/functions/f/policy/sid", "", "RemovePermission"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/functions/f/policy", "", "GetPolicy"},
		{"aws.lambda", http.MethodPut, "/2017-10-31/functions/f/concurrency", "", "PutFunctionConcurrency"},
		{"aws.lambda", http.MethodDelete, "/2017-10-31/functions/f/concurrency", "", "DeleteFunctionConcurrency"},
		{"aws.lambda", http.MethodGet, "/2017-10-31/functions/f/concurrency", "", "GetFunctionConcurrency"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/functions/f", "", "GetFunction"},
		{"aws.lambda", http.MethodGet, "/2015-03-31/functions", "", "ListFunctions"},
		{"aws.lambda", http.MethodDelete, "/2015-03-31/functions/f", "", "DeleteFunction"},
		{"aws.lambda", http.MethodPost, "/2015-03-31/functions", "", "CreateFunction"},

		{"aws.apigateway", http.MethodPost, "/restapis", "", "CreateRestApi"},
		{"aws.apigateway", http.MethodGet, "/restapis", "", "GetRestApis"},
		{"aws.apigateway", http.MethodGet, "/restapis/api", "", "GetRestApi"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api", "", "DeleteRestApi"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/resources", "", "GetResources"},
		{"aws.apigateway", http.MethodPost, "/restapis/api/resources/root", "", "CreateResource"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/resources/id", "", "GetResource"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/resources/id", "", "DeleteResource"},
		{"aws.apigateway", http.MethodPut, "/restapis/api/resources/id/methods/GET/integration/responses/200", "", "PutIntegrationResponse"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/resources/id/methods/GET/integration/responses/200", "", "DeleteIntegrationResponse"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/resources/id/methods/GET/integration/responses/200", "", "GetIntegrationResponse"},
		{"aws.apigateway", http.MethodPut, "/restapis/api/resources/id/methods/GET/responses/200", "", "PutMethodResponse"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/resources/id/methods/GET/responses/200", "", "DeleteMethodResponse"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/resources/id/methods/GET/responses/200", "", "GetMethodResponse"},
		{"aws.apigateway", http.MethodPut, "/restapis/api/resources/id/methods/GET/integration", "", "PutIntegration"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/resources/id/methods/GET/integration", "", "DeleteIntegration"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/resources/id/methods/GET/integration", "", "GetIntegration"},
		{"aws.apigateway", http.MethodPut, "/restapis/api/resources/id/methods/GET", "", "PutMethod"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/resources/id/methods/GET", "", "DeleteMethod"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/resources/id/methods/GET", "", "GetMethod"},
		{"aws.apigateway", http.MethodPost, "/restapis/api/deployments", "", "CreateDeployment"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/deployments/id", "", "GetDeployment"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/deployments/id", "", "DeleteDeployment"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/deployments", "", "GetDeployments"},
		{"aws.apigateway", http.MethodPost, "/restapis/api/stages", "", "CreateStage"},
		{"aws.apigateway", http.MethodPatch, "/restapis/api/stages/dev", "", "UpdateStage"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/stages/dev", "", "DeleteStage"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/stages/dev", "", "GetStage"},
		{"aws.apigateway", http.MethodPost, "/restapis/api/authorizers", "", "CreateAuthorizer"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/authorizers", "", "GetAuthorizers"},
		{"aws.apigateway", http.MethodDelete, "/restapis/api/authorizers/id", "", "DeleteAuthorizer"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/authorizers/id", "", "GetAuthorizer"},
		{"aws.apigateway", http.MethodPost, "/apikeys", "", "CreateApiKey"},
		{"aws.apigateway", http.MethodGet, "/apikeys", "", "GetApiKeys"},
		{"aws.apigateway", http.MethodDelete, "/apikeys/id", "", "DeleteApiKey"},
		{"aws.apigateway", http.MethodGet, "/apikeys/id", "", "GetApiKey"},
		{"aws.apigateway", http.MethodPost, "/usageplans", "", "CreateUsagePlan"},
		{"aws.apigateway", http.MethodGet, "/usageplans", "", "GetUsagePlans"},
		{"aws.apigateway", http.MethodDelete, "/usageplans/id", "", "DeleteUsagePlan"},
		{"aws.apigateway", http.MethodGet, "/usageplans/id", "", "GetUsagePlan"},
		{"aws.apigateway", http.MethodPatch, "/restapis/api", "", "UpdateRestApi"},
		{"aws.apigateway", http.MethodGet, "/restapis/api/dev/_user_request_/path", "", "ExecuteApi"},

		{"aws.eks", http.MethodPost, "/clusters/c/node-groups", "", "CreateNodegroup"},
		{"aws.eks", http.MethodGet, "/clusters/c/node-groups/n", "", "DescribeNodegroup"},
		{"aws.eks", http.MethodGet, "/clusters/c/node-groups", "", "ListNodegroups"},
		{"aws.eks", http.MethodDelete, "/clusters/c/node-groups/n", "", "DeleteNodegroup"},
		{"aws.eks", http.MethodPost, "/clusters", "", "CreateCluster"},
		{"aws.eks", http.MethodGet, "/clusters", "", "ListClusters"},
		{"aws.eks", http.MethodDelete, "/clusters/c", "", "DeleteCluster"},
		{"aws.eks", http.MethodGet, "/clusters/c", "", "DescribeCluster"},

		{"aws.es", http.MethodPost, "/2021-01-01/opensearch/domain", "", "CreateDomain"},
		{"aws.es", http.MethodGet, "/2021-01-01/opensearch/domain/d", "", "DescribeDomain"},
		{"aws.es", http.MethodDelete, "/2021-01-01/opensearch/domain/d", "", "DeleteDomain"},
		{"aws.es", http.MethodPost, "/index/_search", "", "Search"},
		{"aws.es", http.MethodPut, "/index/_doc/id", "", "IndexDocument"},
		{"aws.es", http.MethodGet, "/index/_doc/id", "", "GetDocument"},
		{"aws.es", http.MethodDelete, "/index/_doc/id", "", "DeleteDocument"},
		{"aws.es", http.MethodPost, "/", "OpenSearch_20210101.Custom", "Custom"},

		{"vercel.api", http.MethodGet, "/v2/user", "", "GetUser"},
		{"vercel.api", http.MethodPost, "/v11/projects", "", "CreateProject"},
		{"vercel.api", http.MethodGet, "/v9/projects", "", "ListProjects"},
		{"vercel.api", http.MethodGet, "/v9/projects/app", "", "GetProject"},
		{"vercel.api", http.MethodDelete, "/v9/projects/app", "", "DeleteProject"},
		{"vercel.api", http.MethodGet, "/v9/projects/app/env", "", "ListProjectEnv"},
		{"vercel.api", http.MethodPost, "/v10/projects/app/env", "", "CreateProjectEnv"},
		{"vercel.api", http.MethodDelete, "/v9/projects/app/env/env_1", "", "DeleteProjectEnv"},
		{"vercel.api", http.MethodGet, "/v10/projects/app/domains", "", "ListProjectDomains"},
		{"vercel.api", http.MethodPost, "/v10/projects/app/domains", "", "AddProjectDomain"},
		{"vercel.api", http.MethodPost, "/v13/deployments", "", "CreateDeployment"},
		{"vercel.api", http.MethodGet, "/v6/deployments", "", "ListDeployments"},
		{"vercel.api", http.MethodGet, "/v13/deployments/dpl_1", "", "GetDeployment"},
		{"vercel.api", http.MethodDelete, "/v13/deployments/dpl_1", "", "DeleteDeployment"},
		{"vercel.api", http.MethodPost, "/", "", "KvCommand"},
		{"vercel.api", http.MethodGet, "/v9/unknown", "", "Unknown"},

		// Cloudflare's rows are gone with its route table, exactly as
		// Hostinger's were. TestCloudflareRoutesFromItsGeneratedModel below
		// routes the same URIs against the model instead, which is what the
		// edge does now.

		// Hostinger's rows are gone with its route table. It is served from a
		// bundle now, so the URIs come from the generated model and
		// httpuri.Match routes them like every other modelled service; the
		// behaviour is covered end to end by test/behavior/hostinger. A case
		// here would have to assert against a hand-written table that no
		// longer exists.

		{"digitalocean.v2", http.MethodPost, "/v2/droplets", "", "CreateDroplet"},
		{"digitalocean.v2", http.MethodGet, "/v2/droplets", "", "ListDroplets"},
		{"digitalocean.v2", http.MethodGet, "/v2/droplets/1", "", "GetDroplet"},
		{"digitalocean.v2", http.MethodDelete, "/v2/droplets/1", "", "DeleteDroplet"},
		{"digitalocean.v2", http.MethodPost, "/v2/domains", "", "CreateDomain"},
		{"digitalocean.v2", http.MethodGet, "/v2/domains", "", "ListDomains"},
		{"digitalocean.v2", http.MethodGet, "/v2/domains/ex.test", "", "GetDomain"},
		{"digitalocean.v2", http.MethodDelete, "/v2/domains/ex.test", "", "DeleteDomain"},
		{"digitalocean.v2", http.MethodGet, "/v2/unknown", "", "Unknown"},

		// Hetzner's rows are gone with its route table, for the same reason
		// Hostinger's are: it is served from a bundle, so httpuri.Match routes
		// it from the generated model and test/behavior/hetzner covers the
		// URIs end to end.
	} {
		request := httptest.NewRequest(test.method, test.path, nil)
		if test.target != "" {
			request.Header.Set("X-Amz-Target", test.target)
		}
		op, err := codec.Route(&model.Service{ID: test.service}, request)
		if err != nil || op.Name != test.want {
			t.Errorf("%s %s %s: %#v %v, want %s", test.service, test.method, test.path, op, err, test.want)
		}
	}
}

func TestRESTJSONActionAndModeledRoutes(t *testing.T) {
	codec := Codec{}
	for _, id := range []string{"aws.lambda", "aws.apigateway", "aws.eks"} {
		svc := &model.Service{ID: id, Operations: []model.Operation{{Name: "Modeled"}}}
		for _, action := range []string{"Modeled", "Synthetic"} {
			op, err := codec.Route(svc, httptest.NewRequest(http.MethodPost, "/?Action="+action, nil))
			if err != nil || op.Name != action {
				t.Fatalf("%s Action=%s: %#v %v", id, action, op, err)
			}
		}
	}
	// A restJson1 request is addressed by its path. An operation the model
	// gives no URI cannot be reached by one, however its method lines up:
	// routing on the method alone answered every GET a service served with
	// whichever GET the model listed first.
	svc := &model.Service{ID: "custom", Operations: []model.Operation{
		{Name: "ByMethod", HTTP: model.HTTPBinding{Method: http.MethodGet}},
		{Name: "ByPath", HTTP: model.HTTPBinding{Method: http.MethodGet, URI: "/anything"}},
		{Name: "ByTarget"},
	}}
	op, err := codec.Route(svc, httptest.NewRequest(http.MethodGet, "/anything", nil))
	if err != nil || op.Name != "ByPath" {
		t.Fatalf("path route %#v %v", op, err)
	}
	if op, err := codec.Route(svc, httptest.NewRequest(http.MethodGet, "/elsewhere", nil)); err == nil {
		t.Fatalf("a path no operation claims routed to %#v", op)
	}
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("X-Amz-Target", "Custom.ByTarget")
	op, err = codec.Route(svc, request)
	if err != nil || op.Name != "ByTarget" {
		t.Fatalf("target route %#v %v", op, err)
	}
}

func TestRESTJSONDecodeEncodeAndFault(t *testing.T) {
	codec := Codec{}
	svc := &model.Service{ID: "aws.lambda"}
	op := &model.Operation{Name: "Invoke", HTTP: model.HTTPBinding{Code: http.StatusAccepted}}
	request := httptest.NewRequest(http.MethodPost, "/invoke?body=query&extra=value", strings.NewReader(`{"body":"json"}`))
	decoded, err := codec.Decode(svc, op, request)
	if err != nil || decoded.Input["body"] != "json" || decoded.Input["extra"] != "value" {
		t.Fatalf("decode %#v %v", decoded, err)
	}

	// The path is part of the input. DeleteDetector is `DELETE
	// /detector/{DetectorId}` and carries no body at all, so an input built
	// from body and query alone names no detector.
	pathOp := &model.Operation{
		Name: "GetFilter",
		HTTP: model.HTTPBinding{Method: http.MethodGet, URI: "/detector/{DetectorId}/filter/{FilterName}"},
	}
	decoded, err = codec.Decode(svc, pathOp,
		httptest.NewRequest(http.MethodGet, "/detector/d-1/filter/f%2F1", nil))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Input["DetectorId"] != "d-1" || decoded.Input["FilterName"] != "f/1" {
		t.Fatalf("path members %#v", decoded.Input)
	}

	w := httptest.NewRecorder()
	if err := codec.Encode(svc, op, w, &spi.Response{Headers: http.Header{"X-Test": {"one", "two"}}, Output: map[string]any{"ok": true}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusAccepted || len(w.Header().Values("X-Test")) != 2 || !strings.Contains(w.Body.String(), `"ok":true`) {
		t.Fatalf("JSON response %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := codec.Encode(svc, op, w, &spi.Response{Status: http.StatusCreated, Stream: io.NopCloser(strings.NewReader("payload"))}); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusCreated || w.Header().Get("Content-Type") != "application/octet-stream" || w.Body.String() != "payload" {
		t.Fatalf("stream response %d %#v %q", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "Empty"}, w, &spi.Response{}); err != nil || w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Fatalf("empty response %d %v %q", w.Code, err, w.Body.String())
	}

	w = httptest.NewRecorder()
	if err := codec.EncodeFault(svc, op, w, spi.NotImplemented(svc.ID, op.Name, "emulate"), "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusNotImplemented || w.Header().Get("x-amzn-errortype") != "MirrorNotImplemented" || w.Header().Get("x-mirror-not-implemented") != "aws.lambda.Invoke" || !strings.Contains(w.Body.String(), `"__type":"MirrorNotImplemented"`) {
		t.Fatalf("fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}

	vercel := &model.Service{ID: "vercel.api"}
	decoded, err = codec.Decode(vercel, &model.Operation{Name: "KvCommand"}, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`["GET","k"]`)))
	if err != nil {
		t.Fatal(err)
	}
	cmd, _ := decoded.Input["_redis"].([]any)
	if len(cmd) != 2 || cmd[0] != "GET" || cmd[1] != "k" {
		t.Fatalf("redis decode %#v", decoded.Input)
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(vercel, &model.Operation{Name: "GetProject"}, w, &spi.Fault{Code: "not_found", Message: "missing", HTTPStatus: 404, Fault: "client"}, "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || w.Header().Get("x-amzn-errortype") != "" || !strings.Contains(w.Body.String(), `"code":"not_found"`) {
		t.Fatalf("vercel fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}

	// The bundle is served from the generated model, so the codec is handed
	// the real shapes: `body` is the payload member and `workers-kv_value` is
	// a union of a string and a blob, which is what makes it the body rather
	// than something to parse.
	cf := generatedcf.Model()
	putReq := httptest.NewRequest(http.MethodPut, "/client/v4/accounts/a/storage/kv/namespaces/n/values/k", strings.NewReader("hello"))
	decoded, err = codec.Decode(cf, cf.OperationByName("WorkersKvNamespaceWriteKeyValuePairWithMetadata"), putReq)
	if err != nil || decoded.Input["body"] != "hello" {
		t.Fatalf("put decode %#v %v", decoded, err)
	}
	// The path labels still bind: a payload member takes the body and nothing
	// else, where the branch this replaced returned early and left the
	// namespace and the key unbound.
	if decoded.Input["namespace_id"] != "n" || decoded.Input["key_name"] != "k" {
		t.Fatalf("put labels %#v", decoded.Input)
	}
	// No envelope is synthesized any more: the document declares
	// {success, errors, messages, result} as the response shape, so whatever
	// the bundle projects is what the body is.
	w = httptest.NewRecorder()
	if err := codec.Encode(cf, cf.OperationByName("WorkersKvNamespaceCreateANamespace"), w,
		&spi.Response{Output: map[string]any{"success": true, "errors": []any{}, "messages": []any{}, "result": map[string]any{"id": "n1", "title": "t"}}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"success":true`) || !strings.Contains(w.Body.String(), `"id":"n1"`) {
		t.Fatalf("cf encode %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(cf, cf.OperationByName("WorkersKvNamespaceReadKeyValuePair"), w,
		&spi.Response{Output: map[string]any{bir.TopLevelRaw: "hello"}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || w.Body.String() != "hello" || w.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("cf raw %d %q %q", w.Code, w.Body.String(), w.Header().Get("Content-Type"))
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(cf, cf.OperationByName("WorkersKvNamespaceGetANamespace"), w, &spi.Fault{Code: "10013", Message: "missing", HTTPStatus: 404, Fault: "client"}, "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || w.Header().Get("x-amzn-errortype") != "" || !strings.Contains(w.Body.String(), `"code":10013`) || !strings.Contains(w.Body.String(), `"success":false`) {
		t.Fatalf("cf fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}

	hs := &model.Service{ID: "hostinger.api"}
	w = httptest.NewRecorder()
	if err := codec.Encode(hs, &model.Operation{Name: "ListDomains"}, w, &spi.Response{Output: map[string]any{"_list": []any{map[string]any{"domain": "ex.test"}}}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || strings.Contains(w.Body.String(), `"_list"`) || !strings.Contains(w.Body.String(), `"domain":"ex.test"`) {
		t.Fatalf("hs list encode %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(hs, &model.Operation{Name: "CreateDomain"}, w, &spi.Response{Output: map[string]any{"domain": "ex.test", "status": "active"}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"domain":"ex.test"`) {
		t.Fatalf("hs encode %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(hs, &model.Operation{Name: "GetDomain"}, w, &spi.Fault{Code: "not_found", Message: "Domain not found", HTTPStatus: 404, Fault: "client"}, "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || w.Header().Get("x-amzn-errortype") != "" || !strings.Contains(w.Body.String(), `"correlation_id":"mirror"`) || !strings.Contains(w.Body.String(), `"message":"Domain not found"`) {
		t.Fatalf("hs fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}

	do := &model.Service{ID: "digitalocean.v2"}
	w = httptest.NewRecorder()
	if err := codec.Encode(do, &model.Operation{Name: "ListDroplets"}, w, &spi.Response{Output: map[string]any{"_list": []any{map[string]any{"name": "web"}}, "_wrap": "droplets"}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"droplets"`) || !strings.Contains(w.Body.String(), `"total":1`) || strings.Contains(w.Body.String(), `"_list"`) {
		t.Fatalf("do list encode %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(do, &model.Operation{Name: "GetDomain"}, w, &spi.Response{Output: map[string]any{"_wrap": "domain", "domain": map[string]any{"name": "ex.test"}}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"domain"`) || !strings.Contains(w.Body.String(), `"ex.test"`) {
		t.Fatalf("do encode %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(do, &model.Operation{Name: "DeleteDomain"}, w, &spi.Response{Status: 204}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 204 || w.Body.Len() != 0 {
		t.Fatalf("do delete %d %q", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(do, &model.Operation{Name: "GetDroplet"}, w, &spi.Fault{Code: "not_found", Message: "missing", HTTPStatus: 404, Fault: "client"}, "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || w.Header().Get("x-amzn-errortype") != "" || !strings.Contains(w.Body.String(), `"id":"not_found"`) {
		t.Fatalf("do fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}

	// Hetzner keeps only its fault envelope. The response encoder branch went
	// with the route table: the bundle answers the document's own members, so
	// the generic encoder serializes them and there is nothing left to wrap.
	hz := &model.Service{ID: "hetzner.v1"}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(hz, &model.Operation{Name: "GetServer"}, w, &spi.Fault{Code: "not_found", Message: "Server not found", HTTPStatus: 404, Fault: "client"}, "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || w.Header().Get("x-amzn-errortype") != "" || !strings.Contains(w.Body.String(), `"code":"not_found"`) || !strings.Contains(w.Body.String(), `"error"`) {
		t.Fatalf("hz fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
	// A status that forbids a body gets none, whatever the operation
	// projected. An engine-served DeleteSshKey answers 204 with an empty
	// output map, and without this the map reaches net/http as `{}`.
	w = httptest.NewRecorder()
	if err := codec.Encode(hz, &model.Operation{Name: "DeleteSshKey", HTTP: model.HTTPBinding{Code: 204}}, w, &spi.Response{Output: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 204 || w.Body.Len() != 0 {
		t.Fatalf("hz 204 %d %q", w.Code, w.Body.String())
	}

	rw := &model.Service{ID: "railway.graphql"}
	for _, test := range []struct{ query, want string }{
		{`mutation { projectCreate(input:{name:"web"}) { id } }`, "projectCreate"},
		{`{ projects { edges { node { id } } } }`, "projects"},
		{`{ project(id:"x") { id } }`, "project"},
		{`mutation { projectDelete(id:"x") }`, "projectDelete"},
		{`mutation { serviceCreate(input:{name:"api"}) { id } }`, "serviceCreate"},
		{`mutation { serviceDelete(id:"x") }`, "serviceDelete"},
		{`{ service(id:"x") { id } }`, "service"},
		{`{ unknown }`, "Unknown"},
	} {
		req := httptest.NewRequest(http.MethodPost, "/graphql/v2", strings.NewReader(`{"query":`+jsonQuote(test.query)+`}`))
		op, err := codec.Route(rw, req)
		if err != nil || op.Name != test.want {
			t.Errorf("railway %q: %#v %v, want %s", test.query, op, err, test.want)
		}
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(rw, &model.Operation{Name: "projects"}, w, &spi.Response{Output: map[string]any{"_list": []any{map[string]any{"id": "1", "name": "web"}}, "_wrap": "projects"}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"data"`) || !strings.Contains(w.Body.String(), `"edges"`) || !strings.Contains(w.Body.String(), `"node"`) {
		t.Fatalf("rw list encode %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(rw, &model.Operation{Name: "project"}, w, &spi.Fault{Code: "NOT_FOUND", Message: "Project not found", HTTPStatus: 200, Fault: "client"}, "id"); err != nil {
		t.Fatal(err)
	}
	if w.Header().Get("x-amzn-errortype") != "" || !strings.Contains(w.Body.String(), `"errors"`) || !strings.Contains(w.Body.String(), `"NOT_FOUND"`) {
		t.Fatalf("rw fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}

	// Fly keeps only its fault envelope. The route table and the response
	// encoder went with the pack: the bundle answers the document's own
	// members -- an App and a Machine are the body, not something nested under
	// one -- so httpuri.Match routes it and the generic encoder serializes it.
	fly := &model.Service{ID: "fly.machines"}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(fly, &model.Operation{Name: "AppsShow"}, w, &spi.Fault{Code: "not_found", Message: "app not found", HTTPStatus: 404, Fault: "client"}, "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || w.Header().Get("x-amzn-errortype") != "" || !strings.Contains(w.Body.String(), `"error":"app not found"`) {
		t.Fatalf("fly fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
}

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// TestCloudflareRoutesFromItsGeneratedModel replaces the six route-table rows
// that went with the pack. The URIs are the same; what answers them is
// httpuri.Match over the model's own patterns, which is how every modelled
// service routes. A key carrying a slash must arrive percent-encoded, and that
// is asserted here rather than left to be discovered: the deleted table joined
// every remaining segment into the key and this does not.
func TestCloudflareRoutesFromItsGeneratedModel(t *testing.T) {
	cf := generatedcf.Model()
	const base = "/client/v4/accounts/a/storage/kv/namespaces"
	for _, test := range []struct{ method, path, want string }{
		{http.MethodPost, base, "WorkersKvNamespaceCreateANamespace"},
		{http.MethodGet, base, "WorkersKvNamespaceListNamespaces"},
		{http.MethodGet, base + "/nid", "WorkersKvNamespaceGetANamespace"},
		{http.MethodPut, base + "/nid", "WorkersKvNamespaceRenameANamespace"},
		{http.MethodDelete, base + "/nid", "WorkersKvNamespaceRemoveANamespace"},
		{http.MethodGet, base + "/nid/keys", "WorkersKvNamespaceListANamespace'SKeys"},
		{http.MethodPut, base + "/nid/values/k", "WorkersKvNamespaceWriteKeyValuePairWithMetadata"},
		{http.MethodGet, base + "/nid/values/k", "WorkersKvNamespaceReadKeyValuePair"},
		{http.MethodDelete, base + "/nid/values/k", "WorkersKvNamespaceDeleteKeyValuePair"},
		{http.MethodGet, base + "/nid/values/a%2Fb", "WorkersKvNamespaceReadKeyValuePair"},
	} {
		req := httptest.NewRequest(test.method, "http://api.cloudflare.com"+test.path, nil)
		op, err := (Codec{}).Route(cf, req)
		if err != nil || op == nil || op.Name != test.want {
			t.Fatalf("%s %s: op %v err %v, want %s", test.method, test.path, op, err, test.want)
		}
	}
	// An unencoded slash is two segments, and no pattern has that shape.
	req := httptest.NewRequest(http.MethodGet, "http://api.cloudflare.com"+base+"/nid/values/a/b", nil)
	if op, err := (Codec{}).Route(cf, req); err == nil && op != nil && op.Name == "WorkersKvNamespaceReadKeyValuePair" {
		t.Fatal("an unencoded slash in a key should not route to the read")
	}
}

// TestStructuredPayloadStillDecodesAsAStructure is the other half of the
// payload rule, and the reason it is not simply "a payload member takes the
// body".
//
// Both spellings live in the same protocol. Cloudflare's KV write binds `body`
// to a union of a string and a blob and means "these bytes"; Pinpoint's
// CreateApp binds `CreateApplicationRequest` to a structure and means "this
// object, serialized". Twenty-two operations across nine generated models are
// the first kind and a hundred and thirty-two the second, so reading them the
// same way would either drop the structure or hand a pack a JSON string where
// it expects members.
func TestStructuredPayloadStillDecodesAsAStructure(t *testing.T) {
	pp := generatedpp.Model()
	op := pp.OperationByName("CreateApp")
	if op == nil {
		t.Fatal("pinpoint has no CreateApp")
	}
	if _, ok := pp.PayloadMember(op); ok {
		t.Fatal("a structured payload member must not claim the body")
	}
	body := `{"CreateApplicationRequest":{"Name":"app"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/apps", strings.NewReader(body))
	decoded, err := (Codec{}).Decode(pp, op, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.Input["CreateApplicationRequest"].(map[string]any); !ok {
		t.Fatalf("structured payload arrived as %T: %#v", decoded.Input["CreateApplicationRequest"], decoded.Input)
	}
}
