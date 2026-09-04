package restjson

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
}
