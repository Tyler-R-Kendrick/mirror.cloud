package openapi_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver/openapi"
)

// doc is a compact OpenAPI 3.0 document carrying one of everything the receiver
// has to understand: a shared path parameter, each placement, a request body, a
// referenced response, composition across a reference, a map, a list and an
// enum.
const doc = `{
  "openapi": "3.0.3",
  "info": {"title": "Demo API"},
  "servers": [{"url": "https://api.demo.invalid"}],
  "paths": {
    "/v1/zones/{domain}": {
      "parameters": [
        {"name": "domain", "in": "path", "required": true, "schema": {"type": "string"}}
      ],
      "get": {
        "operationId": "get-dns-records",
        "parameters": [
          {"name": "page-size", "in": "query", "schema": {"type": "integer"}},
          {"name": "If-Match", "in": "header", "schema": {"type": "string"}},
          {"name": "session", "in": "cookie", "schema": {"type": "string"}}
        ],
        "responses": {
          "200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Zone"}}}},
          "404": {"content": {"application/json": {"schema": {"type": "object"}}}}
        }
      },
      "put": {
        "operationId": "update-zone",
        "requestBody": {
          "required": true,
          "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ZoneUpdate"}}}
        },
        "responses": {"204": {}}
      },
      "delete": {"responses": {"200": {}}}
    }
  },
  "components": {
    "schemas": {
      "Zone": {
        "allOf": [
          {"$ref": "#/components/schemas/Named"},
          {"type": "object", "required": ["ttl"], "properties": {
            "ttl": {"type": "integer", "format": "int64"},
            "records": {"type": "array", "items": {"$ref": "#/components/schemas/Record"}},
            "labels": {"type": "object", "additionalProperties": {"type": "string"}}
          }}
        ]
      },
      "Named": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}},
      "Record": {"type": "object", "properties": {
        "kind": {"type": "string", "enum": ["A", "AAAA"]},
        "value": {"type": "string"}
      }},
      "ZoneUpdate": {"type": "object", "required": ["ttl"], "properties": {
        "ttl": {"type": "integer"},
        "domain": {"type": "string"}
      }}
    }
  }
}`

func ingest(t *testing.T, path, body string) model.Service {
	t.Helper()
	svcs, err := (openapi.Receiver{}).Ingest(context.Background(), model.SourceRef{Path: path}, []byte(body))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if len(svcs) != 1 {
		t.Fatalf("ingest produced %d services, want 1", len(svcs))
	}
	return svcs[0]
}

func TestDetectAcceptsOpenAPIAndNothingElse(t *testing.T) {
	r := openapi.Receiver{}
	if !r.Detect("api.json", []byte(`{"openapi":"3.0.3","paths":{}}`)) {
		t.Error("an OpenAPI 3 document was not detected")
	}
	if !r.Detect("api.json", []byte(`{"openapi":"3.1.0"}`)) {
		t.Error("an OpenAPI 3.1 document was not detected")
	}
	for _, other := range []string{
		`{"smithy":"2.0","shapes":{}}`,
		`{"discoveryVersion":"v1","name":"storage"}`,
		`{"swagger":"2.0"}`,
	} {
		if r.Detect("x.json", []byte(other)) {
			t.Errorf("detected a document that is not OpenAPI 3: %s", other)
		}
	}
}

func TestIngestBindsEachParameterWhereTheDocumentPutsIt(t *testing.T) {
	svc := ingest(t, "demo/api.json", doc)
	if svc.ID != "demo.api" {
		t.Errorf("service ID is %q, want demo.api", svc.ID)
	}
	// The endpoint label is the provider, never the service's own short name:
	// a service answering to the generic word `api` claims every AWS endpoint
	// whose prefix begins `api.`.
	if svc.EndpointPrefix != "demo" {
		t.Errorf("endpoint prefix is %q, want demo", svc.EndpointPrefix)
	}
	if svc.Protocol != model.ProtoRESTJSON1 {
		t.Errorf("protocol is %q", svc.Protocol)
	}

	op := svc.OperationByName("GetDnsRecords")
	if op == nil {
		t.Fatalf("no GetDnsRecords; got %v", names(svc))
	}
	if op.HTTP.Method != "GET" || op.HTTP.URI != "/v1/zones/{domain}" || op.HTTP.Code != 200 {
		t.Errorf("binding is %+v", op.HTTP)
	}
	if !op.Readonly {
		t.Error("a GET was not marked readonly")
	}
	in := svc.Shapes[op.Input]
	for member, want := range map[string]string{
		"domain":    "label",
		"page-size": "query",
		"If-Match":  "header",
	} {
		m, ok := in.Members[member]
		if !ok {
			t.Errorf("%s is not on the request shape", member)
			continue
		}
		if m.Binding.Location != want {
			t.Errorf("%s sits at %q, want %q", member, m.Binding.Location, want)
		}
		if m.Binding.Name != member {
			t.Errorf("%s is named %q on the wire", member, m.Binding.Name)
		}
	}
	// A path parameter is required whether or not the document says so, and a
	// shared one is as binding as one written on the operation.
	if !in.Members["domain"].Required {
		t.Error("a path parameter was not required")
	}
	// A cookie has no placement in the model. Carrying it as a header would be
	// a wrong answer rather than a missing one.
	if _, ok := in.Members["session"]; ok {
		t.Error("a cookie parameter was given a placement")
	}
}

func TestIngestMergesARequestBodyIntoTheRequest(t *testing.T) {
	// The codec flattens a JSON body into the same map the parameters land in,
	// so the body's members have to be on the request shape rather than behind
	// a reference no operation reads.
	svc := ingest(t, "demo/api.json", doc)
	op := svc.OperationByName("UpdateZone")
	if op == nil {
		t.Fatalf("no UpdateZone; got %v", names(svc))
	}
	in := svc.Shapes[op.Input]
	if _, ok := in.Members["ttl"]; !ok {
		t.Errorf("a body member is missing from the request: %v", memberNames(in))
	}
	if !in.Members["ttl"].Required {
		t.Error("a required body member was not required")
	}
	// `domain` is both a path parameter and a body property. The parameter wins,
	// because it is the one with a placement on the wire.
	if got := in.Members["domain"].Binding.Location; got != "label" {
		t.Errorf("domain sits at %q, want the path parameter to win", got)
	}
	if op.HTTP.Code != 204 {
		t.Errorf("success code is %d, want the 204 the document declares", op.HTTP.Code)
	}
}

func TestIngestComposesAcrossAReference(t *testing.T) {
	// A branch of an allOf may name a component defined later in the document,
	// so composition cannot be resolved as it is met. Losing it drops every
	// inherited member silently.
	svc := ingest(t, "demo/api.json", doc)
	zone, ok := svc.Shapes["Zone"]
	if !ok {
		t.Fatal("Zone was not defined")
	}
	for _, member := range []string{"name", "ttl", "records", "labels"} {
		if _, ok := zone.Members[member]; !ok {
			t.Errorf("Zone is missing %s; has %v", member, memberNames(zone))
		}
	}
	if !zone.Members["name"].Required {
		t.Error("a required member of a composed branch lost its requiredness")
	}
	if got := svc.Shapes[zone.Members["ttl"].Shape].Kind; got != model.KindLong {
		t.Errorf("ttl is %s, want long from its int64 format", got)
	}
	records := svc.Shapes[zone.Members["records"].Shape]
	if records.Kind != model.KindList || records.Member != "Record" {
		t.Errorf("records is %+v, want a list of Record", records)
	}
	labels := svc.Shapes[zone.Members["labels"].Shape]
	if labels.Kind != model.KindMap {
		t.Errorf("labels is %s, want a map from additionalProperties", labels.Kind)
	}
	kind := svc.Shapes[svc.Shapes["Record"].Members["kind"].Shape]
	if kind.Kind != model.KindEnum || !reflect.DeepEqual(kind.EnumValues, []string{"A", "AAAA"}) {
		t.Errorf("kind is %s %v, want an enum of A and AAAA", kind.Kind, kind.EnumValues)
	}
}

func TestIngestNamesAnOperationTheDocumentDidNot(t *testing.T) {
	// An operation with no operationId still has to be callable and still has
	// to get the same name on every ingest.
	svc := ingest(t, "demo/api.json", doc)
	if op := svc.OperationByName("DeleteV1ZonesByDomain"); op == nil {
		t.Errorf("the unnamed DELETE was not named from its method and path; got %v", names(svc))
	}
}

func TestIngestAnswersAnEmptyShapeForAnEmptyResponse(t *testing.T) {
	// A bundle projects its answer against the output shape, so an operation
	// answering nothing needs a shape to be checked against rather than none.
	svc := ingest(t, "demo/api.json", doc)
	op := svc.OperationByName("UpdateZone")
	out, ok := svc.Shapes[op.Output]
	if !ok {
		t.Fatalf("UpdateZone has no output shape (%q)", op.Output)
	}
	if out.Kind != model.KindStructure || len(out.Members) != 0 {
		t.Errorf("output is %+v, want an empty structure", out)
	}
}

func TestIngestRefusesAReferenceTheDocumentNeverDefines(t *testing.T) {
	// A `$ref` is carried through without being looked up, because a schema may
	// name one defined after it. Without this check the model would carry
	// members pointing at shapes that are not there and look populated.
	_, err := (openapi.Receiver{}).Ingest(context.Background(),
		model.SourceRef{Path: "demo/api.json"}, []byte(`{
			"openapi": "3.0.3",
			"paths": {"/a": {"get": {"operationId": "GetA", "responses": {
				"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Absent"}}}}}}}},
			"components": {"schemas": {}}
		}`))
	if err == nil {
		t.Fatal("a document naming an undefined schema was ingested")
	}
	if !strings.Contains(err.Error(), "Absent") {
		t.Errorf("the error does not name the missing schema: %v", err)
	}
}

func TestIngestIsDeterministic(t *testing.T) {
	// The models are committed and compared byte for byte, so two ingests of
	// one document must not differ by map order.
	first := ingest(t, "demo/api.json", doc)
	second := ingest(t, "demo/api.json", doc)
	if !reflect.DeepEqual(first, second) {
		t.Error("two ingests of the same document differ")
	}
	if !sortedByName(first.Operations) {
		t.Error("operations are not in name order")
	}
}

func names(svc model.Service) []string {
	out := make([]string, 0, len(svc.Operations))
	for _, op := range svc.Operations {
		out = append(out, op.Name)
	}
	return out
}

func memberNames(sh model.Shape) []string {
	out := make([]string, 0, len(sh.Members))
	for n := range sh.Members {
		out = append(out, n)
	}
	return out
}

func sortedByName(ops []model.Operation) bool {
	for i := 1; i < len(ops); i++ {
		if ops[i-1].Name > ops[i].Name {
			return false
		}
	}
	return true
}

// TestIngestCarriesTheServerBasePathIntoEveryURI states the half of an address
// OpenAPI keeps outside the path items. Hetzner declares its server as
// `https://api.hetzner.cloud/v1` and writes its paths as `/servers`; a client
// calls `/v1/servers`. Dropping the base produced a model that was well-formed
// and unroutable, with no symptom but a service that answered nothing.
func TestIngestCarriesTheServerBasePathIntoEveryURI(t *testing.T) {
	const based = `{
	  "openapi": "3.1.0",
	  "info": {"title": "Based API"},
	  "servers": [{"url": "https://api.example.invalid/v1/"}],
	  "paths": {
	    "/servers": {"get": {"operationId": "list_servers", "responses": {"200": {}}}}
	  }
	}`
	svcs, err := openapi.Receiver{}.Ingest(context.Background(),
		model.SourceRef{Path: "example/v1.json"}, []byte(based))
	if err != nil {
		t.Fatal(err)
	}
	if got := svcs[0].Operations[0].HTTP.URI; got != "/v1/servers" {
		t.Errorf("URI = %q, want /v1/servers; the server URL's path is part of "+
			"every address a client uses", got)
	}
}

// TestIngestDropsATemplatedServerBasePath keeps the fix from inventing an
// address. A server URL with a variable in it cannot be resolved from the
// document alone, and substituting anything would bind every operation to a
// path no client calls -- worse than leaving the paths as the document wrote
// them, which is what every document got before base paths existed.
func TestIngestDropsATemplatedServerBasePath(t *testing.T) {
	const templated = `{
	  "openapi": "3.1.0",
	  "info": {"title": "Templated API"},
	  "servers": [{"url": "https://api.example.invalid/{version}"}],
	  "paths": {
	    "/servers": {"get": {"operationId": "list_servers", "responses": {"200": {}}}}
	  }
	}`
	svcs, err := openapi.Receiver{}.Ingest(context.Background(),
		model.SourceRef{Path: "example/v1.json"}, []byte(templated))
	if err != nil {
		t.Fatal(err)
	}
	if got := svcs[0].Operations[0].HTTP.URI; got != "/servers" {
		t.Errorf("URI = %q, want /servers", got)
	}
}

// TestServiceIDIgnoresTheSerialization pins that a document's identity is its
// path, not how the vendor spelled it.
//
// serviceID trimmed only `.json`, so the first YAML document the ingest walk
// accepted became `digitalocean.v2.yaml` -- an ID no line of specs/mirror.set
// names, so the service was ingested and then dropped for want of a
// declaration. The run reported it, which is C42's repair working; this stops
// it happening at all.
func TestServiceIDIgnoresTheSerialization(t *testing.T) {
	const body = `{"openapi":"3.0.0","info":{"title":"Demo"},"paths":{}}`
	for _, path := range []string{
		"digitalocean/v2.json",
		"digitalocean/v2.yaml",
		"digitalocean/v2.yml",
		"digitalocean/V2.YAML",
	} {
		t.Run(path, func(t *testing.T) {
			svcs, err := (openapi.Receiver{}).Ingest(context.Background(),
				model.SourceRef{Path: path}, []byte(body))
			if err != nil {
				t.Fatal(err)
			}
			if len(svcs) != 1 {
				t.Fatalf("want one service, got %d", len(svcs))
			}
			if svcs[0].ID != "digitalocean.v2" {
				t.Fatalf("ID = %q, want digitalocean.v2", svcs[0].ID)
			}
			if svcs[0].EndpointPrefix != "digitalocean" {
				t.Fatalf("EndpointPrefix = %q, want digitalocean", svcs[0].EndpointPrefix)
			}
		})
	}
}

// TestSharedResponseIsResolved covers the shape that made DigitalOcean
// unmodellable: a response that is a `$ref` into components.responses rather
// than an inline body.
//
// It decoded to a value with no content, `jsonSchema` found nothing, and the
// operation was handed an empty output structure -- so the model looked whole
// and projected nothing. DigitalOcean writes 3,731 of its 3,885 responses that
// way, which is every operation it has.
func TestSharedResponseIsResolved(t *testing.T) {
	svcs, err := (openapi.Receiver{}).Ingest(context.Background(),
		model.SourceRef{Path: "demo/api.json"}, []byte(`{
			"openapi": "3.0.3",
			"paths": {"/v2/widgets": {"get": {"operationId": "list-widgets", "responses": {
				"200": {"$ref": "#/components/responses/all_widgets"}}}}},
			"components": {
				"responses": {"all_widgets": {"content": {"application/json": {"schema": {
					"type": "object", "properties": {"widgets": {"type": "array", "items": {"type": "string"}}}}}}}},
				"schemas": {}
			}
		}`))
	if err != nil {
		t.Fatal(err)
	}
	out := svcs[0].Shapes[svcs[0].Operations[0].Output]
	if _, ok := out.Members["widgets"]; !ok {
		t.Fatalf("the shared response did not reach the output shape: %v", memberNames(out))
	}
}

// TestSharedResponseChainIsResolved: nothing forbids a shared response from
// referring to another, so the reference is followed rather than hopped once.
func TestSharedResponseChainIsResolved(t *testing.T) {
	svcs, err := (openapi.Receiver{}).Ingest(context.Background(),
		model.SourceRef{Path: "demo/api.json"}, []byte(`{
			"openapi": "3.0.3",
			"paths": {"/a": {"get": {"operationId": "GetA", "responses": {
				"200": {"$ref": "#/components/responses/outer"}}}}},
			"components": {
				"responses": {
					"outer": {"$ref": "#/components/responses/inner"},
					"inner": {"content": {"application/json": {"schema": {
						"type": "object", "properties": {"ok": {"type": "boolean"}}}}}}
				},
				"schemas": {}
			}
		}`))
	if err != nil {
		t.Fatal(err)
	}
	out := svcs[0].Shapes[svcs[0].Operations[0].Output]
	if _, ok := out.Members["ok"]; !ok {
		t.Fatalf("the chained response did not reach the output shape: %v", memberNames(out))
	}
}

// TestIngestRefusesASharedResponseTheDocumentNeverDefines keeps the new
// reference under the same rule as every other one: unresolved is fatal and
// named, not a quietly empty shape.
func TestIngestRefusesASharedResponseTheDocumentNeverDefines(t *testing.T) {
	_, err := (openapi.Receiver{}).Ingest(context.Background(),
		model.SourceRef{Path: "demo/api.json"}, []byte(`{
			"openapi": "3.0.3",
			"paths": {"/a": {"get": {"operationId": "GetA", "responses": {
				"200": {"$ref": "#/components/responses/absent_response"}}}}},
			"components": {"responses": {}, "schemas": {}}
		}`))
	if err == nil {
		t.Fatal("a document naming an undefined response was ingested")
	}
	if !strings.Contains(err.Error(), "absent_response") {
		t.Errorf("the error does not name the missing response: %v", err)
	}
}

// TestSharedResponseCycleIsRefusedNotFollowedForever bounds the walk, so a
// document that references itself in a circle fails the receiver rather than
// hanging it.
func TestSharedResponseCycleIsRefusedNotFollowedForever(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		_, err := (openapi.Receiver{}).Ingest(context.Background(),
			model.SourceRef{Path: "demo/api.json"}, []byte(`{
				"openapi": "3.0.3",
				"paths": {"/a": {"get": {"operationId": "GetA", "responses": {
					"200": {"$ref": "#/components/responses/loop"}}}}},
				"components": {
					"responses": {"loop": {"$ref": "#/components/responses/loop"}},
					"schemas": {}
				}
			}`))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a circular response reference was ingested")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ingest did not return: the reference walk is unbounded")
	}
}
