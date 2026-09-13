package graphql

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// FuzzCodec drives the whole request path on documents nobody vouched for.
//
// The scanner and the value reader are hand-written readers of a grammar, and
// the request body is the least trusted input this process takes. What is being
// checked is not an answer but that there IS one: every loop terminates, no
// index runs off the end, and a document that means nothing routes to a name no
// schema declares rather than to a handler.
func FuzzCodec(f *testing.F) {
	for _, seed := range []string{
		`{"query":"mutation { projectCreate(input:{name:\"web\"}) { id name } }"}`,
		`{"query":"{ projects { edges { node { id } } } }"}`,
		`{"query":"{ project(id:\"x\") { id } }"}`,
		`{"query":"mutation { projectDelete(id:\"x\") }"}`,
		`{"query":"mutation M($in: I!) { serviceCreate(input:$in) { id } }","variables":{"in":{"name":"api"}}}`,
		`{"query":"{ f(a:1, b:1.5e3, c:true, d:null, e:ENUM, g:[1,[2]], h:{i:{j:1}}) }"}`,
		`{"query":"{ f(s:\"\\u00e9\\ud83d\\ude00\\n\\t\\\"\") }"}`,
		"{\"query\":\"{ f(s:\\\"\\\"\\\"\\n  block\\n  string\\n  \\\"\\\"\\\") }\"}",
		`{"query":""}`,
		`{}`,
	} {
		f.Add("POST", "/graphql/v2", seed)
	}
	svc := &model.Service{
		ID:         "demo.graphql",
		Operations: []model.Operation{{Name: "projects", HTTP: model.HTTPBinding{Method: "POST", URI: "/graphql/v2", Code: 200}}},
	}
	f.Fuzz(func(t *testing.T, method, path, body string) {
		if method == "" {
			method = http.MethodPost
		}
		path = "/" + strings.TrimPrefix(path, "/")
		req, err := http.NewRequest(method, "http://example.invalid"+path, strings.NewReader(body))
		if err != nil {
			return
		}
		op, err := (Codec{}).Route(svc, req)
		if err != nil {
			return
		}
		if op.Name == "" {
			t.Fatalf("routed to an empty operation name for %q", body)
		}
		decoded, err := (Codec{}).Decode(svc, op, req)
		if err != nil {
			return
		}
		if decoded.Input == nil {
			t.Fatal("decoded a nil input")
		}
		// Encoding whatever came back must not panic either: an argument value
		// reaches the response through a bundle's projection.
		rec := httptest.NewRecorder()
		_ = (Codec{}).Encode(svc, op, rec, &spi.Response{Output: decoded.Input})
	})
}
