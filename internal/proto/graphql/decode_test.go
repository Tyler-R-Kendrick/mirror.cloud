package graphql

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func demo() *model.Service {
	return &model.Service{
		ID: "demo.graphql",
		Operations: []model.Operation{
			{Name: "projectCreate", HTTP: model.HTTPBinding{Method: "POST", URI: "/graphql/v2", Code: 200}},
			{Name: "projectDelete", HTTP: model.HTTPBinding{Method: "POST", URI: "/graphql/v2", Code: 200}},
			{Name: "projects", HTTP: model.HTTPBinding{Method: "POST", URI: "/graphql/v2", Code: 200}},
		},
	}
}

func post(t *testing.T, path, body string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPost, "http://example.invalid"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestDecodePresentsArguments is the whole point of this codec.
//
// A GraphQL request's arguments are not its envelope. The engine validates a
// request against the shape the schema declares -- for
// `projectCreate(input: ProjectCreateInput!)` that is one member named `input`
// -- so a decoder that hands over `{query, variables}`, or that flattens
// `variables.input` to the top level the way the hand-written pack did, has
// described a different request than the one the model says arrived.
//
// Each case below is a spelling a real client uses for the same call.
func TestDecodePresentsArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want map[string]any
	}{
		{
			"inline object literal",
			`{"query":"mutation { projectCreate(input: {name: \"web\"}) { id } }"}`,
			map[string]any{"input": map[string]any{"name": "web"}},
		},
		{
			// The spelling every generated client uses.
			"whole argument from a variable",
			`{"query":"mutation M($in: ProjectCreateInput!) { projectCreate(input: $in) { id } }","variables":{"in":{"name":"web"}}}`,
			map[string]any{"input": map[string]any{"name": "web"}},
		},
		{
			// A variable may fill one field rather than the whole argument.
			"variable nested inside a literal",
			`{"query":"mutation M($n: String!) { projectCreate(input: {name: $n}) { id } }","variables":{"n":"web"}}`,
			map[string]any{"input": map[string]any{"name": "web"}},
		},
		{
			"variable inside a list",
			`{"query":"mutation M($n: String!) { projectCreate(input: {tags: [$n, \"b\"]}) { id } }","variables":{"n":"a"}}`,
			map[string]any{"input": map[string]any{"tags": []any{"a", "b"}}},
		},
		{
			// A declared variable the caller did not supply is absent, not the
			// string "$n": a validator told otherwise reports a required member
			// present when nothing was sent.
			"variable with nothing supplied",
			`{"query":"mutation M($n: String) { projectCreate(input: {name: $n}) { id } }"}`,
			map[string]any{"input": map[string]any{"name": nil}},
		},
		{
			"scalar argument",
			`{"query":"mutation { projectDelete(id: \"p-1\") }"}`,
			map[string]any{"id": "p-1"},
		},
		{
			"no arguments at all",
			`{"query":"{ projects { edges { node { id } } } }"}`,
			map[string]any{},
		},
		{
			// Every literal kind the grammar defines, so a reader that collapses
			// Int to float64 or drops an enum is caught here rather than by a
			// service whose member happens to be typed that way.
			"every literal kind",
			`{"query":"mutation { projectCreate(i: 3, neg: -7, f: 1.5, exp: 2e3, b: true, no: false, nil: null, e: ACTIVE, l: [1, [2]], o: {a: {b: 1}}, s: \"x\") { id } }"}`,
			map[string]any{
				"i": int64(3), "neg": int64(-7), "f": 1.5, "exp": 2000.0,
				"b": true, "no": false, "nil": nil, "e": "ACTIVE",
				"l": []any{int64(1), []any{int64(2)}},
				"o": map[string]any{"a": map[string]any{"b": int64(1)}},
				"s": "x",
			},
		},
		{
			// An escape is decoded, not skipped: a name written `a\"b` is the
			// three characters it is, and is compared and stored as such.
			"string escapes",
			`{"query":"mutation { projectCreate(s: \"a\\\"b\\n\\u00e9\") { id } }"}`,
			map[string]any{"s": "a\"b\né"},
		},
		{
			// A comment, a string and an alias are all places a name can sit
			// that is not an argument.
			"argument past a comment and an alias",
			"{\"query\":\"mutation { # id: \\\"nope\\\"\\n mine: projectCreate(input: {name: \\\"web\\\"}) { id } }\"}",
			map[string]any{"input": map[string]any{"name": "web"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := demo()
			r := post(t, "/graphql/v2", tc.body)
			op, err := (Codec{}).Route(svc, r)
			if err != nil {
				t.Fatalf("route: %v", err)
			}
			got, err := (Codec{}).Decode(svc, op, r)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got.Input, tc.want) {
				t.Errorf("input  = %#v\nwant = %#v", got.Input, tc.want)
			}
		})
	}
}

// TestDecodeRefusesUnreadableArguments pins the choice against a partial map.
//
// Handing back what was read before a document stopped making sense is how a
// required member arrives present while a nested object arrives truncated --
// which validates, and then acts on input the caller never sent.
func TestDecodeRefusesUnreadableArguments(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"unclosed group", `mutation { projectCreate(input: {name: "web"} `},
		{"unterminated string", `mutation { projectCreate(input: {name: "web) { id } }`},
		{"value that is not one", `mutation { projectCreate(input: @) { id } }`},
		{"nested past the cap", `mutation { projectCreate(a: ` + strings.Repeat("[", 200) + `1` + strings.Repeat("]", 200) + `) { id } }`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := demo()
			body, err := jsonBody(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			r := post(t, "/graphql/v2", body)
			op, err := (Codec{}).Route(svc, r)
			if err != nil {
				return // refusing at the route is also a refusal
			}
			if _, err := (Codec{}).Decode(svc, op, r); err == nil {
				t.Fatal("decoded a document whose arguments cannot be read")
			}
		})
	}
}

// TestEncodePlacesTheAnswerUnderTheField pins the response envelope, including
// the scalar case the ScalarBody widening exists for: projectDelete answers
// `true` and nothing more, and it has to arrive as a JSON boolean rather than
// as the four characters of its spelling.
func TestEncodePlacesTheAnswerUnderTheField(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  map[string]any
		want string
	}{
		{"a structure", map[string]any{"id": "p-1"}, `{"data":{"projectCreate":{"id":"p-1"}}}`},
		{"a scalar body", map[string]any{bir.TopLevelRaw: true}, `{"data":{"projectCreate":true}}`},
		{"a number body", map[string]any{bir.TopLevelRaw: int64(7)}, `{"data":{"projectCreate":7}}`},
		{"a bare list", map[string]any{bir.TopLevelList: []any{"a"}}, `{"data":{"projectCreate":["a"]}}`},
		{"nothing at all", nil, `{"data":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			op := demo().Operations[0]
			if err := (Codec{}).Encode(demo(), &op, w, &spi.Response{Output: tc.out}); err != nil {
				t.Fatal(err)
			}
			if got := strings.TrimSpace(w.Body.String()); got != tc.want {
				t.Errorf("body = %s, want %s", got, tc.want)
			}
			if w.Code != http.StatusOK {
				t.Errorf("status = %d, want 200", w.Code)
			}
		})
	}
}

// TestEncodeFaultIsAGraphQLError pins the status. 200 is not a quirk being
// emulated: a GraphQL request that reached the server and was answered
// succeeded at the transport layer, and its errors live in the body.
func TestEncodeFaultIsAGraphQLError(t *testing.T) {
	w := httptest.NewRecorder()
	op := demo().Operations[0]
	f := &spi.Fault{Code: "BAD_USER_INPUT", Message: "project name is required", HTTPStatus: 400}
	if err := (Codec{}).EncodeFault(demo(), &op, w, f, "req-1"); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 even though the fault carries %d", w.Code, f.HTTPStatus)
	}
	want := `{"errors":[{"extensions":{"code":"BAD_USER_INPUT"},"message":"project name is required"}]}`
	if got := strings.TrimSpace(w.Body.String()); got != want {
		t.Errorf("body = %s, want %s", got, want)
	}
}

// TestRouteRefusesAnotherPath pins an exact match. A substring test is what the
// code this replaces used, and `/graphql/v2-staging` and `/tenant/graphql/v2`
// both contain the endpoint while neither is it.
func TestRouteRefusesAnotherPath(t *testing.T) {
	for _, path := range []string{"/graphql/v2-staging", "/tenant/graphql/v2", "/", "/graphql"} {
		if _, err := (Codec{}).Route(demo(), post(t, path, `{"query":"{ projects { id } }"}`)); err == nil {
			t.Errorf("%s routed, want refused", path)
		}
	}
	if _, err := (Codec{}).Route(demo(), post(t, "/graphql/v2/", `{"query":"{ projects { id } }"}`)); err != nil {
		t.Errorf("the endpoint with a trailing slash was refused: %v", err)
	}
}

// TestRouteNamesAnUnservedField: a field the schema does not declare routes to
// its own name, so the not-implemented fault says which field was asked for.
func TestRouteNamesAnUnservedField(t *testing.T) {
	op, err := (Codec{}).Route(demo(), post(t, "/graphql/v2", `{"query":"mutation { deploymentCreate(input:{}) { id } }"}`))
	if err != nil {
		t.Fatal(err)
	}
	if op.Name != "deploymentCreate" {
		t.Errorf("operation = %q, want deploymentCreate", op.Name)
	}
}
