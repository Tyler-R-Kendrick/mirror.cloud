package awsquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// TestWantsJSONNeedsAnExplicitMediaType keeps the JSON dialect off every client
// that did not ask for it.
//
// `Accept: */*` is what curl, the Go client and most SDKs send when they have
// no preference, and reading it as a preference would switch the whole query
// protocol over to JSON for callers that have parsed XML from it for years.
func TestWantsJSONNeedsAnExplicitMediaType(t *testing.T) {
	for accept, want := range map[string]bool{
		"application/json":                 true,
		"application/json; charset=utf-8":  true,
		"text/xml, application/json":       true,
		"APPLICATION/JSON":                 true,
		"*/*":                              false,
		"":                                 false,
		"text/xml":                         false,
		"application/x-amz-json-1.0":       false,
		"application/jsonp":                false,
		"application/json-seq":             false,
		"text/html, application/xhtml+xml": false,
	} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if accept != "" {
			r.Header.Set("Accept", accept)
		}
		if got := WantsJSON(r); got != want {
			t.Errorf("WantsJSON(Accept: %q) = %v, want %v", accept, got, want)
		}
	}
}

// TestXMLToJSONPromotesRepeatedElements covers the one thing the two dialects
// cannot say identically: a list of one.
func TestXMLToJSONPromotesRepeatedElements(t *testing.T) {
	for _, tc := range []struct{ document, want string }{
		{`<R><Q>a</Q><Q>b</Q></R>`, `{"R":{"Q":["a","b"]}}`},
		{`<R><Q>a</Q></R>`, `{"R":{"Q":"a"}}`},
		{`<R><Q>a</Q><Q>b</Q><Q>c</Q></R>`, `{"R":{"Q":["a","b","c"]}}`},
		{`<R/>`, `{"R":""}`},
		{`<R></R>`, `{"R":""}`},
		{`<R><E><K>x</K><V>y</V></E></R>`, `{"R":{"E":{"K":"x","V":"y"}}}`},
		// An element carrying both text and children keeps the children: the
		// text is whitespace between tags in every document this encoder
		// produces.
		{`<R>
			<Q>a</Q>
		</R>`, `{"R":{"Q":"a"}}`},
	} {
		got, err := xmlToJSON(tc.document)
		if err != nil {
			t.Fatalf("xmlToJSON(%q): %v", tc.document, err)
		}
		if string(got) != tc.want {
			t.Errorf("xmlToJSON(%q) = %s, want %s", tc.document, got, tc.want)
		}
	}
}

// TestJSONDialectIsTheSameDocument asserts the property the transcoding exists
// for: whatever the XML encoder decides about names, flattening and namespaces,
// the JSON dialect says it too, because it is the same document read twice.
func TestJSONDialectIsTheSameDocument(t *testing.T) {
	svc := tagService()
	op := &model.Operation{Name: "ListQueueTags", Output: "Result"}
	out := map[string]any{"Tags": map[string]any{"first": "one"}}

	xmlRec := httptest.NewRecorder()
	if err := (Codec{}).Encode(svc, op, xmlRec, &spi.Response{Output: out}); err != nil {
		t.Fatal(err)
	}
	jsonRec := httptest.NewRecorder()
	if err := (Codec{JSON: true}).Encode(svc, op, jsonRec, &spi.Response{Output: out}); err != nil {
		t.Fatal(err)
	}
	if got := jsonRec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("JSON dialect Content-Type %q", got)
	}
	if got := xmlRec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/xml") {
		t.Errorf("XML Content-Type %q", got)
	}
	var decoded map[string]any
	if err := json.Unmarshal(jsonRec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("JSON dialect body %s: %v", jsonRec.Body.Bytes(), err)
	}
	response, ok := decoded["ListQueueTagsResponse"].(map[string]any)
	if !ok {
		t.Fatalf("no ListQueueTagsResponse in %s", jsonRec.Body.Bytes())
	}
	result, ok := response["ListQueueTagsResult"].(map[string]any)
	if !ok {
		t.Fatalf("no ListQueueTagsResult in %s", jsonRec.Body.Bytes())
	}
	tag, ok := result["Tag"].(map[string]any)
	if !ok || tag["Key"] != "first" || tag["Value"] != "one" {
		t.Fatalf("JSON dialect lost the declared tag names: %s", jsonRec.Body.Bytes())
	}
	// The declared names are the ones the XML carries, so neither dialect may
	// fall back to the map's own key.
	if body := xmlRec.Body.String(); !strings.Contains(body, "<Tag><Key>first</Key><Value>one</Value></Tag>") {
		t.Fatalf("XML lost the declared tag names: %s", body)
	}
}

// TestAnEmptyResultIsSelfClosing pins the shape of a response that carried
// nothing, which is the common one: ReceiveMessage against a queue with no
// messages answers this on every poll.
func TestAnEmptyResultIsSelfClosing(t *testing.T) {
	svc := tagService()
	op := &model.Operation{Name: "ReceiveMessage", Output: "Result"}
	rec := httptest.NewRecorder()
	if err := (Codec{}).Encode(svc, op, rec, &spi.Response{Output: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<ReceiveMessageResult/>") {
		t.Errorf("empty result is not self-closing: %s", body)
	}
	if strings.Contains(body, "<ReceiveMessageResult></ReceiveMessageResult>") {
		t.Errorf("empty result written as a pair: %s", body)
	}
}

// TestAnAbsentNamespaceIsNotAnEmptyOne covers a service whose model carries no
// xmlNamespace. Writing `xmlns=""` undeclares the default namespace rather than
// leaving it unset, and SQS -- whose modern specification is awsJson1_0 and has
// no XML namespace at all -- is one of the services that reaches this.
func TestAnAbsentNamespaceIsNotAnEmptyOne(t *testing.T) {
	op := &model.Operation{Name: "ListQueueTags", Output: "Result"}
	rec := httptest.NewRecorder()
	if err := (Codec{}).Encode(tagService(), op, rec, &spi.Response{Output: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	if body := rec.Body.String(); strings.Contains(body, `xmlns=""`) {
		t.Errorf("empty namespace declared: %s", body)
	}

	withNS := tagService()
	withNS.XMLNamespace = "https://queue.amazonaws.com/doc/2012-11-05/"
	rec = httptest.NewRecorder()
	if err := (Codec{}).Encode(withNS, op, rec, &spi.Response{Output: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	if body := rec.Body.String(); !strings.Contains(body, `xmlns="https://queue.amazonaws.com/doc/2012-11-05/"`) {
		t.Errorf("declared namespace dropped: %s", body)
	}
}

// tagService is SQS's tag shapes as its specification declares them:
// xmlFlattened + xmlName Tag on the member, Key and Value on the map.
func tagService() *model.Service {
	return &model.Service{
		ID: "aws.sqs", Protocol: model.ProtoAWSQuery,
		Operations: []model.Operation{{Name: "ListQueueTags", Output: "Result"}},
		Shapes: map[string]model.Shape{
			"Result": {ID: "Result", Kind: model.KindStructure, Members: map[string]model.Member{
				"Tags": {Shape: "TagMap", Binding: model.MemberBinding{Name: "Tag", XMLFlattened: true}},
			}},
			"TagMap": {
				ID: "TagMap", Kind: model.KindMap,
				Key: "Str", KeyBinding: model.MemberBinding{Name: "Key"},
				Member: "Str", MemberBinding: model.MemberBinding{Name: "Value"},
			},
			"Str": {ID: "Str", Kind: model.KindString},
		},
	}
}
