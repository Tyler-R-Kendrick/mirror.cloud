package awsquery

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestAWSQueryContract(t *testing.T) {
	svc := &model.Service{
		ID: "aws.sqs", Protocol: model.ProtoAWSQuery, XMLNamespace: "https://queue.amazonaws.com/doc/2012-11-05/",
		Operations: []model.Operation{{Name: "SendMessage"}},
	}
	codec := Codec{}
	if codec.Protocol() != model.ProtoAWSQuery {
		t.Fatal(codec.Protocol())
	}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=SendMessage&Version=2012-11-05&MessageBody=%3Chello%3E&Tag=a&Tag=b"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	op, err := codec.Route(svc, request)
	if err != nil || op.Name != "SendMessage" {
		t.Fatalf("route %#v %v", op, err)
	}
	decoded, err := codec.Decode(svc, op, request)
	if err != nil || decoded.Input["MessageBody"] != "<hello>" || len(decoded.Input["Tag"].([]any)) != 2 || decoded.Input["Action"] != nil {
		t.Fatalf("decode %#v %v", decoded, err)
	}

	w := httptest.NewRecorder()
	if err := codec.Encode(svc, op, w, &spi.Response{Status: http.StatusAccepted, Output: map[string]any{
		"MessageId": "a&b", "Items": []any{map[string]any{"Value": "<one>"}}, "Empty": nil,
	}}); err != nil {
		t.Fatal(err)
	}
	body := w.Body.String()
	if w.Code != http.StatusAccepted || w.Header().Get("Content-Type") != "text/xml; charset=UTF-8" || !strings.Contains(body, "<SendMessageResult>") || !strings.Contains(body, "a&amp;b") || !strings.Contains(body, "<member><Value>&lt;one&gt;</Value></member>") || !strings.Contains(body, "<RequestId>mirror</RequestId>") {
		t.Fatalf("query response %d %s", w.Code, body)
	}

	ec2 := *svc
	ec2.Protocol = model.ProtoEC2Query
	w = httptest.NewRecorder()
	if err := codec.Encode(&ec2, op, w, &spi.Response{Output: map[string]any{"Value": true}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(w.Body.String(), "SendMessageResult") || !strings.Contains(w.Body.String(), "<requestId>mirror</requestId>") {
		t.Fatalf("EC2 query response %s", w.Body.String())
	}
}

// shapedService describes one operation whose input carries every nesting the
// query protocol can express, so decoding can be checked against a model rather
// than against the flat keys the wire happens to use.
func shapedService() *model.Service {
	return &model.Service{
		ID: "aws.shaped", Protocol: model.ProtoAWSQuery,
		Operations: []model.Operation{{Name: "Do", Input: "DoInput", Output: "DoOutput"}},
		Shapes: map[string]model.Shape{
			"String": {Kind: model.KindString},
			"DoOutput": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Plain":    {Shape: "String"},
				"Renamed":  {Shape: "String", Binding: model.MemberBinding{Name: "onTheWire"}},
				"Items":    {Shape: "NamedList", Binding: model.MemberBinding{Name: "itemSet"}},
				"Loose":    {Shape: "StringList", Binding: model.MemberBinding{Name: "looseSet", XMLFlattened: true}},
				"Anon":     {Shape: "StringList"},
				"Tags":     {Shape: "StringMap", Binding: model.MemberBinding{Name: "tagSet"}},
				"Labelled": {Shape: "LabelledMap"},
			}},
			"NamedList":   {Kind: model.KindList, Member: "Inner", MemberBinding: model.MemberBinding{Name: "item"}},
			"LabelledMap": {Kind: model.KindMap, Key: "String", Member: "String", KeyBinding: model.MemberBinding{Name: "k"}, MemberBinding: model.MemberBinding{Name: "v"}},
			"DoInput": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Name":       {Shape: "String"},
				"Identities": {Shape: "StringList"},
				"Filters":    {Shape: "StringList", Binding: model.MemberBinding{XMLFlattened: true}},
				"Tags":       {Shape: "StringMap"},
				"Renamed":    {Shape: "String", Binding: model.MemberBinding{Name: "OnTheWire"}},
				"Nested":     {Shape: "Inner"},
				"People":     {Shape: "InnerList"},
			}},
			"StringList": {Kind: model.KindList, Member: "String"},
			"StringMap":  {Kind: model.KindMap, Key: "String", Member: "String"},
			"InnerList":  {Kind: model.KindList, Member: "Inner"},
			"Inner": {Kind: model.KindStructure, Members: map[string]model.Member{
				"First": {Shape: "String"},
				"Last":  {Shape: "String"},
			}},
		},
	}
}

func decodeForm(t *testing.T, svc *model.Service, form url.Values) map[string]any {
	t.Helper()
	form.Set("Action", "Do")
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	op, err := Codec{}.Route(svc, request)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	decoded, err := Codec{}.Decode(svc, op, request)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return decoded.Input
}

func TestDecodeRebuildsShapedInput(t *testing.T) {
	svc := shapedService()
	in := decodeForm(t, svc, url.Values{
		"Name":                  {"n"},
		"Identities.member.1":   {"a"},
		"Identities.member.2":   {"b"},
		"Filters.1":             {"f1"},
		"Tags.entry.1.key":      {"k"},
		"Tags.entry.1.value":    {"v"},
		"OnTheWire":             {"renamed"},
		"Nested.First":          {"ada"},
		"Nested.Last":           {"lovelace"},
		"People.member.1.First": {"grace"},
		"People.member.2.First": {"alan"},
	})

	if got := in["Identities"]; !reflect.DeepEqual(got, []any{"a", "b"}) {
		t.Errorf("Identities = %#v, want [a b]", got)
	}
	if got := in["Filters"]; !reflect.DeepEqual(got, []any{"f1"}) {
		t.Errorf("flattened Filters = %#v, want [f1]", got)
	}
	if got := in["Tags"]; !reflect.DeepEqual(got, map[string]any{"k": "v"}) {
		t.Errorf("Tags = %#v, want {k:v}", got)
	}
	// The member is named Renamed in the model and OnTheWire on the wire; the
	// engine validates the model's name, so that is the one that must appear.
	if got := in["Renamed"]; got != "renamed" {
		t.Errorf("Renamed = %#v, want renamed", got)
	}
	if got := in["Nested"]; !reflect.DeepEqual(got, map[string]any{"First": "ada", "Last": "lovelace"}) {
		t.Errorf("Nested = %#v", got)
	}
	if got := in["People"]; !reflect.DeepEqual(got, []any{
		map[string]any{"First": "grace"}, map[string]any{"First": "alan"},
	}) {
		t.Errorf("People = %#v", got)
	}

	// The flat keys stay beside the structured ones until the last pack reading
	// them is extracted. Dropping them early is what would break those packs, so
	// their presence is asserted rather than merely tolerated.
	if in["Identities.member.1"] != "a" || in["Tags.entry.1.key"] != "k" {
		t.Errorf("flat keys were removed: %#v", in)
	}
}

func TestDecodeStopsListsAtAGap(t *testing.T) {
	// A gap ends the list rather than being skipped: a request that lost its
	// second element must not silently decode as a shorter valid one.
	in := decodeForm(t, shapedService(), url.Values{
		"Identities.member.1": {"a"},
		"Identities.member.3": {"c"},
	})
	if got := in["Identities"]; !reflect.DeepEqual(got, []any{"a"}) {
		t.Errorf("Identities = %#v, want [a] -- the gap must end the list", got)
	}
}

func TestDecodeOmitsAbsentMembers(t *testing.T) {
	in := decodeForm(t, shapedService(), url.Values{"Name": {"n"}})
	for _, absent := range []string{"Identities", "Tags", "Nested", "People", "Renamed"} {
		if v, ok := in[absent]; ok {
			t.Errorf("absent member %s decoded as %#v; a required-member check would pass on nothing", absent, v)
		}
	}
	if in["Name"] != "n" {
		t.Errorf("Name = %#v", in["Name"])
	}
}

func TestDecodeTerminatesOnRecursiveShapes(t *testing.T) {
	// No shipped model has a self-referential query input today, but the models
	// move under specs-refresh without any change here. This must reject such a
	// shape, not exhaust the stack on it.
	svc := &model.Service{
		ID: "aws.recursive", Protocol: model.ProtoAWSQuery,
		Operations: []model.Operation{{Name: "Do", Input: "Node"}},
		Shapes: map[string]model.Shape{
			"String": {Kind: model.KindString},
			"Node": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Value": {Shape: "String"},
				"Child": {Shape: "Node"},
			}},
		},
	}
	in := decodeForm(t, svc, url.Values{"Value": {"v"}, "Child.Value": {"c"}})
	if in["Value"] != "v" {
		t.Errorf("Value = %#v", in["Value"])
	}
}

func TestAWSQueryFaultsAndUnknownActions(t *testing.T) {
	svc := &model.Service{ID: "aws.sqs", Operations: []model.Operation{{Name: "Known"}}}
	codec := Codec{}
	for _, body := range []string{"Version=1", "Action=Unknown"} {
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if _, err := codec.Route(svc, request); err == nil {
			t.Fatalf("routed %q", body)
		}
	}

	op := &model.Operation{Name: "Known"}
	w := httptest.NewRecorder()
	if err := codec.EncodeFault(svc, op, w, &spi.Fault{Code: "Internal", Message: `<bad&>`, Fault: "server"}, "request&1"); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), "<Type>Receiver</Type>") || !strings.Contains(w.Body.String(), "&lt;bad&amp;&gt;") {
		t.Fatalf("server fault %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(svc, op, w, spi.NotImplemented(svc.ID, op.Name, "emulate"), "id"); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusNotImplemented || w.Header().Get("x-mirror-not-implemented") != "aws.sqs.Known" {
		t.Fatalf("not implemented fault %d %#v", w.Code, w.Header())
	}
	if got := FormEncode(url.Values{"b": {"2"}, "a": {"1"}}); got != "a=1&b=2" {
		t.Fatalf("form encoding %q", got)
	}
}

func encodeOutput(t *testing.T, svc *model.Service, out map[string]any) string {
	t.Helper()
	w := httptest.NewRecorder()
	op := &svc.Operations[0]
	if err := (Codec{}).Encode(svc, op, w, &spi.Response{Output: out}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return w.Body.String()
}

func TestEncodeWritesDeclaredMembersOnTheWire(t *testing.T) {
	svc := shapedService()
	body := encodeOutput(t, svc, map[string]any{
		"Plain":    "p",
		"Renamed":  "r",
		"Items":    []any{map[string]any{"First": "ada"}},
		"Loose":    []any{"x", "y"},
		"Anon":     []any{"z"},
		"Tags":     map[string]any{"team": "storage"},
		"Labelled": map[string]any{"a": "b"},
	})
	for _, want := range []string{
		"<Plain>p</Plain>",
		// Declared as Renamed, written as the wire name a client reads.
		"<onTheWire>r</onTheWire>",
		// The wrapper comes from the member, the element from the list's own
		// member -- which is how ec2 gets <vpcSet><item>.
		"<itemSet><item><First>ada</First></item></itemSet>",
		// A flattened list has no wrapper: each element carries the member name.
		"<looseSet>x</looseSet><looseSet>y</looseSet>",
		// A list whose shape names no element keeps the protocol default.
		"<Anon><member>z</member></Anon>",
		"<tagSet><entry><key>team</key><value>storage</value></entry></tagSet>",
		"<Labelled><entry><k>a</k><v>b</v></entry></Labelled>",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("response is missing %s:\n%s", want, body)
		}
	}
	if strings.Contains(body, "<Renamed>") || strings.Contains(body, "<Items>") {
		t.Errorf("declared names reached the wire:\n%s", body)
	}
}

func TestEncodeDoesNotRenameWhatIsAlreadyOnTheWire(t *testing.T) {
	// The hand-written packs answer in wire names already -- ec2's says vpcSet.
	// Renaming a second time would turn those into nothing a client can read,
	// and would make a pack and the bundle replacing it disagree, which is
	// exactly what the equivalence gate compares.
	svc := shapedService()
	declared := encodeOutput(t, svc, map[string]any{
		"Renamed": "r", "Items": []any{map[string]any{"First": "ada"}}, "Loose": []any{"x"},
	})
	wire := encodeOutput(t, svc, map[string]any{
		"onTheWire": "r", "itemSet": []any{map[string]any{"First": "ada"}}, "looseSet": []any{"x"},
	})
	if declared != wire {
		t.Errorf("declared and wire producers disagree:\n%s\n%s", declared, wire)
	}
}

func TestEncodeLeavesUndeclaredKeysAlone(t *testing.T) {
	// A pack answering something the model does not carry keeps the bytes it
	// always produced; the shape is a rename table, not a filter.
	body := encodeOutput(t, shapedService(), map[string]any{
		"Undeclared": []any{map[string]any{"Deep": "d"}},
	})
	if !strings.Contains(body, "<Undeclared><member><Deep>d</Deep></member></Undeclared>") {
		t.Errorf("undeclared member was rewritten:\n%s", body)
	}
}

func TestDecodeReadsEC2RequestNames(t *testing.T) {
	// ec2Query asks for a member under a different name than it answers with,
	// and flattens every list in a request without any trait saying so. Reading
	// an ec2 request by its response names finds no structured member at all.
	svc := shapedService()
	svc.Protocol = model.ProtoEC2Query
	input := svc.Shapes["DoInput"]
	input.Members["Renamed"] = model.Member{Shape: "String", Binding: model.MemberBinding{Name: "onTheWire"}}
	input.Members["Explicit"] = model.Member{Shape: "String", Binding: model.MemberBinding{Name: "dryRun", QueryName: "DryRun"}}
	input.Members["Identities"] = model.Member{Shape: "StringList", Binding: model.MemberBinding{Name: "vpcId"}}
	svc.Shapes["DoInput"] = input

	in := decodeForm(t, svc, url.Values{
		"OnTheWire": {"capitalized"},
		"DryRun":    {"true"},
		"VpcId.1":   {"vpc-1"},
		"VpcId.2":   {"vpc-2"},
	})
	if got := in["Renamed"]; got != "capitalized" {
		t.Errorf("Renamed = %#v, want the capitalized xmlName to be read", got)
	}
	if got := in["Explicit"]; got != "true" {
		t.Errorf("Explicit = %#v, want ec2QueryName to win over xmlName", got)
	}
	if got := in["Identities"]; !reflect.DeepEqual(got, []any{"vpc-1", "vpc-2"}) {
		t.Errorf("Identities = %#v, want the flattened list [vpc-1 vpc-2]", got)
	}
}
