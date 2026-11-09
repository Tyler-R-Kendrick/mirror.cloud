package cloudformation

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// wire boots a server and answers a function that sends one CloudFormation
// query request, as an SDK spells it, and answers the response body.
func wire(t *testing.T) func(url.Values) string {
	t.Helper()
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudformation", "aws.ssm"}
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	t.Cleanup(ts.Close)
	return func(v url.Values) string {
		v.Set("Version", "2010-05-15")
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(v.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return string(b)
	}
}

// outputs answers DescribeStacks' Outputs for the stack, by OutputKey.
func (h *stacks) outputs(stack string) map[string]any {
	h.t.Helper()
	out, err := h.invoke("DescribeStacks", map[string]any{"StackName": stack})
	if err != nil {
		h.t.Fatalf("DescribeStacks %s: %v", stack, err)
	}
	m := map[string]any{}
	for _, o := range out["Stacks"].([]any)[0].(map[string]any)["Outputs"].([]any) {
		m[str(asMap(o)["OutputKey"])] = asMap(o)["OutputValue"]
	}
	return m
}

// given is a request's Parameters list: key, value, key, value and on.
func givenParams(kv ...string) []any {
	var out []any
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, map[string]any{"ParameterKey": kv[i], "ParameterValue": kv[i+1]})
	}
	return out
}

// An empty String parameter is a value: a Ref to it is "" (User Guide,
// Parameters: a Ref "returns the value of the parameter"), given or its
// Default, and so is ${Name} in a Fn::Sub. Before, the presence of the name
// was lost and a Ref to an empty parameter answered the parameter's own name.
func TestCloudFormationAnEmptyParameterIsAValue(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{
		"Parameters": map[string]any{"Given": map[string]any{"Type": "String"}, "Defaulted": map[string]any{"Type": "String", "Default": ""}},
		"Resources":  map[string]any{"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": "v"}}},
		"Outputs": map[string]any{
			"G": map[string]any{"Value": map[string]any{"Fn::Join": []any{"", []any{"<", map[string]any{"Ref": "Given"}, ">"}}}},
			"D": map[string]any{"Value": map[string]any{"Fn::Join": []any{"", []any{"<", map[string]any{"Ref": "Defaulted"}, ">"}}}},
			"S": map[string]any{"Value": map[string]any{"Fn::Sub": "<${Given}${Defaulted}>"}}}})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "empty", "TemplateBody": string(body), "Parameters": givenParams("Given", "")}); err != nil {
		t.Fatal(err)
	}
	if got := h.outputs("empty"); got["G"] != "<>" || got["D"] != "<>" || got["S"] != "<>" {
		t.Errorf("outputs of empty parameters = %v, want <> for G, D and S", got)
	}
}

// The same over the wire, the parameter as an SDK sends it:
// Parameters.member.1.ParameterValue present and empty.
func TestBootedServerCloudFormationAnEmptyParameterIsAValue(t *testing.T) {
	do := wire(t)
	tpl := `{"Parameters":{"K":{"Type":"String"}},"Resources":{"P":{"Type":"AWS::SSM::Parameter","Properties":{"Type":"String","Value":"v"}}},` +
		`"Outputs":{"E":{"Value":{"Fn::Join":["",["<",{"Ref":"K"},">"]]}}}}`
	if b := do(url.Values{"Action": {"CreateStack"}, "StackName": {"s"}, "TemplateBody": {tpl},
		"Parameters.member.1.ParameterKey": {"K"}, "Parameters.member.1.ParameterValue": {""}}); !strings.Contains(b, "<StackId>") {
		t.Fatal(b)
	}
	if b := do(url.Values{"Action": {"DescribeStacks"}, "StackName": {"s"}}); !strings.Contains(b, "<OutputValue>&lt;&gt;</OutputValue>") {
		t.Errorf("DescribeStacks = %s, want the output <>", b)
	}
}

// arnsOf answers DescribeStacks' NotificationARNs for the stack.
func (h *stacks) arnsOf(stack string) any {
	h.t.Helper()
	out, err := h.invoke("DescribeStacks", map[string]any{"StackName": stack})
	if err != nil {
		h.t.Fatalf("DescribeStacks %s: %v", stack, err)
	}
	return out["Stacks"].([]any)[0].(map[string]any)["NotificationARNs"]
}

// A stack's NotificationARNs: DescribeStacks answers them, an UpdateStack
// without the member keeps them and one with an empty list removes them
// ("Specify an empty list to remove all notification topics", API
// reference, UpdateStack), more than five are refused ("Maximum number of 5
// items", CreateStack and UpdateStack), and with none the pseudo parameter
// is the empty list, never null. Before, the ARNs were never stored: an
// update without them lost them and DescribeStacks answered none.
func TestCloudFormationNotificationARNsAreTheStacks(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
		"Outputs": map[string]any{"N": map[string]any{"Value": map[string]any{"Fn::Join": []any{",", map[string]any{"Ref": "AWS::NotificationARNs"}}}}}})
	a, b := "arn:aws:sns:us-east-1:000000000000:a", "arn:aws:sns:us-east-1:000000000000:b"
	six := []any{a, a, a, a, a, a}
	for _, op := range []string{"CreateStack", "UpdateStack"} {
		if _, err := h.invoke(op, map[string]any{"StackName": "six", "TemplateBody": string(body), "NotificationARNs": six}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Member must have length less than or equal to 5") {
			t.Errorf("%s with six NotificationARNs answered %v, want ValidationError", op, err)
		}
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "na", "TemplateBody": string(body), "NotificationARNs": []any{a, b}}); err != nil {
		t.Fatal(err)
	}
	if got := h.arnsOf("na"); !reflect.DeepEqual(got, []any{a, b}) {
		t.Errorf("DescribeStacks' NotificationARNs = %#v, want [%s %s]", got, a, b)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "na", "TemplateBody": string(body)}); err != nil {
		t.Fatal(err)
	}
	if got, joined := h.arnsOf("na"), h.outputs("na")["N"]; !reflect.DeepEqual(got, []any{a, b}) || joined != a+","+b {
		t.Errorf("after an UpdateStack without NotificationARNs, DescribeStacks answers %#v and the output %q, want both ARNs kept", got, joined)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "na", "TemplateBody": string(body), "NotificationARNs": []any{}}); err != nil {
		t.Fatal(err)
	}
	if got := h.arnsOf("na"); !reflect.DeepEqual(got, []any{}) {
		t.Errorf("after an UpdateStack with an empty list, NotificationARNs = %#v, want []", got)
	}
	none := &spi.Request{Identity: h.id, Input: map[string]any{}}
	if got := pseudo(none, "s", "id")["AWS::NotificationARNs"]; got != "[]" {
		t.Errorf("AWS::NotificationARNs with none given = %q, want []", got)
	}
	if got := h.pack.resolve(map[string]any{"Ref": "AWS::NotificationARNs"}, map[string]string{}, none, "s"); !reflect.DeepEqual(got, []any{}) {
		t.Errorf("a Ref to AWS::NotificationARNs with none given resolves to %#v, want []", got)
	}
}

// Over the wire the decoder builds the NotificationARNs list beside the flat
// keys, and the list is read once: before, the engine read both and every
// ARN came twice. The list ends at a gap, as the decoder reads one.
func TestBootedServerCloudFormationNotificationARNs(t *testing.T) {
	do := wire(t)
	tpl := `{"Resources":{"P":{"Type":"AWS::SSM::Parameter","Properties":{"Type":"String","Value":"v"}}},` +
		`"Outputs":{"N":{"Value":{"Fn::Join":[",",{"Ref":"AWS::NotificationARNs"}]}}}}`
	if b := do(url.Values{"Action": {"CreateStack"}, "StackName": {"s"}, "TemplateBody": {tpl},
		"NotificationARNs.member.1": {"arn:a"}, "NotificationARNs.member.2": {"arn:b"}, "NotificationARNs.member.4": {"arn:d"}}); !strings.Contains(b, "<StackId>") {
		t.Fatal(b)
	}
	b := do(url.Values{"Action": {"DescribeStacks"}, "StackName": {"s"}})
	if !strings.Contains(b, "<OutputValue>arn:a,arn:b</OutputValue>") || !strings.Contains(b, "<NotificationARNs><member>arn:a</member><member>arn:b</member></NotificationARNs>") {
		t.Errorf("DescribeStacks = %s, want the output arn:a,arn:b and those two NotificationARNs", b)
	}
}

// UsePreviousValue: "During a stack update, use the existing parameter value
// that the stack is using for a given parameter key" (API reference,
// Parameter), a value given or a Default taken. Before, it was ignored, and
// an update without a ParameterValue lost the value.
func TestCloudFormationUsePreviousValue(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{
		"Parameters": map[string]any{"Key": map[string]any{"Type": "String"}, "Def": map[string]any{"Type": "String", "Default": "dflt"}},
		"Resources":  map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}, // CreateTopic "is idempotent": an update makes it again
		"Outputs":    map[string]any{"K": map[string]any{"Value": map[string]any{"Ref": "Key"}}, "D": map[string]any{"Value": map[string]any{"Ref": "Def"}}}})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "prev", "TemplateBody": string(body), "Parameters": givenParams("Key", "first")}); err != nil {
		t.Fatal(err)
	}
	previous := []any{map[string]any{"ParameterKey": "Key", "UsePreviousValue": "true"}, map[string]any{"ParameterKey": "Def", "UsePreviousValue": true}}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "prev", "TemplateBody": string(body), "Parameters": previous}); err != nil {
		t.Fatal(err)
	}
	if got := h.outputs("prev"); got["K"] != "first" || got["D"] != "dflt" {
		t.Errorf("outputs after UsePreviousValue = %v, want K first and D dflt", got)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "prev", "TemplateBody": string(body), "Parameters": []any{map[string]any{"ParameterKey": "Key"}}}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Parameters: [Key] must have values") {
		t.Errorf("an update with neither a value nor UsePreviousValue answered %v, want Parameters: [Key] must have values", err)
	}
}
