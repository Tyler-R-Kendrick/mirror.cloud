package cloudformation

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
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
