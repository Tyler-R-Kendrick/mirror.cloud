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

// A parameter the template does not declare is refused, "Parameters: [X] do
// not exist in the template", UsePreviousValue or not, and so is one given
// both a value and UsePreviousValue: "If you specify true, do not specify a
// parameter value" (API reference, Parameter). Before, both were taken: the
// undeclared one was ignored, and the value lost to the previous one.
func TestCloudFormationRefusesAParameterItCannotTake(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Key": map[string]any{"Type": "String"}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "taken", "TemplateBody": string(body), "Parameters": givenParams("Key", "k", "Nope", "n")}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Parameters: [Nope] do not exist in the template") {
		t.Errorf("CreateStack with an undeclared parameter answered %v, want ValidationError", err)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "taken", "TemplateBody": string(body), "Parameters": givenParams("Key", "k")}); err != nil {
		t.Fatal(err)
	}
	for name, params := range map[string][]any{
		"undeclared": {map[string]any{"ParameterKey": "Key", "UsePreviousValue": true}, map[string]any{"ParameterKey": "Gone", "UsePreviousValue": true}},
		"both":       {map[string]any{"ParameterKey": "Key", "ParameterValue": "new", "UsePreviousValue": "true"}},
	} {
		want := map[string]string{"undeclared": "Parameters: [Gone] do not exist in the template", "both": "Cannot specify usePreviousValue as true and non empty value"}[name]
		if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "taken", "TemplateBody": string(body), "Parameters": params}); code(err) != "ValidationError" || !strings.Contains(err.Error(), want) {
			t.Errorf("UpdateStack with the %s parameter answered %v, want ValidationError: %s", name, err, want)
		}
	}
	both := []any{map[string]any{"ParameterKey": "Key", "ParameterValue": "new", "UsePreviousValue": "true"}}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "taken", "ChangeSetName": "both", "TemplateBody": string(body), "Parameters": both}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Cannot specify usePreviousValue as true and non empty value") {
		t.Errorf("CreateChangeSet with both answered %v, want ValidationError", err)
	}
}

// A stack keeps its Tags, "Key-value pairs to associate with this stack":
// DescribeStacks answers them, an UpdateStack without them keeps them ("If
// you don't specify this parameter, CloudFormation doesn't modify the stack's
// tags") and one with an empty list removes them (API reference,
// UpdateStack), and a change set's execution sets them. Before, no stack
// kept any.
func TestCloudFormationAStackKeepsItsTags(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	tags := func(stack string) any {
		out, _ := h.invoke("DescribeStacks", map[string]any{"StackName": stack})
		return asMap(spi.Slice(out["Stacks"])[0])["Tags"]
	}
	team := []any{map[string]any{"Key": "team", "Value": "a"}}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "tagged", "TemplateBody": string(body), "Tags": team}); err != nil {
		t.Fatal(err)
	}
	if got := tags("tagged"); !reflect.DeepEqual(got, team) {
		t.Errorf("Tags = %v, want %v", got, team)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "tagged", "TemplateBody": string(body)}); err != nil {
		t.Fatal(err)
	}
	if got := tags("tagged"); !reflect.DeepEqual(got, team) {
		t.Errorf("after an update without Tags, Tags = %v, want %v kept", got, team)
	}
	other := []any{map[string]any{"Key": "team", "Value": "b"}}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "tagged", "ChangeSetName": "retag", "TemplateBody": string(body), "Tags": other}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "tagged", "ChangeSetName": "retag"}); err != nil {
		t.Fatal(err)
	}
	if got := tags("tagged"); !reflect.DeepEqual(got, other) {
		t.Errorf("after the change set, Tags = %v, want %v", got, other)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "tagged", "TemplateBody": string(body), "Tags": []any{}}); err != nil {
		t.Fatal(err)
	}
	if got := tags("tagged"); !reflect.DeepEqual(got, []any{}) {
		t.Errorf("after an update with no Tags, Tags = %v, want []", got)
	}
}

// IAM resources need their Capabilities: "If you have IAM resources, you can
// specify either capability. If you have IAM resources with custom names, you
// must specify CAPABILITY_NAMED_IAM. If you don't specify either of these
// capabilities, CloudFormation returns an InsufficientCapabilities error"
// (API reference, CreateStack); a change set's are its own. Before, no
// capability was asked for.
func TestCloudFormationIAMResourcesNeedTheirCapabilities(t *testing.T) {
	h := newStacks(t)
	role := func(props map[string]any) string {
		props["AssumeRolePolicyDocument"] = map[string]any{"Version": "2012-10-17", "Statement": []any{}}
		body, _ := json.Marshal(map[string]any{"Resources": map[string]any{"R": map[string]any{"Type": "AWS::IAM::Role", "Properties": props}}})
		return string(body)
	}
	for _, c := range []struct {
		name, body string
		caps       []any
		want       string // "" when it is made
	}{
		{"none", role(map[string]any{}), nil, "Requires capabilities : [CAPABILITY_IAM]"},
		{"iam", role(map[string]any{}), []any{"CAPABILITY_IAM"}, ""},
		{"named-with-iam", role(map[string]any{"RoleName": "named-cap-role"}), []any{"CAPABILITY_IAM"}, "Requires capabilities : [CAPABILITY_NAMED_IAM]"},
		{"named", role(map[string]any{"RoleName": "named-cap-role"}), []any{"CAPABILITY_NAMED_IAM"}, ""},
	} {
		_, err := h.invoke("CreateStack", map[string]any{"StackName": c.name, "TemplateBody": c.body, "Capabilities": c.caps})
		if c.want == "" && err != nil || c.want != "" && (code(err) != "InsufficientCapabilitiesException" || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("CreateStack %s answered %v, want %q", c.name, err, c.want)
		}
		if _, left := h.stored(c.name); c.want != "" && left {
			t.Errorf("the refused stack %s was made", c.name)
		}
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "cs", "ChangeSetName": "role", "ChangeSetType": "CREATE", "TemplateBody": role(map[string]any{})}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "cs", "ChangeSetName": "role"}); code(err) != "InsufficientCapabilitiesException" {
		t.Errorf("ExecuteChangeSet of a change set without CAPABILITY_IAM answered %v, want InsufficientCapabilitiesException", err)
	}
}
