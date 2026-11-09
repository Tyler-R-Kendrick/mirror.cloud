package cloudformation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// r17Tpl is a template of the resources r and the outputs outs.
func r17Tpl(r, outs map[string]any) string {
	m := map[string]any{"Resources": r}
	if outs != nil {
		m["Outputs"] = outs
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// A Ref or a Fn::GetAtt naming what the template does not declare, neither a
// parameter, a pseudo parameter nor a resource, is "Unresolved", in its block,
// before anything is made, as the service refuses it; ValidateTemplate refuses
// it too, and a parameter and a pseudo parameter stay what a Ref may name. One
// pack serves the table. Before, CreateStack made a stack, passing the name to
// the provider as a value or rolling back on a value not known.
func TestCloudFormationRefusesAnUndeclaredName(t *testing.T) {
	h := newStacks(t)
	param := func(v any) map[string]any {
		return map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"},
			"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": v}}}
	}
	ref, getAtt := map[string]any{"Ref": "Nope"}, map[string]any{"Fn::GetAtt": []any{"Nope", "Arn"}}
	for name, c := range map[string]struct {
		body, block string
	}{
		"ref":         {r17Tpl(param(ref), nil), "Resources"},
		"getatt":      {r17Tpl(param(getAtt), nil), "Resources"},
		"output-ref":  {r17Tpl(param("v"), map[string]any{"O": map[string]any{"Value": ref}}), "Outputs"},
		"output-attr": {r17Tpl(param("v"), map[string]any{"O": map[string]any{"Value": getAtt}}), "Outputs"},
		"pseudo":      {r17Tpl(param(map[string]any{"Ref": "AWS::Region"}), nil), ""},
	} {
		want := "Unresolved resource dependencies [Nope] in the " + c.block + " block of the template"
		for _, op := range []string{"ValidateTemplate", "CreateStack"} {
			_, err := h.invoke(op, map[string]any{"StackName": "u-" + name, "TemplateBody": c.body})
			if _, made := h.stored("u-" + name); c.block != "" && (err == nil || !strings.Contains(err.Error(), want) || made) {
				t.Errorf("%s %s answered %v (stack made %v), want %q before anything is made", op, name, err, made, want)
			} else if c.block == "" && err != nil {
				t.Errorf("%s %s answered %v", op, name, err)
			}
		}
	}
}

// encode refuses a value not yet known before it reaches a provider, the
// stack record or an answer: a Fn::GetAtt whose logical id is not a literal
// names no resource order waits for, so its value is unknown when it is used.
// Without the refusal the mark reached the parameter's value and the output.
// On this branch parseTemplate refuses such a Fn::GetAtt up front, as an
// unresolved dependency, before encode is reached.
func TestCloudFormationEncodeRefusesAnUnknown(t *testing.T) {
	h := newStacks(t)
	attr := map[string]any{"Fn::GetAtt": []any{map[string]any{"Ref": "AWS::Region"}, "Arn"}}
	r := map[string]any{"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "r17m", "Type": "String", "Value": attr}}}
	_, err := h.invoke("CreateStack", map[string]any{"StackName": "m", "TemplateBody": r17Tpl(r, map[string]any{"O": map[string]any{"Value": attr}})})
	out, _ := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "r17m"})
	if rec, _ := h.stored("m"); err == nil || !strings.Contains(err.Error(), "not known") && !strings.Contains(err.Error(), "Unresolved resource dependencies [] in the Resources block") || out != nil || len(spi.Slice(rec["Outputs"])) != 0 {
		t.Errorf("CreateStack answered %v; parameter %s; outputs %s: want the unknown refused, reaching nothing", err, spi.JSONText(out), spi.JSONText(rec["Outputs"]))
	}
}

// An update reserves its stack, UPDATE_IN_PROGRESS, in the transaction that
// starts it, so a second update is refused at its start, "is in
// UPDATE_IN_PROGRESS state and can not be updated", and changes nothing: the
// stack and the secret are the first update's. Before, the second update ran,
// the first found the stack changed under it and lost, and the first's
// change to the secret both shared outlived it.
func TestCloudFormationAnUpdateHoldsItsStack(t *testing.T) {
	h := newStacks(t)
	tpl := func(v string) string {
		return r17Tpl(map[string]any{
			"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "r17sec", "SecretString": v}},
			"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "r17b", // its CORS, written by each update, the gate holds
				"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"https://" + v}}}}}},
		}, nil)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": tpl("init")}); err != nil {
		t.Fatal(err)
	}
	g := h.gateOn(func(collection, key string, _ []byte) bool {
		return collection == "bktcfg" && strings.HasSuffix(key, "/cors")
	})
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "s", "TemplateBody": tpl("a")})
		done <- err
	}()
	within(t, g.reached, "update a reaching its bucket")
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "s", "TemplateBody": tpl("b")}); err == nil || !strings.Contains(err.Error(), "is in UPDATE_IN_PROGRESS state and can not be updated") {
		t.Errorf("update b during update a answered %v, want the stack UPDATE_IN_PROGRESS", err)
	}
	close(g.release)
	if err := within(t, done, "update a"); err != nil {
		t.Fatal(err)
	}
	out, _ := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": "r17sec"})
	if status, _, _ := h.status("s"); status != "UPDATE_COMPLETE" || out["SecretString"] != "a" {
		t.Errorf("the stack is %s and the secret %v, want UPDATE_COMPLETE and a", status, out["SecretString"])
	}
}

// An update ends its reservation however it ends: one that fails puts the
// stack back as it found it, so the next update is taken; and one that finds
// the stack changed under it, by a DeleteStack that failed, writes nothing
// over it. These pin what a reservation's end writes.
func TestCloudFormationAnUpdateEndsItsReservation(t *testing.T) {
	h := newStacks(t)
	topic := map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "f", "TemplateBody": r17Tpl(topic, nil)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.ssm", "PutParameter", map[string]any{"Name": "r17taken", "Type": "String", "Value": "v"}); err != nil {
		t.Fatal(err)
	}
	taken := map[string]any{"T": topic["T"], "P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "r17taken", "Type": "String", "Value": "v"}}}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "f", "TemplateBody": r17Tpl(taken, nil)}); err == nil {
		t.Fatal("an update making a parameter whose name is taken succeeded")
	}
	two := map[string]any{"T": topic["T"], "U": map[string]any{"Type": "AWS::SNS::Topic"}}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "f", "TemplateBody": r17Tpl(two, nil)}); err != nil {
		t.Errorf("the update after a failed one answered %v, want it taken", err)
	}

	full := map[string]any{"K": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "r17full"}}}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "d", "TemplateBody": r17Tpl(full, nil)}); err != nil {
		t.Fatal(err)
	}
	h.fill("r17full", true) // DeleteStack fails on it
	g := h.gateOn(func(collection, key string, _ []byte) bool {
		return collection == "bktcfg" && strings.HasSuffix(key, "/cors")
	})
	full["C"] = map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "r17cors",
		"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"*"}}}}}}
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "d", "TemplateBody": r17Tpl(full, nil)})
		done <- err
	}()
	within(t, g.reached, "the update reaching its bucket")
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "d"}); err == nil {
		t.Fatal("DeleteStack of a stack holding a full bucket succeeded")
	}
	close(g.release)
	if err := within(t, done, "the update"); code(err) != "ValidationError" {
		t.Errorf("the update, the stack's delete failing during it, answered %v, want ValidationError", err)
	}
	if status, _, _ := h.status("d"); status != "DELETE_FAILED" {
		t.Errorf("the stack is %s, want DELETE_FAILED: the update wrote over the failed delete", status)
	}
}
