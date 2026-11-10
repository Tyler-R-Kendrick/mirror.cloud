package cloudformation

import (
	"encoding/json"
	"strings"
	"testing"
)

func rv18Body(m map[string]any) string { b, _ := json.Marshal(m); return string(b) }

func rv18Topic() map[string]any { return map[string]any{"Type": "AWS::SNS::Topic"} }

// Each template names something it never declares, in a DependsOn (a string
// or a list), in the branch of a Fn::If its condition does not take, in an
// output, or in the Conditions block; the service refuses each at
// ValidateTemplate and at CreateStack, before anything is made. One pack
// serves the table. (The reviewer's round-18 repros, renamed.)
func TestCloudFormationRefusesAnUndeclaredNameAnywhere(t *testing.T) {
	h := newStacks(t)
	cases := map[string]map[string]any{
		// DependsOn: "Template format error: Unresolved resource dependencies [Nope] in the Resources block of the template"
		"dependson":      {"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": "Nope"}}},
		"dependson-list": {"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": []any{"Nope"}}}},
		// a DependsOn names a resource, never a parameter
		"dependson-param": {"Parameters": map[string]any{"Nope": map[string]any{"Type": "String", "Default": "x"}},
			"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": "Nope"}}},
		// Fn::If whose settled branch is the other one
		"if-untaken": {
			"Conditions": map[string]any{"Off": map[string]any{"Fn::Equals": []any{"a", "b"}}},
			"Resources": map[string]any{"T": rv18Topic(), "P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String",
				"Value": map[string]any{"Fn::If": []any{"Off", map[string]any{"Ref": "Nope"}, "v"}}}}},
		},
		"output-if-untaken": {
			"Conditions": map[string]any{"Off": map[string]any{"Fn::Equals": []any{"a", "b"}}},
			"Resources":  map[string]any{"T": rv18Topic()},
			"Outputs":    map[string]any{"O": map[string]any{"Value": map[string]any{"Fn::If": []any{"Off", map[string]any{"Ref": "Nope"}, "v"}}}},
		},
		// Conditions block naming an undeclared parameter
		"condition-ref": {
			"Conditions": map[string]any{"C": map[string]any{"Fn::Equals": []any{map[string]any{"Ref": "Nope"}, "x"}}},
			"Resources":  map[string]any{"T": rv18Topic()},
		},
	}
	for name, tpl := range cases {
		for _, op := range []string{"ValidateTemplate", "CreateStack"} {
			_, err := h.invoke(op, map[string]any{"StackName": "rv18-" + name, "TemplateBody": rv18Body(tpl)})
			_, made := h.stored("rv18-" + name)
			if err == nil || !strings.Contains(err.Error(), "Nope") || made {
				t.Errorf("%s %s answered %v (stack made %v); want Nope refused as unresolved before anything is made", op, name, err, made)
			}
		}
	}
}

// A Fn::GetAtt of a parameter: "Template error: instance of Fn::GetAtt references undefined resource P".
func TestCloudFormationAGetAttNamesOnlyAResource(t *testing.T) {
	h := newStacks(t)
	tpl := map[string]any{"Parameters": map[string]any{"P": map[string]any{"Type": "String", "Default": "d"}},
		"Resources": map[string]any{"S": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String",
			"Value": map[string]any{"Fn::GetAtt": []any{"P", "Arn"}}}}}}
	_, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": rv18Body(tpl)})
	if want := "Template error: instance of Fn::GetAtt references undefined resource P"; err == nil || err.Error() != "ValidationError: "+want && !strings.HasSuffix(err.Error(), want) {
		t.Errorf("ValidateTemplate of a Fn::GetAtt of a parameter answered %v, want %q", err, want)
	}
}

// A failed update puts the record back as it found it, but leaves what it
// made, which the restored record does not list: no DeleteStack removes it.
func TestCloudFormationAFailedUpdateDeletesWhatItMade(t *testing.T) {
	h := newStacks(t)
	base := map[string]any{"Resources": map[string]any{"T": rv18Topic()}}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "lk", "TemplateBody": rv18Body(base)}); err != nil {
		t.Fatal(err)
	}
	upd := map[string]any{"Resources": map[string]any{"T": rv18Topic(),
		"A": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv18-leak"}},
		"Z": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "rv18-nofifo", "FifoQueue": true}}}}
	_, err := h.invoke("UpdateStack", map[string]any{"StackName": "lk", "TemplateBody": rv18Body(upd)})
	if err == nil {
		t.Fatal("the update did not fail")
	}
	st, _, _ := h.status("lk")
	out, _ := h.service("aws.sns", "ListTopics", map[string]any{})
	t.Logf("update answered %v; stack %s; topics %v", err, st, out)
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "lk"}); err != nil {
		t.Fatal(err)
	}
	out, _ = h.service("aws.sns", "ListTopics", map[string]any{})
	if strings.Contains(rv18Body(out), "rv18-leak") {
		t.Errorf("BUG: after the failed update and DeleteStack, the update's topic rv18-leak is left: %v", out)
	}
}

// DeleteStack while an update holds the stack: the service answers
// "Stack [x] cannot be deleted while in status UPDATE_IN_PROGRESS".
func TestCloudFormationADeleteWaitsForAnUpdate(t *testing.T) {
	h := newStacks(t)
	tpl := func(extra bool) string {
		r := map[string]any{"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "r18b", // this branch updates a bucket in place: its CORS, which the gate holds, changes with the version
			"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{map[bool]string{true: "https://b", false: "https://a"}[extra]}}}}}}, "Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "rv18-old"}}}
		if extra {
			r["A"] = map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv18-orphan"}}
		}
		return rv18Body(map[string]any{"Resources": r})
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "dd", "TemplateBody": tpl(false)}); err != nil {
		t.Fatal(err)
	}
	g := h.gateOn(func(collection, key string, _ []byte) bool {
		return collection == "bktcfg" && strings.HasSuffix(key, "/cors")
	})
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "dd", "TemplateBody": tpl(true)})
		done <- err
	}()
	within(t, g.reached, "the update reaching its bucket")
	_, err := h.invoke("DeleteStack", map[string]any{"StackName": "dd"})
	close(g.release)
	uerr := within(t, done, "the update")
	t.Logf("DeleteStack during the update answered %v; the update answered %v", err, uerr)
	_, recorded := h.stored("dd")
	topics, _ := h.service("aws.sns", "ListTopics", map[string]any{})
	if uerr != nil && !recorded && strings.Contains(rv18Body(topics), "rv18-orphan") {
		t.Errorf("BUG: the update failed after the stack was deleted, and its topic rv18-orphan is left with no stack: %v", topics)
	}
	if err == nil {
		t.Errorf("DIVERGENCE: DeleteStack of a stack UPDATE_IN_PROGRESS was taken; the service refuses it")
	}
}

// A DependsOn orders what it names before its resource: "Resource B is created
// before resource A" (Template Reference, DependsOn attribute), as a string or
// a list. Without it the two topics, which name nothing, are made in the
// order of their logical ids, A first.
func TestCloudFormationDependsOnOrders(t *testing.T) {
	h := newStacks(t)
	for name, on := range map[string]any{"one": "Z", "list": []any{"Z"}} {
		if err := rvMake(t, h, "do-"+name, map[string]any{"Resources": map[string]any{"A": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": on}, "Z": rv18Topic()}}); err != nil {
			t.Fatal(err)
		}
		if rs := h.resources("do-" + name); len(rs) != 2 || rs[0].Logical != "Z" {
			t.Errorf("%s: the stack made %v, want Z before A", name, rs)
		}
	}
}
