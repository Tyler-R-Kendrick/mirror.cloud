package cloudformation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func rv19Body(m map[string]any) string { b, _ := json.Marshal(m); return string(b) }

// rv19NotFIFO is a queue whose create fails after what comes before it was made:
// "Queue name must end in .fifo for FIFO queues".
func rv19NotFIFO(name string) map[string]any {
	return map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": name, "FifoQueue": true}}
}

// DependsOn is a resource's own attribute, "a string or a list of strings" naming resources the stack
// makes (DependsOn attribute, Template Reference). A key of that name inside Properties is data (an
// EventPattern field, here); a DependsOn naming a resource whose condition is false, or a parameter, is
// refused as "Unresolved resource dependencies [X] in the Resources block of the template", the
// service's answer to both; and a DependsOn cycle is circular. One pack serves the table. (The
// reviewer's round-19 repros, renamed.)
func TestCloudFormationReadsDependsOnOnlyAsAResourceAttribute(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	topic := func(dependsOn any) map[string]any {
		return map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": dependsOn}
	}
	for name, c := range map[string]struct {
		tpl  map[string]any
		want string // what the refusal says; none: taken
	}{
		"properties-key": {map[string]any{"Resources": map[string]any{"R": map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{
			"EventPattern": map[string]any{"source": []any{"app"}, "detail": map[string]any{"DependsOn": []any{"billing"}}}}}}}, ""},
		"condition-false": {map[string]any{
			"Conditions": map[string]any{"Off": map[string]any{"Fn::Equals": []any{"a", "b"}}},
			"Resources":  map[string]any{"B": map[string]any{"Type": "AWS::SNS::Topic", "Condition": "Off"}, "A": topic("B")}},
			"Unresolved resource dependencies [B] in the Resources block of the template"},
		"parameter": {map[string]any{"Parameters": map[string]any{"Nope": map[string]any{"Type": "String", "Default": "x"}},
			"Resources": map[string]any{"T": topic("Nope")}}, "Unresolved resource dependencies [Nope] in the Resources block of the template"},
		"cycle-pair": {map[string]any{"Resources": map[string]any{"A": topic("B"), "B": topic([]any{"A"})}}, "Circular dependency between resources: [A, B]"},
		"cycle-self": {map[string]any{"Resources": map[string]any{"A": topic("A")}}, "Circular dependency between resources: [A]"},
		"cycle-getatt": {map[string]any{"Resources": map[string]any{"A": topic("B"), "B": map[string]any{"Type": "AWS::SSM::Parameter",
			"Properties": map[string]any{"Type": "String", "Value": map[string]any{"Fn::GetAtt": []any{"A", "TopicName"}}}}}}, "Circular dependency between resources: [A, B]"},
	} {
		for _, op := range []string{"ValidateTemplate", "CreateStack"} {
			_, err := h.invoke(op, map[string]any{"StackName": "rv19-" + name, "TemplateBody": rv19Body(c.tpl)})
			if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
				t.Errorf("%s %s answered %v; want %q (none: taken)", op, name, err, c.want)
			}
			if _, made := h.stored("rv19-" + name); c.want != "" && made {
				t.Errorf("%s %s made the stack it refused", op, name)
			}
		}
	}
}

// A failed update rolls the stack back "to the last known stable state" (Monitor and roll back stack
// operations, User Guide): a secret the update gave a new value in place has the old one again.
func TestCloudFormationAFailedUpdatePutsBackWhatItChangedInPlace(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	mk := func(v string, bad bool) string {
		r := map[string]any{"A": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "rv19sec", "SecretString": v}}}
		if bad {
			r["Z"] = rv19NotFIFO("rv19-nofifo")
		}
		return rv19Body(map[string]any{"Resources": r})
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "ip", "TemplateBody": mk("old", false)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "ip", "TemplateBody": mk("new", true)}); err == nil {
		t.Fatal("the update did not fail")
	}
	if out, err := h.service("aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": "rv19sec"}); err != nil || out["SecretString"] != "old" {
		t.Errorf("after the failed update the secret is %v (%v); want old", out["SecretString"], err)
	}
}

// A Kinesis resource policy's physical id is its stream's ARN (AWS::Kinesis::ResourcePolicy, Template
// Reference), so a second stack's policy on the stream would share the first's id: it is refused as
// the name taken, by a create and by an update, and the failed update deletes nothing the first holds.
func TestCloudFormationAFailedUpdateLeavesAnotherStacksResource(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	arn := "arn:aws:kinesis:us-east-1:000000000000:stream/rv19st"
	policy := func() map[string]any {
		return map[string]any{"Type": "AWS::Kinesis::ResourcePolicy", "Properties": map[string]any{"ResourceArn": arn,
			"ResourcePolicy": map[string]any{"Version": "2012-10-17", "Statement": []any{}}}}
	}
	owner := policy()
	owner["DependsOn"] = "S"
	if err := rvMake(t, h, "owner", map[string]any{"Resources": map[string]any{"S": map[string]any{"Type": "AWS::Kinesis::Stream", "Properties": map[string]any{"Name": "rv19st"}}, "RP": owner}}); err != nil {
		t.Fatal(err)
	}
	if err := rvMake(t, h, "other", map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}}); err != nil {
		t.Fatal(err)
	}
	_, uerr := h.invoke("UpdateStack", map[string]any{"StackName": "other", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"T": map[string]any{"Type": "AWS::SNS::Topic"}, "B": policy(), "Z": rv19NotFIFO("rv19-nofifo2")}})})
	if out, err := h.service("aws.kinesis", "GetResourcePolicy", map[string]any{"ResourceARN": arn}); err != nil || !strings.Contains(str(out["Policy"]), "2012-10-17") {
		t.Errorf("stack owner's stream policy is now %v (%v); want it kept", out["Policy"], err)
	}
	cerr := rvMake(t, h, "third", map[string]any{"Resources": map[string]any{"B": policy()}})
	for op, err := range map[string]error{"UpdateStack": uerr, "CreateStack": cerr} {
		if f := (*spi.Fault)(nil); !errors.As(err, &f) || f.Code != "AlreadyExistsException" {
			t.Errorf("%s of a policy on stack owner's stream answered %v; want AlreadyExistsException", op, err)
		}
	}
}

// An update's UsePreviousValue takes "the existing parameter value" (Parameter, API reference): the one
// the stack has when the update reserves it. Update U1 reads the stack, update U2 changes P and completes,
// then U1 reserves: U1 keeps U2's P.
func TestCloudFormationAnUpdateReadsPreviousValuesFromTheRecordItReserves(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	tpl := rv19Body(map[string]any{"Parameters": map[string]any{"P": map[string]any{"Type": "String"}, "Q": map[string]any{"Type": "String"}},
		"Resources": map[string]any{"S": map[string]any{"Type": "AWS::SNS::Topic"}},
		"Outputs":   map[string]any{"P": map[string]any{"Value": map[string]any{"Ref": "P"}}, "Q": map[string]any{"Value": map[string]any{"Ref": "Q"}}}})
	pv := func(key string, value any) map[string]any {
		if value == nil {
			return map[string]any{"ParameterKey": key, "UsePreviousValue": true}
		}
		return map[string]any{"ParameterKey": key, "ParameterValue": value}
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": tpl, "Parameters": []any{pv("P", "p1"), pv("Q", "q1")}}); err != nil {
		t.Fatal(err)
	}
	g := h.getGate(func(col, key string) bool { return col == "cfn" && key == "s" })
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { // U1: keep P, change Q
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "s", "TemplateBody": tpl, "Parameters": []any{pv("P", nil), pv("Q", "q2")}})
		done <- err
	}()
	within(t, g.reached, "U1 reading the stack")
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "s", "TemplateBody": tpl, "Parameters": []any{pv("P", "p2"), pv("Q", nil)}}); err != nil {
		t.Fatalf("U2: %v", err)
	}
	close(g.release)
	if err := within(t, done, "U1"); err != nil {
		t.Fatalf("U1: %v", err)
	}
	if outs := h.outputs("s"); outs["P"] != "p2" || outs["Q"] != "q2" {
		t.Errorf("outputs after U2 then U1 are %v; want P p2 (U2's) and Q q2 (U1's)", outs)
	}
}

// A failed update deletes only what it made: what the stack held before stays. What it made and cannot
// delete stays on the stack's record as DELETE_FAILED, as a DeleteStack's own failure leaves it
// (DeleteStack, API reference: "Resources that failed to delete"), so the next DeleteStack deletes it.
func TestCloudFormationAFailedUpdateKeepsWhatItCannotDelete(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "pu", map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}}); err != nil {
		t.Fatal(err)
	}
	held := h.resources("pu")[0].Physical
	g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv19m") })
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "pu", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
			"T": map[string]any{"Type": "AWS::SNS::Topic"},
			"A": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "rv19bkt"}},
			"M": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": "A", "Properties": map[string]any{"TopicName": "rv19m"}},
			"Z": map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "M", "Properties": map[string]any{"QueueName": "rv19-nofifo3", "FifoQueue": true}}}})})
		done <- err
	}()
	within(t, g.reached, "the update reaching M")
	h.fill("rv19bkt", true) // the bucket the update made is not empty: S3 refuses its delete
	close(g.release)
	if err := within(t, done, "the update"); err == nil {
		t.Fatal("the update did not fail")
	}
	if _, err := h.service("aws.sns", "GetTopicAttributes", map[string]any{"TopicArn": held}); err != nil {
		t.Errorf("topic T the stack held before the failed update is gone: %v", err)
	}
	if got := h.statuses("pu")["A"]; got != "DELETE_FAILED" {
		t.Errorf("bucket A the failed update could not delete is %q on the stack; want DELETE_FAILED", got)
	}
	h.fill("rv19bkt", false)
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "pu"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv19bkt"}); err == nil {
		t.Errorf("bucket rv19bkt survives the DeleteStack that should retry it")
	}
}

// panicCol panics on the Put of a key it matches: a dependency failing unexpectedly mid-update.
type panicCol struct {
	spi.Collection
	match func(key string) bool
}

func (c panicCol) Put(ctx context.Context, k string, v []byte) error {
	if c.match(k) {
		panic("rv19: a dependency panics")
	}
	return c.Collection.Put(ctx, k, v)
}

type panicScope struct {
	spi.Scope
	match func(key string) bool
}

func (s panicScope) Collection(n string) spi.Collection {
	return panicCol{s.Scope.Collection(n), s.match}
}

type panicStore struct {
	spi.Store
	match func(key string) bool
}

func (p *panicStore) Scope(a, r string) spi.Scope { return panicScope{p.Store.Scope(a, r), p.match} }

// A panic between an update's reservation and its settle answers the service's InternalFailure (500)
// and ends the reservation as a failure does: the stack is rolled back, and can be deleted.
func TestCloudFormationAPanickedUpdateEndsItsReservation(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "pn", map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}}); err != nil {
		t.Fatal(err)
	}
	h.deps.Store = &panicStore{Store: h.deps.Store, match: func(key string) bool { return strings.Contains(key, "rv19pan") }}
	h.pack, h.cfn = New(h.deps), bundled.Handler("aws.cloudformation", h.deps)
	_, err := h.invoke("UpdateStack", map[string]any{"StackName": "pn", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"T": map[string]any{"Type": "AWS::SNS::Topic"}, "M": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv19pan"}}}})})
	if f := (*spi.Fault)(nil); !errors.As(err, &f) || f.Code != "InternalFailure" || f.HTTPStatus != 500 {
		t.Errorf("the panicked update answered %v; want InternalFailure (500)", err)
	}
	if st, _, _ := h.status("pn"); st != "UPDATE_ROLLBACK_COMPLETE" {
		t.Errorf("the stack is %s after the panicked update; want UPDATE_ROLLBACK_COMPLETE, as a failed update leaves it", st)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "pn"}); err != nil {
		t.Errorf("DeleteStack after the panicked update answered %v", err)
	}
}

// An update keeps each resource it made and still declares: SSM's PutParameter of a name it holds takes
// Overwrite (PutParameter, API reference: "ParameterAlreadyExists" otherwise), and a table the template
// leaves as it is is left. One pack serves the table, a stack per type.
func TestCloudFormationAnUpdateKeepsEveryTypeItMade(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	for typ, props := range map[string]map[string]any{
		"AWS::Logs::LogGroup":  {},
		"AWS::Kinesis::Stream": {},
		"AWS::IAM::Role":       {"AssumeRolePolicyDocument": map[string]any{"Version": "2012-10-17"}},
		"AWS::DynamoDB::Table": {"AttributeDefinitions": []any{map[string]any{"AttributeName": "k", "AttributeType": "S"}}, "KeySchema": []any{map[string]any{"AttributeName": "k", "KeyType": "HASH"}}, "BillingMode": "PAY_PER_REQUEST"},
		"AWS::SQS::Queue":      {},
		"AWS::Events::Rule":    {"ScheduleExpression": "rate(1 minute)"},
		"AWS::SSM::Parameter":  {"Type": "String", "Value": "v"},
	} {
		name := "ty-" + strings.ReplaceAll(strings.TrimPrefix(typ, "AWS::"), "::", "-")
		for i, rs := range []map[string]any{{"S": map[string]any{"Type": typ, "Properties": props}},
			{"S": map[string]any{"Type": typ, "Properties": props}, "T": map[string]any{"Type": "AWS::SNS::Topic"}}} {
			op := []string{"CreateStack", "UpdateStack"}[i]
			if _, err := h.invoke(op, map[string]any{"StackName": name, "TemplateBody": rv19Body(map[string]any{"Resources": rs}), "Capabilities": []any{"CAPABILITY_IAM"}}); err != nil {
				t.Errorf("%s of a stack holding a %s answered %v", op, typ, err)
			}
		}
	}
}
