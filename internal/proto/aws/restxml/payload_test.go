package restxml

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// payloadOps are the S3 operations whose body the walker decodes -- the ones
// the hand parser had a case for, less the four named configurations, which
// keep the element-keyed walk.
var payloadOps = []string{"CompleteMultipartUpload", "CreateBucket", "DeleteObjects", "PutBucketAccelerateConfiguration", "PutBucketAcl", "PutBucketCors", "PutBucketEncryption", "PutBucketLifecycleConfiguration", "PutBucketLogging", "PutBucketNotificationConfiguration", "PutBucketOwnershipControls", "PutBucketPolicy", "PutBucketReplication", "PutBucketRequestPayment", "PutBucketTagging", "PutBucketVersioning", "PutBucketWebsite", "PutObjectAcl", "PutObjectLegalHold", "PutObjectLockConfiguration", "PutObjectRetention", "PutObjectTagging", "PutPublicAccessBlock", "RestoreObject"}

// TestPayloadDecodesTheModel: for every operation the hand parser used to
// cover, a body carrying every member the model declares decodes to a
// structure under the payload's name, with the members the S3 pack reads flat
// lifted beside it. A differential run against the hand parser, the day it
// was deleted, showed every key it produced reproduced with the same value;
// the S3 behavior and SDK suites hold each operation end to end.
func TestPayloadDecodesTheModel(t *testing.T) {
	svc, err := generated.Model("aws.s3")
	if err != nil {
		t.Fatal(err)
	}
	for _, opName := range payloadOps {
		op := svc.OperationByName(opName)
		name, m, ok := payloadMember(svc, op)
		if !ok {
			t.Fatalf("%s: no payload member", opName)
		}
		in := map[string]any{}
		decodePayload(svc, op, []byte(sample(svc, m.Shape, wireName(name, m.Binding), 0)), in)
		if _, ok := in["_body"]; ok && opName != "PutBucketPolicy" {
			t.Errorf("%s: the walker rejected the model's own sample: %v", opName, in["_body"])
			continue
		}
		if opName == "PutBucketPolicy" {
			if _, ok := in["Policy"].(string); !ok {
				t.Errorf("PutBucketPolicy: policy is not the raw body: %#v", in)
			}
			continue
		}
		payload, ok := in[name].(map[string]any)
		if !ok || len(payload) != len(svc.Shapes[m.Shape].Members) {
			t.Errorf("%s: %s decoded %d of %d members: %#v", opName, name, len(payload), len(svc.Shapes[m.Shape].Members), in[name])
		}
		for _, lift := range liftedMembers[opName] {
			if _, ok := in[lift]; !ok {
				t.Errorf("%s: %s not lifted beside %s", opName, lift, name)
			}
		}
	}
}

// TestPayloadRejectsWhatTheSchemaDoesNot: the reference validates a body
// against its schema and answers MalformedXML, and the packs answer that
// when the body arrives raw. An element the shape does not declare, a
// boolean that is not one, and a root that is not the payload's all arrive
// raw; a body that merely omits members does not.
func TestPayloadRejectsWhatTheSchemaDoesNot(t *testing.T) {
	svc, err := generated.Model("aws.s3")
	if err != nil {
		t.Fatal(err)
	}
	op := svc.OperationByName("PutPublicAccessBlock")
	for _, tc := range []struct {
		body string
		raw  bool
	}{
		{`<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>`, false},
		{`<PublicAccessBlockConfiguration/>`, false},
		{`<PublicAccessBlockConfiguration><Unknown>true</Unknown></PublicAccessBlockConfiguration>`, true},
		{`<PublicAccessBlockConfiguration><BlockPublicAcls>yes</BlockPublicAcls></PublicAccessBlockConfiguration>`, true},
		{`<Other><BlockPublicAcls>true</BlockPublicAcls></Other>`, true},
		{`<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls>`, true},
	} {
		in := map[string]any{}
		decodePayload(svc, op, []byte(tc.body), in)
		if _, raw := in["_body"]; raw != tc.raw {
			t.Errorf("%s: raw=%v, want %v (%v)", tc.body, raw, tc.raw, in)
		}
	}
}

// sample writes an element carrying every member its shape declares, so the
// comparison above sees the whole surface, not the fixtures someone thought of.
func sample(svc *model.Service, shapeID, elem string, depth int) string {
	shape, ok := svc.Shapes[shapeID]
	if !ok || depth > 8 {
		return "<" + elem + ">" + elem + "-v</" + elem + ">"
	}
	var b strings.Builder
	switch shape.Kind {
	case model.KindStructure, model.KindUnion:
		var attrs, body strings.Builder
		for name, m := range shape.Members {
			wire := wireName(name, m.Binding)
			child := svc.Shapes[m.Shape]
			switch {
			case m.Binding.XMLAttribute:
				fmt.Fprintf(&attrs, " %s=%q", m.Binding.Name, scalar(child, name))
			case m.Binding.XMLFlattened && child.Kind == model.KindList:
				body.WriteString(sample(svc, child.Member, wire, depth+1))
				body.WriteString(sample(svc, child.Member, wire, depth+1))
			default:
				body.WriteString(sample(svc, m.Shape, wire, depth+1))
			}
		}
		b.WriteString("<" + elem + attrs.String() + ">" + body.String() + "</" + elem + ">")
	case model.KindList:
		item := shape.MemberBinding.Name
		if item == "" {
			item = "member"
		}
		b.WriteString("<" + elem + ">" + sample(svc, shape.Member, item, depth+1) + sample(svc, shape.Member, item, depth+1) + "</" + elem + ">")
	case model.KindMap:
		b.WriteString("<" + elem + "></" + elem + ">")
	default:
		b.WriteString("<" + elem + ">" + scalar(shape, elem) + "</" + elem + ">")
	}
	return b.String()
}

func scalar(shape model.Shape, name string) string {
	switch shape.Kind {
	case model.KindBoolean:
		return "true"
	case model.KindInteger, model.KindLong:
		return "7"
	case model.KindFloat, model.KindDouble:
		return "1.5"
	case model.KindTimestamp:
		return "2024-01-02T03:04:05.000Z"
	case model.KindEnum:
		if len(shape.EnumValues) > 0 {
			return shape.EnumValues[0]
		}
	}
	return name + "-v"
}
