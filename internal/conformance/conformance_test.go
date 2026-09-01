package conformance

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/awsjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/awsquery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/restjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/restxml"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/gcp/gcprest"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/secretsmanager"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ssm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcp/gcs"
)

func codecFor(p model.Protocol) proto.Codec {
	switch p {
	case model.ProtoAWSJSON10:
		return awsjson.New10()
	case model.ProtoAWSJSON11:
		return awsjson.New11()
	case model.ProtoAWSQuery, model.ProtoEC2Query:
		return awsquery.Codec{}
	case model.ProtoRESTXML:
		return restxml.Codec{}
	case model.ProtoRESTJSON1:
		return restjson.Codec{}
	case model.ProtoGCPRESTSON:
		return gcprest.Codec{}
	default:
		return nil
	}
}

func TestCodecRoundTripAndFaultEnvelope(t *testing.T) {
	b := catalog.Bundle()
	for i := range b.Services {
		svc := &b.Services[i]
		c := codecFor(svc.Protocol)
		if c == nil || len(svc.Operations) == 0 {
			continue
		}
		op := &svc.Operations[0]
		r := requestFor(svc, op)
		got, err := c.Route(svc, r)
		if err != nil {
			t.Errorf("%s route: %v", svc.ID, err)
			continue
		}
		if got.Name == "" {
			t.Errorf("%s empty op name", svc.ID)
		}
		req, err := c.Decode(svc, got, r)
		if err != nil {
			t.Errorf("%s decode: %v", svc.ID, err)
			continue
		}
		if req.ServiceID != svc.ID {
			t.Errorf("%s decode service %q", svc.ID, req.ServiceID)
		}
		w := httptest.NewRecorder()
		if err := c.Encode(svc, got, w, &spi.Response{Output: map[string]any{"ok": true}}); err != nil {
			t.Errorf("%s encode: %v", svc.ID, err)
		}
		if w.Code == 0 {
			t.Errorf("%s encode wrote no status", svc.ID)
		}
		fw := httptest.NewRecorder()
		if err := c.EncodeFault(svc, got, fw, spi.NotImplemented(svc.ID, got.Name, "emulate"), "rid"); err != nil {
			t.Errorf("%s fault: %v", svc.ID, err)
		}
		if fw.Code != 501 {
			t.Errorf("%s fault status %d", svc.ID, fw.Code)
		}
		if fw.Header().Get("x-mirror-not-implemented") == "" && !strings.Contains(fw.Body.String(), "MirrorNotImplemented") {
			t.Errorf("%s fault not distinguishable: %s %v", svc.ID, fw.Body.String(), fw.Header())
		}
	}
}

func requestFor(svc *model.Service, op *model.Operation) *http.Request {
	switch svc.Protocol {
	case model.ProtoAWSJSON10, model.ProtoAWSJSON11:
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		target := op.Name
		if svc.TargetPrefix != "" {
			target = svc.TargetPrefix + "." + op.Name
		}
		r.Header.Set("X-Amz-Target", target)
		return r
	case model.ProtoAWSQuery, model.ProtoEC2Query:
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+op.Name+"&Version="+svc.QueryVersion))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return r
	case model.ProtoGCPRESTSON:
		return httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil)
	default:
		r := httptest.NewRequest(http.MethodGet, "/bucket", nil)
		return r
	}
}

func TestPackOperationsAreCatalogued(t *testing.T) {
	b := catalog.Bundle()
	reg, err := registry.New(spitest.Deps(t), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range reg.Enabled() {
		pack, ok := reg.Resolve(id)
		if !ok {
			t.Fatalf("enabled %s missing", id)
		}
		svc := b.ServiceByID(id)
		if svc == nil {
			t.Errorf("pack %s not in catalog", id)
			continue
		}
		for _, op := range pack.Operations() {
			if svc.OperationByName(op) == nil {
				t.Errorf("%s.%s not in catalog", id, op)
			}
		}
	}
}

func TestFaultErrorString(t *testing.T) {
	f := spi.NotImplemented("aws.s3", "Foo", "emulate")
	if f.HTTPStatus != 501 || !strings.Contains(f.Error(), "aws.s3.Foo") {
		t.Fatalf("%+v %q", f, f.Error())
	}
	if (*spi.Fault)(nil).Error() != "" {
		t.Fatal("nil")
	}
}

func TestDecodeConsumesBodyOnce(t *testing.T) {
	svc := catalog.Bundle().ServiceByID("aws.dynamodb")
	c := awsjson.New10()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"TableName":"T"}`)))
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	op, err := c.Route(svc, r)
	if err != nil {
		t.Fatal(err)
	}
	req, err := c.Decode(svc, op, r)
	if err != nil {
		t.Fatal(err)
	}
	if req.Input["TableName"] != "T" {
		t.Fatalf("%v", req.Input)
	}
	rest, _ := io.ReadAll(r.Body)
	if len(rest) != 0 {
		t.Fatalf("body not consumed: %q", rest)
	}
}

func TestStatesWaitExecutionContract(t *testing.T) {
	deps := spitest.Deps(t)
	p := states.New(deps)
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStateMachine", Input: map[string]any{
		"name": "wait-contract", "definition": `{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":1,"End":true}}}`, "roleArn": "arn:aws:iam::000000000000:role/states",
	}})
	if err != nil {
		t.Fatal(err)
	}
	started, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "StartExecution", Input: map[string]any{"stateMachineArn": created.Output["stateMachineArn"], "name": "run"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		described, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DescribeExecution", Input: map[string]any{"executionArn": started.Output["executionArn"]}})
		if err != nil {
			t.Fatal(err)
		}
		if described.Output["status"] == "SUCCEEDED" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Wait execution remained %#v", described.Output)
		}
		time.Sleep(time.Millisecond)
	}
}
