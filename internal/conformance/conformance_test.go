package conformance

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/awsjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/awsquery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/gcprest"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/restjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/restxml"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/secretsmanager"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sns"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sqs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/ssm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sts"
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
