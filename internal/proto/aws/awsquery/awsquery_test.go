package awsquery

import (
	"net/http"
	"net/http/httptest"
	"net/url"
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
