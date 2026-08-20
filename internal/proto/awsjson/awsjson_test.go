package awsjson

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestRouteDecodeEncode(t *testing.T) {
	svc := &model.Service{ID: "aws.dynamodb", Operations: []model.Operation{{Name: "ListTables", HTTP: model.HTTPBinding{Code: 200}}}}
	c := New10()
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"Limit":1}`))
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	op, err := c.Route(svc, r)
	if err != nil || op.Name != "ListTables" {
		t.Fatalf("%v %v", op, err)
	}
	req, err := c.Decode(svc, op, r)
	if err != nil {
		t.Fatal(err)
	}
	if req.Input["Limit"].(float64) != 1 {
		t.Fatalf("%v", req.Input)
	}
	w := httptest.NewRecorder()
	if err := c.Encode(svc, op, w, &spi.Response{Output: map[string]any{"TableNames": []any{}}}); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	if !strings.Contains(w.Body.String(), "TableNames") {
		t.Fatal(w.Body.String())
	}
}

func TestEncodeFaultNotImplemented(t *testing.T) {
	svc := &model.Service{ID: "aws.dynamodb"}
	op := &model.Operation{Name: "X"}
	w := httptest.NewRecorder()
	_ = New10().EncodeFault(svc, op, w, spi.NotImplemented("aws.dynamodb", "X", "emulate"), "rid")
	if w.Code != 501 {
		t.Fatal(w.Code)
	}
	if w.Header().Get("x-mirror-not-implemented") == "" {
		t.Fatal(w.Header())
	}
}
