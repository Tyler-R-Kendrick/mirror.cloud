package mock_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/mock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func exampleService() *model.Service {
	return &model.Service{
		ID: "example.svc",
		Operations: []model.Operation{
			{
				Name:   "GetWidget",
				Input:  "GetWidgetInput",
				Output: "GetWidgetOutput",
				HTTP:   model.HTTPBinding{Code: 200},
			},
		},
		Shapes: map[string]model.Shape{
			"GetWidgetInput": {
				Kind: model.KindStructure,
				Members: map[string]model.Member{
					"Name": {Shape: "String", Required: true},
				},
			},
			"GetWidgetOutput": {
				Kind: model.KindStructure,
				Members: map[string]model.Member{
					"Name":  {Shape: "String"},
					"Count": {Shape: "Integer"},
					"Ok":    {Shape: "Boolean"},
				},
			},
			"String":  {Kind: model.KindString},
			"Integer": {Kind: model.KindInteger},
			"Boolean": {Kind: model.KindBoolean},
		},
	}
}

func TestSynthesizeDeterministic(t *testing.T) {
	svc := exampleService()
	in := map[string]any{"Name": "w"}
	req := func() *spi.Request {
		return &spi.Request{ServiceID: svc.ID, Operation: "GetWidget", Input: in}
	}
	a, err := mock.New(svc, spitest.Deps(t), false).Invoke(context.Background(), req())
	if err != nil {
		t.Fatal(err)
	}
	b, err := mock.New(svc, spitest.Deps(t), false).Invoke(context.Background(), req())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Output, b.Output) {
		t.Fatalf("not deterministic:\n%v\n%v", a.Output, b.Output)
	}
	if got := a.Headers["x-mirror-fidelity"]; len(got) != 1 || got[0] != "mock" {
		t.Fatalf("fidelity header: %v", a.Headers)
	}
	if _, ok := a.Output["Name"].(string); !ok {
		t.Fatalf("Name: %T %v", a.Output["Name"], a.Output["Name"])
	}
	if _, ok := a.Output["Count"].(float64); !ok {
		t.Fatalf("Count: %T %v", a.Output["Count"], a.Output["Count"])
	}
	if _, ok := a.Output["Ok"].(bool); !ok {
		t.Fatalf("Ok: %T %v", a.Output["Ok"], a.Output["Ok"])
	}
}

func TestStrictNotImplemented(t *testing.T) {
	svc := exampleService()
	_, err := mock.New(svc, spitest.Deps(t), true).Invoke(context.Background(), &spi.Request{
		ServiceID: svc.ID,
		Operation: "GetWidget",
		Input:     map[string]any{"Name": "w"},
	})
	f, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("got %T %v", err, err)
	}
	if f.Code != "MirrorNotImplemented" || f.HTTPStatus != 501 {
		t.Fatalf("strict: %+v", f)
	}
}

func TestMissingRequired(t *testing.T) {
	svc := exampleService()
	_, err := mock.New(svc, spitest.Deps(t), false).Invoke(context.Background(), &spi.Request{
		ServiceID: svc.ID,
		Operation: "GetWidget",
		Input:     map[string]any{},
	})
	f, ok := err.(*spi.Fault)
	if !ok || f.Code != "ValidationException" {
		t.Fatalf("required: %T %v", err, err)
	}
}
